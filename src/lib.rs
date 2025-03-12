/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use async_channel::Receiver;
use bytes::Bytes;
use futures::stream::StreamExt;
use log::{debug, trace, warn};
use mqtt_client::{MqttClientOperations, PahoBasedMqttClientOperations};
pub use mqtt_client::{MqttClientOptions, SslOptions};
use paho_mqtt::{self as mqtt, Message, QOS_1};
use protobuf::{Enum, EnumOrUnknown, MessageField};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use up_rust::{
    ComparableListener, UAttributes, UAttributesValidators, UCode, UMessage, UMessageType,
    UPayloadFormat, UPriority, UStatus, UUri, UUriError, UUID,
};

mod mqtt_client;
mod transport;

const MQTT_TOPIC_ANY_SEGMENT_WILDCARD: &str = "+";

type SubscriptionIdentifier = u16;

/// The transport's mode of operation.
pub enum TransportMode {
    /// Indicates communication via an in-vehicle MQTT broker. This is used by uEntities within the same vehicle
    /// (uEntity-2-uEntity).
    InVehicle,
    /// Indicates communication via an off-vehicle MQTT broker. This is used by uProtocol streamers to connect a
    /// vehicle's uEntities to uEntities running on a (cloud based) back end (Device-2-Device).
    OffVehicle,
}

impl TransportMode {
    /// Creates an MQTT topic segment from the authority name of a uProtocol URI.
    // [impl->dsn~up-transport-mqtt5-d2d-topic-names~1]
    fn uri_to_authority_topic_segment(uri: &UUri, fallback_authority: &str) -> String {
        if uri.has_empty_authority() {
            fallback_authority.to_owned()
        } else if uri.has_wildcard_authority() {
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD.to_string()
        } else {
            uri.authority_name()
        }
    }

    /// Converts a uProtocol URI to an MQTT topic.
    ///
    /// # Arguments
    ///
    /// * `fallback_authority` - The authority name to use if the given URI does not contain an authority.
    /// * `uri` - The URI to convert.
    // [impl->dsn~up-transport-mqtt5-e2e-topic-names~1]
    fn uri_to_e2e_mqtt_topic(uri: &UUri, fallback_authority: &str) -> String {
        let authority = Self::uri_to_authority_topic_segment(uri, fallback_authority);

        let ue_type_id = if uri.has_wildcard_entity_type() {
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD.into()
        } else {
            format!("{:X}", uri.uentity_type_id())
        };

        let ue_instance_id = if uri.has_wildcard_entity_instance() {
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD.into()
        } else {
            format!("{:X}", uri.uentity_instance_id())
        };

        let ue_ver = if uri.has_wildcard_version() {
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD.into()
        } else {
            format!("{:X}", uri.uentity_major_version())
        };

        let res_id = if uri.has_wildcard_resource_id() {
            MQTT_TOPIC_ANY_SEGMENT_WILDCARD.into()
        } else {
            format!("{:X}", uri.resource_id())
        };

        format!("{authority}/{ue_type_id}/{ue_instance_id}/{ue_ver}/{res_id}")
    }

    /// Creates an MQTT topic for a source and sink uProtocol URI.
    ///
    /// # Arguments
    /// * `source` - Source URI.
    /// * `sink` - Sink URI.
    /// * `fallback_authority` - The authority name to use if any of the URIs do not contain an authority.
    pub(crate) fn to_mqtt_topic(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        fallback_authority: &str,
    ) -> Result<String, UUriError> {
        match self {
            // [impl->dsn~up-transport-mqtt5-e2e-topic-names~1]
            TransportMode::InVehicle => {
                let mut topic = String::new();
                topic.push_str(&Self::uri_to_e2e_mqtt_topic(source, fallback_authority));
                if let Some(uri) = sink {
                    topic.push('/');
                    topic.push_str(&Self::uri_to_e2e_mqtt_topic(uri, fallback_authority));
                }
                Ok(topic)
            }
            // [impl->dsn~up-transport-mqtt5-d2d-topic-names~1]
            TransportMode::OffVehicle => {
                if let Some(uri) = sink {
                    let mut topic = String::new();
                    topic.push_str(&Self::uri_to_authority_topic_segment(
                        source,
                        fallback_authority,
                    ));
                    topic.push('/');
                    topic.push_str(&Self::uri_to_authority_topic_segment(
                        uri,
                        fallback_authority,
                    ));
                    Ok(topic)
                } else {
                    Err(UUriError::serialization_error(
                        "Off-Vehicle transport requires sink URI for creating MQTT topic",
                    ))
                }
            }
        }
    }
}

/// An MQTT 5 based uProtocol transport implementation.
pub struct Mqtt5Transport {
    /// Client instance for connecting to mqtt broker.
    mqtt_client: Arc<Mutex<dyn MqttClientOperations>>,
    /// Map of subscription identifiers to subscribed topics.
    subscription_topic_map: Arc<RwLock<HashMap<SubscriptionIdentifier, String>>>,
    /// Map of topics to listeners.
    topic_listener_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
    /// My authority
    authority_name: String,
    /// The transport's mode of operation.
    mode: TransportMode,
    /// List of free subscription identifiers to use for the client subscriptions.
    free_subscription_ids: Arc<RwLock<HashSet<SubscriptionIdentifier>>>,
    /// Handle to the message callback.
    message_callback_handle: Option<JoinHandle<()>>,
}

impl Mqtt5Transport {
    /// Creates a new transport.
    ///
    /// # Arguments
    /// * `mode` - The transport's mode of operation.
    /// * `options` - Configuration options for connecting to the MQTT broker.
    /// * `authority_name` - Authority name of the local uEntity.
    pub async fn new(
        mode: TransportMode,
        options: MqttClientOptions,
        authority_name: String,
    ) -> Result<Self, UStatus> {
        let subscription_topic_map = Arc::new(RwLock::new(HashMap::new()));
        let topic_listener_map = Arc::new(RwLock::new(HashMap::new()));
        let free_subscription_ids =
            Arc::new(RwLock::new((1..(options.max_subscriptions) + 1).collect()));

        let subscription_topic_map_handle = subscription_topic_map.clone();
        let topic_listener_map_handle = topic_listener_map.clone();

        // Create the MQTT client
        let client_operations = Arc::new(Mutex::new(
            mqtt_client::PahoBasedMqttClientOperations::new_client(&options)?,
        ));

        let stream = client_operations.lock().await.get_message_stream();

        // Create the callback for processing messages received from the broker
        let message_callback_handle = Some(Self::create_cb_message_handler(
            client_operations.clone(),
            subscription_topic_map_handle,
            topic_listener_map_handle,
            stream,
        ));

        let res = client_operations.lock().await.connect(&options).await;

        res.map(|_| Self {
            mqtt_client: client_operations.clone(),
            subscription_topic_map,
            topic_listener_map,
            authority_name,
            mode,
            free_subscription_ids,
            message_callback_handle,
        })
    }

    /// Stops processing of incoming messages.
    pub fn shutdown(&self) {
        if let Some(cb_message_handle) = self.message_callback_handle.as_ref() {
            cb_message_handle.abort();
        }
    }
    async fn do_reconnect(
        client_operations: Arc<Mutex<PahoBasedMqttClientOperations>>,
        subscription_map: Arc<RwLock<HashMap<SubscriptionIdentifier, String>>>,
    ) {
        warn!("Connection lost, reconnecting");
        let client = client_operations.lock().await;
        let session_present = client.reconnect().await;
        match session_present {
            mqtt_client::HasSession::SessionPresent => {}
            mqtt_client::HasSession::NoSession => {
                debug!("Re-subscribing to previously subscribed topics");
                let subs = subscription_map.read().await;
                for id_topic in subs.iter() {
                    // TODO: Error handling if resubscribe fails ... but how? panic?
                    let _ = client.subscribe(id_topic.1, *id_topic.0).await;
                }
            }
        }
    }
    // Creates a callback message handler that listens for incoming messages and notifies listeners asynchronously.
    //
    // # Arguments
    // * `subscription_map` - Map of subscription identifiers to subscribed topics.
    // * `topic_map` - Map of topics to listeners.
    // * `message_stream` - Stream of incoming mqtt messages.
    fn create_cb_message_handler(
        client_operations: Arc<Mutex<PahoBasedMqttClientOperations>>,
        subscription_map: Arc<RwLock<HashMap<SubscriptionIdentifier, String>>>,
        topic_map: Arc<RwLock<HashMap<String, HashSet<ComparableListener>>>>,
        mut message_stream: Receiver<Option<Message>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(msg_opt) = message_stream.next().await {
                let Some(msg) = msg_opt else {
                    Self::do_reconnect(client_operations.clone(), subscription_map.clone()).await;
                    continue;
                };
                let topic = msg.topic();
                let sub_id = msg
                    .properties()
                    .get_int(paho_mqtt::PropertyCode::SubscriptionIdentifier)
                    .and_then(|v| SubscriptionIdentifier::try_from(v).ok());

                // Get attributes from mqtt header.
                let umessage = if msg.properties().is_empty() {
                    protobuf::Message::parse_from_bytes(msg.payload()).unwrap()
                } else {
                    let uattributes = {
                        match get_uattributes_from_mqtt_properties(msg.properties()) {
                            Ok(uattributes) => uattributes,
                            Err(e) => {
                                debug!("Unable to get UAttributes from NQTT properties: {}", e);
                                continue;
                            }
                        }
                    };
                    UMessage {
                        attributes: Some(uattributes).into(),
                        payload: Some(Bytes::copy_from_slice(msg.payload())),
                        ..Default::default()
                    }
                };

                // If subscription ID is present, only notify listeners for that subscription.
                let listeners = if let Some(sub_id) = sub_id {
                    let sub_topic = {
                        let subscription_map_read = subscription_map.read().await;

                        let Some(sub_topic) = subscription_map_read.get(&sub_id) else {
                            trace!(
                                "Received message with subscription id that is not registered: {}",
                                sub_id
                            );
                            continue;
                        };
                        sub_topic.clone()
                    };

                    let owned_listeners = {
                        let topic_map_read = topic_map.read().await;

                        let Some(listeners) = topic_map_read.get(&sub_topic) else {
                            trace!("No listeners registered for topic: {}", sub_topic);
                            continue;
                        };
                        listeners.clone()
                    };

                    owned_listeners
                } else {
                    // Filter the topic map for topics that match the received topic, including wildcards.
                    let listeners = {
                        let topic_map_read = topic_map.read().await;

                        topic_map_read
                            .iter()
                            .filter(|(key, _)| Mqtt5Transport::compare_topic(topic, key))
                            .flat_map(|(_topic, listener)| listener.to_owned())
                            .collect()
                    };
                    listeners
                };

                for listener in listeners {
                    let msg = umessage.clone();
                    tokio::spawn(async move {
                        listener.on_receive(msg).await;
                    });
                }
            }
        })
    }

    /// Compare a topic to a topic pattern. Supports single level wildcards.
    ///
    /// # Arguments
    /// * `topic` - Topic to compare.
    /// * `pattern` - Topic pattern to compare against.
    fn compare_topic(topic: &str, pattern: &str) -> bool {
        let topic_parts = topic.split('/');
        let pattern_parts = pattern.split('/');

        for (topic_part, pattern_part) in topic_parts.zip(pattern_parts) {
            if topic_part != pattern_part && pattern_part != "+" {
                return false;
            }
        }

        true
    }

    /// Get an available subscription id to use.
    async fn get_free_subscription_id(&self) -> Result<SubscriptionIdentifier, UStatus> {
        // Get a random subscription id from the free subscription ids.
        let mut free_ids = self.free_subscription_ids.write().await;
        if let Some(&id) = free_ids.iter().next() {
            free_ids.remove(&id);
            Ok(id)
        } else {
            Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Max number of subscriptions reached on this client.",
            ))
        }
    }

    /// Add an unused subscription id to the free subscription ids.
    ///
    /// # Arguments
    /// * `id` - Subscription id to add to the free subscription ids.
    async fn add_free_subscription_id(&self, id: SubscriptionIdentifier) {
        let mut free_ids = self.free_subscription_ids.write().await;
        free_ids.insert(id);
    }

    /// Send UMessage to mqtt topic.
    ///
    /// # Arguments
    /// * `topic` - Mqtt topic to send message to.
    /// * `attributes` - UAttributes to send with message.
    /// * `payload` - UMessage payload to send.
    async fn send_message(
        &self,
        topic: &str,
        attributes: &UAttributes,
        payload: Option<Bytes>,
    ) -> Result<(), UStatus> {
        let props = create_mqtt_properties_from_uattributes(attributes)?;

        let mut msg_builder = mqtt::MessageBuilder::new()
            .topic(topic)
            .properties(props)
            .qos(QOS_1); // QOS 1 - Delivered and received at least once

        // If there is payload to send, add it to the message.
        // [impl->dsn~up-transport-mqtt5-payload-mapping~1]
        if let Some(data) = payload {
            msg_builder = msg_builder.payload(data);
        }
        let msg = msg_builder.finalize();

        self.mqtt_client
            .lock()
            .await
            .publish(msg)
            .await
            .map(|_| {
                debug!("Successfully published message [topic: {}]", topic);
            })
            .map_err(|e| {
                debug!(
                    "Failed to publish message [topic: {}]: {}",
                    topic,
                    e.to_string()
                );
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to publish message: {e:?}"),
                )
            })
    }

    /// Add a UListener to an MQTT topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to add the listener to.
    /// * `listener` - Listener to call when the topic recieves a message.
    async fn add_listener(
        &self,
        topic: &str,
        listener: Arc<dyn up_rust::UListener>,
    ) -> Result<(), UStatus> {
        let mut topic_listener_map = self.topic_listener_map.write().await;

        if !topic_listener_map.contains_key(topic) {
            let id = self.get_free_subscription_id().await?;
            let mut subscription_topic_map = self.subscription_topic_map.write().await;
            subscription_topic_map.insert(id, topic.to_string());
            // Subscribe to topic.
            if let Err(sub_err) = self.mqtt_client.lock().await.subscribe(topic, id).await {
                // If subscribe fails, add subscription id back to free subscription ids.
                self.add_free_subscription_id(id).await;
                return Err(sub_err);
            };
        }

        let listeners = topic_listener_map
            .entry(topic.to_string())
            .or_insert(HashSet::new());

        // Add listener to hash set.
        let comp_listener = ComparableListener::new(listener);
        listeners.insert(comp_listener);
        debug!("Added listener for MQTT topic: {}", topic);

        Ok(())
    }

    /// Remove a UListener from an mqtt topic.
    ///
    /// # Arguments
    /// * `topic` - Topic to remove the listener from.
    /// * `listener` - Listener to remove from the topic subscription list.
    async fn remove_listener(
        &self,
        topic: &str,
        listener: Arc<dyn up_rust::UListener>,
    ) -> Result<(), UStatus> {
        let mut topic_listener_map = self.topic_listener_map.write().await;
        let mut subscription_topic_map = self.subscription_topic_map.write().await;

        if !topic_listener_map.contains_key(topic) {
            return Err(UStatus::fail_with_code(
                UCode::NOT_FOUND,
                format!("Topic '{topic}' is not registered."),
            ));
        }

        topic_listener_map
            .entry(topic.to_string())
            .and_modify(|listeners| {
                // Remove listener from hash set.
                let comp_listener = ComparableListener::new(listener);
                listeners.remove(&comp_listener);
            });

        // Remove topic if no listeners are left.
        if topic_listener_map.get(topic).unwrap().is_empty() {
            let sub_id = subscription_topic_map.iter_mut().find_map(|(k, v)| {
                if v == topic {
                    Some(*k)
                } else {
                    None
                }
            });

            if let Some(sub_id) = sub_id {
                // Remove subscription id from map.
                subscription_topic_map.remove(&sub_id);

                // Add subscription id back to free subscription ids.
                self.add_free_subscription_id(sub_id).await;
            }

            topic_listener_map.remove(topic);

            // Unsubscribe from topic.
            self.mqtt_client.lock().await.unsubscribe(topic).await?;
        }

        Ok(())
    }

    /// Creates an MQTT topic for a source and sink uProtocol URI.
    ///
    /// # Arguments
    /// * `src_uri` - Source URI.
    /// * `sink_uri` - Sink URI.
    fn to_mqtt_topic_string(
        &self,
        src_uri: &UUri,
        sink_uri: Option<&UUri>,
    ) -> Result<String, UUriError> {
        self.mode
            .to_mqtt_topic(src_uri, sink_uri, &self.authority_name)
    }
}

// Push a user property from UAttributes to the MQTT Properties
fn push_user_property(
    properties: &mut mqtt::Properties,
    code: &str,
    value: &str,
    error_message: &str,
) -> Result<(), UStatus> {
    properties
        .push_string_pair(mqtt::PropertyCode::UserProperty, code, value)
        .map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("{error_message}, err: {e:?}"))
        })
}

/// Create mqtt header properties from UAttributes information.
/// The Message Expiry Interval gets mapped to the TTL MQTT property.
/// An optional UAttribute that is None gets mapped as None.
/// A mandatory UAttribue that has the default value gets mapped to None.
///
/// # Arguments
/// * `attributes` - UAttributes to create properties from.
fn create_mqtt_properties_from_uattributes(
    attributes: &UAttributes,
) -> Result<mqtt::Properties, UStatus> {
    let mut properties = mqtt::Properties::new();

    // Validate UAttributes before conversion.
    UAttributesValidators::get_validator_for_attributes(attributes)
        .validate(attributes)
        .map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Invalid uAttributes, err: {e:?}"))
        })?;

    // Add ttl to properties as message expiry time
    if let Some(message_expiry_interval) = attributes.ttl {
        properties
            .push_u32(
                mqtt::PropertyCode::MessageExpiryInterval,
                message_expiry_interval,
            )
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to create message expiry interval property, err: {e:?}"),
                )
            })?;
    }

    // Add uAttributes version number to user properties.
    push_user_property(
        &mut properties,
        "0",
        "1",
        "Unable to add uAttributes Version to mqtt User Properties",
    )?;

    // Add message ID as user property 1
    push_user_property(
        &mut properties,
        "1",
        &attributes.id.to_hyphenated_string(),
        "Unable to add message ID to mqtt User Properties",
    )?;

    // Add message type as user property 2
    push_user_property(
        &mut properties,
        "2",
        &attributes.type_.value().to_string(),
        "Unable to add message type to mqtt User Properties",
    )?;

    // Add message source as user property 3
    if let Some(source) = attributes.source.to_owned().into_option() {
        push_user_property(
            &mut properties,
            "3",
            &source.to_uri(false),
            "Unable to add message source to mqtt User Properties",
        )?;
    };

    // Add message sink as user property 4
    if let Some(sink) = attributes.sink.to_owned().into_option() {
        push_user_property(
            &mut properties,
            "4",
            &sink.to_uri(false),
            "Unable to add message sink to mqtt User Properties",
        )?
    };

    // Add message priority as user property 5 (map 0 => None)
    if attributes.priority.value() != 0 {
        push_user_property(
            &mut properties,
            "5",
            &attributes.priority.value().to_string(),
            "Unable to add message priority to mqtt User Properties",
        )?
    };

    // Add message permission level as user property 7 (optional)
    if let Some(permission_level) = &attributes.permission_level {
        push_user_property(
            &mut properties,
            "7",
            &permission_level.to_string(),
            "Unable to add message permission level to mqtt User Properties",
        )?;
    }

    // Add message comm Status as user property 8 (optional)
    if let Some(comm_status) = &attributes.commstatus {
        push_user_property(
            &mut properties,
            "8",
            &comm_status.value().to_string(),
            "Unable to add message comm status to mqtt User Properties",
        )?;
    }

    // Add message reqId as user property 9 (map "00000000-0000-0000-0000-000000000000" => None)
    if let Some(req_id) = attributes.reqid.to_owned().into_option() {
        push_user_property(
            &mut properties,
            "9",
            &req_id.to_hyphenated_string(),
            "Unable to add message reqId to mqtt User Properties",
        )?;
    }

    // Add message token as user property 10 (optional)
    if let Some(token) = &attributes.token {
        push_user_property(
            &mut properties,
            "10",
            token,
            "Unable to add message token to mqtt User Properties",
        )?;
    }

    // Add message traceparent as user property 11 (optional)
    if let Some(traceparent) = &attributes.traceparent {
        push_user_property(
            &mut properties,
            "11",
            traceparent,
            "Unable to add message traceparent to mqtt User Properties",
        )?;
    }

    // Add message payload format as user property 12
    if attributes.payload_format.value() != 0 {
        push_user_property(
            &mut properties,
            "12",
            &attributes.payload_format.value().to_string(),
            "Unable to add message payload format to mqtt User Properties",
        )?;
    }

    Ok(properties)
}

/// Get UAttributes from mqtt header properties.
///
/// # Arguments
/// * `props` - Mqtt properties to get UAttributes from.
fn get_uattributes_from_mqtt_properties(props: &mqtt::Properties) -> Result<UAttributes, UStatus> {
    let mut attributes = UAttributes::default();

    // Add the TTL UAttribute from the MessageExpiryInterval if available
    if let Some(ttl) = props.get(paho_mqtt::PropertyCode::MessageExpiryInterval) {
        attributes.ttl = ttl.get()
    }

    // Add UserProperty 1 to UAttributes as Message ID
    if let Some(message_id) = props.find_user_property("1") {
        let id = UUID::from_str(&message_id).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 1 to Message ID",
            )
        })?;
        attributes.id = MessageField::from(Some(id));
    };

    // Add UserProperty 2 to UAttributes as Message Type
    if let Some(message_type_str) = props.find_user_property("2") {
        let result = message_type_str
            .parse::<i32>()
            .ok()
            .and_then(UMessageType::from_i32)
            .map(EnumOrUnknown::from);

        if let Some(message_type) = result {
            attributes.type_ = message_type;
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 2 to Message Type".to_string(),
            ));
        }
    }

    // Add UserProperty 3 to UAttributes as Message Source
    if let Some(source_string) = props.find_user_property("3") {
        let source = UUri::from_str(&source_string).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 3 to Message Source",
            )
        })?;
        attributes.source = MessageField::from(Some(source));
    };

    // Add UserProperty 4 to UAttributes as Message Sink
    if let Some(sink_string) = props.find_user_property("4") {
        let sink = UUri::from_str(&sink_string).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 4 to Message Sink",
            )
        })?;
        attributes.sink = MessageField::from(Some(sink));
    };

    // Add UserProperty 5 to UAttributes as Priority
    if let Some(priority_string) = props.find_user_property("5") {
        let result = priority_string
            .parse::<i32>()
            .ok()
            .and_then(UPriority::from_i32)
            .map(EnumOrUnknown::from);

        if let Some(priority) = result {
            attributes.priority = priority;
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 2 to Message Type".to_string(),
            ));
        }
    }

    // Add UserProperty 7 to UAttributes as Permission Level
    if let Some(permission_string) = props.find_user_property("7") {
        let permission: u32 = permission_string.parse().map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 7 to Permission Level",
            )
        })?;
        attributes.permission_level = Some(permission);
    };

    // Add UserProperty 8 to UAttributes as CommStatus
    if let Some(comm_status_string) = props.find_user_property("8") {
        let comm_status_int = comm_status_string.parse::<i32>().map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 8 to CommStatus",
            )
        })?;

        let comm_status = UCode::from_i32(comm_status_int).ok_or_else(|| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 8 to CommStatus",
            )
        })?;

        attributes.commstatus = Some(EnumOrUnknown::from(comm_status));
    }

    // Add UserProperty 9 to UAttributes as Request ID
    if let Some(req_id) = props.find_user_property("9") {
        let id = UUID::from_str(&req_id).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 9 to Request ID",
            )
        })?;
        attributes.reqid = MessageField::from(Some(id));
    };

    // Add UserProperty 10 to UAttributes as Token
    if let Some(token) = props.find_user_property("10") {
        attributes.token = Some(token);
    };

    // Add UserProperty 11 to UAttributes as Traceparent
    if let Some(traceparent) = props.find_user_property("11") {
        attributes.traceparent = Some(traceparent);
    };

    // Add UserProperty 12 to UAttributes as Payload Format
    if let Some(payload_format_string) = props.find_user_property("12") {
        let result = payload_format_string
            .parse::<i32>()
            .ok()
            .and_then(UPayloadFormat::from_i32)
            .map(EnumOrUnknown::from);

        if let Some(payload_format) = result {
            attributes.payload_format = payload_format;
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to map UserProperty 2 to Message Type".to_string(),
            ));
        }
    }

    // Validate the reconstructed attributes
    UAttributesValidators::get_validator_for_attributes(&attributes)
        .validate(&attributes)
        .map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to construct uAttributes, err: {e:?}"),
            )
        })?;

    Ok(attributes)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use mqtt_client::MockMqttClientOperations;
    use protobuf::Enum;
    use up_rust::{MockUListener, UMessageType, UPayloadFormat, UPriority, UUID};

    use test_case::test_case;

    use super::*;

    /// Constants defining the protobuf field numbers for UAttributes.
    pub const ID_NUM: &str = "1";
    pub const TYPE_NUM: &str = "2";
    pub const SOURCE_NUM: &str = "3";
    pub const SINK_NUM: &str = "4";
    pub const PRIORITY_NUM: &str = "5";
    pub const PERM_LEVEL_NUM: &str = "7";
    pub const COMMSTATUS_NUM: &str = "8";
    pub const REQID_NUM: &str = "9";
    pub const TOKEN_NUM: &str = "10";
    pub const TRACEPARENT_NUM: &str = "11";
    pub const PAYLOAD_NUM: &str = "12";

    // Helper function used to create a UAttributes object and mqtt properties object for testing and comparison.
    #[allow(clippy::too_many_arguments)]
    fn create_test_uattributes_and_properties(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> (UAttributes, mqtt::Properties) {
        let uattributes = create_uattributes(
            id.clone(),
            type_,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid.clone(),
            token,
            traceparent,
            payload_format,
        );

        let properties = create_mqtt_properties(
            id,
            type_,
            source,
            sink,
            priority,
            ttl,
            perm_level,
            commstatus,
            reqid,
            token,
            traceparent,
            payload_format,
        );

        (uattributes, properties)
    }

    // Helper function to construct UAttributes object for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_uattributes(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> UAttributes {
        let mut attributes = UAttributes::default();

        if let Some(id) = id {
            attributes.id = Some(id).into();
        }

        if let Some(type_) = type_ {
            attributes.type_ = type_.into();
        }

        if let Some(source) = source {
            attributes.source =
                Some(UUri::from_str(source).expect("expected valid source UUri string.")).into();
        }

        if let Some(sink) = sink {
            attributes.sink =
                Some(UUri::from_str(sink).expect("expected valid sink UUri string.")).into();
        }

        if let Some(priority) = priority {
            attributes.priority = priority.into();
        }

        if let Some(ttl) = ttl {
            attributes.ttl = Some(ttl);
        }

        if let Some(perm_level) = perm_level {
            attributes.permission_level = Some(perm_level);
        }

        if let Some(commstatus) = commstatus {
            attributes.commstatus = Some(commstatus.into());
        }

        if let Some(reqid) = reqid {
            attributes.reqid = Some(reqid).into();
        }

        if let Some(token) = token {
            attributes.token = Some(token.to_string());
        }

        if let Some(traceparent) = traceparent {
            attributes.traceparent = Some(traceparent.to_string());
        }

        if let Some(payload_format) = payload_format {
            attributes.payload_format = payload_format.into();
        }

        attributes
    }

    // Helper function to create mqtt properties for testing.
    #[allow(clippy::too_many_arguments)]
    fn create_mqtt_properties(
        id: Option<UUID>,
        type_: Option<UMessageType>,
        source: Option<&str>,
        sink: Option<&str>,
        priority: Option<UPriority>,
        ttl: Option<u32>,
        perm_level: Option<u32>,
        commstatus: Option<UCode>,
        reqid: Option<UUID>,
        token: Option<&str>,
        traceparent: Option<&str>,
        payload_format: Option<UPayloadFormat>,
    ) -> mqtt::Properties {
        let mut properties = mqtt::Properties::new();

        // Add TTL as MQTT MessageExpiryInterval
        if let Some(message_expiry_interval) = ttl {
            properties
                .push_u32(
                    mqtt::PropertyCode::MessageExpiryInterval,
                    message_expiry_interval,
                )
                .unwrap();
        }

        // Add uAttributes version number.
        properties
            .push_string_pair(mqtt::PropertyCode::UserProperty, "0", "1")
            .unwrap();

        if let Some(id_val) = id {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    ID_NUM,
                    &id_val.to_hyphenated_string(),
                )
                .unwrap();
        }

        // Add the remaining attributes as MQTT UserProperties
        if let Some(type_val) = type_ {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    TYPE_NUM,
                    &type_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(source_val) = source {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, SOURCE_NUM, source_val)
                .unwrap();
        }
        if let Some(sink_val) = sink {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, SINK_NUM, sink_val)
                .unwrap();
        }
        if let Some(priority_val) = priority {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PRIORITY_NUM,
                    &priority_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(perm_level_val) = perm_level {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PERM_LEVEL_NUM,
                    &perm_level_val.to_string(),
                )
                .unwrap();
        }
        if let Some(commstatus_val) = commstatus {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    COMMSTATUS_NUM,
                    &commstatus_val.value().to_string(),
                )
                .unwrap();
        }
        if let Some(reqid_val) = reqid {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    REQID_NUM,
                    &reqid_val.to_hyphenated_string(),
                )
                .unwrap();
        }
        if let Some(token_val) = token {
            properties
                .push_string_pair(mqtt::PropertyCode::UserProperty, TOKEN_NUM, token_val)
                .unwrap();
        }
        if let Some(traceparent_val) = traceparent {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    TRACEPARENT_NUM,
                    traceparent_val,
                )
                .unwrap();
        }
        if let Some(payload_format_val) = payload_format {
            properties
                .push_string_pair(
                    mqtt::PropertyCode::UserProperty,
                    PAYLOAD_NUM,
                    &payload_format_val.value().to_string(),
                )
                .unwrap();
        }

        properties
    }

    #[tokio::test]
    async fn test_get_free_subscription_id() {
        let up_client = Mqtt5Transport {
            mqtt_client: Arc::new(Mutex::new(MockMqttClientOperations::new())),
            subscription_topic_map: Arc::new(RwLock::new(HashMap::new())),
            topic_listener_map: Arc::new(RwLock::new(HashMap::new())),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            free_subscription_ids: Arc::new(RwLock::new((1..3).collect())),
            message_callback_handle: None,
        };

        let expected_vals: Vec<u16> = up_client
            .free_subscription_ids
            .read()
            .await
            .iter()
            .cloned()
            .collect();
        let mut collected_vals = Vec::<u16>::new();

        let result = up_client.get_free_subscription_id().await;

        assert!(result.is_ok());
        collected_vals.push(result.unwrap());

        let result = up_client.get_free_subscription_id().await;

        assert!(result.is_ok());
        collected_vals.push(result.unwrap());

        assert!(collected_vals.len() == 2);
        assert!(collected_vals.iter().all(|x| expected_vals.contains(x)));
        assert!(up_client.free_subscription_ids.read().await.is_empty());

        let result = up_client.get_free_subscription_id().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_free_subscription_id() {
        let up_client = Mqtt5Transport {
            mqtt_client: Arc::new(Mutex::new(MockMqttClientOperations::new())),
            subscription_topic_map: Arc::new(RwLock::new(HashMap::new())),
            topic_listener_map: Arc::new(RwLock::new(HashMap::new())),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            free_subscription_ids: Arc::new(RwLock::new((1..3).collect())),
            message_callback_handle: None,
        };

        let expected_id = 7;

        up_client.add_free_subscription_id(expected_id).await;

        let free_ids = up_client.free_subscription_ids.read().await;

        assert!(free_ids.contains(&expected_id));
    }

    #[tokio::test]
    async fn test_add_listener() {
        let listener = Arc::new(MockUListener::new());
        let expected_listener = ComparableListener::new(listener.clone());
        let sub_map = Arc::new(RwLock::new(HashMap::new()));
        let topic_map = Arc::new(RwLock::new(HashMap::new()));
        let mut client_operations = MockMqttClientOperations::new();
        client_operations
            .expect_subscribe()
            .once()
            .return_const(Ok(()));

        let up_client = Mqtt5Transport {
            mqtt_client: Arc::new(Mutex::new(client_operations)),
            subscription_topic_map: sub_map.clone(),
            topic_listener_map: topic_map.clone(),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            free_subscription_ids: Arc::new(RwLock::new((1..10).collect())),
            message_callback_handle: None,
        };

        assert!(topic_map.read().await.is_empty());

        let result = up_client.add_listener("test_topic", listener.clone()).await;

        assert!(result.is_ok());

        let actual_topic_map = topic_map.read().await;

        assert!(actual_topic_map.contains_key("test_topic"));
        let actual_listeners = actual_topic_map.get("test_topic").unwrap();
        assert_eq!(actual_listeners.len(), 1);
        assert!(actual_listeners.contains(&expected_listener));
    }

    #[tokio::test]
    async fn test_remove_listener() {
        let listener_1 = Arc::new(MockUListener::new());
        let comparable_listener_1 = ComparableListener::new(listener_1.clone());
        let listener_2 = Arc::new(MockUListener::new());
        let comparable_listener_2 = ComparableListener::new(listener_2.clone());
        let sub_map = Arc::new(RwLock::new(HashMap::new()));
        let topic_map = Arc::new(RwLock::new(HashMap::new()));

        topic_map.write().await.insert(
            "test_topic".to_string(),
            [comparable_listener_1.clone(), comparable_listener_2.clone()]
                .iter()
                .cloned()
                .collect(),
        );

        let mut client_operations = MockMqttClientOperations::new();
        client_operations.expect_unsubscribe().return_const(Ok(()));

        let up_client = Mqtt5Transport {
            mqtt_client: Arc::new(Mutex::new(client_operations)),
            subscription_topic_map: sub_map.clone(),
            topic_listener_map: topic_map.clone(),
            authority_name: "test".to_string(),
            mode: TransportMode::InVehicle,
            free_subscription_ids: Arc::new(RwLock::new((1..10).collect())),
            message_callback_handle: None,
        };

        assert!(!topic_map.read().await.is_empty());

        let result = up_client
            .remove_listener("test_topic", listener_1.clone())
            .await;

        assert!(result.is_ok());

        {
            let actual_topic_map = topic_map.read().await;

            assert!(actual_topic_map.contains_key("test_topic"));
            let actual_listeners = actual_topic_map.get("test_topic").unwrap();
            assert_eq!(actual_listeners.len(), 1);
            assert!(!actual_listeners.contains(&comparable_listener_1));
            assert!(actual_listeners.contains(&comparable_listener_2));
        }

        let result = up_client
            .remove_listener("test_topic", listener_2.clone())
            .await;

        assert!(result.is_ok());
        assert!(topic_map.read().await.is_empty());

        let result = up_client
            .remove_listener("test_topic", listener_2.clone())
            .await;

        assert!(result.is_err());
        assert!(result.err().unwrap().code == UCode::NOT_FOUND.into());
    }

    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_PUBLISH),
            Some("//VIN.vehicles/A8000/2/8A50"),
            None, None, None, None, None, None, None, None, None
        ),
        4,
        None;
        "Publish success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_NOTIFICATION),
            Some("//VIN.vehicles/A8000/2/1A50"),
            Some("//VIN.vehicles/B8000/3/0"),
            None, None, None, None, None, None, None, None
        ),
        5,
        None;
        "Notification success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_REQUEST),
            Some("//VIN.vehicles/A8000/2/0"),
            Some("//VIN.vehicles/B8000/3/1B50"),
            Some(UPriority::UPRIORITY_CS4),
            Some(3600),
            None, None, None, None, None, None
        ),
        7,
        None;
        "Request success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_RESPONSE),
            Some("//VIN.vehicles/B8000/3/1B50"),
            Some("//VIN.vehicles/A8000/2/0"),
            Some(UPriority::UPRIORITY_CS4),
            None, None, None,
            Some(UUID::build()),
            None, None, None
        ),
        7,
        None;
        "Response success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_PUBLISH),
            Some("//VIN.vehicles/A8000/2/1A50"),
            None, None, None, None, None, None, None, None, None
        ),
        4,
        Some(UStatus::fail_with_code(UCode::INTERNAL, "Invalid uAttributes, err: ValidationError(\"Validation failure: Invalid source URI: Validation error: Resource ID must be >= 0x8000\")".to_string()));
        "Publish failure with validation error"
    )]
    fn test_create_mqtt_properties_from_uattributes(
        (attributes, properties): (UAttributes, mqtt::Properties),
        expected_attributes_num: usize,
        expected_error: Option<UStatus>,
    ) {
        let props = create_mqtt_properties_from_uattributes(&attributes);

        // Check if properties could be created at all
        if props.is_ok() {
            let constructed_props = props.unwrap();
            assert_eq!(constructed_props.len(), expected_attributes_num);
            // Iterate over all created properties and compare with the expected properties
            constructed_props.user_iter().for_each(|(key, value)| {
                let expected_prop = properties.find_user_property(&key);
                assert_eq!(Some(value), expected_prop);
            });
        } else {
            assert_eq!(props.err(), expected_error);
        }
    }

    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_PUBLISH),
            Some("//VIN.vehicles/A8000/2/8A50"),
            None, None, None, None, None, None, None, None, None
        ),
        None;
        "Publish success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_NOTIFICATION),
            Some("//VIN.vehicles/A8000/2/1A50"),
            Some("//VIN.vehicles/B8000/3/0"),
            None, None, None, None, None, None, None, None
        ),
        None;
        "Notification success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_REQUEST),
            Some("//VIN.vehicles/A8000/2/0"),
            Some("//VIN.vehicles/B8000/3/1B50"),
            Some(UPriority::UPRIORITY_CS4),
            Some(3600),
            None, None, None, None, None, None
        ),
        None;
        "Request success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_RESPONSE),
            Some("//VIN.vehicles/B8000/3/1B50"),
            Some("//VIN.vehicles/A8000/2/0"),
            Some(UPriority::UPRIORITY_CS4),
            None, None, None,
            Some(UUID::build()),
            None, None, None
        ),
        None;
        "Response success"
    )]
    #[test_case(
        create_test_uattributes_and_properties(
            Some(UUID::build()),
            Some(UMessageType::UMESSAGE_TYPE_PUBLISH),
            Some("//VIN.vehicles/A8000/2/1A50"),
            None, None, None, None, None, None, None, None, None
        ),
        Some(UStatus::fail_with_code(UCode::INTERNAL, "Unable to construct uAttributes, err: ValidationError(\"Validation failure: Invalid source URI: Validation error: Resource ID must be >= 0x8000\")".to_string()));
        "Publish failure with validation error"
    )]
    fn test_get_uattributes_from_mqtt_properties(
        (attributes, properties): (UAttributes, mqtt::Properties),
        expected_error: Option<UStatus>,
    ) {
        let attributes_result = get_uattributes_from_mqtt_properties(&properties);

        if attributes_result.is_ok() {
            let actual_attributes = attributes_result.unwrap();
            assert_eq!(actual_attributes, attributes);
        } else {
            assert_eq!(attributes_result.err(), expected_error);
        }
    }

    #[test_case(
        "up://VIN.vehicles/A8000/2/8A50",
        "VIN.vehicles";
        "Valid UUri"
    )]
    #[test_case(
        "A8000/2/8A50",
        "local_authority";
        "Local UUri"
    )]
    #[test_case(
        "//*/A8000/2/8A50",
        "+";
        "Wildcard authority"
    )]
    // [utest->dsn~up-transport-mqtt5-d2d-topic-names~1]
    fn test_uri_to_authority_topic_segment(uri: &str, expected_segment: &str) {
        let uuri = UUri::from_str(uri).expect("failed to create UUri from URI");
        let actual_segment =
            TransportMode::uri_to_authority_topic_segment(&uuri, "local_authority");
        assert_eq!(&actual_segment, expected_segment);
    }

    #[test_case(
        "up://VIN.vehicles/A8000/2/8A50",
        "VIN.vehicles/8000/A/2/8A50";
        "Valid UUri"
    )]
    #[test_case(
        "A8000/2/8A50",
        "local_authority/8000/A/2/8A50";
        "Local UUri"
    )]
    #[test_case(
        "//*/A8000/2/8A50",
        "+/8000/A/2/8A50";
        "Wildcard authority"
    )]
    #[test_case(
        "//VIN.vehicles/FFFF/2/8A50",
        "VIN.vehicles/+/0/2/8A50";
        "Wildcard entity type id"
    )]
    #[test_case(
        "//VIN.vehicles/FFFF8000/2/8A50",
        "VIN.vehicles/8000/+/2/8A50";
        "Wildcard entity instance id"
    )]
    #[test_case(
        "//VIN.vehicles/A8000/FF/8A50",
        "VIN.vehicles/8000/A/+/8A50";
        "Wildcard entity version"
    )]
    #[test_case(
        "//VIN.vehicles/A8000/2/FFFF",
        "VIN.vehicles/8000/A/2/+";
        "Wildcard resource id"
    )]
    // [utest->dsn~up-transport-mqtt5-e2e-topic-names~1]
    fn test_uri_to_e2e_mqtt_topic(uuri: &str, expected_topic: &str) {
        let uuri = UUri::from_str(uuri).expect("failed to create UUri from URI");

        let actual_segment = TransportMode::uri_to_e2e_mqtt_topic(&uuri, "local_authority");
        assert_eq!(&actual_segment, expected_topic);
    }

    #[test_case(
        "//VIN.vehicles/A8000/2/8A50",
        None,
        TransportMode::InVehicle,
        "VIN.vehicles/8000/A/2/8A50";
        "Publish to a specific topic"
    )]
    #[test_case(
        "//VIN.vehicles/A8000/2/8A50",
        Some("//VIN.vehicles/B8000/3/0"),
        TransportMode::InVehicle,
        "VIN.vehicles/8000/A/2/8A50/VIN.vehicles/8000/B/3/0";
        "Send a notification"
    )]
    #[test_case(
        "/A8000/2/0",
        Some("/B8000/3/1B50"),
        TransportMode::InVehicle,
        "local_authority/8000/A/2/0/local_authority/8000/B/3/1B50";
        "Send a local RPC request"
    )]
    #[test_case(
        "//VIN.vehicles/B8000/3/1B50",
        Some("//VIN.vehicles/A8000/2/0"),
        TransportMode::InVehicle,
        "VIN.vehicles/8000/B/3/1B50/VIN.vehicles/8000/A/2/0";
        "Send an RPC Response"
    )]
    #[test_case(
        "//*/FFFFFFFF/FF/FFFF",
        Some("/AB34/1/12CD"),
        TransportMode::InVehicle,
        "+/+/+/+/+/local_authority/AB34/0/1/12CD";
        "Subscribe to incoming RPC requests for a specific method"
    )]
    #[test_case(
        "//*/FFFFFFFF/FF/FFFF",
        Some("//SERVICE.backend/FFFFFFFF/FF/FFFF"),
        TransportMode::OffVehicle,
        "+/SERVICE.backend";
        "Subscribe to all incoming messages for uEntities on a given authority in the back end"
    )]
    #[test_case(
        "//other_authority/FFFFFFFF/FF/FFFF",
        None,
        TransportMode::InVehicle,
        "other_authority/+/+/+/+";
        "Subscribe to all messages published to topics of a specific authority"
    )]
    #[test_case(
        "//*/FFFFFFFF/FF/FFFF",
        Some("/FFFFFFFF/FF/FFFF"),
        TransportMode::OffVehicle,
        "+/local_authority";
        "Streamer subscribes to all inbound messages from the cloud"
    )]
    #[test_case(
        "//*/FFFFFFFF/FF/FFFF",
        None,
        TransportMode::InVehicle,
        "+/+/+/+/+";
        "Subscribe to all publish messages from devices within the vehicle"
    )]
    #[test_case(
        "//other_authority/FFFFFFFF/FF/FFFF",
        Some("//*/FFFFFFFF/FF/FFFF"),
        TransportMode::InVehicle,
        "other_authority/+/+/+/+/+/+/+/+/+";
        "Subscribe to all message types but publish messages sent from a specific authority"
    )]
    // [utest->dsn~up-transport-mqtt5-e2e-topic-names~1]
    // [utest->dsn~up-transport-mqtt5-d2d-topic-names~1]
    fn test_to_mqtt_topic_string(
        src_uri: &str,
        sink_uri: Option<&str>,
        mode: TransportMode,
        expected_topic: &str,
    ) {
        let src_uri = UUri::from_str(src_uri).expect("failed to create source UUri from URI");
        let sink_uri =
            sink_uri.map(|uri| UUri::from_str(uri).expect("failed to create sink UUri from URI"));

        assert!(mode
            .to_mqtt_topic(&src_uri, sink_uri.as_ref(), "local_authority")
            .is_ok_and(|topic| topic == expected_topic));
    }

    #[test_case("VIN.vehicles/A8000/2/8A50", "VIN.vehicles/A8000/2/8A50", true; "Exact match")]
    #[test_case("VIN.vehicles/A8000/2/8A50", "+/+/+/+", true; "Wildcard pattern")]
    #[test_case("VIN.vehicles/A8000/2/8A50", "VIN.vehicles/B8000/2/8A50", false; "Mismatched entity id")]
    #[test_case("VIN.vehicles/A8000/2/8A50", "+/A8000/2/8A50", true; "Single wildcard matchs")]
    fn test_compare_topic(topic: &str, pattern: &str, expected_result: bool) {
        assert_eq!(
            Mqtt5Transport::compare_topic(topic, pattern),
            expected_result
        );
    }
}
