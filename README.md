# Eclipse uProtocol Rust MQTT5 Client

## Overview

This library implements a uTransport client for MQTT5 in Rust following the uProtocol [uTransport Specifications](https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/up-l1/README.adoc).

## Getting Started

### Building the Library

To build the library, run `cargo build` in the project root directory. Tests can be run with `cargo test`. This library leverages the [up-rust](https://github.com/eclipse-uprotocol/up-rust/tree/main) library for data types and models specified by uProtocol.

### Running the Tests

To run the tests from the repo root directory, run
```bash
cargo test
```

### Running the Examples

First, ensure you have a local MQTT broker running, such as [Mosquitto](https://github.com/eclipse/mosquitto).

Then start the following two examples from your repo root directory.

```bash
cargo run --example publisher_example
```

```bash
cargo run --example subscriber_example
```

This shows an example of a UPMqttClient publishing from one device and a UPMqttClient subscribing to the publishing device to receive data.

### Using the Library

The library contains the following modules:

Package | [uProtocol spec](https://github.com/eclipse-uprotocol/uprotocol-spec) | Purpose
---|---|---
transport | [uP-L1 Specifications](https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/up-l1/README.adoc) | Implementation of MQTT5 uTransport client used for bidirectional point-2-point communication between uEs.

Please refer to the [publisher_example](/examples/publisher_example.rs) and [subscriber_example](/examples/subscriber_example.rs) examples to see how to initialize and use the [UPClientMqtt](/src/transport.rs) client.
