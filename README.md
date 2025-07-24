# Datafusion NATS Integration

This crate provides a DataFusion `TableProvider` implementation for NATS, allowing DataFusion to query real-time data streams from NATS subjects using SQL.

## Features

- **Datafusion Table Provider**: This crate provides a DataFusion `TableProvider` implementation for NATS, allowing DataFusion to query real-time data streams from NATS subjects using SQL.
- **NATS Integration**: The crate uses the [async-nats](https://docs.rs/nats) crate to connect to NATS servers and subscribe to subjects for real-time data streaming.
- **Schema Inference**: The crate infers the schema for the NATS data based on the NATS message payload.
- **Streaming**: The crate supports streaming data from NATS subjects, allowing real-time analysis and processing of data as it arrives.
- **Error Handling**: The crate handles errors gracefully, ensuring that the streaming process is robust and error-free.
- **Documentation**: This crate is well-documented, making it easy to use and understand.
- **Examples**: The crate provides example code snippets and usage examples, making it easy to get started with NATS integration in DataFusion.

## TODO
- [x] PoC
- [ ] Schema Inference/Configuration
- [ ] Streaming and Real-time Data
- [ ] Predicate Pushdown
- [ ] Error Handling and Resilience
- [ ] Testing, Documentation, and Examples

## Usage

To use `datafusion-nats`, you need to:
1. Connect to a NATS server.
2. Define a schema for your NATS data.
3. Create a `NatsDataSource` instance with the NATS client, schema, and subject.
4. Register the `NatsDataSource` as a table in a DataFusion `SessionContext`.
5. Execute SQL queries against the registered table.

```rust
use datafusion_nats::NatsDataSource;
use datafusion::prelude::*;

// Connect to a NATS server
let client = async_nats::connect("nats://localhost:4222").await.unwrap();

// Define a schema for your NATS data
let schema = Schema::new(vec![
    Field::new("id", DataType::Int32, false),
    Field::new("name", DataType::Utf8, false),
]);

// Create a NatsDataSource instance
let nats_data_source = NatsDataSource::new(client, schema, "subject");

// Register the NatsDataSource as a table in a DataFusion SessionContext
let ctx = SessionContext::new();
ctx.register_table("nats_table", Arc::new(nats_data_source));

// Execute SQL queries against the registered table
let df = ctx.sql("SELECT * FROM nats_table").await.unwrap();
let results = df.collect().await.unwrap();
```

## License

This crate is distributed under the Apache License, Version 2.0.