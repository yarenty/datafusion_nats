# datafusion-nats

`datafusion-nats` provides a DataFusion `TableProvider` implementation for NATS, allowing DataFusion to query real-time data streams from NATS subjects using SQL.

## Features

- Connects to NATS server and subscribes to a specified subject.
- Integrates with DataFusion as a `TableProvider`.
- Supports configurable data schemas.
- Handles data conversion from NATS messages to Apache Arrow `RecordBatch`es using pluggable codecs (JSON, CSV).

## Usage

To use `datafusion-nats`, you need to:

1.  Connect to a NATS server.
2.  Define a schema for your NATS data.
3.  Register a NATS table in a DataFusion `SessionContext`.
4.  Execute SQL queries against the registered table.

### Example

```rust
use datafusion::prelude::{SessionContext, SessionConfig};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use anyhow::Result;
use datafusion_nats::reader::{SessionContextExt, TableProperties, ReadEncoding, DataSchemaEncoding, SchemaLocation};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a DataFusion session context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    // Define a schema for the NATS data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create table properties for NATS
    let nats_props = TableProperties::builder()
        .nats_option("servers", "nats://localhost:4222")
        .data_schema_encoding(DataSchemaEncoding::Payload(
            ReadEncoding::Json,
            SchemaLocation::Provided(schema.clone()),
        ))
        .build();

    // Register the NATS table
    ctx.register_nats("test.data", "nats_table", nats_props).await?;

    // Execute a SQL query
    let df = ctx.sql("SELECT id, name FROM nats_table").await?;

    // Print the results
    df.show().await?;

    Ok(())
}
```

## Building and Testing

To build the project, navigate to the `datafusion_nats` directory and run:

```bash
cargo build
```

To run the tests, use:

```bash
cargo test
```
