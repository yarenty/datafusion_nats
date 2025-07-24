mod nats_connection;
mod data_source;

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionContext, SessionConfig};
use anyhow::Result;

use data_source::NatsDataSource;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a NATS connection
    let nc = nats_connection::connect("nats://localhost:4222").await?;

    // Create a Datafusion session context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    // Create a schema for the NATS data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create a NatsDataSource
    let data_source = Arc::new(NatsDataSource::new(schema, nc.clone()));

    // Register the NatsDataSource as a table
    ctx.register_table("nats_table", data_source)?;

    // Execute a SQL query
    let df = ctx.sql("SELECT * FROM nats_table").await?;

    // Print the results
    df.show().await?;

    Ok(())
}
