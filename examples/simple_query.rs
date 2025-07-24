use datafusion::prelude::{SessionContext, SessionConfig};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use anyhow::Result;
use tokio::time::{sleep, Duration};
use tracing::{info, debug, error};
use tracing_subscriber;

use datafusion_nats::{nats_connection, data_source::NatsDataSource};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();
    info!("Starting simple_query example");

    // Create a NATS connection
    debug!("Connecting to NATS server...");
    let nc = nats_connection::connect("nats://localhost:4222").await?;
    info!("Connected to NATS server.");

    // Clone the NATS client for the publishing task
    let publisher_client = nc.clone();

    // Data to publish
    let subject = "test.data";
    let data = vec![
        "1,apple",
        "2,banana",
        "3,orange",
    ];

    // Spawn a task to publish data after a short delay
    tokio::spawn(async move {
        debug!("Publisher task started.");
        // Give the subscriber a moment to set up
        sleep(Duration::from_secs(2)).await;
        for d in data.iter() {
            match publisher_client.publish(subject, d.as_bytes().into()).await {
                Ok(_) => info!("Published: {}", d),
                Err(e) => error!("Failed to publish message: {}", e),
            }
            // Add a small delay to ensure messages are processed
            sleep(Duration::from_millis(10)).await;
        }
        info!("Publisher task finished.");
    });

    // Create a Datafusion session context
    debug!("Creating DataFusion session context...");
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    info!("DataFusion session context created.");

    // Create a schema for the NATS data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    debug!("Schema defined.");

    // Create a NatsDataSource
    let data_source = Arc::new(NatsDataSource::new(schema, nc.clone(), "test.data".to_string()));
    debug!("NatsDataSource created.");

    info!("Registering table 'nats_table'...");
    // Register the NatsDataSource as a table
    ctx.register_table("nats_table", data_source)?;
    info!("Table 'nats_table' registered.");

    info!("Executing SQL query...");
    // Execute a SQL query
    let df = ctx.sql("SELECT * FROM nats_table").await?;
    info!("SQL query executed.");

    info!("Showing results...");
    // Print the results
    df.show().await?;
    info!("Results shown.");

    Ok(())
}
