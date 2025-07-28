use anyhow::Result;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};
use tracing_subscriber;

use datafusion_nats::batch_buffer::{BatchBuffer, BatchBufferConfig};
use datafusion_nats::codec::csv::CsvCodec;
use datafusion_nats::{data_source::NatsDataSource, nats_connection};
use futures::StreamExt;

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
    let data = vec!["1,apple", "2,banana", "3,orange"];

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
    let codec = CsvCodec::new(schema.clone()).expect("Failed to create codec");
    let buffer = BatchBuffer::new(schema.clone(), BatchBufferConfig::default());
    let data_source = Arc::new(NatsDataSource::new(
        schema,
        nc.clone(),
        "test.data".to_string(),
        codec,
        buffer,
    ));
    debug!("NatsDataSource created.");

    info!("Registering table 'nats_table'...");
    // Register the NatsDataSource as a table
    ctx.register_table("nats_table", data_source)?;
    info!("Table 'nats_table' registered.");

    info!("Executing SQL query...");
    // Execute a SQL query
    // let df = ctx.sql("SELECT * FROM nats_table limit 3").await?;
    //TODO: FIXME
    let df = ctx.sql("SELECT * FROM nats_table").await?;
    info!("SQL query executed.");

    let mut stream = df.execute_stream().await?;
    info!("Showing results...");
    while let Some(batch_result) = stream.next().await {
        match batch_result {
            Ok(batch) => {
                println!("SQL streaming batch with {} rows", batch.num_rows());
                // Process batch
                // println!("{:?}", batch); // Process batch
                println!("{}", pretty::pretty_format_batches(&[batch])?);
            }
            Err(e) => {
                eprintln!("SQL stream error: {}", e);
                break;
            }
        }
        println!("\n ------> to finish press Ctrl+C (this is continuous streaming example)");
    }

    // Print the results
    // df.show().await?;
    info!("Results shown.");

    Ok(())
}
