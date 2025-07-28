use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::pretty;
use datafusion::physical_plan::metrics::RecordOutput;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_nats::batch_buffer::{BatchBuffer, BatchBufferConfig};
use datafusion_nats::codec::csv::CsvCodec;
use datafusion_nats::executor::{NatsStreamConfig, NatsStreamingExec, NatsStreamingTableProvider};
use datafusion_nats::nats_connection;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let subject = "test_data";
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

    info!("Config NatsStreamingExec...");
    // Create the streaming exec
    let config = NatsStreamConfig {
        batch_size: 1000,
        batch_timeout: Duration::from_millis(100),
        buffer_size: 10000,
        enable_backpressure: true,
        ..Default::default()
    };

    info!("Creating NatsStreamingExec...");
    let nats_exec = NatsStreamingExec::new(
        schema,
        nc,
        subject.to_string(),
        vec![], // filters
        None,   // no limit
        codec,
        Some(config),
    );

    info!("Registering table provider...");
    // Use as table provider
    let table_provider = NatsStreamingTableProvider::new(nats_exec);
    ctx.register_table(subject, Arc::new(table_provider))?;

    info!("Creating DataFrame...");
    // Now you can query it with SQL
    let df = ctx
        .sql(format!("SELECT * FROM {} ", subject).as_str())
        .await?;

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
    }
    // info!("Showing results...");
    // // Print the results
    //  df.show().await?;
    info!("Results shown.");

    Ok(())
}
