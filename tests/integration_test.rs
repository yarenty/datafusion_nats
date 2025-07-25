use datafusion::prelude::{SessionContext, SessionConfig};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use anyhow::Result;
use tokio::time::{sleep, Duration};
use datafusion_nats::batch_buffer::{BatchBuffer, BatchBufferConfig};
use datafusion_nats::codec::csv::CsvCodec;
use datafusion_nats::nats_connection;
use datafusion_nats::data_source::NatsDataSource;

#[tokio::test]
async fn test_nats_integration() -> Result<()> {
    // Connect to NATS
    let client = nats_connection::connect("nats://localhost:4222").await?;

    // Publish some data
    let subject = "test.data";
    let data = vec![
        "1,apple",
        "2,banana",
        "3,orange",
    ];

    for (i, d) in data.iter().enumerate() {
        client.publish(subject, d.as_bytes().into()).await?;
        println!("Published: {}", d);
        // Add a small delay to ensure messages are processed
        sleep(Duration::from_millis(10)).await;
    }

    // Give NATS some time to process messages
    sleep(Duration::from_secs(1)).await;

    // Create a Datafusion session context
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);

    // Create a schema for the NATS data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create a NatsDataSource
    let codec = CsvCodec::new(schema.clone()).expect("Failed to create CSV codec");
    let buffer = BatchBuffer::new(schema.clone(), BatchBufferConfig::default());
    let data_source = Arc::new(NatsDataSource::new(schema.clone(), client.clone(), subject.to_string(), codec, buffer));

    // Register the NatsDataSource as a table
    ctx.register_table("nats_table", data_source)?;

    // Execute a SQL query
    let df = ctx.sql("SELECT id, name FROM nats_table limit 3").await?;

    // Collect the results
    let results = df.collect().await?;

    // Assert the results
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let id_array = batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int32Array>().unwrap();
    let name_array = batch.column(1).as_any().downcast_ref::<datafusion::arrow::array::StringArray>().unwrap();

    assert_eq!(id_array.value(0), 1);
    assert_eq!(name_array.value(0), "apple");

    assert_eq!(id_array.value(1), 2);
    assert_eq!(name_array.value(1), "banana");

    assert_eq!(id_array.value(2), 3);
    assert_eq!(name_array.value(2), "orange");

    Ok(())
}
