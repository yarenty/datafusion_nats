use std::sync::Arc;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{Field, Schema, DataType};
use crate::batch_buffer::{BatchBuffer, BatchBufferConfig};
use datafusion::arrow::record_batch::RecordBatch;

#[tokio::test]
async fn test_batch_buffer_basic() {
    // Create a simple schema with two fields
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Create batch buffer with small batch size for testing
    let config = BatchBufferConfig {
        batch_size: 2,
        max_buffer_size: 10,
        batch_timeout_ms: 1000,
    };
    let mut buffer = BatchBuffer::new(schema.clone(), config);

    // Test adding rows
    let row1 = vec![
        Arc::new(Int32Array::from(vec![1])) as Arc<_>,
        Arc::new(StringArray::from(vec!["Alice"])) as Arc<_>,
    ];
    let row2 = vec![
        Arc::new(Int32Array::from(vec![2])) as Arc<_>,
        Arc::new(StringArray::from(vec!["Bob"])) as Arc<_>,
    ];

    // Add first row - should not create batch yet
    buffer.add_row(row1.clone()).unwrap();
    assert!(!buffer.should_create_batch());

    // Add second row - should create batch
    buffer.add_row(row2.clone()).unwrap();
    assert!(buffer.should_create_batch());

    // Create batch
    let batch = buffer.create_batch().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);

    // Buffer should be empty after batch creation
    assert!(!buffer.should_create_batch());
}

#[tokio::test]
async fn test_batch_buffer_timeout() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    // Create buffer with timeout
    let config = BatchBufferConfig {
        batch_size: 10,  // Large batch size
        max_buffer_size: 10,
        batch_timeout_ms: 100,  // Short timeout
    };
    let mut buffer = BatchBuffer::new(schema.clone(), config);

    // Add single row
    let row = vec![Arc::new(Int32Array::from(vec![1])) as Arc<_>];
    buffer.add_row(row.clone()).unwrap();

    // Should not create batch immediately
    assert!(!buffer.should_create_batch());

    // Wait for timeout
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Should create batch now due to timeout
    assert!(buffer.should_create_batch());
}

#[tokio::test]
async fn test_batch_buffer_overflow() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    // Create buffer with small max size
    let config = BatchBufferConfig {
        batch_size: 100,  // Large batch size
        max_buffer_size: 3,
        batch_timeout_ms: 1000,
    };
    let mut buffer = BatchBuffer::new(schema.clone(), config);

    // Add rows until we hit max buffer size
    let row = vec![Arc::new(Int32Array::from(vec![1])) as Arc<_>];
    buffer.add_row(row.clone()).unwrap();
    buffer.add_row(row.clone()).unwrap();

    // Adding another row should cause overflow
    assert!(buffer.is_full());
    assert!(buffer.add_row(row.clone()).is_err());
}

#[tokio::test]
async fn test_batch_buffer_multiple_batches() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    // Create buffer with small batch size
    let config = BatchBufferConfig {
        batch_size: 2,
        max_buffer_size: 10,
        batch_timeout_ms: 1000,
    };
    let mut buffer = BatchBuffer::new(schema.clone(), config);

    let row = vec![Arc::new(Int32Array::from(vec![1])) as Arc<_>];

    // Add 5 rows in total
    for _ in 0..5 {
        buffer.add_row(row.clone()).unwrap();
        if buffer.should_create_batch() {
            let batch = buffer.create_batch().unwrap().unwrap();
            assert_eq!(batch.num_rows(), 2);
        }
    }

    // Final batch should have 1 row
    let final_batch = buffer.create_batch().unwrap().unwrap();
    assert_eq!(final_batch.num_rows(), 1);
}
