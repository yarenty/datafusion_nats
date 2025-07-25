use crate::Result;
use async_nats::Message;
use datafusion::arrow::array::{ArrayRef, BinaryBuilder, TimestampMillisecondBuilder};
use datafusion::arrow::datatypes::{SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc::Sender;
use crate::reader::batch_handler::{BatchHandler, BatchHandlerBuilder};

pub struct CsvBatchHandler {
    schema: SchemaRef,
    binary_builder: BinaryBuilder,
    timestamp_builder: TimestampMillisecondBuilder,
    batch_size: usize,
    row_count: usize,
}

impl CsvBatchHandler {
    pub fn new( schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            schema: schema.clone(),
            binary_builder: BinaryBuilder::new(),
            timestamp_builder: TimestampMillisecondBuilder::new(),
            batch_size,
            row_count: 0,
        }
    }
}

impl BatchHandler for CsvBatchHandler {
    fn append_message(&mut self, message: &Message) -> Result<()> {
        let payload = String::from_utf8(message.payload.to_vec())?;

        self.binary_builder.append_value(payload.as_bytes());
        self.timestamp_builder.append_value(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis() as i64,
        );
        self.row_count += 1;
        Ok(())
    }

    fn has_capacity(&self) -> bool {
        self.row_count < self.batch_size
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.binary_builder.finish()) as ArrayRef,
                Arc::new(self.timestamp_builder.finish()) as ArrayRef,
            ],
        )?;
        self.binary_builder = BinaryBuilder::new();
        self.timestamp_builder = TimestampMillisecondBuilder::new();
        self.row_count = 0;
        Ok(record_batch)
    }
}

#[derive(Debug, Clone)]
pub struct CsvBatchHandlerBuilder {}

impl CsvBatchHandlerBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BatchHandlerBuilder for CsvBatchHandlerBuilder {
    type Handler = CsvBatchHandler;

    fn build(&self, schema: SchemaRef, batch_size: usize) -> Result<CsvBatchHandler> {
        Ok(CsvBatchHandler::new( schema, batch_size))
    }
}
