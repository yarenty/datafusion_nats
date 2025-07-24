use crate::{DataFusionNatsError, Result};
use crate::reader::{BatchHandler,BatchHandlerBuilder};
use datafusion::arrow::array::{ArrayRef, BinaryBuilder, TimestampMillisecondBuilder};
use datafusion::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datafusion::arrow::record_batch::RecordBatch;
use async_nats::Message;
use serde_json::Value;
use std::sync::Arc;
use typed_builder::TypedBuilder;

pub struct JsonBatchHandler {
    schema: SchemaRef,
    batch_size: usize,
    timestamp_builder: TimestampMillisecondBuilder,
    payload_builder: BinaryBuilder,
}

impl JsonBatchHandler {
    pub fn new(schema: SchemaRef, batch_size: usize) -> Result<Self> {
        Ok(Self {
            schema,
            batch_size,
            timestamp_builder: TimestampMillisecondBuilder::new(),
            payload_builder: BinaryBuilder::new(),
        })
    }
}

impl BatchHandler for JsonBatchHandler {
    fn append_message(&mut self, message: &Message) -> Result<()> {
        // For JSON, we expect the payload to be a valid JSON string.
        // We'll store the raw payload and the timestamp.
        self.timestamp_builder.append_value(message.timestamp.unix_timestamp_nanos() / 1_000_000)?; // NATS timestamp is SystemTime, convert to milliseconds
        self.payload_builder.append_value(message.payload.as_ref())?;
        Ok(())
    }

    fn has_capacity(&self) -> bool {
        self.payload_builder.len() < self.batch_size
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let timestamp_array = self.timestamp_builder.finish();
        let payload_array = self.payload_builder.finish();

        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(timestamp_array) as ArrayRef, Arc::new(payload_array) as ArrayRef],
        )?;

        // Reset builders for the next batch
        self.timestamp_builder = TimestampMillisecondBuilder::new();
        self.payload_builder = BinaryBuilder::new();

        Ok(record_batch)
    }
}

#[derive(Debug, TypedBuilder)]
pub struct JsonBatchHandlerBuilder {}

impl BatchHandlerBuilder for JsonBatchHandlerBuilder {
    type Handler = JsonBatchHandler;

    fn build(&self, schema: SchemaRef, batch_size: usize) -> Result<Self::Handler> {
        JsonBatchHandler::new(schema, batch_size)
    }
}