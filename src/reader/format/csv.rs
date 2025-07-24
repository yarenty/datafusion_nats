use crate::{DataFusionNatsError, Result};
use crate::reader::{BatchHandler,BatchHandlerBuilder};
use datafusion::arrow::array::{ArrayRef, BinaryBuilder, TimestampMillisecondBuilder};
use datafusion::arrow::datatypes::{SchemaRef, TimestampMillisecondType};
use datafusion::arrow::record_batch::RecordBatch;
use async_nats::Message;
use std::sync::Arc;
use typed_builder::TypedBuilder;
use datafusion::arrow::array::builder::{ArrayBuilder, make_builder};

pub struct CsvBatchHandler {
    schema: SchemaRef,
    batch_size: usize,
    timestamp_builder: TimestampMillisecondBuilder,
    builders: Vec<Box<dyn ArrayBuilder>>,
}

impl CsvBatchHandler {
    pub fn new(schema: SchemaRef, batch_size: usize) -> Result<Self> {
        let builders: Vec<Box<dyn ArrayBuilder>> = schema.fields().iter()
            .filter(|f| f.name() != super::super::COLUMN_EVENT_TIMESTAMP) // Exclude timestamp as it's handled separately
            .map(|field| make_builder(field.data_type(), batch_size))
            .collect();

        Ok(Self {
            schema,
            batch_size,
            timestamp_builder: TimestampMillisecondBuilder::new(),
            builders,
        })
    }
}

impl BatchHandler for CsvBatchHandler {
    fn append_message(&mut self, message: &Message) -> Result<()> {
        self.timestamp_builder.append_value(message.timestamp.unix_timestamp_nanos() / 1_000_000)?; // NATS timestamp is SystemTime, convert to milliseconds

        let payload_str = String::from_utf8_lossy(&message.payload);
        let values: Vec<&str> = payload_str.split(',').collect();

        let mut builder_idx = 0;
        for (field_idx, field) in self.schema.fields().iter().enumerate() {
            if field.name() == super::super::COLUMN_EVENT_TIMESTAMP {
                continue; // Skip timestamp, already handled
            }

            if let Some(value_str) = values.get(builder_idx) {
                let builder = &mut self.builders[builder_idx];
                match field.data_type() {
                    datafusion::arrow::datatypes::DataType::Utf8 => {
                        builder.as_any_mut().downcast_mut::<datafusion::arrow::array::StringBuilder>().unwrap().append_value(value_str.trim())?;
                    },
                    datafusion::arrow::datatypes::DataType::Int64 => {
                        builder.as_any_mut().downcast_mut::<datafusion::arrow::array::Int64Builder>().unwrap().append_value(value_str.trim().parse::<i64>().map_err(|e| DataFusionNatsError::Format(format!("Failed to parse Int64: {}", e)))?)?;
                    },
                    datafusion::arrow::datatypes::DataType::Float64 => {
                        builder.as_any_mut().downcast_mut::<datafusion::arrow::array::Float64Builder>().unwrap().append_value(value_str.trim().parse::<f64>().map_err(|e| DataFusionNatsError::Format(format!("Failed to parse Float64: {}", e)))?)?;
                    },
                    datafusion::arrow::datatypes::DataType::Boolean => {
                        builder.as_any_mut().downcast_mut::<datafusion::arrow::array::BooleanBuilder>().unwrap().append_value(value_str.trim().parse::<bool>().map_err(|e| DataFusionNatsError::Format(format!("Failed to parse Boolean: {}", e)))?)?;
                    },
                    _ => return Err(DataFusionNatsError::Format(format!("Unsupported data type for CSV: {:?}", field.data_type()))),
                }
            } else {
                // If value is missing, append null if nullable, else error
                if field.is_nullable() {
                    self.builders[builder_idx].append_null()?;
                } else {
                    return Err(DataFusionNatsError::Format(format!("Missing non-nullable CSV field: {}", field.name())));
                }
            }
            builder_idx += 1;
        }
        Ok(())
    }

    fn has_capacity(&self) -> bool {
        self.timestamp_builder.len() < self.batch_size
    }

    fn finish(&mut self) -> Result<RecordBatch> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.builders.len() + 1);
        columns.push(Arc::new(self.timestamp_builder.finish()) as ArrayRef);

        for builder in self.builders.iter_mut() {
            columns.push(Arc::new(builder.finish()));
        }

        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            columns,
        )?;

        // Reset builders for the next batch
        self.timestamp_builder = TimestampMillisecondBuilder::new();
        self.builders = self.schema.fields().iter()
            .filter(|f| f.name() != super::super::COLUMN_EVENT_TIMESTAMP)
            .map(|field| make_builder(field.data_type(), self.batch_size))
            .collect();

        Ok(record_batch)
    }
}

#[derive(Debug, TypedBuilder)]
pub struct CsvBatchHandlerBuilder {}

impl BatchHandlerBuilder for CsvBatchHandlerBuilder {
    type Handler = CsvBatchHandler;

    fn build(&self, schema: SchemaRef, batch_size: usize) -> Result<Self::Handler> {
        CsvBatchHandler::new(schema, batch_size)
    }
}