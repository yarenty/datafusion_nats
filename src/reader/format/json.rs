use crate::Result;
use async_nats::Message;
use datafusion::arrow::array::{ArrayRef, BinaryBuilder, TimestampMillisecondBuilder, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{SchemaRef, DataType};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::SystemTime;
use crate::data_schema::COLUMN_PAYLOAD;
use crate::reader::batch_handler::{BatchHandler, BatchHandlerBuilder};

fn get_value_as_json(array: &ArrayRef, row_idx: usize, field_name: &str) -> Result<Value> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Boolean => Ok(json!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_idx))),
        DataType::Int8 => Ok(json!(array.as_any().downcast_ref::<Int8Array>().unwrap().value(row_idx))),
        DataType::Int16 => Ok(json!(array.as_any().downcast_ref::<Int16Array>().unwrap().value(row_idx))),
        DataType::Int32 => Ok(json!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx))),
        DataType::Int64 => Ok(json!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row_idx))),
        DataType::UInt8 => Ok(json!(array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row_idx))),
        DataType::UInt16 => Ok(json!(array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row_idx))),
        DataType::UInt32 => Ok(json!(array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row_idx))),
        DataType::UInt64 => Ok(json!(array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row_idx))),
        DataType::Float32 => Ok(json!(array.as_any().downcast_ref::<Float32Array>().unwrap().value(row_idx))),
        DataType::Float64 => Ok(json!(array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx))),
        DataType::Timestamp(_, _) => Ok(json!(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row_idx))),
        DataType::Utf8 => Ok(json!(array.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx))),
        DataType::Binary => {
            let binary_array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            let bytes = binary_array.value(row_idx);
            // If it's the payload column, assume it's JSON string
            if field_name == COLUMN_PAYLOAD {
                let s = std::str::from_utf8(bytes)
                    .map_err(|e| crate::DatafusionNatsError::Format(format!("Invalid UTF-8 in binary payload column: {}", e)))?;
                serde_json::from_str(s)
                    .map_err(|e| crate::DatafusionNatsError::Format(format!("Failed to parse JSON from binary payload column: {}", e)))
            } else {
                // For other binary columns, just return the bytes as a string (e.g., base64 or hex)
                Ok(json!(format!("{:?}", bytes)))
            }
        }
        _ => Err(crate::DatafusionNatsError::Format(format!(
            "Unsupported data type for JSON serialization: {:?}",
            array.data_type()
        ))),
    }
}

pub struct JsonBatchHandler {
    schema: SchemaRef,
    binary_builder: BinaryBuilder,
    timestamp_builder: TimestampMillisecondBuilder,
    batch_size: usize,
    row_count: usize,
}

impl JsonBatchHandler {
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

impl BatchHandler for JsonBatchHandler {
    fn append_message(&mut self, message: &Message) -> Result<()> {
        let payload = String::from_utf8(message.payload.to_vec())?;
        let json: Value = serde_json::from_str(&payload)?;

        self.binary_builder.append_value(json.to_string().as_bytes());
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
pub struct JsonBatchHandlerBuilder {}

impl JsonBatchHandlerBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BatchHandlerBuilder for JsonBatchHandlerBuilder {
    type Handler = JsonBatchHandler;

    fn build(&self, schema: SchemaRef, batch_size: usize) -> Result<JsonBatchHandler> {
        Ok(JsonBatchHandler::new( schema, batch_size))
    }
}

pub struct JsonWriterFormat {}

impl JsonWriterFormat {
    pub fn serialize_record_batch(batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        let mut messages = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut json_map = serde_json::Map::new();
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let s =  batch.schema();
                let field = s.field(col_idx);
                
                // Assuming the payload column is Utf8 and contains JSON
                if let Some(string_array) = column.as_any().downcast_ref::<datafusion::arrow::array::StringArray>() {
                    let value_str = string_array.value(row_idx);
                    let value: Value = serde_json::from_str(value_str)?;
                    json_map.insert(field.name().clone(), value);
                } else {
                    return Err(crate::DatafusionNatsError::Format(format!("Unsupported column type for JSON serialization: {:?}", field.data_type())));
                }
            }
            messages.push(serde_json::to_vec(&serde_json::Value::Object(json_map))?);
        }
        Ok(messages)
    }
}