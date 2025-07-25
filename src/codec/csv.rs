use std::sync::Arc;
use datafusion::arrow::array::{Array, StringBuilder, Int32Array, Float64Array, BooleanArray, TimestampNanosecondArray, Date32Array, RecordBatch};
use datafusion::arrow::datatypes::{Schema, SchemaRef, Field, DataType};
use datafusion::error::DataFusionError;
use thiserror::Error;
use chrono::{NaiveDateTime, NaiveDate};
use chrono::Datelike;
use std::convert::TryFrom;

#[derive(Error, Debug)]
pub enum CsvCodecError {
    #[error("Invalid CSV format: {0}")]
    InvalidFormat(String),

    #[error("Type conversion error: {0}")]
    TypeError(String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Invalid value for field {field}: {value}")]
    InvalidValue {
        field: String,
        value: String,
    },

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Duplicate field found: {0}")]
    DuplicateField(String),

    #[error("Field {field} has invalid type: {expected}, got {actual}")]
    InvalidFieldType {
        field: String,
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone)]
pub struct CsvCodec {
    schema: SchemaRef,
    field_names: Vec<String>,
}

impl CsvCodec {
    pub fn new(schema: SchemaRef) -> Result<Self, CsvCodecError> {
        // Validate schema
        let fields = schema.fields();

        // Check for empty schema
        if fields.is_empty() {
            return Err(CsvCodecError::SchemaMismatch("Schema must have at least one field".to_string()));
        }

        // Check for duplicate field names
        let mut field_names = Vec::with_capacity(fields.len());
        let mut name_set = std::collections::HashSet::new();

        for field in fields {
            let name = field.name();

            // Check for empty field name
            if name.is_empty() {
                return Err(CsvCodecError::SchemaMismatch("Field name cannot be empty".to_string()));
            }

            // Check for duplicate field name
            if !name_set.insert(name) {
                return Err(CsvCodecError::DuplicateField(name.to_string()));
            }

            // Check for unsupported data types
            match field.data_type() {
                DataType::Int32 | DataType::Float64 | DataType::Boolean | DataType::Utf8
                | DataType::Timestamp(_, _) | DataType::Date32 => {},
                other => {
                    return Err(CsvCodecError::TypeError(format!(
                        "Unsupported data type: {} in field {}",
                        other,
                        name
                    )));
                }
            }

            field_names.push(name.to_string());
        }

        Ok(Self { schema, field_names })
    }

    pub fn parse_payload(&self, payload: &str) -> Result<Vec<Arc<dyn Array>>, CsvCodecError> {
        let parts: Vec<&str> = payload.split(',').collect();

        // Check for empty payload
        if parts.is_empty() {
            return Err(CsvCodecError::SchemaMismatch("Payload cannot be empty".to_string()));
        }

        // Check if payload has correct number of fields
        if parts.len() != self.schema.fields().len() {
            return Err(CsvCodecError::SchemaMismatch(format!(
                "Payload has {} fields but schema expects {}",
                parts.len(),
                self.schema.fields().len()
            )));
        }

        let mut arrays = Vec::with_capacity(self.schema.fields().len());

        for (i, field) in self.schema.fields().iter().enumerate() {
            let value = parts[i];

            // Check for empty field value
            if value.is_empty() {
                return Err(CsvCodecError::InvalidValue {
                    field: field.name().to_string(),
                    value: value.to_string(),
                });
            }

            match field.data_type() {
                DataType::Int32 => {
                    match value.parse::<i32>() {
                        Ok(num) => arrays.push(Arc::new(Int32Array::from(vec![num])) as Arc<dyn Array>),
                        Err(_) => return Err(CsvCodecError::TypeError(format!(
                            "Failed to parse {} as Int32 for field {}",
                            value,
                            field.name()
                        ))),
                    }
                }
                DataType::Float64 => {
                    match value.parse::<f64>() {
                        Ok(num) => arrays.push(Arc::new(Float64Array::from(vec![num])) as Arc<dyn Array>),
                        Err(_) => return Err(CsvCodecError::TypeError(format!(
                            "Failed to parse {} as Float64 for field {}",
                            value,
                            field.name()
                        ))),
                    }
                }
                DataType::Boolean => {
                    match value.to_lowercase().as_str() {
                        "true" | "1" => arrays.push(Arc::new(BooleanArray::from(vec![true]))as Arc<dyn Array>),
                        "false" | "0" => arrays.push(Arc::new(BooleanArray::from(vec![false]))as Arc<dyn Array>),
                        _ => return Err(CsvCodecError::TypeError(format!(
                            "Failed to parse {} as Boolean for field {}. Expected 'true', 'false', '1', or '0'",
                            value,
                            field.name()
                        ))),
                    }
                }
                DataType::Utf8 => {
                    // For Utf8, we just need to ensure the value is not empty
                    let mut builder = StringBuilder::new();
                    builder.append_value(value);
                    arrays.push(Arc::new(builder.finish()) as Arc<dyn Array>);
                }
                DataType::Timestamp(_, _) => {
                    match NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                        Ok(dt) => {
                            let ts = dt.and_utc().timestamp_nanos_opt().unwrap();
                            arrays.push(Arc::new(TimestampNanosecondArray::from(vec![ts])) as Arc<dyn Array>);
                        }
                        Err(_) => return Err(CsvCodecError::TypeError(format!(
                            "Failed to parse {} as Timestamp for field {}. Expected format: YYYY-MM-DD HH:MM:SS",
                            value,
                            field.name()
                        ))),
                    }
                }
                DataType::Date32 => {
                    match NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                        Ok(date) => {
                            let days = date.num_days_from_ce();
                            arrays.push(Arc::new(Date32Array::from(vec![days]))as Arc<dyn Array>);
                        }
                        Err(_) => return Err(CsvCodecError::TypeError(format!(
                            "Failed to parse {} as Date for field {}. Expected format: YYYY-MM-DD",
                            value,
                            field.name()
                        ))),
                    }
                }
                other => {
                    return Err(CsvCodecError::TypeError(format!(
                        "Unsupported data type: {} for field {}",
                        other,
                        field.name()
                    )));
                }
            }
        }

        Ok(arrays)
    }

    pub fn create_record_batch(
        &self,
        arrays: Vec<Arc<dyn Array>>,
    ) -> Result<RecordBatch, CsvCodecError> {
        // Validate array lengths match
        let first_len = arrays.first().map(|a| a.len()).unwrap_or(0);
        for array in &arrays[1..] {
            if array.len() != first_len {
                return Err(CsvCodecError::InvalidFormat("Array lengths don't match".to_string()));
            }
        }
        
       RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| CsvCodecError::InvalidFormat(e.to_string()))
    }
}
