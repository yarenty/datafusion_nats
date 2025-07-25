use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use once_cell::sync::Lazy;
use crate::reader::SessionContextExt;

/// Name of payload column
pub const COLUMN_PAYLOAD: &str = "__payload";
/// Name of column which captures event timestamp
pub const COLUMN_EVENT_TIMESTAMP: &str = "__event_timestamp";

pub static NATS_MESSAGE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COLUMN_EVENT_TIMESTAMP, DataType::Timestamp(TimeUnit::Millisecond, None), true),
        Field::new(COLUMN_PAYLOAD, DataType::Binary, true),
    ]))
});

/// A way to capture where is our data coming from
#[derive(Debug, Clone)]
pub enum DataSchemaEncoding {
    /// Data with given schema is located in `Value`
    Payload(DataEncoding, SchemaLocation),
    /// Return back data with default schema [`NATS_MESSAGE_SCHEMA`]
    NatsMessage,
}

impl DataSchemaEncoding {
    pub fn get_schema(&self) -> &SchemaRef {
        match self {
            DataSchemaEncoding::Payload(_, SchemaLocation::Provided(schema)) => schema,
            DataSchemaEncoding::NatsMessage => &NATS_MESSAGE_SCHEMA,
        }
    }
}


/// Supported data encoding formats
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DataEncoding {
    /// JSON Encoding
    Json,
    /// CSV Encoding
    Csv,
}


/// This enum indicates from where we can retrieve schema for table.
#[derive(Debug, Clone)]
pub enum SchemaLocation {
    Provided(SchemaRef),
}



#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_create_schema_projection() {
        let indices = vec![1]; // __payload column
        let result = NATS_MESSAGE_SCHEMA.project(&indices).unwrap();

        assert!(result.field_with_name(COLUMN_PAYLOAD).is_ok());
        assert!(result.field_with_name(COLUMN_EVENT_TIMESTAMP).is_err());
    }
}