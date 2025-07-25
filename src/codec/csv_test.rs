use std::sync::Arc;
use datafusion::arrow::array::{
    Int32Array, Float64Array, BooleanArray, StringArray,
    TimestampNanosecondArray, Date32Array,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef, Field, DataType};
use datafusion::arrow::record_batch::RecordBatch;
use super::CsvCodec;
use chrono::{NaiveDateTime, NaiveDate};

#[test]
fn test_parse_payload_int32() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("42").unwrap();
    assert_eq!(result.len(), 1);
    
    let array = result[0].as_ref();
    assert_eq!(array.len(), 1);
    assert_eq!(array.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 42);
}

#[test]
fn test_parse_payload_empty_schema() {
    let schema = Arc::new(Schema::new(vec![]));
    let result = CsvCodec::new(schema);
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Schema must have at least one field"
    );
}

#[test]
fn test_parse_payload_duplicate_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
    ]));
    let result = CsvCodec::new(schema);
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Duplicate field found: id"
    );
}

#[test]
fn test_parse_payload_empty_field_name() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("", DataType::Int32, false),
    ]));
    let result = CsvCodec::new(schema);
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Field name cannot be empty"
    );
}

#[test]
fn test_parse_payload_unsupported_type() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Binary, false),
    ]));
    let result = CsvCodec::new(schema);
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Unsupported data type: Binary in field id"
    );
}

#[test]
fn test_parse_payload_float64() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Float64, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("3.14159").unwrap();
    assert_eq!(result.len(), 1);
    
    let array = result[0].as_ref();
    assert_eq!(array.len(), 1);
    assert_eq!(array.as_any().downcast_ref::<Float64Array>().unwrap().value(0), 3.14159);
}

#[test]
fn test_parse_payload_boolean() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("active", DataType::Boolean, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("true").unwrap();
    assert_eq!(result.len(), 1);
    
    let array = result[0].as_ref();
    assert_eq!(array.len(), 1);
    assert_eq!(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(0), true);
}

#[test]
fn test_parse_payload_string() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("John Doe").unwrap();
    assert_eq!(result.len(), 1);
    
    let array = result[0].as_ref();
    assert_eq!(array.len(), 1);
    assert_eq!(array.as_any().downcast_ref::<StringArray>().unwrap().value(0), "John Doe");
}

#[test]
fn test_parse_payload_multiple_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("active", DataType::Boolean, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("created_at", DataType::Timestamp(DataType::TimeUnit::Nanosecond, None), false),
        Field::new("date", DataType::Date32, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("42,John Doe,true,3.14159,2025-07-25 15:42:35,2025-07-25").unwrap();
    assert_eq!(result.len(), 6);
    
    let id_array = result[0].as_ref();
    assert_eq!(id_array.as_any().downcast_ref::<Int32Array>().unwrap().value(0), 42);
    
    let name_array = result[1].as_ref();
    assert_eq!(name_array.as_any().downcast_ref::<StringArray>().unwrap().value(0), "John Doe");
    
    let active_array = result[2].as_ref();
    assert_eq!(active_array.as_any().downcast_ref::<BooleanArray>().unwrap().value(0), true);
    
    let amount_array = result[3].as_ref();
    assert_eq!(amount_array.as_any().downcast_ref::<Float64Array>().unwrap().value(0), 3.14159);
    
    let timestamp_array = result[4].as_ref();
    let timestamp = NaiveDateTime::parse_from_str("2025-07-25 15:42:35", "%Y-%m-%d %H:%M:%S").unwrap();
    assert_eq!(
        timestamp_array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(0),
        timestamp.timestamp_nanos()
    );
    
    let date_array = result[5].as_ref();
    let date = NaiveDate::parse_from_str("2025-07-25", "%Y-%m-%d").unwrap();
    assert_eq!(
        date_array.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
        date.num_days_from_ce()
    );
}

#[test]
fn test_parse_payload_empty_field_value() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Invalid value for field id: "
    );
}

#[test]
fn test_parse_payload_invalid_timestamp() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("created_at", DataType::Timestamp(DataType::TimeUnit::Nanosecond, None), false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("invalid_timestamp");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Failed to parse invalid_timestamp as Timestamp for field created_at. Expected format: YYYY-MM-DD HH:MM:SS"
    );
}

#[test]
fn test_parse_payload_invalid_date() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("date", DataType::Date32, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("invalid_date");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Failed to parse invalid_date as Date for field date. Expected format: YYYY-MM-DD"
    );
}

#[test]
fn test_parse_payload_too_many_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("1,extra_field");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Payload has 2 fields but schema expects 1"
    );
}

#[test]
fn test_parse_payload_too_few_fields() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let codec = CsvCodec::new(schema.clone()).unwrap();
    
    let result = codec.parse_payload("1");
    assert!(result.is_err());
    assert_eq!(
        result.err().unwrap().to_string(),
        "Payload has 1 fields but schema expects 2"
    );
}

#[test]
fn test_parse_payload_invalid_type() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));
    let codec = CsvCodec::new(schema.clone());
    
    let result = codec.parse_payload("not_a_number");
    assert!(result.is_err());
}

#[test]
fn test_create_record_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let codec = CsvCodec::new(schema.clone());
    
    let arrays = vec![
        Arc::new(Int32Array::from(vec![42])),
        Arc::new(StringArray::from(vec!["John Doe"])),
    ];
    
    let batch = codec.create_record_batch(arrays).unwrap();
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.schema(), schema.as_ref());
}

#[test]
fn test_create_record_batch_mismatched_lengths() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let codec = CsvCodec::new(schema.clone());
    
    let arrays = vec![
        Arc::new(Int32Array::from(vec![42, 43])),
        Arc::new(StringArray::from(vec!["John Doe"])),
    ];
    
    let result = codec.create_record_batch(arrays);
    assert!(result.is_err());
}
