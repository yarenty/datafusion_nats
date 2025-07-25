use std::collections::HashMap;
use datafusion::arrow::datatypes::SchemaRef;
use crate::data_schema::{DataEncoding, DataSchemaEncoding, SchemaLocation};

pub struct TableProperties {
    pub(crate) nats_options: Option<HashMap<String, String>>,
    pub(crate) data_schema_encoding: DataSchemaEncoding,
}

pub struct TablePropertiesBuilder {
    nats_options: Option<HashMap<String, String>>,
    data_schema_encoding: DataSchemaEncoding,
}

impl Default for TablePropertiesBuilder {
    fn default() -> Self {
        Self {
            nats_options: Default::default(),
            data_schema_encoding: DataSchemaEncoding::NatsMessage,
        }
    }
}

impl TablePropertiesBuilder {
    /// Initialize TablePropertiesBuilder with already prepared nats_options
    pub fn from_nats_options(nats_options: HashMap<String, String>) -> Self {
        Self {
            nats_options: Some(nats_options),
            data_schema_encoding: DataSchemaEncoding::NatsMessage,
        }
    }

    /// Pass nats_option to save
    pub fn nats_option(mut self, option: &str, value: &str) -> Self {
        if let Some(nats_options) = &mut self.nats_options {
            nats_options.insert(option.to_string(), value.to_string());
        } else {
            let mut nats_options = HashMap::new();
            nats_options.insert(option.to_string(), value.to_string());
            self.nats_options = Some(nats_options);
        }

        self
    }

    /// Sets up schema with data encoding
    pub fn data_schema_encoding(mut self, data_schema_encoding: DataSchemaEncoding) -> Self {
        self.data_schema_encoding = data_schema_encoding;
        self
    }

    /// Builds a final TableProperties
    pub fn build(self) -> TableProperties {
        TableProperties {
            data_schema_encoding: self.data_schema_encoding,
            nats_options: self.nats_options,
        }
    }
}

impl TableProperties {
    /// NATS table config builder
    pub fn builder() -> TablePropertiesBuilder {
        TablePropertiesBuilder::default()
    }
    /// Create default nats table config
    pub fn new() -> Self {
        TableProperties::builder().build()
    }

    /// Create default nats table from json payload
    pub fn json_table(schema: SchemaRef) -> Self {
        TableProperties::builder()
            .data_schema_encoding(DataSchemaEncoding::Payload(
                DataEncoding::Json,
                SchemaLocation::Provided(schema),
            ))
            .build()
    }
}

impl Default for TableProperties {
    fn default() -> Self {
        Self::new()
    }
}