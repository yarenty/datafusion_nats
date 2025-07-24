use crate::{DataFusionNatsError, Result};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        record_batch::RecordBatch,
    },
    datasource::{provider_as_source, TableProvider},
    execution::context::SessionState,
    logical_expr::{CreateExternalTable, LogicalPlanBuilder, UNNAMED_TABLE},
};
use datafusion::{
    error::DataFusionError,
    prelude::{DataFrame, SessionContext},
};
use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Arc};
use datafusion::catalog::TableProviderFactory;
use typed_builder::TypedBuilder;

pub use config::CreateExternalNatsTable;
use crate::reader::format::csv::CsvBatchHandlerBuilder;
use crate::reader::format::json::JsonBatchHandlerBuilder;

mod config;
pub mod format;
pub mod table;
pub mod execution;
pub mod consumer_execution;

pub trait BatchHandler {
    fn append_message(&mut self, message: &async_nats::Message) -> Result<()>;
    fn has_capacity(&self) -> bool;
    fn finish(&mut self) -> Result<RecordBatch>;
}

pub trait BatchHandlerBuilder {
    type Handler: BatchHandler + Send;
    fn build(&self, schema: SchemaRef, batch_size: usize) -> Result<Self::Handler>;
}

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

#[async_trait::async_trait]
pub trait SessionContextExt {
    async fn read_nats(&self, subject_name: &str, nats_props: TableProperties) -> Result<DataFrame>;
    async fn register_nats(
        &self,
        subject_name: &str,
        table_name: &str,
        nats_props: TableProperties,
    ) -> std::result::Result<(), DataFusionError>;
}

#[async_trait::async_trait]
impl SessionContextExt for SessionContext {
    async fn register_nats(
        &self,
        subject_name: &str,
        table_name: &str,
        nats_props: TableProperties,
    ) -> std::result::Result<(), DataFusionError> {
        let (schema, handler_builder) = NatsTable::resolve_schema_from_encoding(&nats_props.data_schema_encoding, subject_name).await?;

        let table_builder = NatsTable::builder()
            .subject(subject_name.to_string())
            .config(nats_props.nats_options)
            .schema(schema)
            .schema_encoding(nats_props.data_schema_encoding)
            .handler_builder(handler_builder);

        let table = table_builder.build();

        self.register_table(table_name, Arc::new(table))?;

        Ok(())
    }

    async fn read_nats(&self, subject_name: &str, nats_props: TableProperties) -> Result<DataFrame> {
        let (schema, handler_builder) = NatsTable::resolve_schema_from_encoding(&nats_props.data_schema_encoding, subject_name).await?;

        let table_builder = NatsTable::builder()
            .subject(subject_name.to_string())
            .config(nats_props.nats_options)
            .schema(schema)
            .schema_encoding(nats_props.data_schema_encoding)
            .handler_builder(handler_builder);

        let table = table_builder.build();

        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(table)), None)?.build()?,
        ))
    }
}



#[derive(TypedBuilder, Debug, Clone)]
pub struct NatsTable {
    /// Subject to read messages from
    subject: String,
    /// Effective table schema
    schema: SchemaRef,

    /// NATS related configuration
    #[builder(default, setter(into))]
    config: Option<HashMap<String, String>>,

    /// data encoding used for this table
    #[builder(default, setter(strip_option), setter(into))]
    schema_encoding: Option<DataSchemaEncoding>,

    /// Batch handler builder for this table
    #[builder(setter(into))]
    handler_builder: Arc<dyn BatchHandlerBuilder<Handler = dyn BatchHandler>>,
}

impl NatsTable{
    // subject is used in some cases
    #[allow(unused_variables)]
    async fn resolve_schema_from_encoding(
        encoding_to_schema: &DataSchemaEncoding,
        subject: &str,
    ) -> Result<(SchemaRef, Arc<dyn BatchHandlerBuilder>)> {
        match encoding_to_schema {
            DataSchemaEncoding::Payload(ReadEncoding::Json, SchemaLocation::Provided(schema)) => {
                Ok((schema.clone(), Arc::new(JsonBatchHandlerBuilder::new())))
            }
            DataSchemaEncoding::Payload(ReadEncoding::Csv, SchemaLocation::Provided(schema)) => {
                Ok((schema.clone(), Arc::new(CsvBatchHandlerBuilder::new())))
            }
            DataSchemaEncoding::NatsMessage => {
                // Default to JSON for NatsMessage if no specific encoding is provided
                Ok((NATS_MESSAGE_SCHEMA.clone(), Arc::new(JsonBatchHandlerBuilder::new())))
            }
        }
    }
}

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
                ReadEncoding::Json,
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

/// A way to capture where is our data coming from
#[derive(Debug, Clone)]
pub enum DataSchemaEncoding {
    /// Data with given schema is located in `Value`
    Payload(ReadEncoding, SchemaLocation),
    /// Return back data with default schema [`NATS_MESSAGE_SCHEMA`]
    NatsMessage,
}

/// A way to capture where is our data is going to
#[derive(Debug, Clone)]
pub enum DataSchemaWriteEncoding {
    /// Data with given schema is located in `Value`
    Payload(WriteEncoding),
    /// Return back data with default schema [`NATS_MESSAGE_SCHEMA`]
    NatsMessage,
}

/// Supported data encoding formats
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ReadEncoding {
    /// JSON Encoding
    Json,
    /// CSV Encoding
    Csv,
}

/// Supported data encoding formats for writing
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WriteEncoding {
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

impl From<DataFusionNatsError> for DataFusionError {
    fn from(error: DataFusionNatsError) -> Self {
        match error {
            DataFusionNatsError::Arrow(e) => DataFusionError::ArrowError(e, None),
            DataFusionNatsError::General(s) => DataFusionError::Execution(s),
            DataFusionNatsError::DataFusion(e) => e,
            DataFusionNatsError::Format(s) => DataFusionError::Execution(s),
        }
    }
}

// TODO: Add NATS specific error conversion here
// impl From<NatsError> for DataFusionNatsError {
//     fn from(err: NatsError) -> Self {
//         Self::DataFusion(DataFusionError::Execution(err.to_string()))
//     }
// }

/// An EXPERIMENTAL example how to use table provider to declarativly register new tables.
#[derive(Debug)]
pub struct NatsTableProvider {}

impl NatsTableProvider {
    fn new() -> Self {
        Self {}
    }
}

impl Default for NatsTableProvider {
    fn default() -> Self {
        NatsTableProvider::new()
    }
}

#[async_trait]
impl TableProviderFactory for NatsTableProvider {
    async fn create(
        &self,
        _state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        log::warn!("table provider has limited support. url: {}", cmd.location);

        let table: NatsTable = cmd.try_into()?;
        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_session_context_interface() {
        let session = SessionContext::new();
        session
            .register_nats("test_subject", "test_table", TableProperties::new())
            .await
            .unwrap();
    }

    #[test]
    fn test_config_builder() {
        let nats_table_props = TableProperties::builder()
            .nats_option("servers", "localhost:4222")
            .data_schema_encoding(DataSchemaEncoding::Payload(
                ReadEncoding::Json,
                SchemaLocation::Provided(NATS_MESSAGE_SCHEMA.clone()),
            ))
            .build();

        assert_eq!(1, nats_table_props.nats_options.unwrap().len());
    }

    #[test]
    fn test_create_schema_projection() {
        let indices = vec![1]; // __payload column
        let result = NATS_MESSAGE_SCHEMA.project(&indices).unwrap();

        assert!(result.field_with_name(COLUMN_PAYLOAD).is_ok());
        assert!(result.field_with_name(COLUMN_EVENT_TIMESTAMP).is_err());
    }
}