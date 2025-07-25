use crate::Result;
use datafusion::{
    datasource::{provider_as_source, TableProvider},
    logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE},
};
use datafusion::{
    error::DataFusionError,
    prelude::{DataFrame, SessionContext},
};
use std::{ sync::Arc};
use datafusion::catalog::TableProviderFactory;
pub use config::CreateExternalNatsTable;
use crate::reader::table::NatsTable;
use crate::reader::table_properties::TableProperties;

mod config;
pub mod format;
pub mod table;
pub mod execution;
pub mod consumer_execution;
mod table_properties;
mod table_provider;
mod batch_handler;



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
        let table_builder = NatsTable::builder()
            .subject(subject_name.to_string())
            .config(nats_props.nats_options)
            .schema(nats_props.data_schema_encoding.get_schema().clone())
            .schema_encoding(nats_props.data_schema_encoding);

        let table = table_builder.build();

        self.register_table(table_name, Arc::new(table))?;

        Ok(())
    }

    async fn read_nats(&self, subject_name: &str, nats_props: TableProperties) -> Result<DataFrame> {
        let table_builder = NatsTable::builder()
            .subject(subject_name.to_string())
            .config(nats_props.nats_options)
            .schema(nats_props.data_schema_encoding.get_schema().clone())
            .schema_encoding(nats_props.data_schema_encoding);

        let table = table_builder.build();

        Ok(DataFrame::new(
            self.state(),
            LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(table)), None)?.build()?,
        ))
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