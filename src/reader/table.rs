use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::prelude::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use async_nats::Client;
use tokio::task::JoinHandle;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::{ExecutionPlan};
use std::any::Any;
use typed_builder::TypedBuilder;
use crate::data_schema::{DataEncoding, DataSchemaEncoding, SchemaLocation, NATS_MESSAGE_SCHEMA};
use crate::reader::batch_handler::BatchHandlerBuilder;
use crate::reader::format::csv::CsvBatchHandlerBuilder;
use crate::reader::format::json::JsonBatchHandlerBuilder;

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
}


/// Used to log dropping of NATS table structure.
impl Drop for NatsTable {
    fn drop(&mut self) {
        log::trace!("dropping NATS table ...")
    }
}

impl NatsTable {


        // subject is used in some cases
        #[allow(unused_variables)]
        async fn resolve_schema_from_encoding(
            encoding_to_schema: &DataSchemaEncoding,
            subject: &str,
        ) -> crate::Result<(SchemaRef, Arc<dyn BatchHandlerBuilder>)> {
            match encoding_to_schema {
                DataSchemaEncoding::Payload(DataEncoding::Json, SchemaLocation::Provided(schema)) => {
                    Ok((schema.clone(), Arc::new(JsonBatchHandlerBuilder::new())))
                }
                DataSchemaEncoding::Payload(DataEncoding::Csv, SchemaLocation::Provided(schema)) => {
                    Ok((schema.clone(), Arc::new(CsvBatchHandlerBuilder::new())))
                }
                DataSchemaEncoding::NatsMessage => {
                    // Default to JSON for NatsMessage if no specific encoding is provided
                    Ok((NATS_MESSAGE_SCHEMA.clone(), Arc::new(JsonBatchHandlerBuilder::new())))
                }
            }
        }


    ///
    /// Create NATS client from a given configuration
    ///
    #[allow(dead_code)]
    pub(crate) async fn setup_nats_client(
        config: &Option<HashMap<String, String>>,
    ) -> DFResult<Client> {
        let config = Self::setup_nats_client_options(config);
        let servers = config.get("servers").cloned().unwrap_or_else(|| "localhost:4222".to_string());
        let client = async_nats::connect(servers).await.map_err(|e| datafusion::error::DataFusionError::Execution(format!("Failed to connect to NATS: {}", e)))?;
        Ok(client)
    }

    pub(crate) fn setup_nats_client_options(
        config: &Option<HashMap<String, String>>,
    ) -> HashMap<String, String> {
        super::config::consumer_config(config)
    }

    pub(crate) fn _supports_filter_pushdown(
        _schema_encoding: &Option<DataSchemaEncoding>,
        filter: &Expr,
    ) -> DFResult<TableProviderFilterPushDown> {
        // NATS does not have native filtering like Kafka, so we won't push down filters for now.
        // All filtering will happen in DataFusion after data is ingested.
        log::debug!("push down filter: [{}] is NOT supported for NATS!", filter);
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}



#[async_trait]
impl TableProvider for crate::reader::NatsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Create the NATS client here
        let client = crate::reader::NatsTable::setup_nats_client(&self.config).await?;

        let (resolved_schema, handler_builder) = crate::reader::NatsTable::resolve_schema_from_encoding(
            &self.schema_encoding.clone().unwrap_or(DataSchemaEncoding::NatsMessage),
            &self.subject,
        ).await.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let exec = crate::reader::consumer_execution::NatsExecutionPlan::new(
            self.subject.clone(),
            resolved_schema,
            None, // Use default batch size from session config
            handler_builder,
            None, // Use default channel size
            Arc::new(client),
        );

        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| {
                crate::reader::NatsTable::_supports_filter_pushdown(
                    &self.schema_encoding,
                    f,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    // Add NATS specific tests here
}
