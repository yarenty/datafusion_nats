use crate::reader::{DataSchemaEncoding, ReadEncoding, Result};
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
use datafusion::physical_plan::{ExecutionPlan, PlanProperties, Partitioning};
use datafusion::physical_expr::EquivalenceProperties;
use std::any::Any;


/// Used to log dropping of NATS table structure.
impl Drop for crate::reader::NatsTable {
    fn drop(&mut self) {
        log::trace!("dropping NATS table ...")
    }
}

impl crate::reader::NatsTable {
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
        schema_encoding: &Option<DataSchemaEncoding>,
        filter: &Expr,
    ) -> DFResult<TableProviderFilterPushDown> {
        // NATS does not have native filtering like Kafka, so we won't push down filters for now.
        // All filtering will happen in DataFusion after data is ingested.
        log::debug!("push down filter: [{}] is NOT supported for NATS!", filter);
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}

/// Currently there is a need for periodically poll of NATS consumer
/// in order to keep connection alive. Thus we keep this watcher reference
/// to keep the task alive and stop it once we finish with execution
///
/// Client watcher takes care of watching NATS client
/// as, it has to be periodically polled to serve notifications.
pub struct ClientWatcher {
    handle: JoinHandle<()>,
}

impl ClientWatcher {
    pub(crate) fn watch(client: Arc<async_nats::Client>) -> Self {
        let handle = tokio::spawn(async move {
            log::debug!("starting NATS client watcher task ...");
            // NATS client handles its own background polling, so we just keep the client alive.
            // In a real scenario, you might want to subscribe to a dummy subject or ping the server
            // to ensure the connection stays active if there's no other activity.
            // For now, simply holding the Arc<Client> is sufficient to keep the connection alive.
            // The client will automatically reconnect if the connection is lost.
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                // You could add a ping here if needed:
                // if let Err(e) = client.flush().await {
                //     log::warn!("NATS client flush failed: {}", e);
                // }
            }
        });

        ClientWatcher { handle }
    }
}

/// Used to drop client watcher once there is no more active executors using it
///
impl Drop for ClientWatcher {
    fn drop(&mut self) {
        log::trace!("Dropping NATS client watcher task ...");
        self.handle.abort();
        log::debug!("Dropped NATS client watcher task ... DONE!");
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
        let watcher = crate::reader::table::ClientWatcher::watch(Arc::new(client.clone()));

        let exec = crate::reader::consumer_execution::NatsExecutionPlan::builder()
            .subject(self.subject.clone())
            .schema(self.schema.clone())
            .batch_size(None) // Use default batch size from session config
            .handler_builder(self.handler_builder.clone())
            .batch_channel_size(None) // Use default channel size
            .client(Arc::new(client))
            .watcher(Arc::new(watcher))
            .build();

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
