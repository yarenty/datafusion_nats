use std::sync::Arc;
use async_trait::async_trait;
use datafusion::catalog::{TableProvider, TableProviderFactory};
use datafusion::logical_expr::CreateExternalTable;
use crate::reader::table::NatsTable;

#[derive(Debug, Clone, Default)]
pub struct NatsTableProvider {}

#[async_trait]
impl TableProviderFactory for NatsTableProvider {
    async fn create(
        &self,
        _session: &dyn datafusion::catalog::Session,
        cmd: &CreateExternalTable,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        log::warn!("table provider has limited support. url: {}", cmd.location);

        let table: NatsTable = cmd.try_into()?;
        Ok(Arc::new(table))
    }
}
