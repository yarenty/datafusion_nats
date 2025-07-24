// Main module for NATS DataFusion writer components
pub mod producer;
pub mod format;
pub mod table_sink;

use crate::{DataFusionNatsError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;

pub trait BatchProducer {
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

#[async_trait]
pub trait BatchProducerBuilder: Send + Sync {
    type Producer: BatchProducer + Send;
    async fn build(&self) -> Result<Self::Producer>;
}
