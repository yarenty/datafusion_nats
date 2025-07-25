use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::{SendableRecordBatchStream, Distribution, DisplayAs, DisplayFormatType};
use std::sync::Arc;
use crate::writer::{BatchProducer, BatchProducerBuilder};
use futures::future::BoxFuture;
use futures::FutureExt;
use datafusion::execution::context::TaskContext;
use futures::StreamExt;

#[derive(Debug)]
pub struct NatsTableSink<B: BatchProducerBuilder + 'static> {
    subject: String,
    producer_builder: Arc<B>,
    schema: SchemaRef,
}

impl<B: BatchProducerBuilder + 'static> NatsTableSink<B> {
    pub fn new(subject: String, producer_builder: Arc<B>, schema: SchemaRef) -> Self {
        Self { subject, producer_builder, schema }
    }
}

#[async_trait]
impl<B: BatchProducerBuilder + 'static + std::fmt::Debug + Send + Sync> DataSink for NatsTableSink<B> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn required_input_distribution(&self) -> Distribution {
        // For now, we'll assume a single partition for simplicity.
        // You might need to adjust this based on your NATS consumer strategy.
        Distribution::SinglePartition
    }

    async fn write_all(&self, stream: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let producer_builder = self.producer_builder.clone();

        let mut producer = producer_builder.build().await?;
        let mut total_rows = 0;

        tokio::pin!(stream);

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            producer.append_batch(&batch)?;
        }

        producer.flush()?;

        Ok(total_rows)
    }
}

impl<B: BatchProducerBuilder + 'static> DisplayAs for NatsTableSink<B> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NatsTableSink: subject={}", self.subject)
    }
}