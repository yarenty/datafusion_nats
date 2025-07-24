use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableSink;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::sync::Arc;
use crate::Result;
use crate::writer::{BatchProducer, BatchProducerBuilder};

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
impl<B: BatchProducerBuilder + 'static> TableSink for NatsTableSink<B> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn write_all(&self, stream: SendableRecordBatchStream) -> DFResult<u64> {
        let mut producer = self.producer_builder.build().await?;
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
