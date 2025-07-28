use std::any::Any;
use std::sync::Arc;

use crate::batch_buffer::BatchBuffer;
use crate::codec::csv::CsvCodec;
use crate::executor::{NatsStreamConfig, NatsStreamMetrics, NatsStreamingExec};
use async_nats::Client;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
use futures::stream::StreamExt;

#[derive(Debug)]
pub struct NatsDataSource {
    schema: SchemaRef,
    client: Client,
    subject: String,
    codec: CsvCodec,
    buffer: BatchBuffer,
}

impl NatsDataSource {
    pub fn new(
        schema: SchemaRef,
        client: Client,
        subject: String,
        codec: CsvCodec,
        buffer: BatchBuffer,
    ) -> Self {
        Self {
            schema,
            client,
            subject,
            codec,
            buffer,
        }
    }
}

#[async_trait]
impl TableProvider for NatsDataSource {
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NatsStreamingExec {
            schema: self.schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ),
            client: self.client.clone(),
            subject: self.subject.clone(),
            filters: filters.to_vec(),
            limit,
            codec: self.codec.clone(),
            config: NatsStreamConfig {
                buffer_size: 100,
                batch_size: 10,
                batch_timeout: std::time::Duration::from_millis(100),
                max_concurrent_messages: 100,
                enable_backpressure: true,
            },
            metrics: Arc::new(NatsStreamMetrics::default()),
            // buffer: Arc::new(Mutex::new(self.buffer.clone())), //self.buffer.clone()
        };
        Ok(Arc::new(exec))
    }
}
