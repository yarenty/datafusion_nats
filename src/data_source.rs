use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, SendableRecordBatchStream, stream::RecordBatchStreamAdapter, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_expr::EquivalenceProperties;
use futures::stream;
use async_nats::Client;

#[derive(Debug)]
pub struct NatsDataSource {
    schema: SchemaRef,
    client: Client,
}

impl NatsDataSource {
    pub fn new(schema: SchemaRef, client: Client) -> Self {
        Self { schema, client }
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
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NatsExec {
            schema: self.schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(self.schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Unbounded { requires_infinite_memory: false }),
            client: self.client.clone(),
        };
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct NatsExec {
    schema: SchemaRef,
    properties: PlanProperties,
    client: Client,
}

impl DisplayAs for NatsExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "NatsExec")
            }
            DisplayFormatType::TreeRender => todo!()
        }
    }
}

impl ExecutionPlan for NatsExec {
    fn name(&self) -> &str {
        "NatsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let stream = stream::empty();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}