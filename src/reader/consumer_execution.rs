use crate::reader::execution::Execution;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream, Statistics};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use derivative::Derivative;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::common::DataFusionError;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use typed_builder::TypedBuilder;
use crate::reader::batch_handler::BatchHandlerBuilder;
use crate::Result;

#[derive(Derivative, Clone, TypedBuilder, Debug)]
#[derivative(Debug)]
pub struct NatsExecutionPlan {
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
    /// Subject to read messages from
    #[builder(default, setter(into))]
    subject: String,

    /// given schema reference for this execution plan
    schema: SchemaRef,

    /// overridden batch size for this query
    #[builder(default, setter(into))]
    batch_size: Option<usize>,

    /// Given handler builder
    #[derivative(Debug = "ignore")]
    handler_builder: dyn BatchHandlerBuilder,

    /// size of batch channel before it blocks
    #[builder(default, setter(strip_option), setter(into))]
    batch_channel_size: Option<usize>,

    #[derivative(Debug = "ignore")]
    client: Arc<async_nats::Client>,

}

impl NatsExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subject: String,
        schema: SchemaRef,
        batch_size: Option<usize>,
        handler_builder: Arc<dyn BatchHandlerBuilder>,
        batch_channel_size: Option<usize>,
        client: Arc<async_nats::Client>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::RoundRobinBatch(batch_size.unwrap_or(1)),
            EmissionType::Incremental,
            Boundedness::Bounded
        );
        let metrics = ExecutionPlanMetricsSet::new();

        Self {
            properties,
            metrics,
            subject,
            schema,
            batch_size,
            handler_builder,
            batch_channel_size,
            client
        }
    }
}

impl Drop for NatsExecutionPlan {
    fn drop(&mut self) {
        log::debug!("execution plan executed (and dropped)!")
    }
}

impl DisplayAs for NatsExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}


#[async_trait]
impl ExecutionPlan for NatsExecutionPlan {
    fn name(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }


    fn children(&self) ->  Vec<&Arc<(dyn ExecutionPlan + 'static)>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<NatsExecutionPlan>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(crate::DatafusionNatsError::General(
                "NatsExecutionPlan only supports a single partition (0).".to_string(),
            ));
        }

        let subject = self.subject.clone();

        let batch_size = self.batch_size.unwrap_or_else(|| context.session_config().batch_size());

        let schema = ExecutionPlan::schema(self);
        let mut batch_stream =
            RecordBatchReceiverStream::builder(self.schema.clone(), self.batch_channel_size.unwrap_or(4));

        let client = self.client.clone();
        let handler_builder = self.handler_builder.clone();

        log::debug!(
            "execution handle for subject: [{}], (session_id: [{}], task_id: [{:?}])",
            &subject,
            context.session_id(),
            context.task_id()
        );

        let message_processor_task = async move {
            let subscriber = client.subscribe(subject.clone()).await.map_err(|e| datafusion::error::DataFusionError::Execution(format!("Failed to subscribe to NATS subject {}: {}", subject, e)))?;
            Execution::message_processor_task(
                subject,
                batch_size,
                schema,
                subscriber,
                <tokio::sync::mpsc::Sender<std::result::Result<datafusion::arrow::array::RecordBatch, DataFusionError>> as Into<T>>::into(batch_stream.tx()).clone(),
                handler_builder,
            ).await.map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
        };

        batch_stream.spawn(message_processor_task);

        Ok(batch_stream.build())
    }

    fn statistics(&self) -> Result<Statistics> {
        // NATS does not provide easy access to total message count without consuming
        // For now, return unknown statistics.
        Ok(Statistics::new_unknown(&self.schema))
    }
}

impl<B: BatchHandlerBuilder + std::fmt::Debug + Sync + Send + 'static> DisplayAs for NatsExecutionPlan<B> {
    fn fmt_as(&self, t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(
                    f,
                    "NatsConsumerExec: subject: [{}]",
                    self.subject
                )
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "NatsConsumerExec: subject: [{}]",
                    self.subject
                )
            }
            datafusion::physical_plan::DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "NatsConsumerExec: subject: [{}]",
                    self.subject
                )
            }
        }
    }
}
