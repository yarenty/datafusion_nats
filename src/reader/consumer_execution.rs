use super::execution::Execution;
use super::BatchHandlerBuilder;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, Statistics};
use derivative::Derivative;
use std::any::Any;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Derivative, Clone, TypedBuilder)]
#[derivative(Debug)]
pub struct NatsExecutionPlan<B: BatchHandlerBuilder + std::fmt::Debug + Sync + Send + 'static> {
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
    handler_builder: Arc<B>,

    /// size of batch channel before it blocks
    #[builder(default, setter(strip_option), setter(into))]
    batch_channel_size: Option<usize>,

    #[derivative(Debug = "ignore")]
    client: Arc<async_nats::Client>,

    /// Currently there is a need for periodically poll of NATS consumer
    /// in order to keep connection alive. Thus we keep this watcher reference
    /// to keep the task alive and stop it once we finish with execution
    #[derivative(Debug = "ignore")]
    watcher: Arc<super::table::ClientWatcher>,
}

impl<B: BatchHandlerBuilder + std::fmt::Debug + Sync + Send + 'static> Drop for NatsExecutionPlan<B> {
    fn drop(&mut self) {
        log::debug!("execution plan executed (and dropped)!")
    }
}

#[async_trait]
impl<B: BatchHandlerBuilder + std::fmt::Debug + Sync + Send + 'static> ExecutionPlan for NatsExecutionPlan<B> {
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
        self: Arc<NatsExecutionPlan<B>>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Execution(
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
        let watcher = self.watcher.clone();

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
                watcher,
                batch_stream.tx(),
                handler_builder,
            ).await
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
