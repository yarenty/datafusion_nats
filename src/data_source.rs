use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{Result, DataFusionError};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{ExecutionPlan, DisplayAs, DisplayFormatType, SendableRecordBatchStream, stream::RecordBatchStreamAdapter, PlanProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_expr::EquivalenceProperties;
use futures::stream::StreamExt;
use async_nats::Client;
use datafusion::arrow::array::{Int32Array, ArrayBuilder, StringBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct NatsDataSource {
    schema: SchemaRef,
    client: Client,
    subject: String,
}

impl NatsDataSource {
    pub fn new(schema: SchemaRef, client: Client, subject: String) -> Self {
        Self { schema, client, subject }
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
            subject: self.subject.clone(),
            filters: Arc::new(filters.to_vec()),
        };
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
struct NatsExec {
    schema: SchemaRef,
    properties: PlanProperties,
    client: Client,
    subject: String,
    filters: Arc<Vec<Expr>>,
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
        let schema_for_spawn = self.schema.clone();
        let client = self.client.clone();
        let subject = self.subject.clone();
        let filters_for_spawn = self.filters.clone();

        // Create a channel to send RecordBatches from the NATS consumer task
        // to the DataFusion execution stream.
        let (sender, receiver) = mpsc::channel(1024);

        // Spawn a new asynchronous task to consume messages from NATS.
        // This task runs concurrently with the DataFusion query execution.
        tokio::spawn(async move {
            // Subscribe to the NATS subject.
            let mut subscriber = match client.subscribe(subject).await {
                Ok(sub) => {
                    tracing::info!("NATS subscriber created for subject: {}", subject);
                    sub
                },
                Err(e) => {
                    let error_msg = format!("Failed to subscribe to NATS: {}", e);
                    tracing::error!("{}", error_msg);
                    let _ = sender.send(Err(DataFusionError::Execution(error_msg))).await;
                    return;
                }
            };

            // Initialize Arrow array builders for efficient data buffering.
            // Using Arrow's native builders is generally more performant than
            // collecting into Vecs and then converting, as they are optimized
            // for memory layout and appending.
            let mut id_builder = Int32Array::builder(1024);
            let mut name_builder = StringBuilder::with_capacity(1024, 1024 * 10);

            // Continuously receive messages from the NATS subscription.
            while let Some(message) = subscriber.next().await {
                tracing::debug!("Received NATS message: {:?}", message);
                // Parse the payload. Assuming a simple CSV-like format: "id,name"
                let payload = String::from_utf8_lossy(&message.payload);
                let parts: Vec<&str> = payload.split(',').collect();

                if parts.len() == 2 {
                    if let Ok(id) = parts[0].parse::<i32>() {
                        // Apply filters. For simplicity, assuming filters are on the 'id' column.
                        let mut should_include = true;
                        for filter_expr in filters_for_spawn.iter() {
                            // This is a very basic example. In a real scenario, you would need
                            // a more robust expression evaluation engine.
                            // For now, we assume a filter like `id = 10`
                            if let Expr::BinaryExpr(binary_expr) = filter_expr {
                                if let (Expr::Column(col), Expr::Literal(scalar, _)) = (&*binary_expr.left, &*binary_expr.right) {
                                    if col.name == "id" {
                                        if let datafusion::scalar::ScalarValue::Int32(Some(filter_id)) = scalar {
                                            if id != *filter_id {
                                                should_include = false;
                                                tracing::debug!("Message filtered out by ID: {}", id);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if should_include {
                            // Append parsed values to the builders.
                            id_builder.append_value(id);
                            name_builder.append_value(parts[1]);
                            tracing::debug!("Message included: id={}, name={}", id, parts[1]);
                        } else {
                            tracing::info!("Filtered out message with id: {}", id);
                        }
                    } else {
                        let error_msg = format!("Failed to parse id: {}", parts[0]);
                        tracing::error!("{}", error_msg);
                        let _ = sender.send(Err(DataFusionError::Execution(error_msg))).await;
                    }
                } else {
                    let error_msg = format!("Invalid message format: {}", payload);
                    tracing::error!("{}", error_msg);
                    let _ = sender.send(Err(DataFusionError::Execution(error_msg))).await;
                }

                // If enough records are buffered, build a RecordBatch and send it.
                // This flushes data in chunks to reduce overhead.
                if id_builder.len() >= 1000 { 
                    let id_array = id_builder.finish();
                    let name_array = name_builder.finish();
                    match RecordBatch::try_new(schema_for_spawn.clone(), vec![Arc::new(id_array), Arc::new(name_array)]) {
                        Ok(record_batch) => {
                            tracing::debug!("Sending record batch with {} rows.", record_batch.num_rows());
                            if let Err(e) = sender.send(Ok(record_batch)).await {
                                tracing::error!("Failed to send record batch: {}", e);
                            }
                        },
                        Err(e) => {
                            let error_msg = format!("Failed to create record batch: {}", e);
                            tracing::error!("{}", error_msg);
                            let _ = sender.send(Err(DataFusionError::Execution(error_msg))).await;
                        }
                    }

                    // Reset builders for the next batch.
                    id_builder = Int32Array::builder(1024);
                    name_builder = StringBuilder::with_capacity(1024, 1024 * 10);
                }
            }

            // After the NATS stream ends (or is closed), flush any remaining buffered records.
            if id_builder.len() > 0 {
                let id_array = id_builder.finish();
                let name_array = name_builder.finish();
                match RecordBatch::try_new(schema_for_spawn.clone(), vec![Arc::new(id_array), Arc::new(name_array)]) {
                    Ok(record_batch) => {
                        tracing::debug!("Sending final record batch with {} rows.", record_batch.num_rows());
                        if let Err(e) = sender.send(Ok(record_batch)).await {
                            tracing::error!("Failed to send final record batch: {}", e);
                        }
                    },
                    Err(e) => {
                        let error_msg = format!("Failed to create final record batch: {}", e);
                        tracing::error!("{}", error_msg);
                        let _ = sender.send(Err(DataFusionError::Execution(error_msg))).await;
                    }
                }
            }
            tracing::info!("NATS message consumption task finished.");
        });

        // Convert the mpsc receiver into a Stream that DataFusion can consume.
        let stream = ReceiverStream::new(receiver);

        // Return the RecordBatchStreamAdapter, which wraps our custom stream.
        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
    }
    
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}