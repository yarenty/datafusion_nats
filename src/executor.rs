use std::any::Any;
use std::sync::Arc;
use async_nats::Client;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use futures::StreamExt;
use crate::codec::csv::CsvCodec;
use crate::raw_filter::RawFilter;
use crate::batch_buffer::{BatchBuffer, BatchBufferConfig};

#[derive(Debug)]
pub struct NatsExec {
    pub schema: SchemaRef,
    pub properties: PlanProperties,
    pub client: Client,
    pub subject: String,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
    pub codec: CsvCodec,
    pub buffer: Arc<Mutex<BatchBuffer>>,
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
            DisplayFormatType::TreeRender => todo!(),
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
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        // Create a channel to send RecordBatches from the NATS consumer task
        let (sender, receiver) = mpsc::channel(1024);

        // Create the filter
        let raw_filter = RawFilter::new(self.filters.clone(), self.schema.clone());

        // Spawn a new asynchronous task to consume messages from NATS.
        tokio::spawn(async move {
            // Ensure the sender is dropped when the task finishes, signaling the end of the stream.
            let _sender = sender.clone();

            // Subscribe to the NATS subject.
            let mut subscriber = match self.client.subscribe(self.subject.clone()).await {
                Ok(sub) => {
                    tracing::info!("NATS subscriber created for subject: {}", &self.subject);
                    sub
                },
                Err(e) => {
                    let error_msg = format!("Failed to subscribe to NATS: {}", e);
                    tracing::error!("{}", error_msg);
                    let _ = _sender.send(Err(DataFusionError::Execution(error_msg))).await;
                    return;
                }
            };

            // Continuously receive messages from the NATS subscription.
            while let Some(message) = subscriber.next().await {
                if !raw_filter.should_include(&message) {
                    continue;
                }

                tracing::debug!("Received NATS message: {:?}", message);
                // Parse the payload using CsvCodec
                let payload = String::from_utf8_lossy(&message.payload);
                match self.codec.parse_payload(&payload) {
                    Ok(parsed_arrays) => {
                        // Add parsed arrays to batch buffer
                        if let Err(e) =
                        self.buffer.lock().await.add_row(parsed_arrays)
                        {
                            tracing::error!("Failed to add row to batch buffer: {}", e);
                            let _ = _sender.send(Err(e)).await;
                            continue;
                        }

                        // Check if we should create a batch
                        if self.buffer.lock().await.should_create_batch() {
                            match self.buffer.lock().await.create_batch() {
                                Ok(Some(batch)) => {
                                    tracing::info!("Sending record batch with {} rows.", batch.num_rows());
                                    if let Err(e) = _sender.send(Ok(batch)).await {
                                        tracing::error!("Failed to send record batch: {}", e);
                                    }
                                }
                                Ok(None) => {
                                    // No batch created, continue
                                }
                                Err(e) => {
                                    tracing::error!("Failed to create batch: {}", e);
                                    let _ = _sender.send(Err(e)).await;
                                }
                            }
                        }

                        // Check if limit is reached
                        if let Some(limit) = self.limit {
                            if self.buffer.lock().await.current_size() >= limit {
                                tracing::info!("Limit of {} records reached. Stopping NATS consumption.", limit);
                                break; // Exit the while loop
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to parse payload: {}", e);
                        let _ = _sender.send(Err(DataFusionError::Execution(e.to_string()))).await;
                    }
                }
            }

            // Send any remaining records in the buffer
            if !self.buffer.lock().await.is_empty() {
                match self.buffer.lock().await.create_batch() {
                    Ok(Some(batch)) => {
                        tracing::info!("Sending final record batch with {} rows.", batch.num_rows());
                        if let Err(e) = _sender.send(Ok(batch)).await {
                            tracing::error!("Failed to send final record batch: {}", e);
                        }
                    }
                    Ok(None) => {
                        // No more records to send
                    }
                    Err(e) => {
                        tracing::error!("Failed to create final batch: {}", e);
                        let _ = _sender.send(Err(e)).await;
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
