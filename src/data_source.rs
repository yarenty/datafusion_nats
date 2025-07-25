use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{SchemaRef};
use crate::raw_filter::RawFilter;
use crate::codec::csv::{CsvCodec, CsvCodecError};
use datafusion::error::DataFusionError;
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
        limit: Option<usize>,
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
            limit,
            csv_codec,
        };
        Ok(Arc::new(exec))
    }
}

#[derive(Debug)]
pub struct NatsExec {
    pub schema: SchemaRef,
    pub properties: PlanProperties,
    pub client: Client,
    pub subject: String,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
    pub csv_codec: CsvCodec,
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
        // Create a channel to send RecordBatches from the NATS consumer task
        // to the DataFusion execution stream.
        let (sender, receiver) = mpsc::channel(1024);

        // Create the filter
        let raw_filter = RawFilter::new(self.filters.clone(), self.schema.clone());

        // Spawn a new asynchronous task to consume messages from NATS.
        // This task runs concurrently with the DataFusion query execution.
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

            // Initialize Arrow array builders for efficient data buffering.
            let mut arrays = Vec::new();
            let mut records_processed = 0;

            // Continuously receive messages from the NATS subscription.
            while let Some(message) = subscriber.next().await {
                if !raw_filter.should_include(&message) {
                    continue;
                }

                tracing::debug!("Received NATS message: {:?}", message);
                // Parse the payload using CsvCodec
                let payload = String::from_utf8_lossy(&message.payload);
                match self.csv_codec.parse_payload(&payload) {
                    Ok(parsed_arrays) => {
                        // Append parsed arrays to our batch
                        for (i, array) in parsed_arrays.iter().enumerate() {
                            if arrays.len() <= i {
                                // Initialize new builder if needed
                                let field = self.schema.field(i);
                                match field.data_type() {
                                    DataType::Int32 => arrays.push(Int32Array::builder(1024)),
                                    DataType::Utf8 => arrays.push(StringBuilder::with_capacity(1024, 1024 * 10)),
                                    _ => return, // We don't support other types yet
                                }
                            }
                            
                            // Append value to the appropriate builder
                            if let Some(builder) = arrays.get_mut(i) {
                                match builder.as_any_mut() {
                                    Some(builder) => builder.append_value(array.as_ref()),
                                    None => return, // Invalid builder type
                                }
                            }
                        }
                        records_processed += 1;
                        tracing::debug!("Message included with {} fields", parsed_arrays.len());
                    }
                    Err(e) => {
                        tracing::error!("Failed to parse payload: {}", e);
                        let _ = _sender.send(Err(DataFusionError::Execution(e.to_string()))).await;
                    }
                }

                // If enough records are buffered, build a RecordBatch and send it.
                if records_processed > 0 && records_processed % 1000 == 0 {
                    // Convert builders to arrays
                    let arrays: Vec<Arc<dyn Array>> = arrays.iter_mut()
                        .map(|builder| builder.finish())
                        .map(|array| Arc::new(array))
                        .collect();

                    match self.csv_codec.create_record_batch(arrays) {
                        Ok(record_batch) => {
                            tracing::info!("Sending record batch with {} rows.", record_batch.num_rows());
                            if let Err(e) = _sender.send(Ok(record_batch)).await {
                                tracing::error!("Failed to send record batch: {}", e);
                            }
                        },
                        Err(e) => {
                            let error_msg = format!("Failed to create record batch: {}", e);
                            tracing::error!("{}", error_msg);
                            let _ = _sender.send(Err(DataFusionError::Execution(error_msg))).await;
                        }
                    }

                    // Reset builders for the next batch
                    arrays.clear();
                }

                // Check if limit is reached
                if let Some(limit) = limit_for_spawn {
                    if records_processed >= limit {
                        tracing::info!("Limit of {} records reached. Stopping NATS consumption.", limit);
                        break; // Exit the while loop
                    }
                }
            }

            // After the NATS stream ends (or is closed), flush any remaining buffered records.
            if id_builder.len() > 0 {
                let id_array = id_builder.finish();
                let name_array = name_builder.finish();
                match RecordBatch::try_new(schema_for_spawn.clone(), vec![Arc::new(id_array), Arc::new(name_array)]) {
                    Ok(record_batch) => {
                        tracing::info!("Sending final record batch with {} rows.", record_batch.num_rows());
                        if let Err(e) = _sender.send(Ok(record_batch)).await {
                            tracing::error!("Failed to send final record batch: {}", e);
                        }
                    },
                    Err(e) => {
                        let error_msg = format!("Failed to create final record batch: {}", e);
                        tracing::error!("{}", error_msg);
                        let _ = _sender.send(Err(DataFusionError::Execution(error_msg))).await;
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