use crate::batch_buffer::{BatchBuffer, BatchBufferConfig};
use crate::codec::csv::CsvCodec;
use crate::raw_filter::RawFilter;
use async_nats::Client;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;

/// Configuration for NATS streaming behavior
#[derive(Debug, Clone)]
pub struct NatsStreamConfig {
    pub batch_size: usize,
    pub batch_timeout: Duration,
    pub buffer_size: usize,
    pub max_concurrent_messages: usize,
    pub enable_backpressure: bool,
}

impl Default for NatsStreamConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout: Duration::from_millis(100),
            buffer_size: 10000,
            max_concurrent_messages: 1000,
            enable_backpressure: true,
        }
    }
}

/// Metrics for monitoring NATS stream performance
#[derive(Debug, Default)]
pub struct NatsStreamMetrics {
    pub messages_received: Arc<RwLock<u64>>,
    pub messages_processed: Arc<RwLock<u64>>,
    pub batches_sent: Arc<RwLock<u64>>,
    pub parse_errors: Arc<RwLock<u64>>,
    pub last_message_time: Arc<RwLock<Option<Instant>>>,
}

impl NatsStreamMetrics {
    pub async fn increment_received(&self) {
        *self.messages_received.write().await += 1;
        *self.last_message_time.write().await = Some(Instant::now());
    }

    pub async fn increment_processed(&self) {
        *self.messages_processed.write().await += 1;
    }

    pub async fn increment_batches(&self) {
        *self.batches_sent.write().await += 1;
    }

    pub async fn increment_errors(&self) {
        *self.parse_errors.write().await += 1;
    }
}

/// Custom stream implementation for NATS messages
pub struct NatsMessageStream {
    receiver:
        mpsc::Receiver<datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>>,
    schema: SchemaRef,
    metrics: Arc<NatsStreamMetrics>,
}

impl NatsMessageStream {
    fn new(
        receiver: mpsc::Receiver<
            datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>,
        >,
        schema: SchemaRef,
        metrics: Arc<NatsStreamMetrics>,
    ) -> Self {
        Self {
            receiver,
            schema,
            metrics,
        }
    }
}

impl Stream for NatsMessageStream {
    type Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.receiver).poll_recv(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(batch)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Backpressure-aware buffer state
#[derive(Debug)]
struct BufferState {
    current_size: usize,
    max_size: usize,
    last_flush: Instant,
    flush_timeout: Duration,
}

impl BufferState {
    fn new(max_size: usize, flush_timeout: Duration) -> Self {
        Self {
            current_size: 0,
            max_size,
            last_flush: Instant::now(),
            flush_timeout,
        }
    }

    fn should_flush(&self, batch_size: usize) -> bool {
        self.current_size >= batch_size
            || self.current_size >= self.max_size
    }

    fn should_flush_timeout(&self) -> bool {
        self.current_size > 0 && self.last_flush.elapsed() >= self.flush_timeout
    }

    fn is_full(&self) -> bool {
        self.current_size >= self.max_size
    }

    fn add_record(&mut self) {
        self.current_size += 1;
    }

    fn reset(&mut self) {
        self.current_size = 0;
        self.last_flush = Instant::now();
    }
}
#[derive(Debug)]
pub struct NatsStreamingExec {
    pub schema: SchemaRef,
    pub properties: PlanProperties,
    pub client: Client,
    pub subject: String,
    pub filters: Vec<Expr>,
    pub limit: Option<usize>,
    pub codec: CsvCodec,
    pub config: NatsStreamConfig,
    pub metrics: Arc<NatsStreamMetrics>,
}

impl NatsStreamingExec {
    pub fn new(
        schema: SchemaRef,
        client: Client,
        subject: String,
        filters: Vec<Expr>,
        limit: Option<usize>,
        codec: CsvCodec,
        config: Option<NatsStreamConfig>,
    ) -> Self {
        let config = config.unwrap_or_default();

        // Create properties for streaming execution
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1), // Single partition for NATS stream
            EmissionType::Both,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            }, // This is key for streaming!
        );

        Self {
            schema,
            properties,
            client,
            subject,
            filters,
            limit,
            codec,
            config,
            metrics: Arc::new(NatsStreamMetrics::default()),
        }
    }

    fn eq_properties(schema: &SchemaRef) -> EquivalenceProperties {
        EquivalenceProperties::new(schema.clone())
    }

    /// Start the NATS message consumption task with proper streaming semantics
    async fn start_message_consumer(
        client: Client,
        subject: String,
        codec: CsvCodec,
        schema: SchemaRef,
        config: NatsStreamConfig,
        metrics: Arc<NatsStreamMetrics>,
        filters: Vec<Expr>,
        limit: Option<usize>,
        sender: mpsc::Sender<
            datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>,
        >,
    ) {
        let batch_buffer_config = BatchBufferConfig {
            batch_size: config.batch_size,
            max_buffer_size: config.buffer_size,
            batch_timeout_ms: config.batch_timeout.as_millis() as u64,
        };

        let buffer = Arc::new(Mutex::new(BatchBuffer::new(
            schema.clone(),
            batch_buffer_config,
        )));

        let mut buffer_state = BufferState::new(config.buffer_size, config.batch_timeout);
        let raw_filter = RawFilter::new(filters, schema.clone());

        // Subscribe to NATS subject
        let mut subscriber = match client.subscribe(subject.clone()).await {
            Ok(sub) => {
                tracing::info!(
                    "NATS streaming subscriber created for subject: {}",
                    &subject
                );
                sub
            }
            Err(e) => {
                let error_msg = format!("Failed to subscribe to NATS: {}", e);
                tracing::error!("{}", error_msg);
                let _ = sender
                    .send(Err(DataFusionError::Execution(error_msg)))
                    .await;
                return;
            }
        };

        // Create flush interval for time-based batching - this is critical for streaming!
        let mut flush_interval = tokio::time::interval(config.batch_timeout);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut records_processed = 0usize;
        let mut last_message_time = Instant::now();

        // Keep the consumer running indefinitely for true streaming
        loop {
            tokio::select! {
                // Handle incoming NATS messages
                message = subscriber.next() => {
                    match message {
                        Some(message) => {
                            last_message_time = Instant::now();
                            metrics.increment_received().await;

                            // Apply backpressure if buffer is full
                            if config.enable_backpressure && buffer_state.is_full() {
                                tracing::warn!("Buffer full, applying backpressure");
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }

                            let payload = String::from_utf8_lossy(&message.payload);

                            match codec.parse_payload(&payload) {
                                Ok(parsed_arrays) => {
                                    // Add to buffer
                                    if let Err(e) = buffer.lock().await.add_row(parsed_arrays) {
                                        tracing::error!("Failed to add row to buffer: {}", e);
                                        metrics.increment_errors().await;
                                        continue;
                                    }

                                    buffer_state.add_record();
                                    records_processed += 1;
                                    metrics.increment_processed().await;

                                    // Check if we should create a batch based on size
                                    if buffer_state.current_size >= config.batch_size {
                                        tracing::debug!("Flushing due to batch size: {}", buffer_state.current_size);
                                        if let Err(e) = Self::flush_buffer(&buffer, &sender, &metrics, &mut buffer_state).await {
                                            tracing::error!("Failed to flush buffer: {}", e);
                                            break;
                                        }
                                    }

                                    // Check limit
                                    if let Some(limit) = limit {
                                        if records_processed >= limit {
                                            tracing::info!("Reached limit of {} records", limit);
                                            // Flush remaining records
                                            let _ = Self::flush_buffer(&buffer, &sender, &metrics, &mut buffer_state).await;
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to parse payload: {}", e);
                                    metrics.increment_errors().await;
                                }
                            }
                        }
                        None => {
                            tracing::info!("NATS subscription ended, but continuing to stream buffered data");
                            // Don't break here for streaming! Instead, flush and continue waiting
                            if buffer_state.current_size > 0 {
                                let _ = Self::flush_buffer(&buffer, &sender, &metrics, &mut buffer_state).await;
                            }
                            // For true streaming, we don't exit when there are no messages
                            // The stream should stay alive waiting for new messages
                        }
                    }
                }

                // CRITICAL: Handle periodic flushing - this ensures streaming behavior!
                _ = flush_interval.tick() => {
                    tracing::debug!("Flush interval tick - buffer size: {}", buffer_state.current_size);

                    // Always try to flush if we have data, regardless of time elapsed
                    if buffer_state.current_size > 0 {
                        tracing::debug!("Time-based flush triggered with {} records", buffer_state.current_size);
                        if let Err(e) = Self::flush_buffer(&buffer, &sender, &metrics, &mut buffer_state).await {
                            tracing::error!("Failed to flush buffer on timeout: {}", e);
                            // Don't break - continue streaming
                        }
                    }

                    // Optional: Add heartbeat or keep-alive logic here
                    // This ensures the stream stays active even with no messages
                }
            }

            // Add a small yield to prevent busy waiting
            tokio::task::yield_now().await;
        }
    }

    async fn flush_buffer(
        buffer: &Arc<Mutex<BatchBuffer>>,
        sender: &mpsc::Sender<
            datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>,
        >,
        metrics: &Arc<NatsStreamMetrics>,
        buffer_state: &mut BufferState,
    ) -> Result<(), String> {
        let mut buffer_guard = buffer.lock().await;
        match buffer_guard.create_batch() {
            Ok(Some(batch)) => {
                let row_count = batch.num_rows();
                tracing::debug!("Flushing batch with {} rows", row_count);

                if sender.send(Ok(batch)).await.is_err() {
                    return Err("Failed to send batch - receiver dropped".to_string());
                }

                metrics.increment_batches().await;
                buffer_state.reset();
                Ok(())
            }
            Ok(None) => {
                // No batch created (empty buffer)
                buffer_state.reset();
                Ok(())
            }
            Err(e) => Err(format!("Failed to create batch: {}", e)),
        }
    }
}
impl DisplayAs for NatsStreamingExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "NatsStreamingExec: subject={}, batch_size={}, timeout={:?}",
                    self.subject, self.config.batch_size, self.config.batch_timeout
                )
            }
        }
    }
}

impl ExecutionPlan for NatsStreamingExec {
    fn name(&self) -> &str {
        "NatsStreamingExec"
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
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "NatsStreamingExec only supports single partition".to_string(),
            ));
        }

        let (sender, receiver) = mpsc::channel(self.config.buffer_size);

        // Create the stream
        let stream = NatsMessageStream::new(receiver, self.schema.clone(), self.metrics.clone());

        // Spawn the consumer task
        let consumer_handle = tokio::spawn(Self::start_message_consumer(
            self.client.clone(),
            self.subject.clone(),
            self.codec.clone(),
            self.schema.clone(),
            self.config.clone(),
            self.metrics.clone(),
            self.filters.clone(),
            self.limit,
            sender,
        ));

        // Log the task handle for potential cancellation
        tracing::debug!("Started NATS consumer task: {:?}", consumer_handle);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

// Additional utility for creating a streaming table provider
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

#[derive(Debug)]
pub struct NatsStreamingTableProvider {
    exec: Arc<NatsStreamingExec>,
}

impl NatsStreamingTableProvider {
    pub fn new(exec: NatsStreamingExec) -> Self {
        Self {
            exec: Arc::new(exec),
        }
    }
}

#[async_trait]
impl TableProvider for NatsStreamingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.exec.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // For now, accept all filters but don't push them down
        // You could implement smarter filter pushdown based on your use case
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _session: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // For streaming, we typically don't apply projection/filters at scan time,
        // but you could modify the exec plan here if needed
        Ok(self.exec.clone())
    }
}
