use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{SchemaRef};
use datafusion::error::DataFusionError;
use std::collections::VecDeque;
use std::time::{Instant, Duration};



/// Configuration for the batch buffer
#[derive(Debug, Clone)]
pub struct BatchBufferConfig {
    /// Number of records per batch
    pub batch_size: usize,
    /// Maximum number of messages to buffer before sending
    pub max_buffer_size: usize,
    /// Timeout for batch creation (in milliseconds)
    pub batch_timeout_ms: u64,
}

impl Default for BatchBufferConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            max_buffer_size: 10000,
            batch_timeout_ms: 1000,
        }
    }
}

/// Buffer for accumulating parsed rows and creating RecordBatches
#[derive(Debug, Clone)]
pub struct BatchBuffer {
    schema: SchemaRef,
    config: BatchBufferConfig,
    buffers: Vec<VecDeque<ArrayRef>>,  // One buffer per field
    current_batch_size: usize,
    last_batch_time: Option<Instant>,
}

impl BatchBuffer {
    /// Create a new batch buffer with the given schema and configuration
    pub fn new(schema: SchemaRef, config: BatchBufferConfig) -> Self {
        let buffers = vec![VecDeque::new(); schema.fields().len()];
        Self {
            schema,
            config,
            buffers,
            current_batch_size: 0,
            last_batch_time: None,
        }
    }

    /// Add a parsed row (array of values) to the buffer
    pub fn add_row(&mut self, arrays: Vec<ArrayRef>) -> Result<(), DataFusionError> {
        if arrays.len() != self.schema.fields().len() {
            return Err(DataFusionError::Execution(
                format!("Number of fields in row ({}) doesn't match schema ({})",
                        arrays.len(), self.schema.fields().len())
            ));
        }

        for (buffer, array) in self.buffers.iter_mut().zip(arrays) {
            buffer.push_back(array);
        }
        self.current_batch_size += 1;
        self.last_batch_time = Some(Instant::now());

        Ok(())
    }

    /// Check if we should create a batch based on size or timeout
    pub fn should_create_batch(&self) -> bool {
        if self.current_batch_size >= self.config.batch_size {
            return true;
        }
        
        if let Some(last_time) = self.last_batch_time {
            let elapsed = last_time.elapsed();
            if elapsed >= Duration::from_millis(self.config.batch_timeout_ms) {
                return true;
            }
        }

        false
    }

    /// Create a RecordBatch from the current buffer contents
    pub fn create_batch(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        if self.current_batch_size == 0 {
            return Ok(None);
        }

        // Concatenate arrays from each buffer
        let mut arrays = Vec::with_capacity(self.buffers.len());
        // let arrays = self.buffers.iter().map(VecDeque::iter).flatten().collect::<Vec<_>>();
        for buffer in &self.buffers {
            if buffer.is_empty() {
                return Err(DataFusionError::Execution("Empty buffer encountered".to_string()));
            }

            // Concatenate all arrays in this buffer
            let concatenated = buffer.iter().flatten().collect::<Vec<_>>()?;
            arrays.push(concatenated);
        }

        // Create and return the batch
        let batch = RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| DataFusionError::Execution(
                format!("Failed to create batch: {}", e)
            ))?;

        // Clear the buffers
        self.clear_buffers();
        
        Ok(Some(batch))
    }

    /// Clear all buffers
    fn clear_buffers(&mut self) {
        for buffer in self.buffers.iter_mut() {
            buffer.clear();
        }
        self.current_batch_size = 0;
        self.last_batch_time = None;
    }

    /// Get the current number of records in the buffer
    pub fn current_size(&self) -> usize {
        self.current_batch_size
    }

    pub fn is_empty(&self) -> bool {
        self.current_batch_size == 0
    }

    /// Check if we've reached the maximum buffer size
    pub fn is_full(&self) -> bool {
        self.current_batch_size >= self.config.max_buffer_size
    }
}

/// Error type for batch buffer operations
#[derive(Debug, thiserror::Error)]
pub enum BatchBufferError {
    #[error("Buffer overflow: maximum buffer size ({}) exceeded", .0)]
    BufferOverflow(usize),
    
    #[error("Batch creation error: {0}")]
    BatchCreationError(String),
}
