use std::sync::Arc;
use datafusion::arrow::array::{Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Date32Builder, Float64Array, Float64Builder, Int32Array, Int32Builder, RecordBatch, StringArray, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
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
            for buffer in &self.buffers {
                if buffer.is_empty() {
                    return Err(DataFusionError::Execution("Empty buffer encountered".to_string()));
                }

                // Create a new array of the same type as the first array in buffer
                let first_array = buffer.front().unwrap();
                let data_type = first_array.data_type().clone();

                // Create a new array builder of the appropriate type
                match data_type {
                    DataType::Int32 => {
                        let mut builder = Int32Builder::with_capacity(buffer.len());
                        for array in buffer.iter() {
                            let int_array = array.as_ref().as_any().downcast_ref::<Int32Array>().unwrap();
                            for i in 0..int_array.len() {
                                builder.append_value(int_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    DataType::Float64 => {
                        let mut builder = Float64Builder::with_capacity(buffer.len());
                        for array in buffer.iter() {
                            let float_array = array.as_ref().as_any().downcast_ref::<Float64Array>().unwrap();
                            for i in 0..float_array.len() {
                                builder.append_value(float_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    DataType::Boolean => {
                        let mut builder = BooleanBuilder::with_capacity(buffer.len());
                        for array in buffer.iter() {
                            let bool_array = array.as_ref().as_any().downcast_ref::<BooleanArray>().unwrap();
                            for i in 0..bool_array.len() {
                                builder.append_value(bool_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    DataType::Utf8 => {
                        let mut builder = StringBuilder::with_capacity(buffer.len(), buffer.len() * 10);
                        for array in buffer.iter() {
                            let string_array = array.as_ref().as_any().downcast_ref::<StringArray>().unwrap();
                            for i in 0..string_array.len() {
                                builder.append_value(string_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    DataType::Timestamp(_, _) => {
                        let mut builder = TimestampNanosecondBuilder::with_capacity(buffer.len());
                        for array in buffer.iter() {
                            let timestamp_array = array.as_ref().as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                            for i in 0..timestamp_array.len() {
                                builder.append_value(timestamp_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    DataType::Date32 => {
                        let mut builder = Date32Builder::with_capacity(buffer.len());
                        for array in buffer.iter() {
                            let date_array = array.as_ref().as_any().downcast_ref::<Date32Array>().unwrap();
                            for i in 0..date_array.len() {
                                builder.append_value(date_array.value(i));
                            }
                        }
                        arrays.push(Arc::new(builder.finish()) as ArrayRef);
                    }
                    _ => return Err(DataFusionError::Execution(format!(
                        "Unsupported data type: {}",
                        data_type
                    ))),
                }
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
