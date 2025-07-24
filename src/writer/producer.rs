// NATS producer logic
use async_nats::Client;
use std::sync::Arc;
use crate::Result;
use datafusion::arrow::record_batch::RecordBatch;

pub struct NatsProducer {
    client: Arc<Client>,
    subject: String,
}

impl NatsProducer {
    pub fn new(client: Arc<Client>, subject: String) -> Self {
        Self { client, subject }
    }
}

impl super::BatchProducer for NatsProducer {
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // This is a simplified implementation. In a real scenario, you'd serialize the batch
        // according to the chosen format (JSON, CSV) before sending.
        // For now, we'll just convert the first column to string and send.
        // This will be replaced by format-specific serialization.
        for row_idx in 0..batch.num_rows() {
            let mut message_payload = String::new();
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                // This is a very basic conversion, needs to be replaced by proper serialization
                message_payload.push_str(&format!("{}", array.value(row_idx)));
                if col_idx < batch.num_columns() - 1 {
                    message_payload.push_str(",");
                }
            }
            // In a real implementation, you'd use the appropriate format handler here
            // For now, just sending as a string
            let client = self.client.clone();
            let subject = self.subject.clone();
            tokio::spawn(async move {
                if let Err(e) = client.publish(subject, message_payload.into()).await {
                    log::error!("Failed to publish message to NATS: {}", e);
                }
            });
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        // NATS client handles flushing internally for publish operations
        Ok(())
    }
}
