use crate::{DataFusionNatsError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::json;
use log;
use async_trait::async_trait;
use std::sync::Arc;
use async_nats::Client;
use crate::reader::format::json::JsonWriterFormat;
use crate::writer::BatchProducer;

pub struct JsonBatchProducer {
    client: Arc<Client>,
    subject: String,
}

impl JsonBatchProducer {
    pub fn new(client: Arc<Client>, subject: String) -> Self {
        Self { client, subject }
    }
}

impl BatchProducer for JsonBatchProducer {
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let messages = JsonWriterFormat::serialize_record_batch(batch)?;
        for message_payload in messages {
            let client = self.client.clone();
            let subject = self.subject.clone();
            tokio::spawn(async move {
                if let Err(e) = client.publish(subject, message_payload.into()).await {
                    log::error!("Failed to publish JSON message to NATS: {}", e);
                }
            });
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct JsonBatchProducerBuilder {
    client: Arc<Client>,
    subject: String,
}

impl JsonBatchProducerBuilder {
    pub fn new(client: Arc<Client>, subject: String) -> Self {
        Self { client, subject }
    }
}

#[async_trait]
impl super::super::BatchProducerBuilder for JsonBatchProducerBuilder {
    type Producer = JsonBatchProducer;

    async fn build(&self) -> Result<Self::Producer> {
        Ok(JsonBatchProducer::new(self.client.clone(), self.subject.clone()))
    }
}