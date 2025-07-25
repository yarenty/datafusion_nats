use crate::{DataFusionNatsError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::csv::writer::WriterBuilder;
use log;
use async_trait::async_trait;
use std::sync::Arc;
use async_nats::Client;
use crate::reader::format::csv::CsvWriterFormat;
use crate::writer::BatchProducer;

pub struct CsvBatchProducer {
    client: Arc<Client>,
    subject: String,
}

impl CsvBatchProducer {
    pub fn new(client: Arc<Client>, subject: String) -> Self {
        Self { client, subject }
    }
}

impl BatchProducer for CsvBatchProducer {
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let messages = CsvWriterFormat::serialize_record_batch(batch)?;
        for message_payload in messages {
            let client = self.client.clone();
            let subject = self.subject.clone();
            tokio::spawn(async move {
                if let Err(e) = client.publish(subject, message_payload.into()).await {
                    log::error!("Failed to publish CSV message to NATS: {}", e);
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
pub struct CsvBatchProducerBuilder {
    client: Arc<Client>,
    subject: String,
}

impl CsvBatchProducerBuilder {
    pub fn new(client: Arc<Client>, subject: String) -> Self {
        Self { client, subject }
    }
}

#[async_trait]
impl super::super::BatchProducerBuilder for CsvBatchProducerBuilder {
    type Producer = CsvBatchProducer;

    async fn build(&self) -> Result<Self::Producer> {
        Ok(CsvBatchProducer::new(self.client.clone(), self.subject.clone()))
    }
}