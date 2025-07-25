use async_nats::Message;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;

pub trait BatchHandler {
    fn append_message(&mut self, message: &Message) -> crate::Result<()>;
    fn has_capacity(&self) -> bool;
    fn finish(&mut self) -> crate::Result<RecordBatch>;
}

pub trait BatchHandlerBuilder {
    // type Handler: BatchHandler + Send;
    fn build(&self, schema: SchemaRef, batch_size: usize) -> crate::Result<dyn BatchHandler>;
}
