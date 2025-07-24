use crate::reader::{BatchHandlerBuilder, BatchHandler};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use std::sync::Arc;
use tokio::sync::mpsc::*;
use async_nats::Message;
use futures_util::stream::StreamExt;


///
/// # Message processing logic
///
/// A common piece of code which can be executed in both client and consumer
/// execution type, actually receiving messages from the topic and converting them
/// to arrow format.
///
pub(crate) struct Execution {}

impl Execution {
    ///
    /// starts a thread which will take the message from given partition
    /// and convert them according to the specified handler_builder format
    ///

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn message_processor_task<B: BatchHandlerBuilder + std::fmt::Debug + Sync + Send + 'static>(
        subject: String,
        batch_size: usize,
        schema: SchemaRef,
        mut subscriber: async_nats::Subscriber,
        watcher: Arc<super::table::ClientWatcher>,
        response_tx: Sender<Result<RecordBatch>>,
        handler_builder: Arc<B>,
    ) -> Result<()> {
        log::debug!(
            "execution thread for subject: [{}] ... started",
            &subject,
        );
        let mut handler = handler_builder.build(schema.clone(), batch_size).unwrap();
        loop {
            log::trace!(
                "executor entered loop section for subject: [{}] ...",
                subject,
            );

            while handler.has_capacity() {
                let message = subscriber.next().await;

                let response = match message {
                    Some(msg) => {
                        log::trace!(
                            "received message for subject: [{}]",
                            subject,
                        );
                        log::trace!("received message payload: {:?}", msg.payload);
                        handler.append_message(&msg)
                    }
                    None => {
                        log::debug!("NATS subscriber closed for subject: [{}]", subject);
                        match response_tx.send(Ok(handler.finish().unwrap())).await {
                            Ok(_) => log::trace!("batch send successfully"),
                            Err(_) => log::warn!(
                                "batch send failed (this happens when datafusion does not need more data, or execution is dropped)"
                            ),
                        };
                        drop(watcher);
                        return Ok(());
                    }
                };

                if let Err(error) = response {
                    log::warn!(
                        "message handling - error: [{:?}]",
                        error,
                    );
                    // TODO: should we ignore error or stop processing?
                }
            }

            match response_tx.send(Ok(handler.finish().unwrap())).await {
                Ok(_) => log::trace!("batch send successfully"),
                Err(_) => {
                    log::warn!("batch send failed (this happens when datafusion does not need more data, or execution is dropped)");
                    drop(watcher);
                    return Ok(());
                }
            };
        }
    }
}

/// Add support to trace received message
/// which would be computed only if appropriate log level is
/// configured
trait TraceMessage {
    fn trace(&self) -> String;
}

impl TraceMessage for Message {
    fn trace(&self) -> String {
        // try to convert it to string and return string
        // otherwise just return message default (which does not help much)
        let result = std::str::from_utf8(&self.payload);
        match result {
            Ok(str) => str.to_string(),
            Err(_) => format!("{:?}", self.payload),
        }
    }
}
