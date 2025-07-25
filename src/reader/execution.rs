use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::mpsc::*;
use log;
use async_nats::Message;
use futures_util::stream::StreamExt;
use crate::reader::batch_handler::{BatchHandler, BatchHandlerBuilder};
use crate::Result;


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
    pub(crate) async fn message_processor_task(
        subject: String,
        batch_size: usize,
        schema: SchemaRef,
        mut subscriber: async_nats::Subscriber,
        response_tx: Sender<Result<RecordBatch>>,
        handler_builder: Arc<dyn BatchHandlerBuilder>,
    ) -> Result<()> {
        log::debug!(
            "execution thread for subject: [{}] ... started",
            &subject,
        );

        let mut handler = handler_builder.build( // Clone the sender since it's a channel
            schema.clone(),
            batch_size
        )?;

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
                        match response_tx.send(handler.finish()).await {
                            Ok(_) => log::trace!("batch send successfully"),
                            Err(_) => log::warn!(
                                "batch send failed (this happens when datafusion does not need more data, or execution is dropped)"
                            ),
                        };
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

            match response_tx.send(handler.finish()).await {
                Ok(_) => log::trace!("batch send successfully"),
                Err(_) => {
                    log::warn!("batch send failed (this happens when datafusion does not need more data, or execution is dropped)");
                    return Ok(());
                }
            };
        }
    }
}

