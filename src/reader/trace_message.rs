use async_nats::Message;

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