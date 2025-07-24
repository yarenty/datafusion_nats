use std::{collections::HashMap, sync::Arc};

use datafusion::{arrow::datatypes::SchemaRef, error::DataFusionError};
use url::Url;

use crate::reader::{NATS_MESSAGE_SCHEMA, DataSchemaEncoding, NatsTable, ReadEncoding, SchemaLocation, WriteEncoding};

/// Default NATS consumer config to be used
pub fn consumer_config(config_overrides: &Option<HashMap<String, String>>) -> HashMap<String, String> {
    let mut config: HashMap<String, String> = HashMap::new();

    config.insert("servers".into(), "localhost:4222".into());

    if let Some(overrides) = config_overrides {
        for (key, value) in overrides {
            config.insert(key.into(), value.into());
        }
    }

    config
}

///
/// Parse NATS specific url we use for declarative table definitions
///
/// current supported format:
///
/// ```text,no_run
/// nats_connection_type+message_format://nats_address/subject_name
/// ```
///
/// for example:
///
/// ```text,no_run
/// plaintext+json://localhost:4222/test_subject
/// ```
///
pub trait NatsUrl {
    /// subject provided
    fn nats_subject(&self) -> Result<String, String>;
    /// NATS server connection string
    fn nats_servers(&self) -> String;
    /// Message format for reading
    fn nats_message_read_encoding(&self) -> Option<ReadEncoding>;
    /// Message format for writing
    fn nats_message_write_encoding(&self) -> Option<WriteEncoding>;
    /// NATS connection protocol
    fn nats_connection_protocol(&self) -> Option<String>;
}

impl NatsUrl for Url {
    fn nats_subject(&self) -> Result<String, String> {
        self.path_segments()
            .map(|c| c.collect::<Vec<_>>())
            .unwrap()
            .first()
            .map(|s| s.to_string())
            .ok_or("subject has to be defined".to_string())
    }

    fn nats_servers(&self) -> String {
        format!(
            "{}:{}",
            self.domain().unwrap_or("localhost"),
            self.port().unwrap_or(4222)
        )
    }

    fn nats_message_read_encoding(&self) -> Option<ReadEncoding> {
        let schema = self.scheme().to_lowercase();
        let r: Vec<&str> = schema.split('+').collect();

        match r[..] {
            [_, "json", ..] => Some(ReadEncoding::Json),
            [_, "csv", ..] => Some(ReadEncoding::Csv),
            _ => None,
        }
    }

    fn nats_message_write_encoding(&self) -> Option<WriteEncoding> {
        let schema = self.scheme().to_lowercase();
        let r: Vec<&str> = schema.split('+').collect();

        match r[..] {
            [_, "json", ..] => Some(WriteEncoding::Json),
            [_, "csv", ..] => Some(WriteEncoding::Csv),
            _ => None,
        }
    }

    fn nats_connection_protocol(&self) -> Option<String> {
        let schema = self.scheme().to_lowercase();
        let r: Vec<&str> = schema.split('+').collect();

        match r[..] {
            [connection, ..] if !self.cannot_be_a_base() => Some(connection.into()),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ClientOptions {
    // Add NATS specific client options here if needed
}

impl ClientOptions {
    fn default() -> Self {
        Self {}
    }
}

#[derive(Debug)]
pub struct CreateExternalNatsTable {
    pub table_name: String,
    pub nats_servers: String,
    pub nats_subject: String,
    pub message_read_encoding: DataSchemaEncoding,
    pub message_write_encoding: Option<WriteEncoding>,
    pub nats_options: HashMap<String, String>,
    pub schema: SchemaRef,
    pub client_options: ClientOptions,
}

impl CreateExternalNatsTable {
    pub fn try_from_params(
        schema: &SchemaRef,
        name: &datafusion::common::OwnedTableReference,
        location: &String,
        options: &HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        let url = &location;
        log::warn!("table provider has limited support. url: {}", url);

        let (nats_servers, nats_subject, nats_message_read_encoding, nats_message_write_encoding) = match Url::parse(url) {
            Ok(url) => (url.nats_servers(), url.nats_subject(), url.nats_message_read_encoding(), url.nats_message_write_encoding()),
            Err(e) => Err(DataFusionError::Execution(e.to_string()))?,
        };

        let nats_subject = nats_subject.map_err(DataFusionError::Execution)?;

        let mut nats_options = HashMap::new();
        let client_options = ClientOptions::default();

        nats_options.insert("servers".to_string(), nats_servers.to_owned());

        options
            .iter()
            .filter(|(k, _)| k.to_lowercase().starts_with("nats."))
            .for_each(|(k, v)| {
                nats_options.insert(
                    k.to_owned().to_ascii_lowercase().replace("nats.", ""),
                    v.to_owned().to_ascii_lowercase(),
                );
            });

        for (k, v) in options {
            match k.to_ascii_lowercase().as_str() {
                // Add NATS specific options here
                _ => (),
            };
        }

        let schema = match schema.fields().len() {
            0 => NATS_MESSAGE_SCHEMA.clone(),
            _ => schema.clone(),
        };

        let message_read_encoding = match nats_message_read_encoding {
            None => DataSchemaEncoding::NatsMessage,
            Some(encoding) => DataSchemaEncoding::Payload(encoding, SchemaLocation::Provided(schema.clone())),
        };

        Ok(CreateExternalNatsTable {
            table_name: name.to_string(),
            nats_servers,
            nats_subject,
            message_read_encoding,
            message_write_encoding: nats_message_write_encoding,
            nats_options,
            schema,
            client_options,
        })
    }
}

impl TryFrom<&datafusion::logical_expr::CreateExternalTable> for NatsTable {
    type Error = datafusion::error::DataFusionError;

    fn try_from(cmd: &datafusion::logical_expr::CreateExternalTable) -> Result<Self, Self::Error> {
        let table: CreateExternalNatsTable = cmd.try_into()?;
        let table: NatsTable = table.into();

        Ok(table)
    }
}

impl From<CreateExternalNatsTable> for NatsTable {
    fn from(table: CreateExternalNatsTable) -> Self {
        let builder = NatsTable::builder()
            .subject(table.nats_subject)
            .config(table.nats_options)
            .schema(table.schema)
            .schema_encoding(table.message_read_encoding);

        builder.build()
    }
}

impl TryFrom<&datafusion::logical_expr::CreateExternalTable> for CreateExternalNatsTable {
    type Error = datafusion::error::DataFusionError;

    fn try_from(cmd: &datafusion::logical_expr::CreateExternalTable) -> Result<Self, Self::Error> {
        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
        Self::try_from_params(&schema, &cmd.name, &cmd.location, &cmd.options)
    }
}

#[cfg(test)]
mod url_spec {
    use std::collections::HashMap;

    use super::{CreateExternalNatsTable, NatsUrl};
    use crate::ReadEncoding;
    use datafusion::common::{Constraints, DFSchema};
    use url::Url;

    #[test]
    fn subject() {
        let url = Url::parse("plaintext://localhost:4222/test_subject").unwrap();
        assert_eq!("test_subject", url.nats_subject().unwrap());

        let url = Url::parse("plaintext:///test_subject").unwrap();
        assert_eq!("test_subject", url.nats_subject().unwrap());
    }

    #[test]
    fn servers() {
        let url = Url::parse("plaintext://host:4223/test_subject").unwrap();
        assert_eq!("host:4223", &url.nats_servers());

        let url = Url::parse("plaintext:///test_subject").unwrap();
        assert_eq!("localhost:4222", &url.nats_servers());
    }

    #[test]
    fn encoding() {
        let url = Url::parse("plaintext+json://localhost:4222/test_subject").unwrap();
        assert_eq!(ReadEncoding::Json, url.nats_message_encoding().unwrap());
        let url = Url::parse("plaintext+csv://localhost:4222/test_subject").unwrap();
        assert_eq!(ReadEncoding::Csv, url.nats_message_encoding().unwrap());
        let url = Url::parse("plaintext://localhost:4222/test_subject").unwrap();
        assert!(url.nats_message_encoding().is_none());
        let url = Url::parse("localhost:4222/test_subject").unwrap();
        assert!(url.nats_message_encoding().is_none());
    }

    #[test]
    fn protocol() {
        let url = Url::parse("plaintext+json://localhost:4222/test_subject").unwrap();
        assert_eq!("plaintext", url.nats_connection_protocol().unwrap());
        let url = Url::parse("tls://localhost:4222/test_subject").unwrap();
        assert_eq!("tls", url.nats_connection_protocol().unwrap());
        let url = Url::parse("localhost:4222/test_subject").unwrap();
        assert_eq!(None, url.nats_connection_protocol());
    }

    #[test]
    fn should_create_external_nats_table() {
        let options = maplit::hashmap! {
            "nats.client.id".to_string() => "ironman".to_string(),
            "nats.group.id".to_string() => "mm".to_string(),
        };

        let statement = datafusion::logical_expr::CreateExternalTable {
            schema: DFSchema::empty().into(),
            name: datafusion::sql::TableReference::from("my_table"),
            location: "plaintext://nats_host:4222/test_subject".into(),
            file_type: "nats".into(),
            table_partition_cols: vec![],
            if_not_exists: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            options,
            unbounded: false,
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
            
        };

        let nats_create_table: CreateExternalNatsTable = (&statement).try_into().unwrap();

        assert_eq!("test_subject", nats_create_table.nats_subject);
        assert_eq!("my_table", nats_create_table.table_name);
        assert_eq!("ironman", nats_create_table.nats_options.get("client.id").unwrap());
        assert_eq!(
            "nats_host:4222",
            nats_create_table.nats_options.get("servers").unwrap()
        );
        assert_eq!(3, nats_create_table.nats_options.len()); // servers will be injected 2 specified + servers
        assert_eq!(crate::reader::NATS_MESSAGE_SCHEMA.clone(), nats_create_table.schema);
    }
}