//! # datafusion-nats
//! 
//! `datafusion-nats` provides a DataFusion `TableProvider` implementation for NATS, 
//! allowing DataFusion to query real-time data streams from NATS subjects using SQL.
//! 
//! ## Features
//! 
//! - Connects to NATS server and subscribes to a specified subject.
//! - Integrates with DataFusion as a `TableProvider`.
//! - Basic in-memory predicate pushdown for filtering data at the source.
//! - Handles data conversion from NATS messages (CSV-like format) to Apache Arrow `RecordBatch`es.
//! 
//! ## Usage
//! 
//! To use `datafusion-nats`, you need to:
//! 
//! 1. Connect to a NATS server.
//! 2. Define a schema for your NATS data.
//! 3. Create a `NatsDataSource` instance with the NATS client, schema, and subject.
//! 4. Register the `NatsDataSource` as a table in a DataFusion `SessionContext`.
//! 5. Execute SQL queries against the registered table.
//! 
//! ### Example
//! 
//! ```no_run
//! use datafusion::prelude::{SessionContext, SessionConfig};
//! use datafusion::arrow::datatypes::{DataType, Field, Schema};
//! use std::sync::Arc;
//! use anyhow::Result;
//! 
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create a NATS connection
//!     let client = datafusion_nats::nats_connection::connect("nats://localhost:4222").await?;
//! 
//!     // Define a schema for the NATS data
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int32, false),
//!         Field::new("name", DataType::Utf8, false),
//!     ]));
//! 
//!     // Create a NatsDataSource
//!     let data_source = Arc::new(datafusion_nats::data_source::NatsDataSource::new(
//!         schema.clone(),
//!         client.clone(),
//!         "test.data".to_string(),
//!     ));
//! 
//!     // Create a DataFusion session context
//!     let config = SessionConfig::new().with_information_schema(true);
//!     let ctx = SessionContext::new_with_config(config);
//! 
//!     // Register the NatsDataSource as a table
//!     ctx.register_table("nats_table", data_source)?;
//! 
//!     // Execute a SQL query
//!     let df = ctx.sql("SELECT id, name FROM nats_table").await?;
//! 
//!     // Print the results
//!     df.show().await?;
//! 
//!     Ok(())
//! }
//! ```
//! 
//! ## Modules
//! 
//! - `nats_connection`: Handles NATS client connection and subscription.
//! - `data_source`: Implements DataFusion `TableProvider` and `ExecutionPlan` for NATS data.

pub mod nats_connection;
pub mod data_source;

pub mod raw_filter;

pub mod codec;
