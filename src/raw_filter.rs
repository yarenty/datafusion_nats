use datafusion::logical_expr::Expr;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;
use async_nats::Message;
use tracing::debug;

pub struct RawFilter {
    filters: Vec<Expr>,
    schema: SchemaRef,
}

impl RawFilter {
    pub fn new(filters: Vec<Expr>, schema: SchemaRef) -> Self {
        Self { filters, schema }
    }

    pub fn should_include(&self, message: &Message) -> bool {
        // For now, we only handle simple equality filters on integer columns
        // This is a very basic implementation and should be replaced with a proper expression evaluator
        
        // Try to parse the message payload as JSON
        let payload = match String::from_utf8(message.payload.to_vec()) {
            Ok(p) => p,
            Err(_) => return false,
        };

        // Simple JSON parsing - this should be replaced with proper schema-aware parsing
        let parts: Vec<&str> = payload.split(',').collect();
        
        // Check each filter
        for filter in &self.filters {
            if let Expr::BinaryExpr(binary_expr) = filter {
                if let (Expr::Column(col), Expr::Literal(scalar, _)) = (&*binary_expr.left, &*binary_expr.right) {
                    // For now, we only handle the 'id' column
                    if col.name == "id" {
                        if let ScalarValue::Int32(Some(filter_id)) = scalar {
                            if let Ok(id) = parts[0].parse::<i32>() {
                                if id != *filter_id {
                                    debug!("Message filtered out by ID: {}", id);
                                    return false;
                                }
                            }
                        }
                    }
                }
            }
        }
        true
    }
}
