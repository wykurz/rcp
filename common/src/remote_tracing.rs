use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TracingMessage {
    Log {
        timestamp: std::time::SystemTime,
        level: String,
        target: String,
        message: String,
    },
    Progress(crate::progress::SerializableProgress),
}

#[derive(Debug)]
pub struct RemoteTracingLayer {
    pub sender: tokio::sync::mpsc::UnboundedSender<TracingMessage>,
}

impl RemoteTracingLayer {
    #[must_use]
    pub fn new() -> (
        Self,
        tokio::sync::mpsc::UnboundedSender<TracingMessage>,
        tokio::sync::mpsc::UnboundedReceiver<TracingMessage>,
    ) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                sender: sender.clone(),
            },
            sender,
            receiver,
        )
    }
}

pub fn send_progress_update(
    sender: &tokio::sync::mpsc::UnboundedSender<TracingMessage>,
    progress: &crate::progress::Progress,
) -> anyhow::Result<()> {
    let serializable_progress = crate::progress::SerializableProgress::from(progress);
    sender.send(TracingMessage::Progress(serializable_progress))?;
    Ok(())
}

struct FieldVisitor {
    fields: std::collections::HashMap<String, String>,
    message: Option<String>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
            message: None,
        }
    }
}

impl tracing_subscriber::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value_str = format!("{value:?}");
        if field.name() == "message" {
            self.message = Some(value_str);
        } else {
            self.fields.insert(field.name().to_string(), value_str);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

impl<S> tracing_subscriber::Layer<S> for RemoteTracingLayer
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);
        let message = visitor.message.unwrap_or_else(|| {
            if visitor.fields.is_empty() {
                String::new()
            } else {
                format!("{:?}", visitor.fields)
            }
        });
        let tracing_message = TracingMessage::Log {
            timestamp: std::time::SystemTime::now(),
            level: event.metadata().level().to_string(),
            target: event.metadata().target().to_string(),
            message,
        };
        if self.sender.send(tracing_message).is_err() {
            // If we can't send the tracing message, there's not much we can do
            // The receiver has probably been dropped
        }
    }
}
