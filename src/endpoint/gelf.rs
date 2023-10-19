//! GELF support
use std::collections::HashMap;
use std::time::SystemTime;

use serde::Serialize;
use serde_json::Value;

/// Syslog Criticity level
#[derive(Debug)]
pub enum Criticity {
    Emergency,
    Alert,
    Critical,
    Error,
    Warning,
    Notice,
    Info,
    Debug,
}

impl Criticity {
    /// Criticity as a human readable label
    pub fn label(&self) -> &'static str {
        match self {
            Self::Emergency => "EMERGENCY",
            Self::Alert => "ALERT",
            Self::Critical => "CRITICAL",
            Self::Error => "ERROR",
            Self::Warning => "WARNING",
            Self::Notice => "NOTICE",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
        }
    }
}

pub type ExtraFields = HashMap<String, Value>;

/// The GELF Payload we use for Service Logging, including custom fields
#[derive(Debug, Serialize)]
pub struct GelfPayload<'a> {
    version: &'a str,
    short_message: &'a str,
    full_message: &'a str,
    level: u8,
    #[serde(rename = "_levelname")]
    levelname: &'a str,
    host: &'a str,
    timestamp: f64,
    #[serde(rename = "_conversation_id")]
    conversation_id: &'a str,
    #[serde(rename = "_logical_service")]
    logical_service: &'a str,
    #[serde(rename = "_worker_pid")]
    worker_pid: u32,
    #[serde(flatten)]
    extra: ExtraFields,
}

impl<'a> GelfPayload<'a> {
    pub fn new(
        criticity: Criticity,
        short_message: &'a str,
        full_message: &'a str,
        conversation_id: &'a str,
        logical_service: &'a str,
        host: &'a str,
        extra: ExtraFields,
    ) -> Self {
        let mut escaped_extra: ExtraFields = HashMap::with_capacity(extra.len());
        for (key, value) in extra {
            escaped_extra.insert(format!("_{key}"), value);
        }
        Self {
            version: "1.1",
            short_message,
            full_message,
            levelname: criticity.label(),
            level: criticity as u8,
            conversation_id,
            logical_service,
            worker_pid: std::process::id(),
            host,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as f64
                / 1000.0,
            extra: escaped_extra,
        }
    }

    pub fn routing_key(&self) -> String {
        format!("{}.{}", self.logical_service, self.levelname)
    }
}
