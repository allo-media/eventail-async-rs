//! `Delivery` represents messages received from the EDA Bus.

use amqprs::BasicProperties;
use amqprs::Deliver;
use serde::Deserialize;

/// A message received from the EDA bus
pub struct Delivery {
    deliver: Deliver,
    properties: BasicProperties,
    payload: Vec<u8>,
}

/// Error type for `Delivery` methods.
pub enum Error {
    Deserialization(String),
}

impl<T: std::error::Error> From<T> for Error {
    fn from(value: T) -> Self {
        Error::Deserialization(value.to_string())
    }
}

impl Delivery {
    /// Create a new `Delivery` from lower level parts.
    /// This method is private to the crate.
    pub fn new(deliver: Deliver, properties: BasicProperties, payload: Vec<u8>) -> Self {
        Self {
            deliver,
            properties,
            payload,
        }
    }

    /// Return the delivery tag.
    pub fn tag(&self) -> u64 {
        self.deliver.delivery_tag()
    }

    pub fn routing_key(&self) -> &String {
        self.deliver.routing_key()
    }

    pub fn content_type(&self) -> Option<&String> {
        self.properties.content_type()
    }

    /// Return true if the message was already presented to this or another worker (and was nack'ed).
    pub fn redelivered(&self) -> bool {
        self.deliver.redelivered()
    }

    pub fn conversation_id(&self) -> Option<&String> {
        self.properties
            .headers()
            .and_then(|h| h.get(&"conversation_id".try_into().unwrap()))
            .and_then(|v| v.try_into().ok())
    }

    /// For Results to Commands, indicates "success" or "error".
    pub fn status(&self) -> Option<&String> {
        self.properties
            .headers()
            .and_then(|h| h.get(&"status".try_into().unwrap()))
            .and_then(|v| v.try_into().ok())
    }

    pub fn correlation_id(&self) -> Option<&String> {
        self.properties.correlation_id()
    }

    pub fn exchange(&self) -> &String {
        self.deliver.exchange()
    }

    pub fn reply_to(&self) -> Option<&String> {
        self.properties.reply_to()
    }

    pub fn deserialize_payload<'a, T: Deserialize<'a>>(&'a self) -> Result<T, Error> {
        if let Some("application/json") = self.properties.content_type().map(|s| s.as_str()) {
            Ok(serde_json::from_slice(&self.payload)?)
        } else {
            Ok(serde_cbor::from_slice(&self.payload)?)
        }
    }
}
