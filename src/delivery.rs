use amqprs::BasicProperties;
use amqprs::Deliver;
use serde::Deserialize;

pub struct Delivery {
    deliver: Deliver,
    properties: BasicProperties,
    pub payload: Vec<u8>,
}

pub enum Error {
    Deserialization(String),
}

impl<T: std::error::Error> From<T> for Error {
    fn from(value: T) -> Self {
        Error::Deserialization(value.to_string())
    }
}

impl Delivery {
    pub fn new(deliver: Deliver, properties: BasicProperties, payload: Vec<u8>) -> Self {
        Self {
            deliver,
            properties,
            payload,
        }
    }

    pub fn tag(&self) -> u64 {
        self.deliver.delivery_tag()
    }

    pub fn routing_key(&self) -> &String {
        self.deliver.routing_key()
    }

    pub fn content_type(&self) -> Option<&String> {
        self.properties.content_type()
    }

    pub fn redelivered(&self) -> bool {
        self.deliver.redelivered()
    }

    pub fn conversation_id(&self) -> Option<&String> {
        self.properties
            .headers()
            .and_then(|h| h.get(&"conversation_id".try_into().unwrap()))
            .and_then(|v| v.try_into().ok())
    }

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
