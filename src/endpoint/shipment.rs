use amqprs::channel::BasicPublishArguments;
use amqprs::{BasicProperties, FieldTable};
use serde::Serialize;
use tokio::time::Instant;

use super::manager::{COMMAND_EXCHANGE, CONFIG_EXCHANGE, EVENT_EXCHANGE, LOG_EXCHANGE};

#[derive(Clone)]
pub struct Shipment {
    pub properties: BasicProperties,
    pub args: BasicPublishArguments,
    pub payload: Vec<u8>,
    pub sent: Instant,
}

impl Shipment {
    pub fn new(properties: BasicProperties, args: BasicPublishArguments, payload: Vec<u8>) -> Self {
        Self {
            properties,
            args,
            payload,
            sent: Instant::now(),
        }
    }

    fn make_payload(message: &impl Serialize, json: bool) -> (Vec<u8>, &'static str) {
        let payload = if json {
            serde_json::to_vec(message).unwrap()
        } else {
            cbor4ii::serde::to_vec(Vec::new(), message).unwrap()
        };
        let content_type = if json {
            "application/json"
        } else {
            "application/cbor"
        };
        (payload, content_type)
    }

    fn make_headers(conversation_id: &str) -> FieldTable {
        let mut headers = FieldTable::new();
        headers.insert(
            "conversation_id".try_into().unwrap(),
            conversation_id.into(),
        );
        headers
    }

    pub fn new_event(
        event: &str,
        message: &impl Serialize,
        conversation_id: &str,
        mandatory: bool,
        json: bool,
    ) -> Self {
        let (payload, content_type) = Self::make_payload(message, json);
        let headers = Self::make_headers(conversation_id);
        let properties = BasicProperties::default()
            .with_headers(headers)
            .with_persistence(true)
            .with_content_type(content_type)
            .finish();
        let mut args = BasicPublishArguments::new(EVENT_EXCHANGE, event);
        args.mandatory = mandatory;
        Self::new(properties, args, payload)
    }

    pub fn new_command(
        command: &str,
        message: &impl Serialize,
        conversation_id: &str,
        reply_to: &str,
        correlation_id: &str,
        mandatory: bool,
        json: bool,
    ) -> Self {
        let (payload, content_type) = Self::make_payload(message, json);
        let headers = Self::make_headers(conversation_id);
        let properties = BasicProperties::default()
            .with_headers(headers)
            .with_persistence(true)
            .with_content_type(content_type)
            .with_correlation_id(correlation_id)
            .with_reply_to(reply_to)
            .finish();
        let mut args = BasicPublishArguments::new(COMMAND_EXCHANGE, command);
        args.mandatory = mandatory;
        Self::new(properties, args, payload)
    }

    pub fn new_result(
        destination: &str,
        status: &str,
        message: &impl Serialize,
        conversation_id: &str,
        correlation_id: &str,
        mandatory: bool,
        json: bool,
    ) -> Self {
        let (payload, content_type) = Self::make_payload(message, json);
        let mut headers = Self::make_headers(conversation_id);
        headers.insert("status".try_into().unwrap(), status.into());
        let properties = BasicProperties::default()
            .with_headers(headers)
            .with_persistence(true)
            .with_content_type(content_type)
            .with_correlation_id(correlation_id)
            .finish();
        let mut args = BasicPublishArguments::new(COMMAND_EXCHANGE, destination);
        args.mandatory = mandatory;
        Self::new(properties, args, payload)
    }

    pub fn new_configuration(
        config: &str,
        message: &impl Serialize,
        conversation_id: &str,
        json: bool,
    ) -> Self {
        let (payload, content_type) = Self::make_payload(message, json);
        let headers = Self::make_headers(conversation_id);
        let properties = BasicProperties::default()
            .with_headers(headers)
            .with_persistence(true)
            .with_content_type(content_type)
            .finish();
        let args = BasicPublishArguments::new(CONFIG_EXCHANGE, config);
        Self::new(properties, args, payload)
    }

    pub fn new_log(routing_key: &str, message: &impl Serialize, conversation_id: &str) -> Self {
        let (payload, content_type) = Self::make_payload(message, true);
        let headers = Self::make_headers(conversation_id);
        let properties = BasicProperties::default()
            .with_headers(headers)
            .with_content_type(content_type)
            .finish();
        let args = BasicPublishArguments::new(LOG_EXCHANGE, routing_key);
        Self::new(properties, args, payload)
    }
}
