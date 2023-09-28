/*!
The Enpoint API.

This API is used to start and manage a new endpoint, aka service, and to publish on the EDA Bus.

# Quickstart

```
use eventail::endpoint::{Config, Endpoint};

#[tokio::main]
async fn main() {

    let config = Config {
        prefetch_count: 3,
        logical_service: String::from("my_service_name"),
        hosts: vec![String::from("amqp://localhost")],
        event_subscriptions: vec![],
        command_subscriptions: vec![],
        configuration_subscriptions: vec![],
    };
    let (endpoint, _client_rx) = Endpoint::new(config);
    // do something
    // ...
    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
}
```
*/

use log::debug;
use serde::Serialize;

use tokio::sync::mpsc::{error, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinError, JoinHandle};

mod gelf;
mod manager;
pub(crate) mod shipment;

pub use super::Delivery;
pub use gelf::Criticity;
use gelf::GelfPayload;
use manager::{run, EndpointManager, Message};
use shipment::Shipment;

#[derive(Debug)]
pub enum Error {
    EndpointDown,
    Shutdown(JoinError),
}

impl From<error::SendError<Message>> for Error {
    fn from(_value: error::SendError<Message>) -> Self {
        Self::EndpointDown
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Shutdown(value)
    }
}

/// Events sent by the endpoint and consumed by the user code.
pub enum ClientMessage {
    ClockTicked(u64),
    CommandReceived(Box<Delivery>),
    ConfigurationReceived(Box<Delivery>),
    EventReceived(Box<Delivery>),
    MessageReturned(String),
    Ready,
    ResultReceived(Box<Delivery>),
    WaitingForConfiguration,
}

/// Configure the Enpoint.
pub struct Config {
    pub prefetch_count: u16,
    pub hosts: Vec<String>,
    pub logical_service: String,
    pub event_subscriptions: Vec<String>,
    pub command_subscriptions: Vec<String>,
    pub configuration_subscriptions: Vec<String>,
}

/// The endpoint API.
pub struct Endpoint {
    endpoint_tx: UnboundedSender<Message>,
    pub force_json: bool,
    manager_handle: JoinHandle<()>,
    logical_service: String,
}

impl Endpoint {
    /// Create and start a new endpoint.
    ///
    /// A background task is launched that manages the connection and the topology and automatically
    /// switches over or reconnects in case of network or broker failure.
    /// Return an `Endpoint` and a channel receiver for EDA and local messages.
    /// The background task should be cleanly shutdown when not needed anymore with the `.shutdown()` method.
    pub fn new(config: Config) -> (Self, UnboundedReceiver<ClientMessage>) {
        let (endpoint_tx, endpoint_rx) = unbounded_channel();
        let (client_tx, client_rx) = unbounded_channel();
        let manager = EndpointManager::new(endpoint_tx.clone(), endpoint_rx, client_tx);
        let logical_service = config.logical_service.clone();
        let manager_handle = run(manager, config);
        (
            Self {
                endpoint_tx,
                manager_handle,
                force_json: false,
                logical_service,
            },
            client_rx,
        )
    }

    /// Send a new Event on the EDA bus.
    ///
    /// * `event` is the name (class name) of the event;
    /// * `message` is the event payload;
    /// * `conversation_id` is the mandatory conversation ID;
    /// * if `mandatory` is true, a `ClientMessage::MessageReturned(_)` will be sent if the event is
    /// unroutable, that is, the exchange did not find any queue to put it in.
    pub fn publish_event(
        &self,
        event: &str,
        message: &impl Serialize,
        conversation_id: &str,
        mandatory: bool,
    ) -> Result<(), Error> {
        Ok(self
            .endpoint_tx
            .send(Message::PublishMain(Box::new(Shipment::new_event(
                event,
                message,
                conversation_id,
                mandatory,
                self.force_json,
            ))))?)
    }

    /// Send a new Command on the EDA bus.
    ///
    /// * `command` is the name (class name) of the command;
    /// * `message` is the command payload;
    /// * `conversation_id` is the mandatory conversation ID;
    /// * `reply_to`: is the routing key to send the result to;
    /// * `correlation_id`: is the mandatory correlation ID used to reconcile the result with the command;
    /// * if `mandatory` is true, a `ClientMessage::MessageReturned(_)` will be sent if the command is
    /// unroutable, that is, the exchange did not find any queue to put it in.
    pub fn send_command(
        &self,
        command: &str,
        message: &impl Serialize,
        conversation_id: &str,
        reply_to: &str,
        correlation_id: &str,
        mandatory: bool,
    ) -> Result<(), Error> {
        Ok(self
            .endpoint_tx
            .send(Message::PublishMain(Box::new(Shipment::new_command(
                command,
                message,
                conversation_id,
                reply_to,
                correlation_id,
                mandatory,
                self.force_json,
            ))))?)
    }

    /// Send a new successful Result on the EDA bus.
    ///
    /// * `destination` is routing key to which the result is to be sent;
    /// * `message` is the result payload;
    /// * `conversation_id` is the mandatory conversation ID;
    /// * `correlation_id`: is the mandatory correlation ID used to reconcile the result with the command;
    /// * if `mandatory` is true, a `ClientMessage::MessageReturned(_)` will be sent if the result is
    /// unroutable, that is, the exchange did not find any queue to put it in.
    pub fn return_success(
        &self,
        destination: &str,
        message: &impl Serialize,
        conversation_id: &str,
        correlation_id: &str,
        mandatory: bool,
    ) -> Result<(), Error> {
        Ok(self
            .endpoint_tx
            .send(Message::PublishMain(Box::new(Shipment::new_result(
                destination,
                "success",
                message,
                conversation_id,
                correlation_id,
                mandatory,
                self.force_json,
            ))))?)
    }

    /// Send a new failure Result on the EDA bus.
    ///
    /// * `destination` is routing key to which the result is to be sent;
    /// * `message` is the result payload;
    /// * `conversation_id` is the mandatory conversation ID;
    /// * `correlation_id`: is the mandatory correlation ID used to reconcile the result with the command;
    /// * if `mandatory` is true, a `ClientMessage::MessageReturned(_)` will be sent if the result is
    /// unroutable, that is, the exchange did not find any queue to put it in.
    pub fn return_error(
        &self,
        destination: &str,
        message: &impl Serialize,
        conversation_id: &str,
        correlation_id: &str,
        mandatory: bool,
    ) -> Result<(), Error> {
        Ok(self
            .endpoint_tx
            .send(Message::PublishMain(Box::new(Shipment::new_result(
                destination,
                "error",
                message,
                conversation_id,
                correlation_id,
                mandatory,
                self.force_json,
            ))))?)
    }

    /// Log on the EDA log bus.
    pub fn log(
        &self,
        criticity: Criticity,
        short_message: &str,
        full_message: &str,
        conversation_id: &str,
    ) -> Result<(), Error> {
        debug!("{}: {short_message}\n{full_message}", criticity.label());
        let payload = GelfPayload::new(
            criticity,
            short_message,
            full_message,
            conversation_id,
            &self.logical_service,
        );
        Ok(self
            .endpoint_tx
            .send(Message::PublishLog(Box::new(Shipment::new_log(
                &payload.routing_key(),
                &payload,
                conversation_id,
            ))))?)
    }

    /// Tell the endpoint that the client is configured and ready to accept events and commands.
    ///
    /// You don't need to call this method explicitly if you use the [`Callback API`](crate::service).
    pub fn configured(&self) -> Result<(), Error> {
        self.endpoint_tx.send(Message::Configured)?;
        Ok(())
    }

    /// Tell the broker that the message was successfully processed and can be safely removed from the queue.
    ///
    /// You don't need to call this method explicitly if you use the [`Callback API`](crate::service).
    pub fn ack_delivery(&self, delivery_tag: u64) -> Result<(), Error> {
        self.endpoint_tx.send(Message::AckDelivery(delivery_tag))?;
        Ok(())
    }

    /// Tell the broker that the message was not properly handled (transient error) and should be requeued to
    /// be retried later.
    ///
    /// You don't need to call this method explicitly if you use the [`Callback API`](crate::service).
    pub fn nack_delivery(&self, delivery_tag: u64) -> Result<(), Error> {
        self.endpoint_tx.send(Message::NackDelivery(delivery_tag))?;
        Ok(())
    }

    /// Tell the broker that the message in unprocessable and must be rejected (to dead-letter).
    ///
    /// You don't need to call this method explicitly if you use the [`Callback API`](crate::service).
    pub fn reject_delivery(&self, delivery_tag: u64) -> Result<(), Error> {
        self.endpoint_tx.send(Message::AckDelivery(delivery_tag))?;
        Ok(())
    }

    /// Shutdown the endpoint gracefully.
    pub async fn shutdown(self) -> Result<(), Error> {
        self.endpoint_tx.send(Message::Shutdown)?;
        self.manager_handle.await?;
        Ok(())
    }
}
