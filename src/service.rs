//! Callback API and helpers for implementing services.
//!

use log::error;
use tokio::sync::mpsc::UnboundedReceiver;

use super::delivery::{Delivery, Error as DError};
use super::endpoint::{ClientMessage as Message, Criticity, Endpoint, Error as EError};

/// Error type for message handlers.
/// In case of recoverable error, the error is logged and the message is requeued (only once).
/// In case of critical error, the error is logged and the message is rejected to the dead-letter.
pub enum Error {
    Recoverable(String),
    Critical(String),
}

impl From<DError> for Error {
    fn from(value: DError) -> Self {
        match value {
            DError::Deserialization(msg) => Self::Critical(msg),
        }
    }
}

/// The traditional Callback API
///
/// A service should be run with [`run_service`].
pub trait Service {
    /// Called when the Endpoint starts consuming the subscribed messages.
    fn on_ready(&mut self, endpoint: &Endpoint);
    /// Called when the Endpoint is connected and Configuration events were subscribed.
    ///
    /// Usually, the implementation publishes a configuration request event on the EDA Bus.
    fn on_waiting_for_configuration(&mut self, endpoint: &Endpoint);
    /// Called when a subscribed configuration event is received.
    ///
    /// The implementation returns `Ok(true)` only if it is fully configured and ready to
    /// consume business events.
    fn on_configuration(
        &mut self,
        endpoint: &Endpoint,
        message: Box<Delivery>,
    ) -> Result<bool, Error>;
    /// Called when a subscribed event is received.
    ///
    /// If an [`Error`] is returned, the message is acknowledged accordingly.
    fn on_event(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    /// Called when a subscribed command is received.
    ///
    /// If an [`Error`] is returned, the message is acknowledged accordingly.
    fn on_command(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    /// Called when a subscribed result is received.
    ///
    /// If an [`Error`] is returned, the message is acknowledged accordingly.
    fn on_result(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    /// Called when a message sent with the `mandatory` flag true is unroutable.
    fn on_message_returned(&mut self, endpoint: &Endpoint, routing_key: String);
    /// This method is called every second with the `time` elapsed seconds since the endpoint was started.
    fn on_clock_ticked(&mut self, endpoint: &Endpoint, time: u64);
}

/// Run the service forever, consuming the [`Message`] events and triggering the callbacks.
pub async fn run_service<T: Service>(
    mut service: T,
    endpoint: &Endpoint,
    mut client_rx: UnboundedReceiver<Message>,
) {
    while let Some(message) = client_rx.recv().await {
        match message {
            Message::ClockTicked(time) => service.on_clock_ticked(endpoint, time),
            Message::CommandReceived(msg) => {
                safely_apply_sync(endpoint, msg, |e, d| service.on_command(e, d))
                    .unwrap_or_else(|_| error!("Endpoint dropped!"));
            }
            Message::ConfigurationReceived(_msg) => todo!(),
            Message::EventReceived(msg) => {
                safely_apply_sync(endpoint, msg, |e, d| service.on_event(e, d))
                    .unwrap_or_else(|_| error!("Endpoint dropped!"));
            }
            Message::MessageReturned(routing_key) => {
                service.on_message_returned(endpoint, routing_key)
            }
            Message::Ready => service.on_ready(endpoint),
            Message::ResultReceived(msg) => {
                safely_apply_sync(endpoint, msg, |e, d| service.on_result(e, d))
                    .unwrap_or_else(|_| error!("Endpoint dropped!"));
            }
            Message::WaitingForConfiguration => service.on_waiting_for_configuration(endpoint),
        }
    }
}

/// Execute the synchronous `callback` to process the `delivery` in a transaction like manner.
/// The `delivery` is ACKed, NACKed (and requeued) or REJECTed depending on the `Error` returned.
pub fn safely_apply_sync<F>(
    endpoint: &Endpoint,
    delivery: Box<Delivery>,
    mut callback: F,
) -> Result<(), EError>
where
    F: FnMut(&Endpoint, Box<Delivery>) -> Result<(), Error>,
{
    let delivery_tag = delivery.tag();
    let message_type = delivery.routing_key().clone();
    let conversation_id = delivery.conversation_id().unwrap().clone();
    let redelivered = delivery.redelivered();
    match callback(endpoint, delivery) {
        Ok(_) => endpoint.ack_delivery(delivery_tag),
        Err(Error::Recoverable(msg)) => {
            endpoint.log(
                Criticity::Critical,
                &format!("Error processing {message_type}"),
                &msg,
                &conversation_id,
            )?;
            if redelivered {
                endpoint.log(
                    Criticity::Emergency,
                    &format!("Too many retries for {message_type}; giving up!"),
                    "rejecting message",
                    &conversation_id,
                )?;
                endpoint.reject_delivery(delivery_tag)
            } else {
                endpoint.nack_delivery(delivery_tag)
            }
        }
        Err(Error::Critical(msg)) => {
            endpoint.log(
                Criticity::Emergency,
                &format!("Unrecoverable error processing {message_type}; giving up!"),
                &msg,
                &conversation_id,
            )?;
            endpoint.reject_delivery(delivery_tag)
        }
    }
}

/// Execute the async `callback` to process the `delivery` in a transaction like manner.
/// The `delivery` is ACKed, NACKed (and requeued) or REJECTed depending on the `Error` returned.
pub async fn safely_apply<'a, F, Fut>(
    endpoint: &'a Endpoint,
    delivery: Box<Delivery>,
    mut callback: F,
) -> Result<(), EError>
where
    F: FnMut(&'a Endpoint, Box<Delivery>) -> Fut,
    Fut: std::future::Future<Output = Result<(), Error>>,
{
    let delivery_tag = delivery.tag();
    let message_type = delivery.routing_key().clone();
    let conversation_id = delivery.conversation_id().unwrap().clone();
    let redelivered = delivery.redelivered();
    match callback(endpoint, delivery).await {
        Ok(_) => endpoint.ack_delivery(delivery_tag),
        Err(Error::Recoverable(msg)) => {
            endpoint.log(
                Criticity::Critical,
                &format!("Error processing {message_type}"),
                &msg,
                &conversation_id,
            )?;
            if redelivered {
                endpoint.log(
                    Criticity::Emergency,
                    &format!("Too many retries for {message_type}; giving up!"),
                    "rejecting message",
                    &conversation_id,
                )?;
                endpoint.reject_delivery(delivery_tag)
            } else {
                endpoint.nack_delivery(delivery_tag)
            }
        }
        Err(Error::Critical(msg)) => {
            endpoint.log(
                Criticity::Emergency,
                &format!("Unrecoverable error processing {message_type}; giving up!"),
                &msg,
                &conversation_id,
            )?;
            endpoint.reject_delivery(delivery_tag)
        }
    }
}
