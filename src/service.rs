use log::error;
use tokio::sync::mpsc::UnboundedReceiver;

use super::delivery::{Delivery, Error as DError};
use super::endpoint::{ClientMessage as Message, Criticity, Endpoint, Error as EError};

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

/// The traditional
pub trait Service {
    fn on_ready(&mut self, endpoint: &Endpoint);
    fn on_waiting_for_configuration(&mut self, endpoint: &Endpoint);
    /// A configuration message arrived.
    /// Return `true` if the configuration is complete.
    fn on_configuration(
        &mut self,
        endpoint: &Endpoint,
        message: Box<Delivery>,
    ) -> Result<bool, Error>;
    fn on_event(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    fn on_command(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    fn on_result(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error>;
    fn on_message_returned(&mut self, endpoint: &Endpoint, routing_key: String);
    fn on_clock_ticked(&mut self, endpoint: &Endpoint, time: u64);
}

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
