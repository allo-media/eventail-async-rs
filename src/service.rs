use log::error;
use tokio::sync::mpsc::UnboundedReceiver;

use super::delivery::{Delivery, Error as DError};
use super::endpoint::{ClientMessage as Message, Criticity, Endpoint};

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
            Message::ClockTicked(time) => service.on_clock_ticked(&endpoint, time),
            Message::CommandReceived(msg) => {
                transaction_call(&endpoint, msg, |e, d| service.on_command(e, d)).await
            }
            Message::ConfigurationReceived(_msg) => todo!(),
            Message::EventReceived(msg) => {
                transaction_call(&endpoint, msg, |e, d| service.on_event(e, d)).await
            }
            Message::MessageReturned(routing_key) => {
                service.on_message_returned(&endpoint, routing_key)
            }
            Message::Ready => service.on_ready(&endpoint),
            Message::ResultReceived(msg) => {
                transaction_call(&endpoint, msg, |e, d| service.on_result(e, d)).await
            }
            Message::WaitingForConfiguration => service.on_waiting_for_configuration(&endpoint),
        }
    }
}

pub async fn transaction_call<F>(endpoint: &Endpoint, delivery: Box<Delivery>, mut callback: F)
where
    F: FnMut(&Endpoint, Box<Delivery>) -> Result<(), Error>,
{
    let delivery_tag = delivery.tag();
    let message_type = delivery.routing_key().clone();
    let conversation_id = delivery.conversation_id().unwrap().clone();
    let redelivered = delivery.redelivered();
    match callback(endpoint, delivery) {
        Ok(_) => endpoint
            .ack_delivery(delivery_tag)
            .unwrap_or_else(|_| error!("Endpoint dropped!")),
        Err(Error::Recoverable(msg)) => {
            endpoint
                .log(
                    Criticity::Critical,
                    &format!("Error processing {message_type}"),
                    &msg,
                    &conversation_id,
                )
                .ok();
            if redelivered {
                endpoint
                    .log(
                        Criticity::Emergency,
                        &format!("Too many retries for {message_type}; giving up!"),
                        "rejecting message",
                        &conversation_id,
                    )
                    .ok();
                endpoint
                    .reject_delivery(delivery_tag)
                    .unwrap_or_else(|_| error!("Endpoint dropped!"));
            } else {
                endpoint
                    .nack_delivery(delivery_tag)
                    .unwrap_or_else(|_| error!("Endpoint dropped!"));
            }
        }
        Err(Error::Critical(msg)) => {
            endpoint
                .log(
                    Criticity::Emergency,
                    &format!("Unrecoverable error processing {message_type}; giving up!"),
                    &msg,
                    &conversation_id,
                )
                .ok();
            endpoint
                .reject_delivery(delivery_tag)
                .unwrap_or_else(|_| error!("Endpoint dropped!"));
        }
    }
}
