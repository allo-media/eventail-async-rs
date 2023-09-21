use serde::Serialize;

use tokio::sync::mpsc::{error, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinError, JoinHandle};

mod gelf;
mod manager;
pub(crate) mod shipment;

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

pub enum ClientMessage {
    ClockTicked(u64),
    Ready,
    WaitingForConfiguration,
}

pub struct Config {
    pub prefetch_count: u16,
    pub hosts: Vec<String>,
    pub logical_service: String,
}

pub struct Endpoint {
    endpoint_tx: UnboundedSender<Message>,
    pub force_json: bool,
    manager_handle: JoinHandle<()>,
    logical_service: String,
}

impl Endpoint {
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

    pub fn log(
        &self,
        criticity: Criticity,
        short_message: &str,
        full_message: &str,
        conversation_id: &str,
    ) -> Result<(), Error> {
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

    pub async fn shutdown(self) -> Result<(), Error> {
        self.endpoint_tx.send(Message::Shutdown)?;
        self.manager_handle.await?;
        Ok(())
    }
}
