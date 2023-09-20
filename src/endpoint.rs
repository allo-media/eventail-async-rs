use tokio::sync::mpsc::{error, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinError, JoinHandle};

mod manager;

use manager::{run, EndpointManager, Message};

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
}

impl Endpoint {
    pub fn new(config: Config) -> (Self, UnboundedReceiver<ClientMessage>) {
        let (endpoint_tx, endpoint_rx) = unbounded_channel();
        let (client_tx, client_rx) = unbounded_channel();
        let manager = EndpointManager::new(endpoint_tx.clone(), endpoint_rx, client_tx);
        let manager_handle = run(manager, config);
        (
            Self {
                endpoint_tx,
                manager_handle,
                force_json: false,
            },
            client_rx,
        )
    }

    pub async fn shutdown(self) -> Result<(), Error> {
        self.endpoint_tx.send(Message::Shutdown)?;
        self.manager_handle.await?;
        Ok(())
    }
}
