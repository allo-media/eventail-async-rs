use std::collections::BTreeMap;

use amqprs::callbacks::{ChannelCallback, ConnectionCallback};
use amqprs::channel::{
    BasicQosArguments, Channel, ConfirmSelectArguments, ExchangeDeclareArguments,
};
use amqprs::connection::Connection;
use amqprs::error::Error;
use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};

use super::shipment::Shipment;
use super::{ClientMessage, Config};

pub const EVENT_EXCHANGE: &'static str = "events";
pub const COMMAND_EXCHANGE: &'static str = "commands";
pub const CONFIG_EXCHANGE: &'static str = "configurations";
pub const LOG_EXCHANGE: &'static str = "logs";

const COLD_DELAY: Duration = Duration::from_secs(30);

pub enum Message {
    ChannelClose,
    ClockTicked(u64),
    ConnectionBlocked,
    ConnectionClose,
    ConnectionUnblocked,
    ConsumerCancel(String),
    PublishAck(amqprs::Ack),
    PublishLog(Box<Shipment>),
    PublishMain(Box<Shipment>),
    PublishNack(amqprs::Nack),
    PublishReturn(String),
    Shutdown,
}

pub struct EndpointManager {
    endpoint_tx: mpsc::UnboundedSender<Message>,
    endpoint_rx: mpsc::UnboundedReceiver<Message>,
    client_tx: mpsc::UnboundedSender<ClientMessage>,
    connection: Option<Connection>,
    main_channel: Option<Channel>,
    log_channel: Option<Channel>,
    next_message_tag: u64,
    pending_publishes: BTreeMap<u64, Shipment>,
}

impl EndpointManager {
    pub fn new(
        endpoint_tx: mpsc::UnboundedSender<Message>,
        endpoint_rx: mpsc::UnboundedReceiver<Message>,
        client_tx: mpsc::UnboundedSender<ClientMessage>,
    ) -> Self {
        Self {
            endpoint_tx,
            endpoint_rx,
            client_tx,
            connection: None,
            main_channel: None,
            log_channel: None,
            next_message_tag: 1,
            pending_publishes: BTreeMap::new(),
        }
    }

    /// Setup connection and topology
    async fn try_setup(&mut self, host: &str, prefetch_count: u16) -> Result<(), Error> {
        let connection = Connection::open(&host.try_into().unwrap()).await?;
        connection
            .register_callback(ConnectionCallbackProxy::new(self.endpoint_tx.clone()))
            .await?;

        let main_channel = connection.open_channel(None).await?;
        main_channel
            .register_callback(ChannelCallbackProxy::new(self.endpoint_tx.clone()))
            .await?;
        main_channel
            .confirm_select(ConfirmSelectArguments::default())
            .await?;
        main_channel
            .basic_qos(
                BasicQosArguments::default()
                    .prefetch_count(prefetch_count)
                    .finish(),
            )
            .await?;
        main_channel
            .exchange_declare(
                ExchangeDeclareArguments::new(EVENT_EXCHANGE, "topic")
                    .durable(true)
                    .finish(),
            )
            .await?;
        main_channel
            .exchange_declare(
                ExchangeDeclareArguments::new(COMMAND_EXCHANGE, "topic")
                    .durable(true)
                    .finish(),
            )
            .await?;
        main_channel
            .exchange_declare(
                ExchangeDeclareArguments::new(CONFIG_EXCHANGE, "topic")
                    .durable(true)
                    .finish(),
            )
            .await?;

        let log_channel = connection.open_channel(None).await?;
        log_channel
            .register_callback(ChannelCallbackProxy::new(self.endpoint_tx.clone()))
            .await?;
        main_channel
            .exchange_declare(
                ExchangeDeclareArguments::new(LOG_EXCHANGE, "topic")
                    .durable(true)
                    .finish(),
            )
            .await?;

        self.connection.replace(connection);
        self.main_channel.replace(main_channel);
        self.log_channel.replace(log_channel);
        self.next_message_tag = 1;
        Ok(())
    }

    async fn cleanup(&mut self) {
        info!("Disconnecting…");
        if let Some(c) = self.log_channel.take() {
            c.close().await.ok();
        };
        if let Some(c) = self.main_channel.take() {
            c.close().await.ok();
        };
        if let Some(c) = self.connection.take() {
            c.close().await.ok();
        };
    }

    async fn publish_main(&mut self, mut shipment: Shipment) {
        shipment.sent = Instant::now();
        self.pending_publishes
            .insert(self.next_message_tag, shipment.clone());
        self.next_message_tag += 1;
        if let Some(ref c) = self.main_channel {
            c.basic_publish(shipment.properties, shipment.payload, shipment.args)
                .await
                .unwrap_or_else(|e| warn!("Unable to publish on main: {e}; will retry later."))
        }
    }

    fn publish_ack(&mut self, delivery_tag: u64, multiple: bool) {
        debug!("publish confirm for {delivery_tag} (multiple {multiple})");
        if multiple {
            let kept = self.pending_publishes.split_off(&(delivery_tag + 1));
            self.pending_publishes = kept;
        } else {
            self.pending_publishes.remove(&delivery_tag);
        }
        debug!(
            "{} message(s) pending confirmation",
            self.pending_publishes.len()
        );
    }

    async fn publish_nack(&mut self, delivery_tag: u64, multiple: bool) {
        debug!("re-publish rejected or old pending messages up to {delivery_tag} (multiple {multiple})");
        if multiple {
            let kept = self.pending_publishes.split_off(&(delivery_tag + 1));
            let removed = std::mem::replace(&mut self.pending_publishes, kept);
            debug!("{} message to resend", removed.len());
            for shipment in removed.into_values() {
                self.publish_main(shipment).await
            }
        } else if let Some(shipment) = self.pending_publishes.remove(&delivery_tag) {
            self.publish_main(shipment).await
        }
    }

    async fn refresh_pending_publishes(&mut self) {
        debug!("refreshing pending confirms…");
        let first_fresh_seqno = self.pending_publishes.iter().rev().find_map(|(d, s)| {
            if s.sent.elapsed() >= COLD_DELAY {
                Some(*d)
            } else {
                None
            }
        });
        if let Some(seqno) = first_fresh_seqno {
            self.publish_nack(seqno, true).await
        }
    }

    async fn publish_log(&self, shipment: Shipment) {
        if let Some(ref c) = self.log_channel {
            c.basic_publish(shipment.properties, shipment.payload, shipment.args)
                .await
                .unwrap_or_else(|e| warn!("Unable to publish on log: {e}; log message dropped."))
        }
    }

    async fn setup(&mut self, config: &Config) {
        for host in config.hosts.iter() {
            if let Err(err) = self.try_setup(host, config.prefetch_count).await {
                warn!(
                    "Unable to connect to {}: {err}",
                    host.split('@').last().unwrap()
                );
            } else {
                break;
            }
        }
        if self.connection.is_none() {
            error!("Unable to connect to EDA! Will retry later.");
        } else {
            info!("Connected to EDA bus.");
        }
    }

    async fn run(&mut self, config: Config) {
        self.setup(&config).await;
        while let Some(msg) = self.endpoint_rx.recv().await {
            match msg {
                Message::ChannelClose => self.cleanup().await,
                Message::ClockTicked(t) => {
                    if self.client_tx.send(ClientMessage::ClockTicked(t)).is_err() {
                        warn!("no more client!");
                    }
                    if t % 4 == 0 && self.connection.is_none() {
                        self.setup(&config).await;
                        if self.connection.is_some() && !self.pending_publishes.is_empty() {
                            self.publish_nack(
                                *self.pending_publishes.last_key_value().unwrap().0,
                                true,
                            )
                            .await;
                        }
                    }
                    if t % 16 == 0 {
                        self.refresh_pending_publishes().await
                    }
                }
                Message::ConnectionBlocked => (), // TODO later
                Message::ConnectionClose => self.cleanup().await,
                Message::ConnectionUnblocked => (), // TODO later
                Message::ConsumerCancel(_) => todo!(),
                Message::PublishAck(ack) => self.publish_ack(ack.delivery_tag(), ack.mutiple()),
                Message::PublishLog(shipment) => self.publish_log(*shipment).await,
                Message::PublishMain(shipment) => self.publish_main(*shipment).await,
                Message::PublishNack(nack) => {
                    self.publish_nack(nack.delivery_tag(), nack.multiple())
                        .await
                }
                Message::PublishReturn(_) => todo!(),
                Message::Shutdown => break,
            }
        }
        self.cleanup().await
    }
}

async fn clock(endpoint_tx: mpsc::UnboundedSender<Message>) {
    let mut clock = interval(Duration::from_secs(1));
    let mut uptime: u64 = 0;
    loop {
        clock.tick().await;
        uptime += 1;
        if endpoint_tx.send(Message::ClockTicked(uptime)).is_err() {
            break;
        }
    }
}

pub fn run(mut manager: EndpointManager, config: Config) -> tokio::task::JoinHandle<()> {
    tokio::spawn(clock(manager.endpoint_tx.clone()));
    tokio::spawn(async move { manager.run(config).await })
}

struct ConnectionCallbackProxy {
    endpoint_tx: mpsc::UnboundedSender<Message>,
}

impl ConnectionCallbackProxy {
    fn new(endpoint_tx: mpsc::UnboundedSender<Message>) -> Self {
        Self { endpoint_tx }
    }
}

#[async_trait]
impl ConnectionCallback for ConnectionCallbackProxy {
    async fn close(&mut self, _connection: &Connection, close: amqprs::Close) -> Result<(), Error> {
        error!("Disconnected: {close}");
        self.endpoint_tx
            .send(Message::ConnectionClose)
            .map_err(|_| Error::ConnectionCloseError("no endpoint!".to_owned()))
    }

    async fn blocked(&mut self, _connection: &Connection, reason: String) -> () {
        warn!("Connection blocked: {reason}");
        self.endpoint_tx.send(Message::ConnectionBlocked).ok();
    }

    async fn unblocked(&mut self, _connection: &Connection) -> () {
        info!("Connection unblocked");
        self.endpoint_tx.send(Message::ConnectionUnblocked).ok();
    }
}

struct ChannelCallbackProxy {
    endpoint_tx: mpsc::UnboundedSender<Message>,
}

impl ChannelCallbackProxy {
    fn new(endpoint_tx: mpsc::UnboundedSender<Message>) -> Self {
        Self { endpoint_tx }
    }
}

#[async_trait]
impl ChannelCallback for ChannelCallbackProxy {
    async fn close(
        &mut self,
        _channel: &Channel,
        close: amqprs::CloseChannel,
    ) -> Result<(), Error> {
        error!("Channel closed: {close}");
        self.endpoint_tx
            .send(Message::ChannelClose)
            .map_err(|_| Error::ChannelCloseError("no endpoint!".to_owned()))
    }
    async fn cancel(&mut self, _channel: &Channel, cancel: amqprs::Cancel) -> Result<(), Error> {
        self.endpoint_tx
            .send(Message::ConsumerCancel(cancel.consumer_tag().to_owned()))
            .map_err(|_| Error::InternalChannelError("no endpoint!".to_owned()))
    }
    async fn flow(&mut self, _channel: &Channel, active: bool) -> Result<bool, Error> {
        info!("receive flow request from broker: flow = {active}");
        Ok(true)
    }
    async fn publish_ack(&mut self, _channel: &Channel, ack: amqprs::Ack) {
        self.endpoint_tx.send(Message::PublishAck(ack)).ok();
    }
    async fn publish_nack(&mut self, _channel: &Channel, nack: amqprs::Nack) {
        self.endpoint_tx.send(Message::PublishNack(nack)).ok();
    }
    async fn publish_return(
        &mut self,
        _channel: &Channel,
        ret: amqprs::Return,
        _basic_properties: amqprs::BasicProperties,
        _content: Vec<u8>,
    ) {
        self.endpoint_tx
            .send(Message::PublishReturn(ret.routing_key().to_owned()))
            .ok();
    }
}
