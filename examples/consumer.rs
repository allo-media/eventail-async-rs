use std::env;

use eventail::endpoint::{ClientMessage as Message, Config, Criticity, Endpoint};
use serde::Deserialize;
use tokio::signal;
use tokio::sync::mpsc::UnboundedReceiver;

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Ping<'a> {
    who: &'a str,
    what: &'a str,
    comment: &'a str,
}

async fn consume(mut client_rx: UnboundedReceiver<Message>, endpoint: &Endpoint) {
    while let Some(msg) = client_rx.recv().await {
        match msg {
            Message::Ready => println!("Ready"),
            Message::EventReceived(delivery) => {
                if delivery.routing_key() != "Ping" {
                    println!("unsupported message {}", delivery.routing_key());
                    endpoint.nack_delivery(delivery.tag()).ok();
                } else {
                    let conversation_id = delivery.conversation_id().unwrap();
                    match delivery.deserialize_payload::<Ping>() {
                        Ok(ping) => {
                            println!("Received Ping {:?}", ping);
                            endpoint.ack_delivery(delivery.tag()).ok();
                        }
                        Err(e) => {
                            endpoint
                                .log(
                                    Criticity::Critical,
                                    "Unable to deserialize payload",
                                    &e.to_string(),
                                    conversation_id,
                                )
                                .ok();
                            endpoint.reject_delivery(delivery.tag()).ok();
                        }
                    }
                }
            }
            Message::ClockTicked(t) => {
                if t % 60 == 0 {
                    endpoint
                        .log(Criticity::Notice, "healthcheck", "I'm alive!", "")
                        .ok();
                }
            }
            _ => {
                println!("Ignoring unsupported message");
            }
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let hosts: Vec<String> = env::args().skip(1).collect();
    if hosts.is_empty() {
        panic!("Usage: publisher broker_uri [broker_uri…]");
    }
    let config = Config {
        prefetch_count: 20,
        logical_service: String::from("pong"),
        hosts,
        event_subscriptions: vec!["Ping".to_owned()],
        command_subscriptions: vec![],
        configuration_subscriptions: vec![],
    };
    let (endpoint, client_rx) = Endpoint::new(config);
    endpoint
        .log(
            Criticity::Info,
            "test consumer",
            "consumer starting…",
            "noparticularcivd",
        )
        .unwrap();

    tokio::select! {
        _ = signal::ctrl_c() => {println!("CTL-C: exited")},
        _ = consume(client_rx, &endpoint) => {println!("Consumer crashed");}
    }

    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    println!("bye!");
}
