//! Consumer example using the traditional Callback API.
//!
//! Use the publisher example to send events.


use std::env;

use eventail::endpoint::{Config, Criticity, Endpoint};
use eventail::service::{run_service, Error, Service};
use eventail::Delivery;
use serde::Deserialize;
use tokio::signal;

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Ping<'a> {
    who: &'a str,
    what: &'a str,
    comment: &'a str,
}

struct MyService;

impl Service for MyService {
    fn on_ready(&mut self, _endpoint: &Endpoint) {}

    fn on_waiting_for_configuration(&mut self, _endpoint: &Endpoint) {}

    fn on_configuration(
        &mut self,
        _endpoint: &Endpoint,
        _message: Box<Delivery>,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn on_event(&mut self, _endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error> {
        if message.routing_key() != "Ping" {
            println!(
                "unsupported message {}; check subscriptions.",
                message.routing_key()
            );
            return Err(Error::Recoverable("unexpected message".to_owned()));
        }
        let ping: Ping = message.deserialize_payload()?;
        println!("Received Ping {:?}", ping);
        Ok(())
    }

    fn on_command(&mut self, _endpoint: &Endpoint, _message: Box<Delivery>) -> Result<(), Error> {
        Ok(())
    }

    fn on_result(&mut self, _endpoint: &Endpoint, _message: Box<Delivery>) -> Result<(), Error> {
        Ok(())
    }

    fn on_message_returned(&mut self, _endpoint: &Endpoint, _routing_key: String) {}

    fn on_clock_ticked(&mut self, endpoint: &Endpoint, time: u64) {
        if time % 60 == 0 {
            endpoint
                .log(Criticity::Notice, "healthcheck", "I'm alive!", "")
                .ok();
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
    let service = MyService;
    tokio::select! {
        _ = signal::ctrl_c() => {println!("CTL-C: exited")},
        _ = run_service(service, &endpoint, client_rx) => {println!("Consumer crashed");}
    }
    endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    println!("bye!");
}
