//! This example start a business service and a data processing ("command") service.
//!
//! The business service makes asynchronous API calls to the data processing service (a trivial lower case provider)
//! each time it receives a `IdentifierChosen` event. When it receives the results, it publishes a `IdentifierNormalized`
//! event.
//!
//! To trigger the behavior, use the Eventail command line tool `publish_event.py` to publish a `IdentifierChosen` event.
//! A sample payload is provided : `IdentifierChosen.json`.

use std::env;

use eventail::endpoint::{Config, Endpoint};
use eventail::service::{run_service, Error, Service};
use eventail::Delivery;
use serde::{Deserialize, Serialize};
use tokio::signal;

#[derive(Deserialize, Serialize)]
struct ToLowerCase<'a> {
    string: &'a str,
}

#[derive(Deserialize)]
struct IdentifierChosen<'a> {
    identifier: &'a str,
}

#[derive(Serialize)]
struct IdentifierNormalized<'a> {
    identifier: &'a str,
}

struct LowerCaseService;
struct IdentifierService;

impl Service for LowerCaseService {
    fn on_ready(&mut self, _endpoint: &Endpoint) {}

    fn on_waiting_for_configuration(&mut self, _endpoint: &Endpoint) {}

    fn on_configuration(
        &mut self,
        _endpoint: &Endpoint,
        _message: Box<Delivery>,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn on_event(&mut self, _endpoint: &Endpoint, _message: Box<Delivery>) -> Result<(), Error> {
        unreachable!()
    }

    fn on_command(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error> {
        let mut request: ToLowerCase = message.deserialize_payload()?;
        let processed = request.string.to_lowercase();
        request.string = &processed;
        // unwrap is safe here because the delivery was checked by the endpoint.
        endpoint
            .return_success(
                message.reply_to().unwrap(),
                &request,
                message.conversation_id().unwrap(),
                message.correlation_id().unwrap(),
                false,
            )
            .ok();
        Ok(())
    }

    fn on_result(&mut self, _endpoint: &Endpoint, _message: Box<Delivery>) -> Result<(), Error> {
        unreachable!()
    }

    fn on_message_returned(&mut self, _endpoint: &Endpoint, _routing_key: String) {}

    fn on_clock_ticked(&mut self, _endpoint: &Endpoint, _time: u64) {}
}

impl Service for IdentifierService {
    fn on_ready(&mut self, _endpoint: &Endpoint) {}

    fn on_waiting_for_configuration(&mut self, _endpoint: &Endpoint) {}

    fn on_configuration(
        &mut self,
        _endpoint: &Endpoint,
        _message: Box<Delivery>,
    ) -> Result<bool, Error> {
        Ok(true)
    }

    fn on_event(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error> {
        let payload: IdentifierChosen = message.deserialize_payload()?;
        let command = ToLowerCase {
            string: payload.identifier,
        };
        endpoint
            .send_command(
                "string.ToLowerCase",
                &command,
                message.conversation_id().unwrap(),
                "is.Result",
                "osef",
                false,
            )
            .ok();
        Ok(())
    }

    fn on_command(&mut self, _endpoint: &Endpoint, _message: Box<Delivery>) -> Result<(), Error> {
        unreachable!()
    }

    fn on_result(&mut self, endpoint: &Endpoint, message: Box<Delivery>) -> Result<(), Error> {
        if message.status().unwrap() == "success" {
            let result: ToLowerCase = message.deserialize_payload()?;
            let norm_id = IdentifierNormalized {
                identifier: result.string,
            };
            endpoint
                .publish_event(
                    "IdentifierNormalized",
                    &norm_id,
                    message.conversation_id().unwrap(),
                    false,
                )
                .ok();
        }
        Ok(())
    }

    fn on_message_returned(&mut self, _endpoint: &Endpoint, _routing_key: String) {}

    fn on_clock_ticked(&mut self, _endpoint: &Endpoint, _time: u64) {}
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let hosts: Vec<String> = env::args().skip(1).collect();
    if hosts.is_empty() {
        panic!("Usage: publisher broker_uri [broker_uri…]");
    }
    let lcs_config = Config {
        prefetch_count: 20,
        logical_service: String::from("StringProcessor"),
        hosts: hosts.clone(),
        event_subscriptions: vec![],
        command_subscriptions: vec!["string.ToLowerCase".to_owned()],
        configuration_subscriptions: vec![],
    };
    let (lcs_endpoint, lcs_client_rx) = Endpoint::new(lcs_config);
    let is_config = Config {
        prefetch_count: 20,
        logical_service: String::from("IdentifierService"),
        hosts,
        event_subscriptions: vec!["IdentifierChosen".to_owned()],
        command_subscriptions: vec!["is.Result".to_owned()],
        configuration_subscriptions: vec![],
    };
    let (is_endpoint, is_client_rx) = Endpoint::new(is_config);
    tokio::select! {
        _ = signal::ctrl_c() => {println!("CTL-C: exited")},
        _ = run_service(LowerCaseService, &lcs_endpoint, lcs_client_rx) => {println!("Command service crashed");}
        _ = run_service(IdentifierService, &is_endpoint, is_client_rx) => {println!("Service crashed");}
    }
    is_endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
    lcs_endpoint
        .shutdown()
        .await
        .expect("problem when shutting down the service!");
}
