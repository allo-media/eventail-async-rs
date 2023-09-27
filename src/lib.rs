/*!
Eventail for Rust + Tokio

User generally starts by creating an [`Endpoint`] and a channel receiver (the Client Receiver).

The Endpoint manages the connection and event publishing, including publisher confirms and retries.

The Client Receiver receives the subscribed messages and local events.

The User code can choose to consume events ([`endpoint::ClientMessage`]) directly from the Client Receiver,
but then it must acknowledge (ACK or NACk) the processed messages explicitely
([`Endpoint::ack_delivery`], [`Endpoint::nack_delivery`] or [`Endpoint::reject_delivery`]) and inform the endpoint
when it is configured and ready to consume business messages ([`Endpoint::configured`]).

The User can also choose to implement the [`service::Service`] trait and let [`service::run_service`] handle the rest.
*/

pub mod delivery;
pub mod endpoint;
pub mod service;

pub use delivery::Delivery;
pub use endpoint::Endpoint;
