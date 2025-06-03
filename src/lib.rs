/*!
Eventail for Rust + Tokio

User generally starts by creating an [`Endpoint`] and a channel receiver (the Client Receiver).

The Endpoint manages the connection and event publishing, including publisher confirms and retries.

To use the EDA API, the client code uses a [`EndpointHandle`]. Any number of handles can be created with the [`Endpoint::new_handle()`] method.

The Client Receiver receives the subscribed messages and local events.

The User code can choose to consume events ([`endpoint::ClientMessage`]) directly from the Client Receiver,
but then it must acknowledge (ACK or NACK) the processed messages explicitly
([`EndpointHandle::ack_delivery`], [`EndpointHandle::nack_delivery`] or [`EndpointHandle::reject_delivery`]) and inform the endpoint
when it is configured and ready to consume business messages ([`EndpointHandle::configured`]).

The User can also choose to implement the [`service::Service`] trait and let [`service::run_service`] handle the rest.
*/

pub mod delivery;
pub mod endpoint;
pub mod service;

pub use delivery::Delivery;
pub use endpoint::{Endpoint, EndpointHandle};
