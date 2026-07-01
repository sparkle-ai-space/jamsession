#![deny(unused_must_use)] // Must-use warnings are almost always bugs. Propagate results or document why you do not have to.
#![deny(unsafe_code)] // Unsafe code does not belond in this crate. Avoid it or, if truly needed, create a carefully thought out abstraction.

pub mod agent;
pub mod config;
pub mod daemon;
pub mod db;
pub mod debug;
mod dispatcher;
mod eof_signal;
pub mod error;
pub mod logging;
mod session;

pub use session::{LifecycleEvent, LifecycleEventSender};
