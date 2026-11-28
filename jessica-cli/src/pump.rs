//! Event pump and rendering ports.
//!
//! The pump implementation is owned by `jessica-engine`. This module only
//! keeps the CLI's existing import path.

pub(crate) use jessica_engine::{
    EventPump, PumpIntegrity, PumpOutput, SessionSnapshot, UpdateSink,
};
