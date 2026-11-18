//! Core contracts and data types for the Jessica agent runtime.
//!
//! This crate is the frozen contract layer: every implementation crate depends
//! on it, and it depends on no other internal crate. It must not contain WASM,
//! database, or HTTP concepts; those belong to the implementation layers.

#![deny(missing_docs)]

pub mod acceptance;
pub mod guard;
pub mod hook;
pub mod llm;
pub mod memory;
pub mod policy;
pub mod sensor;
pub mod tool;
pub mod trace;
pub mod types;
