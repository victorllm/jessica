//! Event persistence adapters.
//!
//! The concrete recorder, its journal seam, and its error type are owned by
//! `jessica-engine`. This module only keeps the CLI's existing import path.

pub(crate) use jessica_engine::{EventJournal, EventRecorder, RecorderError, StoredSession};
