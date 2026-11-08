//! Memory contract: session-scoped write and recall.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::SessionId;

/// A single unit of memory.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryEntry {
    /// Textual content of the entry.
    pub content: String,
    /// Free-form structured metadata such as tags, source, or timestamps.
    #[serde(default)]
    pub metadata: serde_json::Value,
}

impl MemoryEntry {
    /// Creates an entry without metadata.
    pub fn new(content: impl Into<String>) -> Self {
        Self {
            content: content.into(),
            metadata: serde_json::Value::Null,
        }
    }
}

/// Errors raised by a memory backend.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MemoryError {
    /// The backend failed to read or write memory.
    #[error("memory backend error: {message}")]
    Backend {
        /// Human-readable error description.
        message: String,
    },
}

/// Session-scoped memory storage.
#[async_trait]
pub trait MemoryStore: Send + Sync {
    /// Persists `entry` for `session`.
    async fn write(&self, session: &SessionId, entry: MemoryEntry) -> Result<(), MemoryError>;

    /// Returns up to `limit` entries relevant to `query` for `session`.
    async fn recall(
        &self,
        session: &SessionId,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, MemoryError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    fn assert_round_trip<T>(value: &T)
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let encoded = serde_json::to_string(value).expect("value should serialize");
        let decoded: T = serde_json::from_str(&encoded).expect("value should deserialize");
        assert_eq!(&decoded, value);
    }

    struct InMemoryStore {
        entries: Mutex<Vec<MemoryEntry>>,
    }

    #[async_trait]
    impl MemoryStore for InMemoryStore {
        async fn write(&self, _session: &SessionId, entry: MemoryEntry) -> Result<(), MemoryError> {
            self.entries
                .lock()
                .expect("test mutex should not be poisoned")
                .push(entry);
            Ok(())
        }

        async fn recall(
            &self,
            _session: &SessionId,
            query: &str,
            limit: usize,
        ) -> Result<Vec<MemoryEntry>, MemoryError> {
            let entries = self
                .entries
                .lock()
                .expect("test mutex should not be poisoned");
            Ok(entries
                .iter()
                .filter(|entry| entry.content.contains(query))
                .take(limit)
                .cloned()
                .collect())
        }
    }

    #[test]
    fn entries_construct_and_round_trip() {
        let entry = MemoryEntry::new("the build command is cargo test");
        assert_eq!(entry.metadata, serde_json::Value::Null);
        assert_round_trip(&entry);

        let tagged = MemoryEntry {
            content: "prefer rustfmt".into(),
            metadata: json!({"tags": ["style"], "source": "user"}),
        };
        assert_round_trip(&tagged);
    }

    #[test]
    fn store_writes_and_recalls_relevant_entries() {
        let store = InMemoryStore {
            entries: Mutex::new(Vec::new()),
        };
        let session = SessionId::new("session-1");

        futures::executor::block_on(async {
            store
                .write(&session, MemoryEntry::new("rust is memory safe"))
                .await
                .expect("write should succeed");
            store
                .write(&session, MemoryEntry::new("bananas are yellow"))
                .await
                .expect("write should succeed");

            let recalled = store
                .recall(&session, "rust", 1)
                .await
                .expect("recall should succeed");
            assert_eq!(recalled.len(), 1);
            assert_eq!(recalled[0].content, "rust is memory safe");
        });
    }

    #[test]
    fn backend_errors_are_structured_and_readable() {
        let error = MemoryError::Backend {
            message: "disk full".into(),
        };
        assert!(error.to_string().contains("disk full"));
    }
}
