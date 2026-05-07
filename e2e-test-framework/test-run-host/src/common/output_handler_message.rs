// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Unified handler message types for the E2E Test Framework
//!
//! This module provides a unified message type system that consolidates
//! result stream and reaction handler messages. The unified approach:
//! - Reduces code duplication
//! - Provides consistent handling of common fields (tracing, timing, etc.)
//! - Simplifies message processing in the query observer
//! - Makes it easier to add new handler types
//!
//! ## Migration Guide
//!
//! The legacy `ResultStreamRecord` and `ReactionInvocationRecord` types are
//! being replaced by the unified `HandlerRecord` type. During migration:
//!
//! 1. New code should use `UnifiedHandlerMessage::Record(HandlerRecord)`
//! 2. Legacy variants are maintained for backward compatibility
//! 3. Use conversion methods to transform between legacy and new formats
//! 4. The query observer handles both formats transparently

use crate::queries::result_stream_record::QueryResultRecord;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum OutputHandlerMessage {
    // Common messages
    HandlerStopping {
        handler_type: HandlerType,
    },
    Error {
        handler_type: HandlerType,
        error: HandlerError,
    },

    // Unified record type
    Record(HandlerRecord),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandlerType {
    ResultStream,
    Reaction,
}

#[derive(Debug, thiserror::Error)]
pub enum HandlerError {
    #[error("Invalid stream data")]
    InvalidStreamData,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("HTTP instance error: {0}")]
    HttpInstanceError(String),
    #[error("Conversion error")]
    ConversionError,
}

// New unified record structure
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandlerRecord {
    // Common identification
    pub id: String,
    pub sequence: u64,

    // Common timing (unified naming)
    pub created_time_ns: u64,   // When record was created
    pub processed_time_ns: u64, // When record was processed

    // Common tracing
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,

    // Handler-specific payload
    pub payload: HandlerPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HandlerPayload {
    ResultStream {
        query_result: QueryResultRecord,
    },
    ReactionInvocation {
        reaction_type: String,
        query_id: String,
        request_method: String,
        request_path: String,
        request_body: serde_json::Value,
        headers: HashMap<String, String>,
    },
    ReactionOutput {
        reaction_output: serde_json::Value,
    },
}

impl opentelemetry_api::propagation::Extractor for HandlerRecord {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => {
                // For reaction payloads, also check headers
                if let HandlerPayload::ReactionInvocation { headers, .. } = &self.payload {
                    headers.get(key).map(|s| s.as_str())
                } else {
                    None
                }
            }
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = vec!["traceparent", "tracestate"];
        if let HandlerPayload::ReactionInvocation { headers, .. } = &self.payload {
            keys.extend(headers.keys().map(|s| s.as_str()));
        }
        keys
    }
}

impl std::fmt::Display for HandlerRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(json_data) => {
                let json_data_unescaped = json_data.replace("\\\"", "\"").replace("\\'", "'");
                write!(f, "{json_data_unescaped}")
            }
            Err(e) => write!(f, "Error serializing HandlerRecord: {self:?}. Error: {e}"),
        }
    }
}
