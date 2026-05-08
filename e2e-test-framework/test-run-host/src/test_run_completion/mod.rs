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

//! # TestRun Completion System
//!
//! This module provides a channel-based completion tracking system for TestRuns.
//! Components (Sources, Queries, Reactions) emit lifecycle events through an unbounded
//! channel, allowing the TestRun to monitor when all components have finished and
//! execute configured CompletionHandlers.
//!
//! ## Architecture
//!
//! ```text
//! TestRun creates channel → Passes sender to components → Components emit events
//!                                                                     ↓
//!                                                          tokio::mpsc::channel
//!                                                                     ↓
//!                                                       Monitoring task receives
//!                                                                     ↓
//!                                                     ComponentStateTracker updates
//!                                                                     ↓
//!                                              all_components_finished() == true?
//!                                                                     ↓
//!                                                      Execute CompletionHandlers
//! ```
//!
//! ## Usage
//!
//! 1. Create channel in TestRun initialization
//! 2. Pass `UnboundedSender` to each component constructor
//! 3. Components emit `ComponentLifecycleEvent` at state transitions
//! 4. Monitoring task tracks state and executes handlers when complete

pub mod completion_handlers;
pub mod events;
pub mod lifecycle_tx;
pub mod tracker;
pub mod types;

pub use completion_handlers::{create_completion_handler, CompletionHandler};
pub use events::ComponentLifecycleEvent;
pub use lifecycle_tx::LifecycleTx;
pub use tracker::ComponentStateTracker;
pub use types::{
    ComponentCompletionSummary, DrasiLibInstanceState, QueryState, ReactionState, SourceState,
};
