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

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ===== Common Types =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ComponentStatus {
    Running,
    Stopped,
    Starting,
    Stopping,
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StatusResponse {
    pub status: String,
    pub message: Option<String>,
}

// ===== Source Models =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "postgresql-source",
    "source_type": "PostgreSQL",
    "status": "running"
}))]
pub struct SourceInfo {
    pub name: String,
    pub source_type: String,
    pub status: ComponentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "postgresql-source",
    "source_type": "PostgreSQL",
    "status": "running",
    "auto_start": true,
    "properties": {
        "host": "localhost",
        "port": 5432,
        "database": "mydb"
    }
}))]
pub struct SourceDetails {
    pub name: String,
    pub source_type: String,
    pub status: ComponentStatus,
    pub auto_start: bool,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "postgresql-source",
    "source_type": "PostgreSQL",
    "auto_start": true,
    "properties": {
        "host": "localhost",
        "port": 5432,
        "database": "mydb",
        "user": "postgres",
        "password": "password"
    }
}))]
pub struct CreateSourceRequest {
    pub name: String,
    pub source_type: String,
    #[serde(default)]
    pub auto_start: bool,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateSourceRequest {
    pub source_type: Option<String>,
    pub auto_start: Option<bool>,
    pub properties: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SourceCreatedResponse {
    pub name: String,
    pub message: String,
}

// ===== Query Models =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "active-users",
    "sources": ["postgresql-source"],
    "status": "running"
}))]
pub struct QueryInfo {
    pub name: String,
    pub sources: Vec<String>,
    pub status: ComponentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "active-users",
    "query": "MATCH (u:User)-[:LOGGED_IN]->(s:Session) WHERE s.active = true RETURN u",
    "sources": ["postgresql-source"],
    "status": "running",
    "auto_start": true,
    "profiling": false
}))]
pub struct QueryDetails {
    pub name: String,
    pub query: String,
    pub sources: Vec<String>,
    pub status: ComponentStatus,
    pub auto_start: bool,
    #[serde(default)]
    pub profiling: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "active-users",
    "query": "MATCH (u:User)-[:LOGGED_IN]->(s:Session) WHERE s.active = true RETURN u",
    "sources": ["postgresql-source"],
    "auto_start": true,
    "profiling": false
}))]
pub struct CreateQueryRequest {
    pub name: String,
    pub query: String,
    pub sources: Vec<String>,
    #[serde(default)]
    pub auto_start: bool,
    #[serde(default)]
    pub profiling: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateQueryRequest {
    pub query: Option<String>,
    pub sources: Option<Vec<String>>,
    pub auto_start: Option<bool>,
    pub profiling: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueryCreatedResponse {
    pub name: String,
    pub message: String,
}

// ===== Reaction Models =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "user-notification",
    "reaction_type": "HttpReaction",
    "queries": ["active-users"],
    "status": "running"
}))]
pub struct ReactionInfo {
    pub name: String,
    pub reaction_type: String,
    pub queries: Vec<String>,
    pub status: ComponentStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "user-notification",
    "reaction_type": "HttpReaction",
    "queries": ["active-users"],
    "status": "running",
    "auto_start": true,
    "properties": {
        "endpoint": "https://api.example.com/notify",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        }
    }
}))]
pub struct ReactionDetails {
    pub name: String,
    pub reaction_type: String,
    pub queries: Vec<String>,
    pub status: ComponentStatus,
    pub auto_start: bool,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "name": "user-notification",
    "reaction_type": "HttpReaction",
    "queries": ["active-users"],
    "auto_start": true,
    "properties": {
        "endpoint": "https://api.example.com/notify",
        "method": "POST",
        "headers": {
            "Content-Type": "application/json"
        }
    }
}))]
pub struct CreateReactionRequest {
    pub name: String,
    pub reaction_type: String,
    pub queries: Vec<String>,
    #[serde(default)]
    pub auto_start: bool,
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateReactionRequest {
    pub reaction_type: Option<String>,
    pub queries: Option<Vec<String>>,
    pub auto_start: Option<bool>,
    pub properties: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReactionCreatedResponse {
    pub name: String,
    pub message: String,
}

// ===== Error Response =====

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "error": "ComponentNotFound",
    "message": "Source 'postgresql-source' not found",
    "instance_id": "drasi-instance-1",
    "component_id": "postgresql-source"
}))]
pub struct DrasiLibInstanceError {
    pub error: String,
    pub message: String,
    pub instance_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_id: Option<String>,
}
