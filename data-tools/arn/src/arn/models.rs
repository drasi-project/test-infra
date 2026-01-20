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

use chrono::{DateTime, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

/// Generate a random publisher info (70% Microsoft.Resources, 30% Default)
fn random_publisher_info() -> String {
    let mut rng = rand::thread_rng();
    if rng.gen_bool(0.7) {
        "Microsoft.Resources".to_string()
    } else {
        "Default".to_string()
    }
}

/// Notification format types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum NotificationFormat {
    /// Single resource with full ARM payload
    FullPayload,
    /// Single resource with resourceId only (no payload)
    PayloadLess,
    /// Multiple resources with resourceId only (batched)
    BatchedIds,
    /// Multiple resources with full ARM payload (batched)
    BatchedPayloads,
}

/// Event Grid event envelope wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventGridEvent {
    /// Event Grid topic
    pub topic: String,
    /// Resource subject (typically resource ID or subscription)
    pub subject: String,
    /// Event type
    #[serde(rename = "eventType")]
    pub event_type: String,
    /// Event timestamp
    #[serde(rename = "eventTime")]
    pub event_time: String,
    /// Unique event ID
    pub id: String,
    /// ARN notification data
    pub data: ArnNotificationData,
    /// Data version (3.0 for ARN v3)
    #[serde(rename = "dataVersion")]
    pub data_version: String,
    /// Metadata version
    #[serde(rename = "metadataVersion")]
    pub metadata_version: String,
}

/// ARN notification data (inside Event Grid event.data)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArnNotificationData {
    /// Resource location
    pub resource_location: String,
    /// Publisher info
    pub publisher_info: String,
    /// API version (at data level for batched notifications)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    /// Resources array
    pub resources: Vec<ArnResourcesData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceType {
    ManagementGroup,
    ServiceGroup,
    Relationship,
}

impl ResourceType {
    pub fn as_label(&self) -> &str {
        match self {
            ResourceType::ManagementGroup => "ManagementGroup",
            ResourceType::ServiceGroup => "ServiceGroup",
            ResourceType::Relationship => "Relationship",
        }
    }

    pub fn as_arm_type(&self) -> &str {
        match self {
            ResourceType::ManagementGroup => "Microsoft.Management/managementGroups",
            ResourceType::ServiceGroup => "Microsoft.Management/serviceGroups",
            ResourceType::Relationship => "microsoft.relationships/servicegroupmember",
        }
    }
}

/// Base ARN Resource structure that maps to Drasi Node format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArnResource {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: ArnResourceProperties,
}

impl ArnResource {
    pub fn new(
        resource_id: String,
        resource_type: ResourceType,
        properties: ArnResourceProperties,
    ) -> Self {
        let labels = match resource_type {
            ResourceType::ManagementGroup => vec!["ManagementGroup".to_string()],
            ResourceType::ServiceGroup => vec!["ServiceGroup".to_string()],
            ResourceType::Relationship => {
                vec!["Relationship".to_string(), "ServiceGroupMember".to_string()]
            }
        };

        Self {
            id: resource_id,
            labels,
            properties,
        }
    }
}

/// ARN Resource Properties (notification level properties)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArnResourceProperties {
    /// Tenant from which the resource is managed (notification level)
    pub home_tenant_id: String,
    /// Tenant in which the resource exists (notification level)
    pub resource_home_tenant_id: String,
    /// Inline or Blob
    pub resources_container: String,
    /// Resource location
    pub resource_location: String,
    /// Publisher info: Microsoft.Resources or Default
    pub publisher_info: String,
    /// For inline payload, resources is a list containing ArnResourcesData
    pub resources: Vec<ArnResourcesData>,
}

/// Individual resource data within the resources array (NotificationResourceData)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArnResourcesData {
    /// Resource identifier in ARM format (REQUIRED)
    pub resource_id: String,
    /// Correlation id for tracking (REQUIRED)
    pub correlation_id: String,
    /// Populated for resource moves - the source resource identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_resource_id: Option<String>,
    /// Version of the resource schema (optional for payload-less)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    /// When the activity for this resource occurred
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_event_time: Option<String>,
    /// Tenant from which the resource is managed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub home_tenant_id: Option<String>,
    /// Tenant in which the resource exists
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_home_tenant_id: Option<String>,
    /// HTTP status code for the operation (OK, BadRequest, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<String>,
    /// Resource payload (armResource) - optional for payload-less format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arm_resource: Option<Value>,
    /// System properties for the resource
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_system_properties: Option<ResourceSystemProperties>,
    /// Additional metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_resource_properties: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSystemProperties {
    pub created_by: String,
    pub created_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified_time: Option<String>,
    pub change_action: String,
}

/// Management Group Resource
#[derive(Debug, Clone)]
pub struct ManagementGroupResource {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub tenant_id: String,
    pub parent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

impl ManagementGroupResource {
    pub fn new(
        id: String,
        name: String,
        display_name: String,
        tenant_id: String,
        parent_id: Option<String>,
        created_at: DateTime<Utc>,
        created_by: String,
    ) -> Self {
        Self {
            id,
            name,
            display_name,
            tenant_id,
            parent_id,
            created_at,
            created_by,
        }
    }

    pub fn to_arn_resource(&self, change_action: &str) -> ArnResource {
        let resource_id = format!("/providers/Microsoft.Management/managementGroups/{}", self.id);
        let correlation_id = Uuid::new_v4().to_string();

        let parent_value = match &self.parent_id {
            Some(parent_id) => json!({
                "id": format!("/providers/Microsoft.Management/managementGroups/{}", parent_id)
            }),
            None => Value::Null,
        };

        let arm_resource = json!({
            "id": resource_id,
            "name": self.name,
            "type": "Microsoft.Management/managementGroups",
            "properties": {
                "displayName": self.display_name,
                "tenantId": self.tenant_id,
                "details": {
                    "parent": parent_value
                }
            }
        });

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            source_resource_id: None,
            api_version: Some("2021-04-01".to_string()),
            correlation_id,
            resource_event_time: Some(self.created_at.to_rfc3339()),
            home_tenant_id: Some(self.tenant_id.clone()),
            resource_home_tenant_id: Some(self.tenant_id.clone()),
            status_code: Some("OK".to_string()),
            arm_resource: Some(arm_resource),
            resource_system_properties: Some(ResourceSystemProperties {
                created_by: self.created_by.clone(),
                created_time: self.created_at.to_rfc3339(),
                last_modified_by: None,
                last_modified_time: None,
                change_action: change_action.to_string(),
            }),
            additional_resource_properties: None,
        };

        let properties = ArnResourceProperties {
            home_tenant_id: self.tenant_id.clone(),
            resource_home_tenant_id: self.tenant_id.clone(),
            resources_container: "Inline".to_string(),
            resource_location: "global".to_string(),
            publisher_info: random_publisher_info(),
            resources: vec![resource_data],
        };

        ArnResource::new(resource_id, ResourceType::ManagementGroup, properties)
    }

    pub fn to_updated_arn_resource(
        &self,
        new_display_name: String,
        updated_at: DateTime<Utc>,
        updated_by: String,
    ) -> ArnResource {
        let resource_id = format!("/providers/Microsoft.Management/managementGroups/{}", self.id);
        let correlation_id = Uuid::new_v4().to_string();

        let parent_value = match &self.parent_id {
            Some(parent_id) => json!({
                "id": format!("/providers/Microsoft.Management/managementGroups/{}", parent_id)
            }),
            None => Value::Null,
        };

        let arm_resource = json!({
            "id": resource_id,
            "name": self.name,
            "type": "Microsoft.Management/managementGroups",
            "properties": {
                "displayName": new_display_name,
                "tenantId": self.tenant_id,
                "details": {
                    "parent": parent_value
                }
            }
        });

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            source_resource_id: None,
            api_version: Some("2021-04-01".to_string()),
            correlation_id,
            resource_event_time: Some(updated_at.to_rfc3339()),
            home_tenant_id: Some(self.tenant_id.clone()),
            resource_home_tenant_id: Some(self.tenant_id.clone()),
            status_code: Some("OK".to_string()),
            arm_resource: Some(arm_resource),
            resource_system_properties: Some(ResourceSystemProperties {
                created_by: self.created_by.clone(),
                created_time: self.created_at.to_rfc3339(),
                last_modified_by: Some(updated_by),
                last_modified_time: Some(updated_at.to_rfc3339()),
                change_action: "Update".to_string(),
            }),
            additional_resource_properties: None,
        };

        let properties = ArnResourceProperties {
            home_tenant_id: self.tenant_id.clone(),
            resource_home_tenant_id: self.tenant_id.clone(),
            resources_container: "Inline".to_string(),
            resource_location: "global".to_string(),
            publisher_info: random_publisher_info(),
            resources: vec![resource_data],
        };

        ArnResource::new(resource_id, ResourceType::ManagementGroup, properties)
    }
}

/// Service Group Resource
#[derive(Debug, Clone)]
pub struct ServiceGroupResource {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub tenant_id: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

impl ServiceGroupResource {
    pub fn new(
        id: String,
        name: String,
        display_name: String,
        description: String,
        tenant_id: String,
        created_at: DateTime<Utc>,
        created_by: String,
    ) -> Self {
        Self {
            id,
            name,
            display_name,
            description,
            tenant_id,
            created_at,
            created_by,
        }
    }

    pub fn to_arn_resource(&self, change_action: &str) -> ArnResource {
        let resource_id = format!("/providers/Microsoft.Management/serviceGroups/{}", self.id);
        let correlation_id = Uuid::new_v4().to_string();

        let arm_resource = json!({
            "id": resource_id,
            "name": self.name,
            "type": "Microsoft.Management/serviceGroups",
            "properties": {
                "displayName": self.display_name,
                "description": self.description,
                "tenantId": self.tenant_id,
            }
        });

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            source_resource_id: None,
            api_version: Some("2021-04-01".to_string()),
            correlation_id,
            resource_event_time: Some(self.created_at.to_rfc3339()),
            home_tenant_id: Some(self.tenant_id.clone()),
            resource_home_tenant_id: Some(self.tenant_id.clone()),
            status_code: Some("OK".to_string()),
            arm_resource: Some(arm_resource),
            resource_system_properties: Some(ResourceSystemProperties {
                created_by: self.created_by.clone(),
                created_time: self.created_at.to_rfc3339(),
                last_modified_by: None,
                last_modified_time: None,
                change_action: change_action.to_string(),
            }),
            additional_resource_properties: None,
        };

        let properties = ArnResourceProperties {
            home_tenant_id: self.tenant_id.clone(),
            resource_home_tenant_id: self.tenant_id.clone(),
            resources_container: "Inline".to_string(),
            resource_location: "global".to_string(),
            publisher_info: random_publisher_info(),
            resources: vec![resource_data],
        };

        ArnResource::new(resource_id, ResourceType::ServiceGroup, properties)
    }
}

/// Relationship Resource (Service Group Member)
#[derive(Debug, Clone)]
pub struct RelationshipResource {
    pub id: String,
    pub name: String,
    pub source_id: String,
    pub target_id: String,
    pub relationship_type: String,
    pub tenant_id: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

impl RelationshipResource {
    pub fn new(
        id: String,
        name: String,
        source_id: String,
        target_id: String,
        tenant_id: String,
        created_at: DateTime<Utc>,
        created_by: String,
    ) -> Self {
        Self {
            id,
            name,
            source_id,
            target_id,
            relationship_type: "ServiceGroupMember".to_string(),
            tenant_id,
            created_at,
            created_by,
        }
    }

    pub fn to_arn_resource(&self, change_action: &str) -> ArnResource {
        let resource_id = format!(
            "/providers/microsoft.relationships/servicegroupmembers/{}",
            self.id
        );
        let correlation_id = Uuid::new_v4().to_string();

        let arm_resource = json!({
            "id": resource_id,
            "name": self.name,
            "type": "microsoft.relationships/servicegroupmember",
            "properties": {
                "SourceId": self.source_id,
                "TargetId": self.target_id,
                "RelationshipType": self.relationship_type,
                "tenantId": self.tenant_id,
            }
        });

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            source_resource_id: None,
            api_version: Some("2021-04-01".to_string()),
            correlation_id,
            resource_event_time: Some(self.created_at.to_rfc3339()),
            home_tenant_id: Some(self.tenant_id.clone()),
            resource_home_tenant_id: Some(self.tenant_id.clone()),
            status_code: Some("OK".to_string()),
            arm_resource: Some(arm_resource),
            resource_system_properties: Some(ResourceSystemProperties {
                created_by: self.created_by.clone(),
                created_time: self.created_at.to_rfc3339(),
                last_modified_by: None,
                last_modified_time: None,
                change_action: change_action.to_string(),
            }),
            additional_resource_properties: None,
        };

        let properties = ArnResourceProperties {
            home_tenant_id: self.tenant_id.clone(),
            resource_home_tenant_id: self.tenant_id.clone(),
            resources_container: "Inline".to_string(),
            resource_location: "global".to_string(),
            publisher_info: random_publisher_info(),
            resources: vec![resource_data],
        };

        ArnResource::new(resource_id, ResourceType::Relationship, properties)
    }
}

/// Helper functions for creating Event Grid events
impl ManagementGroupResource {
    /// Create Event Grid event with full payload
    pub fn to_event_grid_event_full(&self, change_action: &str) -> EventGridEvent {
        let resource_id = format!("/providers/Microsoft.Management/managementGroups/{}", self.id);
        let correlation_id = Uuid::new_v4().to_string();
        let event_type = format!("Microsoft.Management/managementGroups/{}",
            if change_action == "Create" { "write" } else { "write" });

        let parent_value = match &self.parent_id {
            Some(parent_id) => json!({
                "id": format!("/providers/Microsoft.Management/managementGroups/{}", parent_id)
            }),
            None => Value::Null,
        };

        let arm_resource = json!({
            "id": resource_id.clone(),
            "name": self.name,
            "type": "Microsoft.Management/managementGroups",
            "location": "global",
            "properties": {
                "displayName": self.display_name,
                "tenantId": self.tenant_id,
                "details": {
                    "parent": parent_value
                },
                "provisioningState": "Succeeded"
            }
        });

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            correlation_id,
            source_resource_id: None,
            api_version: Some("2021-04-01".to_string()),
            resource_event_time: Some(self.created_at.to_rfc3339()),
            home_tenant_id: Some(self.tenant_id.clone()),
            resource_home_tenant_id: Some(self.tenant_id.clone()),
            status_code: Some("OK".to_string()),
            arm_resource: Some(arm_resource),
            resource_system_properties: Some(ResourceSystemProperties {
                created_by: self.created_by.clone(),
                created_time: self.created_at.to_rfc3339(),
                last_modified_by: None,
                last_modified_time: None,
                change_action: change_action.to_string(),
            }),
            additional_resource_properties: None,
        };

        EventGridEvent {
            topic: format!("custom-domain-topic/eg-topic"),
            subject: resource_id.clone(),
            event_type,
            event_time: self.created_at.to_rfc3339(),
            id: Uuid::new_v4().to_string(),
            data: ArnNotificationData {
                resource_location: "global".to_string(),
                publisher_info: random_publisher_info(),
                api_version: Some("2020-03-01-preview".to_string()),
                resources: vec![resource_data],
            },
            data_version: "3.0".to_string(),
            metadata_version: "1".to_string(),
        }
    }

    /// Create Event Grid event with payload-less format (resourceId only)
    pub fn to_event_grid_event_payloadless(&self) -> EventGridEvent {
        let resource_id = format!("/providers/Microsoft.Management/managementGroups/{}", self.id);
        let correlation_id = Uuid::new_v4().to_string();
        let event_type = "Microsoft.Management/managementGroups/write".to_string();

        let resource_data = ArnResourcesData {
            resource_id: resource_id.clone(),
            correlation_id,
            source_resource_id: None,
            api_version: None,
            resource_event_time: None,
            home_tenant_id: None,
            resource_home_tenant_id: None,
            status_code: None,
            arm_resource: None,
            resource_system_properties: None,
            additional_resource_properties: None,
        };

        EventGridEvent {
            topic: format!("custom-domain-topic/eg-topic"),
            subject: resource_id.clone(),
            event_type,
            event_time: self.created_at.to_rfc3339(),
            id: Uuid::new_v4().to_string(),
            data: ArnNotificationData {
                resource_location: "global".to_string(),
                publisher_info: random_publisher_info(),
                api_version: None,
                resources: vec![resource_data],
            },
            data_version: "3.0".to_string(),
            metadata_version: "1".to_string(),
        }
    }
}

/// Helper to create batched Event Grid events
pub fn create_batched_event_grid_event(
    resources: Vec<&ManagementGroupResource>,
    format: NotificationFormat,
    subscription_id: &str,
) -> EventGridEvent {
    let subject = format!("/subscriptions/{}", subscription_id);
    let event_time = resources.first().map(|r| r.created_at).unwrap_or_else(|| Utc::now());

    let resources_data: Vec<ArnResourcesData> = resources.iter().map(|mg| {
        let resource_id = format!("/providers/Microsoft.Management/managementGroups/{}", mg.id);
        let correlation_id = Uuid::new_v4().to_string();

        match format {
            NotificationFormat::BatchedIds => {
                // Payload-less batched format
                ArnResourcesData {
                    resource_id,
                    correlation_id,
                    source_resource_id: None,
                    api_version: None,
                    resource_event_time: None,
                    home_tenant_id: None,
                    resource_home_tenant_id: None,
                    status_code: None,
                    arm_resource: None,
                    resource_system_properties: None,
                    additional_resource_properties: None,
                }
            },
            NotificationFormat::BatchedPayloads => {
                // Full payload batched format
                let parent_value = match &mg.parent_id {
                    Some(parent_id) => json!({
                        "id": format!("/providers/Microsoft.Management/managementGroups/{}", parent_id)
                    }),
                    None => Value::Null,
                };

                let arm_resource = json!({
                    "id": resource_id.clone(),
                    "name": mg.name,
                    "type": "Microsoft.Management/managementGroups",
                    "location": "global",
                    "properties": {
                        "displayName": mg.display_name,
                        "tenantId": mg.tenant_id,
                        "details": {
                            "parent": parent_value
                        },
                        "provisioningState": "Succeeded"
                    }
                });

                ArnResourcesData {
                    resource_id,
                    correlation_id,
                    source_resource_id: None,
                    api_version: None, // At data level for batched
                    resource_event_time: Some(mg.created_at.to_rfc3339()),
                    home_tenant_id: Some(mg.tenant_id.clone()),
                    resource_home_tenant_id: Some(mg.tenant_id.clone()),
                    status_code: Some("OK".to_string()),
                    arm_resource: Some(arm_resource),
                    resource_system_properties: Some(ResourceSystemProperties {
                        created_by: mg.created_by.clone(),
                        created_time: mg.created_at.to_rfc3339(),
                        last_modified_by: None,
                        last_modified_time: None,
                        change_action: "Create".to_string(),
                    }),
                    additional_resource_properties: None,
                }
            },
            _ => panic!("Invalid format for batched event"),
        }
    }).collect();

    EventGridEvent {
        topic: "custom-domain-topic/eg-topic".to_string(),
        subject,
        event_type: "Microsoft.Management/managementGroups/write".to_string(),
        event_time: event_time.to_rfc3339(),
        id: Uuid::new_v4().to_string(),
        data: ArnNotificationData {
            resource_location: "global".to_string(),
            publisher_info: random_publisher_info(),
            api_version: if format == NotificationFormat::BatchedPayloads {
                Some("2020-03-01".to_string())
            } else {
                Some("2020-03-01-privatepreview".to_string())
            },
            resources: resources_data,
        },
        data_version: "3.0".to_string(),
        metadata_version: "1".to_string(),
    }
}
