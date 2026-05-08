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

use chrono::{DateTime, Duration, Utc};
use rand::seq::SliceRandom;
use rand::Rng;
use uuid::Uuid;

use super::models::{ManagementGroupResource, RelationshipResource, ServiceGroupResource};

pub struct GeneratorConfig {
    pub tenant_id: String,
    pub management_group_count: usize,
    pub service_group_count: usize,
    pub relationship_count: usize,
    pub hierarchy_depth: usize,
    pub start_time: DateTime<Utc>,
}

pub struct ArnDataGenerator {
    config: GeneratorConfig,
    rng: rand::rngs::ThreadRng,
}

impl ArnDataGenerator {
    pub fn new(config: GeneratorConfig) -> Self {
        Self {
            config,
            rng: rand::thread_rng(),
        }
    }

    /// Generate a set of Management Groups with hierarchical relationships
    pub fn generate_management_groups(&mut self) -> Vec<ManagementGroupResource> {
        let mut management_groups = Vec::new();
        let users = vec![
            "admin@contoso.com",
            "user@contoso.com",
            "sysadmin@contoso.com",
        ];

        // Create root management group
        let root_id = format!("mg-root-{}", Uuid::new_v4().to_string()[..8].to_string());
        let root_mg = ManagementGroupResource::new(
            root_id.clone(),
            root_id.clone(),
            "Root Management Group".to_string(),
            self.config.tenant_id.clone(),
            None,
            self.config.start_time,
            users.choose(&mut self.rng).unwrap().to_string(),
        );
        management_groups.push(root_mg);

        // Generate hierarchical management groups
        let mut current_level_groups = vec![root_id.clone()];
        let mut groups_per_level = vec![1];

        // Calculate how many groups to create per level
        let remaining_groups = self.config.management_group_count - 1;
        let levels = self.config.hierarchy_depth.min(remaining_groups);

        if levels > 0 {
            let groups_per_remaining_level = remaining_groups / levels;
            for _ in 0..levels {
                groups_per_level.push(groups_per_remaining_level);
            }
        }

        // Generate groups level by level
        let mut created_count = 1;
        for (level, &count) in groups_per_level.iter().enumerate().skip(1) {
            if created_count >= self.config.management_group_count {
                break;
            }

            let mut next_level_groups = Vec::new();

            for i in 0..count {
                if created_count >= self.config.management_group_count {
                    break;
                }

                // Pick a random parent from the current level
                let parent_id = current_level_groups
                    .choose(&mut self.rng)
                    .unwrap()
                    .clone();

                let mg_id = format!("mg-l{}-{:03}", level, i);
                let mg_name = format!("Management Group L{} #{}", level, i + 1);

                let time_offset = Duration::seconds(created_count as i64 * 60);
                let mg = ManagementGroupResource::new(
                    mg_id.clone(),
                    mg_id.clone(),
                    mg_name,
                    self.config.tenant_id.clone(),
                    Some(parent_id),
                    self.config.start_time + time_offset,
                    users.choose(&mut self.rng).unwrap().to_string(),
                );

                management_groups.push(mg);
                next_level_groups.push(mg_id);
                created_count += 1;
            }

            current_level_groups = next_level_groups;
        }

        management_groups
    }

    /// Generate Service Groups
    pub fn generate_service_groups(&mut self) -> Vec<ServiceGroupResource> {
        let mut service_groups = Vec::new();
        let users = vec![
            "admin@contoso.com",
            "user@contoso.com",
            "sysadmin@contoso.com",
        ];
        let service_types = vec![
            "Compute",
            "Storage",
            "Networking",
            "Database",
            "Security",
            "Analytics",
            "AI/ML",
        ];

        for i in 0..self.config.service_group_count {
            let sg_id = format!("sg-{:03}", i);
            let service_type = service_types.choose(&mut self.rng).unwrap();
            let sg_name = format!("{} Service Group {}", service_type, i + 1);
            let description = format!("Service group for {} resources", service_type);

            let time_offset = Duration::seconds((i * 120) as i64);
            let sg = ServiceGroupResource::new(
                sg_id.clone(),
                sg_id,
                sg_name,
                description,
                self.config.tenant_id.clone(),
                self.config.start_time + time_offset,
                users.choose(&mut self.rng).unwrap().to_string(),
            );

            service_groups.push(sg);
        }

        service_groups
    }

    /// Generate Relationship Resources (Service Group Members)
    pub fn generate_relationships(
        &mut self,
        management_groups: &[ManagementGroupResource],
        service_groups: &[ServiceGroupResource],
    ) -> Vec<RelationshipResource> {
        let mut relationships = Vec::new();
        let users = vec![
            "admin@contoso.com",
            "user@contoso.com",
            "sysadmin@contoso.com",
        ];

        for i in 0..self.config.relationship_count {
            // Pick random source (management group) and target (service group)
            let source_mg = management_groups.choose(&mut self.rng).unwrap();
            let target_sg = service_groups.choose(&mut self.rng).unwrap();

            let rel_id = format!("rel-{:04}", i);
            let source_id = format!(
                "/providers/Microsoft.Management/managementGroups/{}",
                source_mg.id
            );
            let target_id = format!(
                "/providers/Microsoft.Management/serviceGroups/{}",
                target_sg.id
            );

            let time_offset = Duration::seconds((i * 90) as i64);
            let rel = RelationshipResource::new(
                rel_id.clone(),
                rel_id,
                source_id,
                target_id,
                self.config.tenant_id.clone(),
                self.config.start_time + time_offset,
                users.choose(&mut self.rng).unwrap().to_string(),
            );

            relationships.push(rel);
        }

        relationships
    }

    /// Generate change events (updates and inserts)
    pub fn generate_change_events(
        &mut self,
        management_groups: &[ManagementGroupResource],
        _service_groups: &[ServiceGroupResource],
        change_count: usize,
        duration_secs: u64,
    ) -> Vec<ChangeEvent> {
        let mut events = Vec::new();
        let users = vec![
            "admin@contoso.com",
            "user@contoso.com",
            "operator@contoso.com",
        ];

        let interval_secs = if change_count > 0 {
            duration_secs / change_count as u64
        } else {
            1
        };

        for i in 0..change_count {
            let time_offset = Duration::seconds((i as u64 * interval_secs) as i64);
            let event_time = self.config.start_time + time_offset;

            // 70% updates, 30% inserts
            let is_update = self.rng.gen_bool(0.7);

            if is_update && !management_groups.is_empty() {
                // Generate an update event for an existing management group
                let mg = management_groups.choose(&mut self.rng).unwrap();
                let new_display_name = format!("{} - Updated", mg.display_name);
                let updated_by = users.choose(&mut self.rng).unwrap().to_string();

                events.push(ChangeEvent::Update {
                    management_group: mg.clone(),
                    new_display_name,
                    updated_at: event_time,
                    updated_by,
                });
            } else {
                // Generate an insert event for a new management group
                let mg_id = format!("mg-new-{:04}", i);
                let mg_name = format!("New Management Group {}", i + 1);

                // Pick a random parent from existing groups
                let parent_id = if !management_groups.is_empty() && self.rng.gen_bool(0.8) {
                    Some(management_groups.choose(&mut self.rng).unwrap().id.clone())
                } else {
                    None
                };

                let mg = ManagementGroupResource::new(
                    mg_id.clone(),
                    mg_id,
                    mg_name,
                    self.config.tenant_id.clone(),
                    parent_id,
                    event_time,
                    users.choose(&mut self.rng).unwrap().to_string(),
                );

                events.push(ChangeEvent::Insert { management_group: mg });
            }
        }

        events
    }
}

#[derive(Debug, Clone)]
pub enum ChangeEvent {
    Insert {
        management_group: ManagementGroupResource,
    },
    Update {
        management_group: ManagementGroupResource,
        new_display_name: String,
        updated_at: DateTime<Utc>,
        updated_by: String,
    },
}
