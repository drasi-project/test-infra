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

use std::{collections::{BTreeMap, HashSet}, fmt, str::FromStr};

use anyhow::Context;
use parking_lot::{Mutex, MutexGuard};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal};
use serde::Serialize;
use serde_json::{Map, Value};
use test_data_store::test_repo_storage::models::SensorDefinition;

use super::BuildingHierarchyDataGeneratorSettings;

#[derive(Debug)]
pub enum ModelChange {
    BuildingAdded(BuildingNode),
    BuildingUpdated(BuildingNode, BuildingNode),
    BuildingDeleted(BuildingNode),
    FloorAdded(FloorNode),
    FloorUpdated(FloorNode, FloorNode),
    FloorDeleted(FloorNode),
    RoomAdded(RoomNode),
    RoomUpdated(RoomNode, RoomNode),
    RoomDeleted(RoomNode),
    BuildingFloorRelationAdded(BuildingFloorRelation),
    BuildingFloorRelationDeleted(BuildingFloorRelation),
    FloorRoomRelationAdded(FloorRoomRelation),
    FloorRoomRelationDeleted(FloorRoomRelation),
}

// Compound key for locations
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Location {
    Building(u32),           // e.g., Building(0) -> "B_000"
    Floor(u32, u32),       // e.g., Floor(0, 1) -> "F_000_001"
    Room(u32, u32, u32), // e.g., Room(0, 1, 2) -> "R_000_001_002"
    Sensor(u32, u32, u32, String), // e.g., Sensor(0, 1, 2, "CO2".to_string()) -> "R_000_001_002_CO2"
}

impl Location {
    pub fn get_building_location(&self) -> Option<Location> {
        match self {
            Location::Building(_) => Some(self.clone()),
            Location::Floor(b, _) => Some(Location::Building(*b)),
            Location::Room(b, _, _) => Some(Location::Building(*b)),
            Location::Sensor(b, _, _, _) => Some(Location::Building(*b)),
        }
    }

    pub fn get_floor_location(&self) -> Option<Location> {
        match self {
            Location::Building(_) => None,
            Location::Floor(_, _) => Some(self.clone()),
            Location::Room(b, f, _) => Some(Location::Floor(*b,*f)),
            Location::Sensor(b, f, _, _) => Some(Location::Floor(*b,*f)),
        }
    }

    pub fn get_room_location(&self) -> Option<Location> {
        match self {
            Location::Building(_) => None,
            Location::Floor(_, _) => None,
            Location::Room(_, _, _) => Some(self.clone()),
            Location::Sensor(b, f, r, _) => Some(Location::Room(*b,*f,*r)),
        }
    }

    pub fn get_sensor_id(&self) -> Option<String> {
        match self {
            Location::Building(_) => None,
            Location::Floor(_, _) => None,
            Location::Room(_, _, _) => None,
            Location::Sensor(_, _, _, s) => Some(s.clone()),
        }
    }

    pub fn is_building(&self) -> bool {
        matches!(self, Location::Building(_))
    }

    pub fn is_floor(&self) -> bool {
        matches!(self, Location::Floor(_, _))
    }

    pub fn is_room(&self) -> bool {
        matches!(self, Location::Room(_, _, _))
    }

    pub fn is_sensor(&self) -> bool {
        matches!(self, Location::Sensor(_, _, _, _))
    }

    pub fn contains(&self, other: &Location) -> bool {

        if self == other {
            return true;
        };

        match self {
            Location::Building(b) => match other {
                Location::Floor(b_other, _) => b == b_other,
                Location::Room(b_other, _, _) => b == b_other,             
                _ => false,   
            },
            Location::Floor(b, f) => match other {
                Location::Room(b_other, f_other, _) => b == b_other && f == f_other,
                _ => false,
            },
            Location::Room(b, f, r) => match other {
                Location::Sensor(b_other, f_other, r_other, _) => b == b_other && f == f_other && r == r_other,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn with_floor(&self, floor: u32) -> anyhow::Result<Location> {
        match self {
            Location::Building(b) => Ok(Location::Floor(*b, floor)),
            _ => {
                anyhow::bail!("Cannot set floor on non-building location: {}", self);
            }
        }
    }

    pub fn with_room(&self, room: u32) -> anyhow::Result<Location> {
        match self {
            Location::Floor(b, f) => Ok(Location::Room(*b, *f, room)),
            _ => {
                anyhow::bail!("Cannot set room on non-floor location: {}", self);
            }
        }
    }

    pub fn with_sensor(&self, sensor: String) -> anyhow::Result<Location> {
        match self {
            Location::Room(b, f, r) => Ok(Location::Sensor(*b, *f, *r, sensor)),
            _ => {
                anyhow::bail!("Cannot set sensor on non-room location: {}", self);
            }
        }
    }
}

// Implement Display for Location
impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Location::Building(b) => write!(f, "B_{:03}", b),
            Location::Floor(b, fl) => write!(f, "F_{:03}_{:03}", b, fl),
            Location::Room(b, fl, r) => write!(f, "R_{:03}_{:03}_{:03}", b, fl, r),
            Location::Sensor(b, fl, r, sensor) => write!(f, "R_{:03}_{:03}_{:03}_{}", b, fl, r, sensor),
        }
    }
}

// Implement FromStr for Location
impl FromStr for Location {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('_').collect();
        match parts.as_slice() {
            ["B", b] => {
                let b_num = b.parse::<u32>()
                    .context(format!("Invalid building number in '{}'", s))?;
                Ok(Location::Building(b_num))
            }
            ["F", b, f] => {
                let b_num = b.parse::<u32>()
                    .context(format!("Invalid building number in '{}'", s))?;
                let f_num = f.parse::<u32>()
                    .context(format!("Invalid floor number in '{}'", s))?;
                Ok(Location::Floor(b_num, f_num))
            }
            ["R", b, f, r] => {
                let b_num = b.parse::<u32>()
                    .context(format!("Invalid building number in '{}'", s))?;
                let f_num = f.parse::<u32>()
                    .context(format!("Invalid floor number in '{}'", s))?;
                let r_num = r.parse::<u32>()
                    .context(format!("Invalid room number in '{}'", s))?;
                Ok(Location::Room(b_num, f_num, r_num))
            },
            ["S", b, f, r, sensor] => {
                let b_num = b.parse::<u32>()
                    .context(format!("Invalid building number in '{}'", s))?;
                let f_num = f.parse::<u32>()
                    .context(format!("Invalid floor number in '{}'", s))?;
                let r_num = r.parse::<u32>()
                    .context(format!("Invalid room number in '{}'", s))?;
                Ok(Location::Sensor(b_num, f_num, r_num, sensor.to_string()))
            }
            _ => anyhow::bail!("Invalid location format: '{}'. Expected 'B_xxx', 'F_xxx_yyy', 'R_xxx_yyy_zzz', or 'S_xxx_yyy_zzz_sss'", s),
        }
    }
}

// Define graph element types as constants for consistency
pub struct GraphElementType;

impl GraphElementType {
    pub const BUILDING: &'static str = "Building";
    pub const FLOOR: &'static str = "Floor";
    pub const ROOM: &'static str = "Room";
    pub const BUILDING_FLOOR: &'static str = "BuildingFloor";
    pub const FLOOR_ROOM: &'static str = "FloorRoom";
}

// Structs for building, floor, and room
#[derive(Debug, Clone, Serialize)]
pub struct BuildingNode {
    #[serde(skip_serializing)]
    pub effective_from: u64, 
    pub id: String,
    pub labels: Vec<String>,    
}

impl From<&Building> for BuildingNode {
    fn from(building: &Building) -> Self {
        BuildingNode {
            id: building.id.to_string(),
            effective_from: building.effective_from,
            labels: vec![GraphElementType::BUILDING.to_string()],
        }
    }
}

// Structs for building, floor, and room
#[derive(Debug, Clone)]
pub struct Building {
    pub effective_from: u64, 
    pub floors: BTreeMap<Location, Floor>, 
    pub id: Location, 
    pub next_floor: u32, 
}

impl Building {
    pub fn new(id: Location, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let mut building = Building {
            id,
            effective_from,
            floors: BTreeMap::new(),
            next_floor: 0,
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::BuildingAdded((&building).into()));

        // Create initial floors
        for _ in 0..change_generator.get_random_floor_count() {
            let (_, floor_changes) = building.add_floor(effective_from, change_generator)?;
            changes.extend(floor_changes);
        }

        Ok((building, changes))
    }

    pub fn add_floor(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {
        
        let floor_id = self.id.with_floor(self.next_floor)?;

        let (floor, changes) = Floor::new(floor_id.clone(), effective_from, change_generator)?;
        self.floors.insert(floor_id.clone(), floor);

        self.next_floor += 1;

        Ok((floor_id, changes))
    }        

    pub fn add_room(&mut self, floor_id: &Location, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {

        let floor = self.floors.get_mut(floor_id)
            .context(format!("Floor {} not found in building {}", floor_id, self.id))?;

        Ok(floor.add_room(effective_from, change_generator)?)
    }    

    pub fn get_floor(&self, floor_id: &Location) -> Option<&Floor> {

        if self.id.contains(floor_id) {
            self.floors.get(floor_id)
        } else {
            None
        }
    }

    pub fn get_room(&self, room_id: &Location) -> Option<&Room> {
        
        match self.get_floor(room_id) {
            Some(floor) => floor.get_room(room_id),
            None => None,
        }
    }
    
    pub fn update_random_room(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<Option<ModelChange>> {

        match self.floors.len() {
            0 => return Ok(None),
            len => {
                let idx = change_generator.get_usize_in_range(0, len, false);
                let (_, floor) = self.floors.iter_mut().nth(idx).unwrap();

                Ok(floor.update_random_room(effective_from, change_generator)?)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FloorNode {
    #[serde(skip_serializing)]
    pub effective_from: u64,  
    pub id: String,      
    pub labels: Vec<String>,    
}

impl From<&Floor> for FloorNode {
    fn from(floor: &Floor) -> Self {
        FloorNode {
            id: floor.id.to_string(),
            effective_from: floor.effective_from,
            labels: vec![GraphElementType::FLOOR.to_string()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Floor {
    pub id: Location,       // Location::Floor(building, floor)
    pub effective_from: u64,  // Timestamp of creation or last update
    pub rooms: BTreeMap<Location, Room>, // Maps room IDs to Room objects
    pub next_room: u32
}

impl Floor {
    pub fn new(id: Location, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let mut floor = Floor {
            id: id.clone(),
            effective_from,
            rooms: BTreeMap::new(),
            next_room: 0,
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::FloorAdded((&floor).into()));
        changes.push(ModelChange::BuildingFloorRelationAdded(BuildingFloorRelation::new(effective_from,&id)?));

        // Create Rooms
        for _ in 0..change_generator.get_random_room_count() {
            let (_, room_changes) = floor.add_room(effective_from, change_generator)?;
            changes.extend(room_changes);
        }

        Ok((floor, changes))
    }

    pub fn add_room(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {
        
        let room_id = self.id.with_room(self.next_room)?;

        let (room, changes) = Room::new(room_id.clone(), effective_from, change_generator)?;
        self.rooms.insert(room_id.clone(), room);

        self.next_room += 1;

        Ok((room_id, changes))
    }

    pub fn get_room(&self, room_id: &Location) -> Option<&Room> {
        if self.id.contains(room_id) {
            self.rooms.get(room_id)
        } else {
            None
        }
    }
    
    pub fn update_random_room(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<Option<ModelChange>> {

        match self.rooms.len() {
            0 => return Ok(None),
            len => {
                let idx = change_generator.get_usize_in_range(0, len, false);
                let (_, room) = self.rooms.iter_mut().nth(idx).unwrap();

                Ok(Some(room.update_sensor_values(effective_from, change_generator)?))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RoomNode {
    #[serde(skip_serializing)]
    pub effective_from: u64,
    pub id: String,
    pub labels: Vec<String>,    
    pub properties: Map<String, Value>
}

impl From<&Room> for RoomNode {
    fn from(room: &Room) -> Self {

        let mut properties = Map::new();

        for sensor_value in &room.sensor_values {
            match sensor_value {
                SensorValue::NormalFloat(sensor) => {
                    properties.insert(sensor.id.get_sensor_id().unwrap(), Value::Number(serde_json::Number::from_f64(sensor.value).unwrap()));
                },
                SensorValue::NormalInt(sensor) => {
                    properties.insert(sensor.id.get_sensor_id().unwrap(), Value::Number(serde_json::Number::from(sensor.value)));
                }
            }
        }

        RoomNode {
            effective_from: room.effective_from,
            id: room.id.to_string(),
            labels: vec![GraphElementType::ROOM.to_string()],
            properties
        }
    }
}

#[derive(Debug, Clone)]
pub struct Room {
    pub effective_from: u64,      
    pub id: Location,           
    pub sensor_values: Vec<SensorValue>, 
}

impl Room {
    pub fn new(id: Location, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let room = Room {
            effective_from,
            id: id.clone(),
            sensor_values: change_generator.initialize_sensor_values(&id, effective_from)?
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::RoomAdded((&room).into()));
        changes.push(ModelChange::FloorRoomRelationAdded(FloorRoomRelation::new(effective_from, &id)?));

        Ok((room, changes))
    }

    pub fn update_sensor_values(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<ModelChange> {

        let old_room_node: RoomNode = (&*self).into();

        self.effective_from = effective_from;
        change_generator.update_sensor_values(effective_from, &mut self.sensor_values);

        Ok(ModelChange::RoomUpdated(old_room_node, (&*self).into()))
    }
}

#[derive(Debug, Clone)]
pub struct BuildingFloorRelation {
    pub id: String,
    pub effective_from: u64,
    pub building_id: String,
    pub floor_id: String,
    pub labels: Vec<String>,
}

impl BuildingFloorRelation {
    pub fn new(effective_from: u64, floor_id: &Location) -> anyhow::Result<Self> {

        match floor_id {
            Location::Building(_) => anyhow::bail!("Floor ID must be a Floor or Room location"),
            _ => {
                let building_id = floor_id.get_building_location().unwrap();

                Ok(BuildingFloorRelation {
                    id: format!("BFR_{}_{}", building_id, floor_id),
                    effective_from,
                    building_id: building_id.to_string(),
                    floor_id: floor_id.to_string(),
                    labels: vec![GraphElementType::BUILDING_FLOOR.to_string()],
                })        
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct FloorRoomRelation {
    pub id: String,
    pub effective_from: u64,
    pub floor_id: String,
    pub room_id: String,
    pub labels: Vec<String>,
}

impl FloorRoomRelation {
    pub fn new(effective_from: u64, room_id: &Location) -> anyhow::Result<Self> {

        match room_id {
            Location::Room(_, _, _) => {
                let floor_id = room_id.get_floor_location().unwrap();

                Ok(FloorRoomRelation {
                    id: format!("BFR_{}_{}", floor_id, room_id),
                    effective_from,
                    floor_id: floor_id.to_string(),
                    room_id: room_id.to_string(),
                    labels: vec![GraphElementType::FLOOR_ROOM.to_string()],
                })        
            },
            _ => anyhow::bail!("Room ID must be a Room location"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SensorValue {
    NormalFloat(FloatNormalDistSensorValue),
    NormalInt(IntNormalDistSensorValue),
}

#[derive(Debug, Clone)]
pub struct FloatNormalDistSensorValue {
    pub effective_from: u64,
    pub id: Location,
    pub momentum: i32,
    pub value: f64,    
}

#[derive(Debug, Clone)]
pub struct IntNormalDistSensorValue {
    pub effective_from: u64,
    pub id: Location,
    pub momentum: i32,
    pub value: i64,    
}

#[derive(Debug, Clone)]
pub enum SensorValueGenerator {
    NormalFloat(FloatNormalDistSensorValueGenerator),
    NormalInt(IntNormalDistSensorValueGenerator),
}

#[derive(Debug, Clone)]
pub struct FloatNormalDistSensorValueGenerator {
    pub id: String,
    pub momentum_init_dist: Normal<f64>,
    pub momentum_reverse_prob: f64,
    pub value_change_dist: Normal<f64>,
    pub value_init_dist: Normal<f64>,
    pub value_range: (f64, f64),
}

#[derive(Debug, Clone)]
pub struct IntNormalDistSensorValueGenerator {
    pub id: String,
    pub momentum_init_dist: Normal<f64>,
    pub momentum_reverse_prob: f64,
    pub value_change_dist: Normal<f64>,
    pub value_init_dist: Normal<f64>,
    pub value_range: (i64, i64),
}

#[derive(Debug, Clone)]
pub struct GraphChangeGenerator {
    pub building_count_dist: Normal<f64>,
    pub floor_count_dist: Normal<f64>,
    pub rng: ChaCha8Rng,
    pub room_count_dist: Normal<f64>,
    pub room_sensor_value_generators: Vec<SensorValueGenerator>
}

impl GraphChangeGenerator {
    pub fn new(settings: &BuildingHierarchyDataGeneratorSettings) -> Self {
        log::debug!("Initialized GraphChangeGenerator with seed: {}", settings.seed);

        let mut change_generator = GraphChangeGenerator {
            building_count_dist: Normal::new(settings.building_count.0 as f64, settings.building_count.1).unwrap(),
            floor_count_dist: Normal::new(settings.floor_count.0 as f64, settings.floor_count.1).unwrap(),
            rng: ChaCha8Rng::seed_from_u64(settings.seed),
            room_count_dist: Normal::new(settings.room_count.0 as f64, settings.room_count.1).unwrap(),
            room_sensor_value_generators: Vec::new()
        };

        for sensor in &settings.room_sensors {
            match sensor {
                SensorDefinition::NormalFloat(def) => {
                    let (change_mean, change_std_dev) = def.value_change.unwrap_or((3.0, 5.0));
                    let (init_mean, init_std_dev) = def.value_init.unwrap_or((0.0, 1.0));
                    let (momentum_mean, momentum_std_dev, momentum_reverse_prob) = def.momentum_init.unwrap_or((3, 1.0, 0.5));
                    let (range_min, range_max) = def.value_range.unwrap_or((0.0, 100.0));

                    let svg = FloatNormalDistSensorValueGenerator {
                        id: def.id.clone(),
                        momentum_init_dist: Normal::new(momentum_mean as f64, momentum_std_dev).unwrap(),
                        momentum_reverse_prob,
                        value_change_dist: Normal::new(change_mean, change_std_dev).unwrap(),
                        value_init_dist: Normal::new(init_mean, init_std_dev).unwrap(),
                        value_range: (range_min, range_max),
                    };
                    change_generator.room_sensor_value_generators.push(SensorValueGenerator::NormalFloat(svg));
                },
                SensorDefinition::NormalInt(def) => {
                    let (change_mean, change_std_dev) = def.value_change.unwrap_or((3, 5.0));
                    let (init_mean, init_std_dev) = def.value_init.unwrap_or((0, 1.0));
                    let (momentum_mean, momentum_std_dev, momentum_reverse_prob) = def.momentum_init.unwrap_or((3, 1.0, 0.5));
                    let (range_min, range_max) = def.value_range.unwrap_or((0, 100));

                    let svg = IntNormalDistSensorValueGenerator {
                        id: def.id.clone(),
                        momentum_init_dist: Normal::new(momentum_mean as f64, momentum_std_dev).unwrap(),
                        momentum_reverse_prob,
                        value_change_dist: Normal::new(change_mean as f64, change_std_dev).unwrap(),
                        value_init_dist: Normal::new(init_mean as f64, init_std_dev).unwrap(),
                        value_range: (range_min, range_max),
                    };
                    change_generator.room_sensor_value_generators.push(SensorValueGenerator::NormalInt(svg));
                }
            }
        };

        change_generator
    }
    
    pub fn get_random_boolean(&mut self) -> bool {
        self.rng.random_bool(0.5)
    }

    pub fn get_random_building_count(&mut self) -> u32 {
        let value = self.building_count_dist.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_random_floor_count(&mut self) -> u32 {
        let value = self.floor_count_dist.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_random_room_count(&mut self) -> u32 {
        let value = self.room_count_dist.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_usize_in_range(&mut self, min: usize, max: usize, inclusive: bool) -> usize {
        match inclusive {
            true => self.rng.random_range(min..=max),
            false => self.rng.random_range(min..max),
        }   
    }

    pub fn initialize_sensor_values(&mut self, room_id: &Location, effective_from: u64) -> anyhow::Result<Vec<SensorValue>> {
        let mut sensor_values = Vec::new();

        for sensor in &self.room_sensor_value_generators {
            match sensor {
                SensorValueGenerator::NormalFloat(svg) => {
                    let mut val = FloatNormalDistSensorValue {
                        effective_from,
                        id: room_id.with_sensor(svg.id.clone())?,
                        momentum: (svg.momentum_init_dist.sample(&mut self.rng).round() as i32).max(1),
                        value: svg.value_init_dist.sample(&mut self.rng).clamp(svg.value_range.0, svg.value_range.1),
                    };

                    if self.rng.random_bool(svg.momentum_reverse_prob) { val.momentum = -val.momentum; }

                    sensor_values.push(SensorValue::NormalFloat(val));
                },
                SensorValueGenerator::NormalInt(svg) => {
                    let mut val = IntNormalDistSensorValue {
                        effective_from,
                        id: room_id.with_sensor(svg.id.clone())?,
                        momentum: (svg.momentum_init_dist.sample(&mut self.rng).round() as i32).max(1),
                        value: (svg.value_init_dist.sample(&mut self.rng) as i64).clamp(svg.value_range.0, svg.value_range.1)
                    };

                    if self.rng.random_bool(svg.momentum_reverse_prob) { val.momentum = -val.momentum; }

                    sensor_values.push(SensorValue::NormalInt(val));
                }
            }
        }

        Ok(sensor_values)
    }

    pub fn update_sensor_values(&mut self, effective_from: u64, sensor_values: &mut Vec<SensorValue>) {

        let sensor_to_update = self.get_usize_in_range(0, self.room_sensor_value_generators.len(), false);

        match &mut self.room_sensor_value_generators[sensor_to_update] {
            SensorValueGenerator::NormalFloat(svg) => {
                if let SensorValue::NormalFloat(ref mut sensor_value) = sensor_values[sensor_to_update] {

                    let value_change = svg.value_change_dist.sample(&mut self.rng);

                    if sensor_value.momentum > 0 {
                        sensor_value.value = (sensor_value.value + value_change).clamp(svg.value_range.0, svg.value_range.1);

                        if sensor_value.momentum > 1 {
                            sensor_value.momentum -= 1;
                        } else {
                            sensor_value.momentum = (svg.momentum_init_dist.sample(&mut self.rng).round() as i32).max(1);
                            if self.rng.random_bool(svg.momentum_reverse_prob) { sensor_value.momentum = -sensor_value.momentum; }
                        }
                    } else if sensor_value.momentum < 0 {
                        sensor_value.value = (sensor_value.value - value_change).clamp(svg.value_range.0, svg.value_range.1);

                        if sensor_value.momentum < -1 {
                            sensor_value.momentum += 1;
                        } else {
                            sensor_value.momentum = -(svg.momentum_init_dist.sample(&mut self.rng).round() as i32).max(1);
                            if self.rng.random_bool(svg.momentum_reverse_prob) { sensor_value.momentum = -sensor_value.momentum; }
                        }
                    }                    

                    sensor_value.effective_from = effective_from;
                }
            },
            SensorValueGenerator::NormalInt(svg) => {
                if let SensorValue::NormalInt(ref mut sensor_value) = sensor_values[sensor_to_update] {

                    let value_change = svg.value_change_dist.sample(&mut self.rng) as i64;

                    if sensor_value.momentum > 0 {
                        sensor_value.value = (sensor_value.value + value_change).clamp(svg.value_range.0, svg.value_range.1);
                        sensor_value.momentum -= 1;
                    } else {
                        sensor_value.value = (sensor_value.value - value_change).clamp(svg.value_range.0, svg.value_range.1);
                        sensor_value.momentum += 1;
                    }
                    sensor_value.effective_from = effective_from;
                }
            }
        }
    }
}

// Thread-safe graph structure
#[derive(Debug)]
pub struct BuildingGraph {
    buildings: Mutex<BTreeMap<Location, Building>>,
    change_generator: GraphChangeGenerator,             
    next_building_num: u32
}

impl BuildingGraph {
    pub fn new(settings: &BuildingHierarchyDataGeneratorSettings) -> anyhow::Result<Self> {

        let mut building_graph = BuildingGraph {
            buildings: Mutex::new(BTreeMap::new()),
            change_generator: GraphChangeGenerator::new(settings),
            next_building_num: 0,
        };

        for _ in 0..building_graph.change_generator.get_random_building_count() {
            building_graph.add_building(0)?;
        };

        Ok(building_graph)
    }

    pub fn add_building(&mut self, effective_from: u64) -> anyhow::Result<(Location, Vec<ModelChange>)> {

        let mut buildings = self.buildings.lock();

        let building_id = Location::Building(self.next_building_num);

        let (building, changes) = Building::new(building_id.clone(), effective_from, &mut self.change_generator)?;
        buildings.insert(building_id.clone(), building);
        self.next_building_num += 1;

        Ok((building_id, changes))
    }        

    // Add a floor with auto-generated ID
    pub fn add_floor(&mut self, building_id: &Location, effective_from: u64) -> anyhow::Result<(Location, Vec<ModelChange>)> {

        let mut buildings = self.buildings.lock();

        let building = buildings.get_mut(building_id).unwrap();

        Ok(building.add_floor(effective_from, &mut self.change_generator)?)
    }

    // Add a room with auto-generated ID, requiring only floor_id
    pub fn add_room(&mut self, floor_id: &Location, effective_from: u64) -> anyhow::Result<(Location, Vec<ModelChange>)> {

        let mut buildings = self.buildings.lock();

        let building = buildings.get_mut(&floor_id.get_building_location().unwrap()).unwrap();

        Ok(building.add_room(floor_id, effective_from, &mut self.change_generator)?)
    }

    pub fn get_current_state(&self, included_types: &HashSet<String>) -> CurrentStateIterator<'_> {
        // Lock the buildings mutex for the duration of the iteration
        let buildings = self.buildings.lock();
        
        // Collect all building IDs for initial state
        let building_ids = buildings.keys()
            .map(|k| k.to_string())
            .collect::<Vec<_>>();
        
        CurrentStateIterator {
            buildings_guard: buildings,
            state: IterationState::Buildings {
                building_index: 0,
                buildings: building_ids,
            },
            included_types: included_types.clone(),
        }
    }

    pub fn update_random_room(&mut self, effective_from: u64) -> anyhow::Result<Option<ModelChange>> {

        let mut buildings = self.buildings.lock();

        match buildings.len() {
            0 => Ok(None),
            _ => {
                let idx = self.change_generator.get_usize_in_range(0, buildings.len(), false);
                let (_, building) = buildings.iter_mut().nth(idx).unwrap();

                Ok(building.update_random_room(effective_from, &mut self.change_generator)?)
            }
        }
    }
}

// Define the possible iteration states
enum IterationState {
    Buildings { building_index: usize, buildings: Vec<String> },
    Floors { building_index: usize, floor_index: usize, buildings: Vec<String>, building_floors: BTreeMap<String, Vec<String>> },
    Rooms { building_index: usize, floor_index: usize, room_index: usize, buildings: Vec<String>, building_floors: BTreeMap<String, Vec<String>>, floor_rooms: BTreeMap<String, Vec<String>> },
    BuildingFloorRelations { relation_index: usize, relations: Vec<(String, String)> },
    FloorRoomRelations { relation_index: usize, relations: Vec<(String, String)> },
    Done,
}

// Define a new iterator structure that will hold the MutexGuard and current state
pub struct CurrentStateIterator<'a> {
    // Hold the mutex guard to ensure the data doesn't change during iteration
    buildings_guard: MutexGuard<'a, BTreeMap<Location, Building>>,
    // Current state of iteration
    state: IterationState,
    // Set of element types to include
    included_types: HashSet<String>,
}

impl<'a> Iterator for CurrentStateIterator<'a> {
    type Item = ModelChange;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            IterationState::Buildings { building_index, buildings } => {
                if *building_index < buildings.len() {
                    let building_id = &buildings[*building_index];
                    let building = self.buildings_guard.get(&building_id.parse::<Location>().unwrap()).unwrap();
                    *building_index += 1;
                    
                    // If buildings are included, return a BuildingNode
                    if self.included_types.contains(GraphElementType::BUILDING) {
                        let building_node = BuildingNode::from(building);
                        return Some(ModelChange::BuildingAdded(building_node));
                    } else {
                        // Skip to next building if not included
                        return self.next();
                    }
                } else {
                    // Prepare for next state: floors
                    let mut building_floors = BTreeMap::new();
                    for (building_id, building) in self.buildings_guard.iter() {
                        let building_id_str = building_id.to_string();
                        let floor_ids = building.floors.keys()
                            .map(|k| k.to_string())
                            .collect::<Vec<_>>();
                        building_floors.insert(building_id_str, floor_ids);
                    }
                    
                    self.state = IterationState::Floors { 
                        building_index: 0, 
                        floor_index: 0, 
                        buildings: buildings.clone(), 
                        building_floors 
                    };
                    self.next()
                }
            },
            IterationState::Floors { building_index, floor_index, buildings, building_floors } => {
                if *building_index < buildings.len() {
                    let building_id = &buildings[*building_index];
                    let empty_vec = Vec::new();
                    let floors = building_floors.get(building_id).unwrap_or(&empty_vec);
                    
                    if *floor_index < floors.len() {
                        let floor_id = &floors[*floor_index];
                        let building = self.buildings_guard.get(&building_id.parse::<Location>().unwrap()).unwrap();
                        let floor = building.floors.get(&floor_id.parse::<Location>().unwrap()).unwrap();
                        
                        *floor_index += 1;
                        
                        // If floors are included, return a FloorNode
                        if self.included_types.contains(GraphElementType::FLOOR) {
                            let floor_node = FloorNode::from(floor);
                            return Some(ModelChange::FloorAdded(floor_node));
                        } else {
                            // Skip to next floor if not included
                            return self.next();
                        }
                    } else {
                        // Move to next building
                        *building_index += 1;
                        *floor_index = 0;
                        self.next()
                    }
                } else {
                    // Prepare for next state: rooms
                    let mut floor_rooms = BTreeMap::new();
                    for (_, building) in self.buildings_guard.iter() {
                        for (floor_id, floor) in building.floors.iter() {
                            let floor_id_str = floor_id.to_string();
                            let room_ids = floor.rooms.keys()
                                .map(|k| k.to_string())
                                .collect::<Vec<_>>();
                            floor_rooms.insert(floor_id_str, room_ids);
                        }
                    }
                    
                    self.state = IterationState::Rooms { 
                        building_index: 0, 
                        floor_index: 0, 
                        room_index: 0,
                        buildings: buildings.clone(), 
                        building_floors: building_floors.clone(),
                        floor_rooms
                    };
                    self.next()
                }
            },
            IterationState::Rooms { building_index, floor_index, room_index, buildings, building_floors, floor_rooms } => {
                if *building_index < buildings.len() {
                    let building_id = &buildings[*building_index];
                    let empty_vec = Vec::new();
                    let floors = building_floors.get(building_id).unwrap_or(&empty_vec);
                    
                    if *floor_index < floors.len() {
                        let floor_id = &floors[*floor_index];
                        let empty_vec = Vec::new();
                        let rooms = floor_rooms.get(floor_id).unwrap_or(&empty_vec);
                        
                        if *room_index < rooms.len() {
                            let room_id = &rooms[*room_index];
                            let building = self.buildings_guard.get(&building_id.parse::<Location>().unwrap()).unwrap();
                            let floor = building.floors.get(&floor_id.parse::<Location>().unwrap()).unwrap();
                            let room = floor.rooms.get(&room_id.parse::<Location>().unwrap()).unwrap();
                            
                            *room_index += 1;
                            
                            // If rooms are included, return a RoomNode
                            if self.included_types.contains(GraphElementType::ROOM) {
                                let room_node = RoomNode::from(room);
                                return Some(ModelChange::RoomAdded(room_node));
                            } else {
                                // Skip to next room if not included
                                return self.next();
                            }
                        } else {
                            // Move to next floor
                            *floor_index += 1;
                            *room_index = 0;
                            self.next()
                        }
                    } else {
                        // Move to next building
                        *building_index += 1;
                        *floor_index = 0;
                        *room_index = 0;
                        self.next()
                    }
                } else {
                    // Prepare for next state: BuildingFloorRelations
                    let mut relations = Vec::new();
                    for (_, building) in self.buildings_guard.iter() {
                        for floor_id in building.floors.keys() {
                            relations.push((building.id.to_string(), floor_id.to_string()));
                        }
                    }
                    
                    self.state = IterationState::BuildingFloorRelations { 
                        relation_index: 0,
                        relations
                    };
                    self.next()
                }
            },
            IterationState::BuildingFloorRelations { relation_index, relations } => {
                if *relation_index < relations.len() {
                    let (_, floor_id_str) = &relations[*relation_index];
                    *relation_index += 1;
                    
                    // If building-floor relations are included, return one
                    if self.included_types.contains(GraphElementType::BUILDING_FLOOR) {
                        let floor_id = floor_id_str.parse::<Location>().unwrap();
                        return Some(ModelChange::BuildingFloorRelationAdded(
                            BuildingFloorRelation::new(0, &floor_id).unwrap()
                        ));
                    } else {
                        // Skip to next relation if not included
                        return self.next();
                    }
                } else {
                    // Prepare for next state: FloorRoomRelations
                    let mut relations = Vec::new();
                    for (_, building) in self.buildings_guard.iter() {
                        for (floor_id, floor) in building.floors.iter() {
                            for room_id in floor.rooms.keys() {
                                relations.push((floor_id.to_string(), room_id.to_string()));
                            }
                        }
                    }
                    
                    self.state = IterationState::FloorRoomRelations { 
                        relation_index: 0,
                        relations
                    };
                    self.next()
                }
            },
            IterationState::FloorRoomRelations { relation_index, relations } => {
                if *relation_index < relations.len() {
                    let (_, room_id_str) = &relations[*relation_index];
                    *relation_index += 1;
                    
                    // If floor-room relations are included, return one
                    if self.included_types.contains(GraphElementType::FLOOR_ROOM) {
                        let room_id = room_id_str.parse::<Location>().unwrap();
                        return Some(ModelChange::FloorRoomRelationAdded(
                            FloorRoomRelation::new(0, &room_id).unwrap()
                        ));
                    } else {
                        // Skip to next relation if not included
                        return self.next();
                    }
                } else {
                    self.state = IterationState::Done;
                    None
                }
            },
            IterationState::Done => None,
        }
    }
}