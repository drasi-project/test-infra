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

use std::{collections::{HashMap, HashSet}, fmt, str::FromStr};

use anyhow::Context;
use parking_lot::{Mutex, MutexGuard};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal};
use serde::Serialize;

use super::BuildingHierarchyDataGeneratorSettings;

// Define element types as constants for consistency
pub struct ElementType;

impl ElementType {
    pub const BUILDING: &'static str = "Building";
    pub const FLOOR: &'static str = "Floor";
    pub const ROOM: &'static str = "Room";
    pub const BUILDING_FLOOR: &'static str = "BuildingFloor";
    pub const FLOOR_ROOM: &'static str = "FloorRoom";
}

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
#[derive(Debug, Clone)]
pub enum Location {
    Building(u32),           // e.g., Building(0) -> "B_000"
    Floor(u32, u32),       // e.g., Floor(0, 1) -> "F_000_001"
    Room(u32, u32, u32), // e.g., Room(0, 1, 2) -> "R_000_001_002"
}

impl Location {
    pub fn get_building_location(&self) -> Option<Location> {
        match self {
            Location::Building(_) => Some(self.clone()),
            Location::Floor(b, _) => Some(Location::Building(*b)),
            Location::Room(b, _, _) => Some(Location::Building(*b)),
        }
    }

    pub fn get_floor_location(&self) -> Option<Location> {
        match self {
            Location::Building(_) => None,
            Location::Floor(_, _) => Some(self.clone()),
            Location::Room(b, f, _) => Some(Location::Floor(*b,*f)),
        }
    }

    pub fn is_in_location(&self, other: &Location) -> bool {
        match self {
            Location::Building(b) => match other {
                Location::Building(b2) => b == b2,
                Location::Floor(b2, _) => b == b2,
                Location::Room(b2, _, _) => b == b2,
            },
            Location::Floor(b, f) => match other {
                Location::Building(b2) => b == b2,
                Location::Floor(b2, f2) => b == b2 && f == f2,
                Location::Room(b2, f2, _) => b == b2 && f == f2,
            },
            Location::Room(b, f, r) => match other {
                Location::Building(b2) => b == b2,
                Location::Floor(b2, f2) => b == b2 && f == f2,
                Location::Room(b2, f2, r2) => b == b2 && f == f2 && r == r2,
            },
        }
    }

    pub fn with_floor(&self, floor: u32) -> Location {
        match self {
            Location::Building(b) => Location::Floor(*b, floor),
            Location::Floor(b, _) => Location::Floor(*b, floor),
            Location::Room(b, _, _) => Location::Floor(*b, floor),
        }
    }

    pub fn with_room(&self, room: u32) -> Location {
        match self {
            Location::Building(b) => Location::Room(*b, 0, room),
            Location::Floor(b, f) => Location::Room(*b, *f, room),
            Location::Room(b, f, _) => Location::Room(*b, *f, room),
        }
    }
}

// Implement Hash and PartialEq manually
impl std::hash::Hash for Location {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Location::Building(b) => {
                b.hash(state);
            }
            Location::Floor(b, f) => {
                b.hash(state);
                f.hash(state);
            }
            Location::Room(b, f, r) => {
                b.hash(state);
                f.hash(state);
                r.hash(state);
            }
        }
    }
}

impl PartialEq for Location {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Location::Building(b1), Location::Building(b2)) => b1 == b2,
            (Location::Floor(b1, f1), Location::Floor(b2, f2)) => b1 == b2 && f1 == f2,
            (Location::Room(b1, f1, r1), Location::Room(b2, f2, r2)) => b1 == b2 && f1 == f2 && r1 == r2,
            _ => false,
        }
    }
}

impl Eq for Location {}

// Implement Display for Location
impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Location::Building(b) => write!(f, "B_{:03}", b),
            Location::Floor(b, fl) => write!(f, "F_{:03}_{:03}", b, fl),
            Location::Room(b, fl, r) => write!(f, "R_{:03}_{:03}_{:03}", b, fl, r),
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
            }
            _ => anyhow::bail!("Invalid location format: '{}'. Expected 'B_xxx', 'F_xxx_yyy', or 'R_xxx_yyy_zzz'", s),
        }
    }
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
            labels: vec![ElementType::BUILDING.to_string()],
        }
    }
}

// Structs for building, floor, and room
#[derive(Debug, Clone)]
pub struct Building {
    pub effective_from: u64, 
    pub floors: HashMap<Location, Floor>, 
    pub id: Location, 
    pub next_floor: u32, 
}

impl Building {
    pub fn new(id: Location, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let mut building = Building {
            id,
            effective_from,
            floors: HashMap::new(),
            next_floor: 0,
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::BuildingAdded((&building).into()));

        // Create initial floors
        for _ in 0..sim_settings.get_random_floor_count() {
            let (_, floor_changes) = building.add_floor(effective_from, sim_settings)?;
            changes.extend(floor_changes);
        }

        Ok((building, changes))
    }

    pub fn add_floor(&mut self, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {
        let id = self.id.with_floor(self.next_floor);

        let (floor, changes) = Floor::new(id.clone(), effective_from, sim_settings)?;
        self.floors.insert(id.clone(), floor);

        self.next_floor += 1;

        Ok((id, changes))
    }        

    pub fn add_room(&mut self, floor_id: &Location, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {

        let floor = self.floors.get_mut(floor_id)
            .context(format!("Floor {} not found in building {}", floor_id, self.id))?;

        Ok(floor.add_room(effective_from, sim_settings)?)
    }    

    pub fn get_floor(&self, id: &Location) -> Option<&Floor> {

        if id.is_in_location(&self.id) {
            self.floors.get(id)
        } else {
            None
        }
    }

    pub fn get_room(&self, id: &Location) -> Option<&Room> {
        
        match self.get_floor(id) {
            Some(floor) => floor.get_room(id),
            None => None,
        }
    }
    
    pub fn update_random_room(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<Option<ModelChange>> {

        match self.floors.len() {
            0 => return Ok(None),
            len => {
                let idx = change_generator.get_usize_in_range(0, len, false);
                let floor = self.floors.values_mut().nth(idx).unwrap();

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
            labels: vec![ElementType::FLOOR.to_string()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Floor {
    pub id: Location,       // Location::Floor(building, floor)
    pub effective_from: u64,  // Timestamp of creation or last update
    pub rooms: HashMap<Location, Room>, // Maps room IDs to Room objects
    pub next_room: u32
}

impl Floor {
    pub fn new(id: Location, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let mut floor = Floor {
            id: id.clone(),
            effective_from,
            rooms: HashMap::new(),
            next_room: 0,
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::FloorAdded((&floor).into()));
        changes.push(ModelChange::BuildingFloorRelationAdded(BuildingFloorRelation::new(effective_from,&id)?));

        // Create Rooms
        for _ in 0..sim_settings.get_random_room_count() {
            let (_, room_changes) = floor.add_room(effective_from, sim_settings)?;
            changes.extend(room_changes);
        }

        Ok((floor, changes))
    }

    pub fn add_room(&mut self, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Location, Vec<ModelChange>)> {
        
        let id = self.id.with_room(self.next_room);

        let (room, changes) = Room::new(id.clone(), effective_from, sim_settings)?;
        self.rooms.insert(id.clone(), room);

        self.next_room += 1;

        Ok((id, changes))
    }

    pub fn get_room(&self, id: &Location) -> Option<&Room> {
        if id.is_in_location(&self.id) {
            self.rooms.get(id)
        } else {
            None
        }
    }
    
    pub fn update_random_room(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<Option<ModelChange>> {

        match self.rooms.len() {
            0 => return Ok(None),
            len => {
                let idx = change_generator.get_usize_in_range(0, len, false);
                let room = self.rooms.values_mut().nth(idx).unwrap();

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
    pub properties: SensorValues,
}

impl From<&Room> for RoomNode {
    fn from(room: &Room) -> Self {
        RoomNode {
            effective_from: room.effective_from,
            id: room.id.to_string(),
            labels: vec![ElementType::ROOM.to_string()],
            properties: room.sensor_values.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Room {
    pub id: Location,           // Location::Room(building, floor, room)
    pub sensor_values: SensorValues,
    pub effective_from: u64,      // Timestamp of creation or last update
}

impl Room {
    pub fn new(id: Location, effective_from: u64, sim_settings: &mut GraphChangeGenerator) -> anyhow::Result<(Self, Vec<ModelChange>)> {

        let room = Room {
            id: id.clone(),
            sensor_values: sim_settings.get_initial_sensor_values(),
            effective_from,
        };

        let mut changes = Vec::new();
        changes.push(ModelChange::RoomAdded((&room).into()));
        changes.push(ModelChange::FloorRoomRelationAdded(FloorRoomRelation::new(effective_from, &id)?));

        Ok((room, changes))
    }

    pub fn update_sensor_values(&mut self, effective_from: u64, change_generator: &mut GraphChangeGenerator) -> anyhow::Result<ModelChange> {

        let old_room_node: RoomNode = (&*self).into();

        self.effective_from = effective_from;
        self.sensor_values = change_generator.update_sensor_values(&self.sensor_values);

        Ok(ModelChange::RoomUpdated(old_room_node, (&*self).into()))
    }
}

// Environmental values for rooms
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize)]
pub struct SensorValues {
    pub co2: f32,         // ppm
    pub humidity: f32,    // Percentage
    pub light: f32,       // Lux
    pub noise: f32,       // dB
    pub temperature: f32, // Celsius
    pub occupancy: u32,   // Count of people
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
                    labels: vec![ElementType::BUILDING_FLOOR.to_string()],
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
                    labels: vec![ElementType::FLOOR_ROOM.to_string()],
                })        
            },
            _ => anyhow::bail!("Room ID must be a Room location"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphChangeGenerator {
    pub building_count_distribution: Normal<f64>,
    pub floor_count_distribution: Normal<f64>,
    pub room_count_distribution: Normal<f64>,
    pub sensor_co2_distribution: Normal<f64>,
    pub sensor_humidity_distribution: Normal<f64>,
    pub sensor_light_distribution: Normal<f64>,
    pub sensor_noise_distribution: Normal<f64>,
    pub sensor_temperature_distribution: Normal<f64>,
    pub sensor_occupancy_distribution: Normal<f64>,
    pub rng: ChaCha8Rng
}

impl GraphChangeGenerator {
    pub fn new(settings: &BuildingHierarchyDataGeneratorSettings) -> Self {
        let mut g = GraphChangeGenerator {
            building_count_distribution: Normal::new(settings.initialization_settings.building_count.0 as f64, settings.initialization_settings.building_count.1).unwrap(),
            floor_count_distribution: Normal::new(settings.initialization_settings.floor_count.0 as f64, settings.initialization_settings.floor_count.1).unwrap(),
            room_count_distribution: Normal::new(settings.initialization_settings.room_count.0 as f64, settings.initialization_settings.room_count.1).unwrap(),
            sensor_co2_distribution: Normal::new(settings.initialization_settings.sensor_co2.0 as f64, settings.initialization_settings.sensor_co2.1).unwrap(),
            sensor_humidity_distribution: Normal::new(settings.initialization_settings.sensor_humidity.0 as f64, settings.initialization_settings.sensor_humidity.1).unwrap(),
            sensor_light_distribution: Normal::new(settings.initialization_settings.sensor_light.0 as f64, settings.initialization_settings.sensor_light.1).unwrap(),
            sensor_noise_distribution: Normal::new(settings.initialization_settings.sensor_noise.0 as f64, settings.initialization_settings.sensor_noise.1).unwrap(),
            sensor_temperature_distribution: Normal::new(settings.initialization_settings.sensor_temperature.0 as f64, settings.initialization_settings.sensor_temperature.1).unwrap(),
            sensor_occupancy_distribution: Normal::new(settings.initialization_settings.sensor_occupancy.0 as f64, settings.initialization_settings.sensor_occupancy.1).unwrap(),
            rng: ChaCha8Rng::seed_from_u64(settings.seed)
        };

        log::error!("RNG initialized with seed: {}", settings.seed);
        log::error!("RNG test: {}", g.rng.random_range(0..100));
        log::error!("RNG test: {}", g.rng.random_range(0..100));
        log::error!("RNG test: {}", g.rng.random_range(0..100));
        log::error!("RNG test: {}", g.rng.random_range(0..100));
        log::error!("RNG test: {}", g.rng.random_range(0..100));

        g
    }
    
    pub fn get_initial_sensor_values(&mut self) -> SensorValues {

        let rng = &mut self.rng;

        SensorValues {
            co2: self.sensor_co2_distribution.sample(rng).max(0.0) as f32,
            humidity: self.sensor_humidity_distribution.sample(rng).max(0.0) as f32,
            light: self.sensor_light_distribution.sample(rng).max(0.0) as f32,
            noise: self.sensor_noise_distribution.sample(rng).max(0.0) as f32,
            temperature: self.sensor_temperature_distribution.sample(rng) as f32,
            occupancy: self.sensor_occupancy_distribution.sample(rng).max(0.0) as u32,
        }
    }

    pub fn get_random_building_count(&mut self) -> u32 {
        let value = self.building_count_distribution.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_random_floor_count(&mut self) -> u32 {
        let value = self.floor_count_distribution.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_random_room_count(&mut self) -> u32 {
        let value = self.room_count_distribution.sample(&mut self.rng);
        value.max(1.0) as u32
    }

    pub fn get_usize_in_range(&mut self, min: usize, max: usize, inclusive: bool) -> usize {
        match inclusive {
            true => self.rng.random_range(min..=max),
            false => self.rng.random_range(min..max),
        }   
    }

    pub fn update_sensor_values(&mut self, _sensor_values: &SensorValues) -> SensorValues {
        
        // TODO - update sensor values based on some logic
        // For now, just return new values
        self.get_initial_sensor_values()
    }

}

// Thread-safe graph structure
#[derive(Debug)]
pub struct BuildingGraph {
    buildings: Mutex<HashMap<Location, Building>>,
    change_generator: GraphChangeGenerator,             
    next_building_num: u32,         
}

impl BuildingGraph {
    pub fn new(settings: &BuildingHierarchyDataGeneratorSettings) -> anyhow::Result<Self> {

        let mut building_graph = BuildingGraph {
            buildings: Mutex::new(HashMap::new()),
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
        let buildings_guard = self.buildings.lock();
        
        // Collect all building IDs for initial state
        let buildings = buildings_guard.keys()
            .map(|k| k.to_string())
            .collect::<Vec<_>>();
        
        CurrentStateIterator {
            buildings_guard,
            state: IterationState::Buildings {
                building_index: 0,
                buildings,
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
                let building = buildings.values_mut().nth(idx).unwrap();

                Ok(building.update_random_room(effective_from, &mut self.change_generator)?)
            }
        }
    }
}

// Define the possible iteration states
enum IterationState {
    Buildings { building_index: usize, buildings: Vec<String> },
    Floors { building_index: usize, floor_index: usize, buildings: Vec<String>, building_floors: HashMap<String, Vec<String>> },
    Rooms { building_index: usize, floor_index: usize, room_index: usize, buildings: Vec<String>, building_floors: HashMap<String, Vec<String>>, floor_rooms: HashMap<String, Vec<String>> },
    BuildingFloorRelations { relation_index: usize, relations: Vec<(String, String)> },
    FloorRoomRelations { relation_index: usize, relations: Vec<(String, String)> },
    Done,
}

// Define a new iterator structure that will hold the MutexGuard and current state
pub struct CurrentStateIterator<'a> {
    // Hold the mutex guard to ensure the data doesn't change during iteration
    buildings_guard: MutexGuard<'a, HashMap<Location, Building>>,
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
                    if self.included_types.contains(ElementType::BUILDING) {
                        let building_node = BuildingNode::from(building);
                        return Some(ModelChange::BuildingAdded(building_node));
                    } else {
                        // Skip to next building if not included
                        return self.next();
                    }
                } else {
                    // Prepare for next state: floors
                    let mut building_floors = HashMap::new();
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
                        if self.included_types.contains(ElementType::FLOOR) {
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
                    let mut floor_rooms = HashMap::new();
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
                            if self.included_types.contains(ElementType::ROOM) {
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
                    if self.included_types.contains(ElementType::BUILDING_FLOOR) {
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
                    if self.included_types.contains(ElementType::FLOOR_ROOM) {
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