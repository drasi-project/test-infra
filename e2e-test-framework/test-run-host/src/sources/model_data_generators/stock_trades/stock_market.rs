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

use parking_lot::{Mutex, MutexGuard};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rand_distr::{Distribution, Normal};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};

use super::StockTradeDataGeneratorSettings;

#[derive(Debug)]
pub enum ModelChange {
    StockAdded(StockNode),
    StockUpdated(StockNode, StockNode),
    TradeExecuted(TradeNode),
}

#[derive(Debug, Clone, Serialize)]
pub struct StockNode {
    #[serde(skip_serializing)]
    pub effective_from: u64,
    pub id: String,
    pub labels: Vec<String>,
    pub properties: Map<String, Value>,
}

impl From<&Stock> for StockNode {
    fn from(stock: &Stock) -> Self {
        let mut properties = Map::new();
        properties.insert("symbol".to_string(), Value::String(stock.symbol.clone()));
        properties.insert("name".to_string(), Value::String(stock.name.clone()));
        properties.insert(
            "price".to_string(),
            Value::Number(serde_json::Number::from_f64(stock.current_price).unwrap()),
        );
        properties.insert(
            "volume".to_string(),
            Value::Number(serde_json::Number::from(stock.current_volume)),
        );
        properties.insert(
            "daily_high".to_string(),
            Value::Number(serde_json::Number::from_f64(stock.daily_high).unwrap()),
        );
        properties.insert(
            "daily_low".to_string(),
            Value::Number(serde_json::Number::from_f64(stock.daily_low).unwrap()),
        );
        properties.insert(
            "daily_open".to_string(),
            Value::Number(serde_json::Number::from_f64(stock.daily_open).unwrap()),
        );

        StockNode {
            effective_from: stock.effective_from,
            id: stock.id.clone(),
            labels: vec!["Stock".to_string()],
            properties,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeNode {
    #[serde(skip_serializing)]
    pub effective_from: u64,
    pub id: String,
    pub labels: Vec<String>,
    pub properties: Map<String, Value>,
}

#[derive(Debug, Clone)]
pub struct Stock {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub effective_from: u64,
    pub current_price: f64,
    pub current_volume: i64,
    pub price_momentum: i32,
    pub volume_momentum: i32,
    pub daily_high: f64,
    pub daily_low: f64,
    pub daily_open: f64,
}

impl Stock {
    pub fn new(
        id: String,
        symbol: String,
        name: String,
        effective_from: u64,
        change_generator: &mut StockChangeGenerator,
    ) -> anyhow::Result<(Self, Vec<ModelChange>)> {
        let initial_price = change_generator.get_initial_price();
        let initial_volume = change_generator.get_initial_volume();

        let stock = Stock {
            id,
            symbol,
            name,
            effective_from,
            current_price: initial_price,
            current_volume: initial_volume,
            price_momentum: change_generator.get_initial_price_momentum(),
            volume_momentum: change_generator.get_initial_volume_momentum(),
            daily_high: initial_price,
            daily_low: initial_price,
            daily_open: initial_price,
        };

        let changes = vec![ModelChange::StockAdded((&stock).into())];

        Ok((stock, changes))
    }

    pub fn update_price(
        &mut self,
        effective_from: u64,
        change_generator: &mut StockChangeGenerator,
    ) -> ModelChange {
        let old_stock_node: StockNode = (&*self).into();

        self.effective_from = effective_from;

        // Update price with momentum
        let (new_price, new_momentum) =
            change_generator.get_next_price(self.current_price, self.price_momentum);
        self.current_price = new_price;
        self.price_momentum = new_momentum;

        // Update daily high/low
        if self.current_price > self.daily_high {
            self.daily_high = self.current_price;
        }
        if self.current_price < self.daily_low {
            self.daily_low = self.current_price;
        }

        // Update volume
        let (new_volume, new_volume_momentum) =
            change_generator.get_next_volume(self.current_volume, self.volume_momentum);
        self.current_volume = new_volume;
        self.volume_momentum = new_volume_momentum;

        ModelChange::StockUpdated(old_stock_node, (&*self).into())
    }
}

#[derive(Debug, Clone)]
pub struct StockChangeGenerator {
    pub rng: ChaCha8Rng,
    pub price_init_dist: Normal<f64>,
    pub price_change_dist: Normal<f64>,
    pub price_momentum_dist: Normal<f64>,
    pub price_momentum_reverse_prob: f64,
    pub price_range: (f64, f64),
    pub volume_init_dist: Normal<f64>,
    pub volume_change_dist: Normal<f64>,
    pub volume_momentum_dist: Normal<f64>,
    pub volume_momentum_reverse_prob: f64,
    pub volume_range: (i64, i64),
}

impl StockChangeGenerator {
    pub fn new(settings: &StockTradeDataGeneratorSettings) -> Self {
        log::debug!(
            "Initialized StockChangeGenerator with seed: {}",
            settings.seed
        );

        StockChangeGenerator {
            rng: ChaCha8Rng::seed_from_u64(settings.seed),
            price_init_dist: Normal::new(settings.price_init.0 as f64, settings.price_init.1)
                .unwrap(),
            price_change_dist: Normal::new(settings.price_change.0 as f64, settings.price_change.1)
                .unwrap(),
            price_momentum_dist: Normal::new(
                settings.price_momentum.0 as f64,
                settings.price_momentum.1,
            )
            .unwrap(),
            price_momentum_reverse_prob: settings.price_momentum.2,
            price_range: settings.price_range,
            volume_init_dist: Normal::new(settings.volume_init.0 as f64, settings.volume_init.1)
                .unwrap(),
            volume_change_dist: Normal::new(
                settings.volume_change.0 as f64,
                settings.volume_change.1,
            )
            .unwrap(),
            volume_momentum_dist: Normal::new(
                settings.volume_momentum.0 as f64,
                settings.volume_momentum.1,
            )
            .unwrap(),
            volume_momentum_reverse_prob: settings.volume_momentum.2,
            volume_range: settings.volume_range,
        }
    }

    pub fn get_initial_price(&mut self) -> f64 {
        self.price_init_dist
            .sample(&mut self.rng)
            .clamp(self.price_range.0, self.price_range.1)
    }

    pub fn get_initial_volume(&mut self) -> i64 {
        (self.volume_init_dist.sample(&mut self.rng) as i64)
            .clamp(self.volume_range.0, self.volume_range.1)
    }

    pub fn get_initial_price_momentum(&mut self) -> i32 {
        let momentum = (self.price_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
        if self.rng.random_bool(self.price_momentum_reverse_prob) {
            -momentum
        } else {
            momentum
        }
    }

    pub fn get_initial_volume_momentum(&mut self) -> i32 {
        let momentum = (self.volume_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
        if self.rng.random_bool(self.volume_momentum_reverse_prob) {
            -momentum
        } else {
            momentum
        }
    }

    pub fn get_next_price(&mut self, current_price: f64, momentum: i32) -> (f64, i32) {
        let price_change = self.price_change_dist.sample(&mut self.rng).abs();

        let (new_price, new_momentum) = if momentum > 0 {
            let new_price =
                (current_price + price_change).clamp(self.price_range.0, self.price_range.1);
            let new_momentum = if momentum > 1 {
                momentum - 1
            } else {
                let m = (self.price_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
                if self.rng.random_bool(self.price_momentum_reverse_prob) {
                    -m
                } else {
                    m
                }
            };
            (new_price, new_momentum)
        } else {
            let new_price =
                (current_price - price_change).clamp(self.price_range.0, self.price_range.1);
            let new_momentum = if momentum < -1 {
                momentum + 1
            } else {
                let m = (self.price_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
                if self.rng.random_bool(self.price_momentum_reverse_prob) {
                    m
                } else {
                    -m
                }
            };
            (new_price, new_momentum)
        };

        (new_price, new_momentum)
    }

    pub fn get_next_volume(&mut self, current_volume: i64, momentum: i32) -> (i64, i32) {
        let volume_change = self.volume_change_dist.sample(&mut self.rng).abs() as i64;

        let (new_volume, new_momentum) = if momentum > 0 {
            let new_volume =
                (current_volume + volume_change).clamp(self.volume_range.0, self.volume_range.1);
            let new_momentum = if momentum > 1 {
                momentum - 1
            } else {
                let m = (self.volume_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
                if self.rng.random_bool(self.volume_momentum_reverse_prob) {
                    -m
                } else {
                    m
                }
            };
            (new_volume, new_momentum)
        } else {
            let new_volume =
                (current_volume - volume_change).clamp(self.volume_range.0, self.volume_range.1);
            let new_momentum = if momentum < -1 {
                momentum + 1
            } else {
                let m = (self.volume_momentum_dist.sample(&mut self.rng).round() as i32).max(1);
                if self.rng.random_bool(self.volume_momentum_reverse_prob) {
                    m
                } else {
                    -m
                }
            };
            (new_volume, new_momentum)
        };

        (new_volume, new_momentum)
    }

    pub fn get_usize_in_range(&mut self, min: usize, max: usize, inclusive: bool) -> usize {
        match inclusive {
            true => self.rng.random_range(min..=max),
            false => self.rng.random_range(min..max),
        }
    }
}

// Thread-safe market structure
#[derive(Debug)]
pub struct StockMarket {
    stocks: Mutex<BTreeMap<String, Stock>>,
    change_generator: StockChangeGenerator,
}

impl StockMarket {
    pub fn new(settings: &StockTradeDataGeneratorSettings) -> anyhow::Result<Self> {
        let mut change_generator = StockChangeGenerator::new(settings);
        let mut stocks_map = BTreeMap::new();

        // Initialize stocks from configuration
        for stock_def in &settings.stock_definitions {
            let (stock, _) = Stock::new(
                stock_def.symbol.clone(),
                stock_def.symbol.clone(),
                stock_def.name.clone(),
                0,
                &mut change_generator,
            )?;
            stocks_map.insert(stock_def.symbol.clone(), stock);
        }

        Ok(StockMarket {
            stocks: Mutex::new(stocks_map),
            change_generator,
        })
    }

    pub fn get_current_state(&self, included_types: &HashSet<String>) -> CurrentStateIterator<'_> {
        let stocks = self.stocks.lock();
        let stock_ids = stocks.keys().cloned().collect::<Vec<_>>();

        CurrentStateIterator {
            stocks_guard: stocks,
            stock_ids,
            stock_index: 0,
            included_types: included_types.clone(),
        }
    }

    pub fn generate_update(&mut self, effective_from: u64) -> anyhow::Result<Option<ModelChange>> {
        let mut stocks = self.stocks.lock();

        if stocks.is_empty() {
            return Ok(None);
        }

        // Pick a random stock to update
        let idx = self
            .change_generator
            .get_usize_in_range(0, stocks.len(), false);
        let (_, stock) = stocks.iter_mut().nth(idx).unwrap();

        Ok(Some(
            stock.update_price(effective_from, &mut self.change_generator),
        ))
    }
}

pub struct CurrentStateIterator<'a> {
    stocks_guard: MutexGuard<'a, BTreeMap<String, Stock>>,
    stock_ids: Vec<String>,
    stock_index: usize,
    included_types: HashSet<String>,
}

impl Iterator for CurrentStateIterator<'_> {
    type Item = ModelChange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stock_index < self.stock_ids.len() {
            let stock_id = &self.stock_ids[self.stock_index];
            let stock = self.stocks_guard.get(stock_id).unwrap();
            self.stock_index += 1;

            if self.included_types.contains("Stock") {
                let stock_node = StockNode::from(stock);
                Some(ModelChange::StockAdded(stock_node))
            } else {
                self.next()
            }
        } else {
            None
        }
    }
}
