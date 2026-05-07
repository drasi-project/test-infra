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

use chrono::NaiveDateTime;
use clap::Args;
use crate::wikidata::ItemType;

#[derive(Args, Debug)]
pub struct MakeScriptCommandArgs {
    /// The types of WikiData Items to script
    #[arg(short = 't', long, value_enum, value_delimiter=',')]
    pub item_types: Vec<ItemType>,
    
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 'e', long)]
    pub rev_end: Option<NaiveDateTime>,

    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 's', long)]
    pub rev_start: Option<NaiveDateTime>,

    /// The source ID to use for naming the source folder.
    #[arg(short = 'd', long)]
    pub source_id: Option<String>,
    
    /// The ID of the test to generate scipts for.
    #[arg(short = 'i', long, )]
    pub test_id: String,
}