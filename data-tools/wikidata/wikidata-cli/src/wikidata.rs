use std::{fmt::Display, hash::{Hash, Hasher}, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use clap::ValueEnum;

/// Enum representing the different types of WikiData Item that can be downloaded
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum ItemType {
    City,
    Country
}

impl ItemType {
    pub fn as_id(&self) -> &str {
        match self {
            ItemType::City => "Q515",
            ItemType::Country => "Q6256",
        }
    }
}

impl FromStr for ItemType {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim().to_lowercase();

        match input.as_str() {
            "city" => Ok(ItemType::City),
            "country" => Ok(ItemType::Country),
            _ => Err(format!("Unknown variant: {}", input)),
        }
    }
}

impl Display for ItemType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemType::City => write!(f, "city"),
            ItemType::Country => write!(f, "country"),
        }
    }
}

impl Hash for ItemType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ItemType::City => "city".hash(state),
            ItemType::Country => "country".hash(state),
        }
    }
}


#[derive(Deserialize, Debug)]
pub struct ApiResponse {
    #[serde(rename = "continue")]
    pub continuation: Option<Continuation>,
    pub query: Query,
}

#[derive(Deserialize, Debug)]
pub struct Continuation {
    #[serde(rename = "continue")]
    pub continuation: String,
    pub rvcontinue: String,
}

#[derive(Deserialize, Debug)]
pub struct Query {
    pub pages: std::collections::HashMap<String, Page>, // Pages keyed by page ID
}

#[derive(Deserialize, Debug)]
pub struct Page {
    pub pageid: u32,
    pub ns: u32,
    pub title: String,    
    pub revisions: Option<Vec<Revision>>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Revision {
    pub revid: u64,
    pub parentid: u64,
    pub timestamp: String,
    pub user: Option<String>,
    pub comment: Option<String>,
    pub slots: Option<std::collections::HashMap<String, Slot>>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Slot {
    pub contentmodel: String,
    pub contentformat: String,
    #[serde(rename = "*")]
    pub content: String,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct ItemRevisionFileContent {
    pub revid: u64,
    pub parentid: u64,
    pub timestamp: String,
    pub user: Option<String>,
    pub comment: Option<String>,
    pub content: Option<Value>,
}

impl ItemRevisionFileContent {
    pub fn new(revision: &Revision) -> anyhow::Result<Self> {
        let mut irfc = Self {
            revid: revision.revid,
            parentid: revision.parentid,
            timestamp: revision.timestamp.clone(),
            user: revision.user.clone(),
            comment: revision.comment.clone(),
            content: None,
        };

        if let Some(slots) = &revision.slots {
            if let Some(slot) = slots.get("main") {
                irfc.content = Some(serde_json::from_str(&slot.content)?);
            }
        }

        Ok(irfc)
    }
}
