use std::{fmt::Display, hash::{Hash, Hasher}, str::FromStr};

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

// pub struct WikiDataItemRevision {
//     pub id: String,
//     pub item_id: String,
//     pub timestamp: String,
//     pub user: String,
//     pub user_id: String,
//     pub comment: String,
//     pub content: String,
// }

// pub struct Country {
//     pub id: String,
//     pub name: String,
//     pub population: Option<u64>,
//     pub area: Option<f64>,
//     pub capital: Option<String>,
//     pub continent: Option<String>,
//     pub currency: Option<String>,
//     pub official_language: Option<String>,
//     pub calling_code: Option<String>,
//     pub flag: Option<String>,
// }

// impl Country {
//     pub fn new(item_rev: WikiDataItemRevision) {

//     }

//     pub fn get_item_list_query() -> String {
//         format!("SELECT ?item ?itemLabel WHERE {{
//             ?item wdt:P31 wd:Q6256.
//             SERVICE wikibase:label {{ bd:serviceParam wikibase:language \"en\". }}
//         }}")
//     }

// }