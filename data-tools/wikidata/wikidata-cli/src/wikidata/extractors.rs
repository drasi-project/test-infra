use serde_json::{json, Value, Map};
use test_data_store::test_repo_storage::scripts::{BootstrapScriptRecord, NodeRecord, RelationRecord};

use super::{ItemRevisionFileContent, ItemType};

fn extract_property_value(claims: &Map<String, Value>, prop: &str, get_first: bool) -> Option<Value> {
    claims.get(prop)
        .and_then(|values| values.as_array())
        .and_then(|array| if get_first { array.first() } else { array.last() })
        .and_then(|claim| claim.get("mainsnak"))
        .and_then(|mainsnak| mainsnak.get("datavalue"))
        .and_then(|datavalue| datavalue.get("value"))
        .cloned()
}

// Extract Area Property (P2046)
fn extract_area_property(claims: &Map<String, Value>, properties: &mut Map<String, Value>) -> anyhow::Result<()> {
    let val_opt = extract_property_value(claims, "P2046", true);
    if let Some(val) = val_opt {
        let val = val.get("amount")
            .and_then(|val| val.as_str())
            .unwrap()
            .parse::<f64>()?;

        properties.insert("area".to_string(), val.into());
    };

    Ok(())
}

// Extract Continent Property (P30)
// This is the ID of the Continent to which some entity is located in, e.g. a Country has this property.
fn extract_continent_id_property(claims: &Map<String, Value>, properties: &mut Map<String, Value>) -> anyhow::Result<()> {
    let val_opt = extract_property_value(claims, "P30", true);
    if let Some(val) = val_opt {
        let val = val.get("id")
            .and_then(|val| val.as_str())
            .unwrap();

        properties.insert("continent_id".to_string(), val.into());
    };

    Ok(())
}

// Extract Coordinate Location Property (P625)
fn extract_coordinate_location_property(claims: &Map<String, Value>, properties: &mut Map<String, Value>) -> anyhow::Result<()> {
    let val_opt = extract_property_value(claims, "P625", false);
    if let Some(val) = val_opt {
        let lat = val.get("latitude").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let lon = val.get("longitude").and_then(|v| v.as_f64()).unwrap_or(0.0);

        let val = format!("{},{}", lat, lon);
        properties.insert("coordinate_location".to_string(), val.into());
    };

    Ok(())
}

// Extract Country Property (P17)
// This is the ID of the Country to which some entity is located in, e.g. a City has this property.
fn extract_country_id_property(claims: &Map<String, Value>, properties: &mut Map<String, Value>) -> anyhow::Result<()> {
    let val_opt = extract_property_value(claims, "P17", true);
    if let Some(val) = val_opt {
        let val = val.get("id")
            .and_then(|val| val.as_str())
            .unwrap();

        properties.insert("country_id".to_string(), val.into());
    };

    Ok(())
}

// Extract Population Property (P1082)
fn extract_population_property(claims: &Map<String, Value>, properties: &mut Map<String, Value>) -> anyhow::Result<()> {
    let val_opt = extract_property_value(claims, "P1082", false);
    if let Some(val) = val_opt {
        let val = val.get("amount")
            .and_then(|val| val.as_str())
            .unwrap()
            .parse::<i64>()?;

        properties.insert("population".to_string(), val.into());
    };

    Ok(())
}

fn extract_name_label(content: &Value, properties: &mut Map<String, Value>, lang: &str) -> anyhow::Result<()> {

    match content.get("labels").and_then(|v| v.as_object()) {
        Some(labels) => {
            match labels.get(lang) {
                Some(label) => {
                    match label.get("value").and_then(|v| v.as_str()) {
                        Some(value) => {
                            properties.insert("name".to_string(), value.into());
                        },
                        None => {},
                    };
                },
                None => {}
            };
        },
        None => {}
    };

    Ok(())
}


pub fn parse_item_revision(item_type:ItemType, revision: &ItemRevisionFileContent) -> anyhow::Result<Value> {

    let content = match revision.content {
        Some(ref content) => content,
        None => anyhow::bail!("No content found in revision"),
    };

    let props = match item_type {
        ItemType::City => parse_city_item_props(content),
        ItemType::Continent => parse_continent_item_props(content),
        ItemType::Country => parse_country_item_props(content),
    }?;

    let value = json!({
        "id": revision.item_id,
        "labels": [item_type.as_label()],
        "properties": props
    });

    Ok(value)
}

pub fn item_revision_to_bootstrap_data_record(item_type:ItemType, revision: &ItemRevisionFileContent) -> anyhow::Result<BootstrapScriptRecord> {

    let id = revision.item_id.clone();

    let content = match revision.content {
        Some(ref content) => content,
        None => anyhow::bail!("No content found in revision"),
    };

    let properties = match item_type {
        ItemType::City => parse_city_item_props(content),
        ItemType::Continent => parse_continent_item_props(content),
        ItemType::Country => parse_country_item_props(content),
    }?;

    if item_type.is_node() {
        return Ok(BootstrapScriptRecord::Node(NodeRecord {
            id,
            labels: vec![item_type.as_label().to_string()],
            properties: json!(properties),
        }));
    } else {
        return Ok(BootstrapScriptRecord::Relation(RelationRecord {
            id,
            labels: vec![item_type.as_label().to_string()],
            start_id: "".to_string(),
            start_label: None,
            end_id: "".to_string(),
            properties: json!(properties),
            end_label: None,
    }));
    }
}

fn parse_city_item_props(content: &Value) -> anyhow::Result<Map<String, Value>> {

    let claims = match content.get("claims").and_then(|v| v.as_object()) {
        Some(claims) => claims,
        None => anyhow::bail!("No claims found in revision"),
    };

    // Initialize a JSON object for properties
    let mut properties = serde_json::Map::new();

    // Check if "claims" field exists and is an object
    extract_area_property(claims, &mut properties)?;    
    extract_coordinate_location_property(claims, &mut properties)?;
    extract_country_id_property(claims, &mut properties)?;    
    extract_population_property(claims, &mut properties)?;

    extract_name_label(content, &mut properties, "en")?;
    
    Ok(properties)
}

fn parse_continent_item_props(content: &Value) -> anyhow::Result<Map<String, Value>> {

    let claims = match content.get("claims").and_then(|v| v.as_object()) {
        Some(claims) => claims,
        None => anyhow::bail!("No claims found in revision"),
    };

    // Initialize a JSON object for properties
    let mut properties = serde_json::Map::new();

    // Check if "claims" field exists and is an object
    extract_area_property(claims, &mut properties)?;
    extract_population_property(claims, &mut properties)?;
    extract_coordinate_location_property(claims, &mut properties)?;

    extract_name_label(content, &mut properties, "en")?;
    
    Ok(properties)
}

fn parse_country_item_props(content: &Value) -> anyhow::Result<Map<String, Value>> {

    let claims = match content.get("claims").and_then(|v| v.as_object()) {
        Some(claims) => claims,
        None => anyhow::bail!("No claims found in revision"),
    };

    // Initialize a JSON object for properties
    let mut properties = serde_json::Map::new();

    // Check if "claims" field exists and is an object
    extract_area_property(claims, &mut properties)?;
    extract_population_property(claims, &mut properties)?;
    extract_continent_id_property(claims, &mut properties)?;    
    extract_coordinate_location_property(claims, &mut properties)?;

    extract_name_label(content, &mut properties, "en")?;
    
    Ok(properties)
}