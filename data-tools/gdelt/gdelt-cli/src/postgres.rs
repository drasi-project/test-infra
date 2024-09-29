use std::{fmt::Write, path::PathBuf};

use csv::ReaderBuilder;
use tokio_postgres::{Client, NoTls, types};

use crate::{gdelt::{GdeltEvent, GdeltGraph, GdeltMention}, DataType, FileInfo};

const CREATE_EVENTS_TABLE_QUERY: &str = "
    CREATE TABLE IF NOT EXISTS gdelt_events (
        global_event_id BIGINT PRIMARY KEY,         -- u64
        day INTEGER NOT NULL,                       -- u32
        month_year INTEGER NOT NULL,                -- u32
        year INTEGER NOT NULL,                      -- u32
        fraction_date DOUBLE PRECISION NOT NULL,    -- f64
        actor1_code TEXT,                           -- Option<String>
        actor1_name TEXT,                           -- Option<String>
        actor1_country_code TEXT,                   -- Option<String>
        actor1_known_group_code TEXT,               -- Option<String>
        actor1_ethnic_code TEXT,                    -- Option<String>
        actor1_religion1_code TEXT,                 -- Option<String>
        actor1_religion2_code TEXT,                 -- Option<String>
        actor1_type1_code TEXT,                     -- Option<String>
        actor1_type2_code TEXT,                     -- Option<String>
        actor1_type3_code TEXT,                     -- Option<String>
        actor2_code TEXT,                           -- Option<String>
        actor2_name TEXT,                           -- Option<String>
        actor2_country_code TEXT,                   -- Option<String>
        actor2_known_group_code TEXT,               -- Option<String>
        actor2_ethnic_code TEXT,                    -- Option<String>
        actor2_religion1_code TEXT,                 -- Option<String>
        actor2_religion2_code TEXT,                 -- Option<String>
        actor2_type1_code TEXT,                     -- Option<String>
        actor2_type2_code TEXT,                     -- Option<String>
        actor2_type3_code TEXT,                     -- Option<String>
        is_root_event INTEGER NOT NULL,             -- u32
        event_code TEXT,                            -- Option<String>
        event_base_code TEXT,                       -- Option<String>
        event_root_code TEXT,                       -- Option<String>
        quad_class INTEGER NOT NULL,                -- u32
        goldstein_scale REAL NOT NULL,              -- f32
        num_mentions INTEGER NOT NULL,              -- u32
        num_sources INTEGER NOT NULL,               -- u32
        num_articles INTEGER NOT NULL,              -- u32
        avg_tone REAL NOT NULL,                     -- f32
        actor1_geo_type INTEGER,                    -- Option<u32>
        actor1_geo_full_name TEXT,                  -- Option<String>
        actor1_geo_country_code TEXT,               -- Option<String>
        actor1_geo_adm1_code TEXT,                  -- Option<String>
        actor1_geo_adm2_code TEXT,                  -- Option<String>
        actor1_geo_lat DOUBLE PRECISION,            -- Option<f64>
        actor1_geo_long DOUBLE PRECISION,           -- Option<f64>
        actor1_geo_feature_id TEXT,                 -- Option<String>
        actor2_geo_type INTEGER,                    -- Option<u32>
        actor2_geo_full_name TEXT,                  -- Option<String>
        actor2_geo_country_code TEXT,               -- Option<String>
        actor2_geo_adm1_code TEXT,                  -- Option<String>
        actor2_geo_adm2_code TEXT,                  -- Option<String>
        actor2_geo_lat DOUBLE PRECISION,            -- Option<f64>
        actor2_geo_long DOUBLE PRECISION,           -- Option<f64>
        actor2_geo_feature_id TEXT,                 -- Option<String>
        action_geo_type INTEGER,                    -- Option<u32>
        action_geo_full_name TEXT,                  -- Option<String>
        action_geo_country_code TEXT,               -- Option<String>
        action_geo_adm1_code TEXT,                  -- Option<String>
        action_geo_adm2_code TEXT,                  -- Option<String>
        action_geo_lat DOUBLE PRECISION,            -- Option<f64>
        action_geo_long DOUBLE PRECISION,           -- Option<f64>
        action_geo_feature_id TEXT,                 -- Option<String>
        date_added BIGINT NOT NULL,                 -- u64 (timestamp)
        source_url TEXT                             -- Option<String>
    );
";

pub struct DbInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    pub use_tls: bool,
}

async fn create_client(dbinfo: &DbInfo) -> anyhow::Result<Client> {
    let conn_string = format!("host={} user={} password={} dbname={}", 
        dbinfo.host, dbinfo.user, dbinfo.password, dbinfo.dbname);

    let (client, connection) = tokio_postgres::connect(&conn_string, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

async fn create_database(dbinfo: &DbInfo, overwrite: bool) -> anyhow::Result<()> {
    let adm_db_info = DbInfo {
        host: dbinfo.host.clone(),
        port: dbinfo.port,
        user: dbinfo.user.clone(),
        password: dbinfo.password.clone(),
        dbname: "postgres".to_string(),
        use_tls: dbinfo.use_tls,
    };

    let client = create_client(&adm_db_info).await?;


    if overwrite {
        // If overwrite is true, drop and recreate the database
        // Drop the database if it exists
        let drop_sql = format!("DROP DATABASE IF EXISTS \"{}\";", dbinfo.dbname);
        client.execute(&drop_sql, &[]).await?;

        // Create the database
        let create_sql = format!("CREATE DATABASE \"{}\";", dbinfo.dbname);
        client.execute(&create_sql, &[]).await?;
    } else {
        // If overwrite is false, only create the database if it doesn't exist
        let check_sql = format!(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}';",
            dbinfo.dbname
        );

        let exists = client.query(&check_sql, &[]).await?;

        if exists.is_empty() {
            // Create the database if it does not exist
            let create_sql = format!("CREATE DATABASE \"{}\";", dbinfo.dbname);
            client.execute(&create_sql, &[]).await?;
        }
    }

    Ok(())
}

pub async fn initialize(dbinfo: &DbInfo, overwrite: bool) -> anyhow::Result<()> {

    // Create the Database
    create_database(dbinfo, overwrite).await?;

    // Create the Client
    let client = create_client(dbinfo).await?;

    // Execute the CREATE TABLE query
    client.batch_execute(CREATE_EVENTS_TABLE_QUERY).await?;

    Ok(())
}

pub async fn load_gdelt_files(dbinfo: &DbInfo, mut files_to_load: Vec<FileInfo>) -> anyhow::Result<Vec<FileInfo>> {
    let batch_size = 10;

    let client = create_client(dbinfo).await?;
    
    for file_info in files_to_load.iter_mut() {
        match &file_info.file_type {
            DataType::Event => {
                let res = load_gdelt_event_file(&file_info.unzip_path, batch_size, &client).await;
                file_info.set_load_result(res);
            },
            DataType::Mention => {
            },
            DataType::Graph => {
            },
        }        
    }

    Ok(files_to_load)
}

async fn load_gdelt_event_file(file_path: &PathBuf, batch_size: usize, client: &Client) -> anyhow::Result<()> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'\t')  // Set tab as the delimiter
        .from_path(file_path)?;
    
    let mut batch: Vec<GdeltEvent> = Vec::with_capacity(batch_size);

    for result in rdr.deserialize()  {
        let event: GdeltEvent = result?;

        log::trace!("Processing event: {:?}", event);

        // Add the event to the batch
        batch.push(event);

        // If the batch size is reached, insert into the DB
        if batch.len() == batch_size {
            log::trace!("Inserting batch of {} events", batch_size);
            insert_gdelt_events(client, &batch).await?;

            // Clear the batch for the next set of events
            batch.clear();
        }        
    }

    // Insert any remaining records that didn't complete a full batch
    if !batch.is_empty() {
        log::trace!("Inserting final batch of {} events", batch.len());
        insert_gdelt_events(client, &batch).await?;
    }

    Ok(())
}

async fn insert_gdelt_events(client: &Client, events: &Vec<GdeltEvent>) -> anyhow::Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut query = String::from("INSERT INTO gdelt_events (global_event_id, day, month_year, year, fraction_date, actor1_code, actor1_name, actor1_country_code, actor1_known_group_code, actor1_ethnic_code, actor1_religion1_code, actor1_religion2_code, actor1_type1_code, actor1_type2_code, actor1_type3_code, actor2_code, actor2_name, actor2_country_code, actor2_known_group_code, actor2_ethnic_code, actor2_religion1_code, actor2_religion2_code, actor2_type1_code, actor2_type2_code, actor2_type3_code, is_root_event, event_code, event_base_code, event_root_code, quad_class, goldstein_scale, num_mentions, num_sources, num_articles, avg_tone, actor1_geo_type, actor1_geo_full_name, actor1_geo_country_code, actor1_geo_adm1_code, actor1_geo_adm2_code, actor1_geo_lat, actor1_geo_long, actor1_geo_feature_id, actor2_geo_type, actor2_geo_full_name, actor2_geo_country_code, actor2_geo_adm1_code, actor2_geo_adm2_code, actor2_geo_lat, actor2_geo_long, actor2_geo_feature_id, action_geo_type, action_geo_full_name, action_geo_country_code, action_geo_adm1_code, action_geo_adm2_code, action_geo_lat, action_geo_long, action_geo_feature_id, date_added, source_url) VALUES ");
    let mut params: Vec<&(dyn types::ToSql + Sync)> = Vec::new();

    for (i, event) in events.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        write!(
            query,
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            i * 61 + 1, i * 61 + 2, i * 61 + 3, i * 61 + 4, i * 61 + 5, i * 61 + 6, i * 61 + 7, i * 61 + 8, i * 61 + 9, i * 61 + 10, i * 61 + 11, i * 61 + 12, i * 61 + 13, i * 61 + 14, i * 61 + 15, i * 61 + 16, i * 61 + 17, i * 61 + 18, i * 61 + 19, i * 61 + 20, i * 61 + 21, i * 61 + 22, i * 61 + 23, i * 61 + 24, i * 61 + 25, i * 61 + 26, i * 61 + 27, i * 61 + 28, i * 61 + 29, i * 61 + 30, i * 61 + 31, i * 61 + 32, i * 61 + 33, i * 61 + 34, i * 61 + 35, i * 61 + 36, i * 61 + 37, i * 61 + 38, i * 61 + 39, i * 61 + 40, i * 61 + 41, i * 61 + 42, i * 61 + 43, i * 61 + 44, i * 61 + 45, i * 61 + 46, i * 61 + 47, i * 61 + 48, i * 61 + 49, i * 61 + 50, i * 61 + 51, i * 61 + 52, i * 61 + 53, i * 61 + 54, i * 61 + 55, i * 61 + 56, i * 61 + 57, i * 61 + 58, i * 61 + 59, i * 61 + 60, i * 61 + 61
        ).unwrap();

        params.push(&event.global_event_id);
        params.push(&event.day);
        params.push(&event.month_year);
        params.push(&event.year);
        params.push(&event.fraction_date);
        params.push(&event.actor1_code);
        params.push(&event.actor1_name);
        params.push(&event.actor1_country_code);
        params.push(&event.actor1_known_group_code);
        params.push(&event.actor1_ethnic_code);
        params.push(&event.actor1_religion1_code);
        params.push(&event.actor1_religion2_code);
        params.push(&event.actor1_type1_code);
        params.push(&event.actor1_type2_code);
        params.push(&event.actor1_type3_code);
        params.push(&event.actor2_code);
        params.push(&event.actor2_name);
        params.push(&event.actor2_country_code);
        params.push(&event.actor2_known_group_code);
        params.push(&event.actor2_ethnic_code);
        params.push(&event.actor2_religion1_code);
        params.push(&event.actor2_religion2_code);
        params.push(&event.actor2_type1_code);
        params.push(&event.actor2_type2_code);
        params.push(&event.actor2_type3_code);
        params.push(&event.is_root_event);
        params.push(&event.event_code);
        params.push(&event.event_base_code);
        params.push(&event.event_root_code);
        params.push(&event.quad_class);
        params.push(&event.goldstein_scale);
        params.push(&event.num_mentions);
        params.push(&event.num_sources);
        params.push(&event.num_articles);
        params.push(&event.avg_tone);
        params.push(&event.actor1_geo_type);
        params.push(&event.actor1_geo_full_name);
        params.push(&event.actor1_geo_country_code);
        params.push(&event.actor1_geo_adm1_code);
        params.push(&event.actor1_geo_adm2_code);
        params.push(&event.actor1_geo_lat);
        params.push(&event.actor1_geo_long);
        params.push(&event.actor1_geo_feature_id);
        params.push(&event.actor2_geo_type);
        params.push(&event.actor2_geo_full_name);
        params.push(&event.actor2_geo_country_code);
        params.push(&event.actor2_geo_adm1_code);
        params.push(&event.actor2_geo_adm2_code);
        params.push(&event.actor2_geo_lat);
        params.push(&event.actor2_geo_long);
        params.push(&event.actor2_geo_feature_id);
        params.push(&event.action_geo_type);
        params.push(&event.action_geo_full_name);
        params.push(&event.action_geo_country_code);
        params.push(&event.action_geo_adm1_code);
        params.push(&event.action_geo_adm2_code);
        params.push(&event.action_geo_lat);
        params.push(&event.action_geo_long);
        params.push(&event.action_geo_feature_id);
        params.push(&event.date_added);
        params.push(&event.source_url);
    }

    client.execute(query.as_str(), &params).await?;

    Ok(())
}