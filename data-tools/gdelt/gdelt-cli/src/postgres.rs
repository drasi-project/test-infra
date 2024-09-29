use std::{path::PathBuf, sync::Arc};

use csv::ReaderBuilder;
use futures::future::join_all;
use tokio::task;
use tokio_postgres::{Client, NoTls};

use crate::{gdelt::{GdeltEvent, GdeltGraph, GdeltMention}, DataType, FileInfo};

async fn connect_to_postgres() -> anyhow::Result<Client> {
    let (client, connection) =
        tokio_postgres::connect("host=localhost user=your_user password=your_password dbname=your_db", NoTls).await?;
    
    // Spawn the connection as a separate task to handle responses.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

pub async fn load_gdelt_files(mut load_tasks: Vec<FileInfo>) -> anyhow::Result<Vec<FileInfo>> {
    // let client = Arc::new(connect_to_postgres().await?);
    let batch_size = 10;

    let mut tasks = Vec::new();
    
    for batch in load_tasks.chunks(batch_size) {
        
        for file in batch {
            // let client = Arc::clone(&client);
            
            let fut = async move {
                load_gdelt_file(file.file_type,file.unzip_path.clone()).await
            };
            
            tasks.push(fut);
        }

        // Wait for all tasks in the batch to complete.
        // join_all(tasks).await;
    }

    // Await all the tasks and update the results
    for (i, task_result) in futures::future::join_all(tasks).await.into_iter().enumerate() {
        load_tasks[i].set_extract_result(task_result);
    }

    Ok(load_tasks)
}

async fn load_gdelt_file(file_type: DataType, file_path: PathBuf) -> anyhow::Result<()> {
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'\t')  // Set tab as the delimiter
        .from_path(file_path)?;
    
    // let mut transaction = client.transaction().await?;
    
    match &file_type {
        DataType::Event => {
            for result in rdr.deserialize()  {
                let event: GdeltEvent = result?;
        
                log::trace!("Processing event: {:?}", event);
            }
        },
        DataType::Mention => {
            for result in rdr.deserialize()  {
                let mention: GdeltMention = result?;
        
                log::trace!("Processing mention: {:?}", mention);
            }
        },
        DataType::Graph => {
            for result in rdr.deserialize()  {
                let graph: GdeltGraph = result?;
        
                log::trace!("Processing graph: {:?}", graph);
            }
        },
    }
    // transaction.commit().await?;
    Ok(())
}

// async fn process_gdelt_event_file(client: &Client, file_path: PathBuf) -> anyhow::Result<()> {
//     let mut rdr = ReaderBuilder::new()
//         .has_headers(false)
//         .delimiter(b'\t')  // Set tab as the delimiter
//         .from_path(file_path)?;
    
//     // let mut transaction = client.transaction().await?;
    
//     // Use pipelining for efficient insertion.
//     for result in rdr.deserialize()  {
//         let event: GdeltEvent = result?;

//         log::trace!("Processing event: {:?}", event);
//     }
//     // transaction.commit().await?;
//     Ok(())
// }