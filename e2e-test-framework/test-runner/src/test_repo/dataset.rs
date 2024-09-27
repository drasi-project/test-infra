use std::collections::HashSet;

use serde::Serialize;

use crate::TestRunSource;

use super::TestSourceContent;

#[derive(Clone, Debug, Serialize)]
pub struct DataSet {
    pub id: String,
    pub content: TestSourceContent,
    pub test_run_source: TestRunSource,
}

impl DataSet {
    pub fn new(test_run_source: TestRunSource, content: TestSourceContent) -> Self {

        let id = format!("{}::{}::{}", &test_run_source.test_repo_id, &test_run_source.test_id, &test_run_source.source_id);
        DataSet {
            id,
            content,
            test_run_source,
        }
    }
    
    pub fn count_bootstrap_type_intersection(&self, requested_labels: &HashSet<String>) -> usize {
        let mut match_count = 0;

        if self.content.bootstrap_script_files.is_some() {
            // Iterate through the requested labels (data types) and count the number of matches.
            self.content.bootstrap_script_files.as_ref().unwrap().keys().filter(|key| requested_labels.contains(*key)).for_each(|_| {
                match_count += 1;
            });
        }

        match_count
    }

    // async fn catalog_existing_content(&mut self) -> anyhow::Result<()> {
    //     let mut change_scripts_path = self.data_cache_path.clone();
    //     change_scripts_path.push("change_scripts");
    //     let change_log_script_files = get_files_in_folder(change_scripts_path);

    //     let mut bootstrap_scripts_path = self.data_cache_path.clone();
    //     bootstrap_scripts_path.push("bootstrap_scripts");
    //     let bootstrap_script_files = build_folder_file_map(bootstrap_scripts_path);

    //     self.content = Some(DataSetContent::new(Some(change_log_script_files), Some(bootstrap_script_files)));

    //     Ok(())
    // }
}


// fn get_files_in_folder(root: PathBuf) -> Vec<PathBuf> {
//     let mut paths = Vec::new();

//     for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
//         let path = entry.path().to_path_buf();
//         if path.is_file() {
//             paths.push(path);
//         }
//     }

//     // Sort the vector of file paths
//     paths.sort();
    
//     paths
// }

// fn build_folder_file_map(root: PathBuf) -> HashMap<String, Vec<PathBuf>> {
//     let mut folder_map: HashMap<String, Vec<PathBuf>> = HashMap::new();

//     for entry in WalkDir::new(&root).into_iter().filter_map(|e| e.ok()) {
//         let path = entry.path().to_path_buf();

//         if path.is_file() {
//             if let Some(parent) = path.parent() {
//                 if let Some(folder_name) = parent.file_name().and_then(|name| name.to_str()) {
//                     folder_map
//                         .entry(folder_name.to_string())
//                         .or_insert_with(Vec::new)
//                         .push(path);
//                 }
//             }
//         }
//     }

//     // Sort the vectors in the HashMap
//     for files in folder_map.values_mut() {
//         files.sort();
//     }

//     folder_map
// }