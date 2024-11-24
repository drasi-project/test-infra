use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TestDefinition {
    pub id: String,
    pub version: u32,
    pub description: Option<String>,
    pub test_folder: Option<String>,
    pub sources: Vec<SourceDefinition>,
    pub queries: Vec<QueryDefinition>,
    pub reactions: Vec<ReactionDefinition>,
    pub clients: Vec<ClientDefinition>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SourceDefinition {
    pub id: String,
    pub bootstrap_data_generator: BootstrapDataGeneratorDefinition,
    pub source_change_generator: SourceChangeGeneratorDefinition,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BootstrapDataGeneratorDefinition {    
    pub kind: String,
    pub script_file_folder: String,
    pub script_file_list: Vec<String>,
    pub time_mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SourceChangeGeneratorDefinition {
    pub ignore_scripted_pause_commands: bool,
    pub kind: String,
    pub script_file_folder: String,
    pub script_file_list: Vec<String>,
    pub spacing_mode: String,
    pub time_mode: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryDefinition {
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReactionDefinition {
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientDefinition {
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::{BufReader, Write}};

    use tempfile::tempdir;

    use super::*;

    fn create_test_file(content: &str) -> File {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("unit_test.test");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "{}", content).unwrap();
        file.sync_all().unwrap();
        File::open(file_path).unwrap()
    }

    #[test]
    fn test_read_bootstrap_data() {
        let content = r#"
        {
            "script_file_folder": "script_files",
            "script_file_list": ["init*.jsonl", "deploy*.jsonl"]
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let bootstrap_data: BootstrapDataGeneratorDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(bootstrap_data.script_file_folder, "script_files");
        assert_eq!(bootstrap_data.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
    }

    #[test]
    fn test_read_change_log() {
        let content = r#"
        {
            "script_file_folder": "script_files",
            "script_file_list": ["change01.jsonl", "change02.jsonl"]
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let change_log: SourceChangeGeneratorDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(change_log.script_file_folder, "script_files");
        assert_eq!(change_log.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
    }

    #[test]
    fn test_read_source() {
        let content = r#"
        {
            "id": "source1",
            "bootstrap_data": {
                "script_file_folder": "script_files",
                "script_file_list": ["init*.jsonl", "deploy*.jsonl"]
            },
            "change_log": {
                "script_file_folder": "script_files",
                "script_file_list": ["change01.jsonl", "change02.jsonl"]
            }
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let source: SourceDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(source.id, "source1");
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "script_files");
        assert_eq!(source.bootstrap_data_generator.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
        assert_eq!(source.source_change_generator.script_file_folder, "script_files");
        assert_eq!(source.source_change_generator.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
    }

    #[test]
    fn test_read_test_definition() {
        let content = r#"
        {
            "id": "test1",
            "version": 1,
            "description": "A test definition",
            "test_folder": "test1",
            "sources": [
                {
                    "id": "source1",
                    "bootstrap_data": {
                        "script_file_folder": "script_files",
                        "script_file_list": ["init*.jsonl", "deploy*.jsonl"]
                    },
                    "change_log": {
                        "script_file_folder": "script_files",
                        "script_file_list": ["change01.jsonl", "change02.jsonl"]
                    }
                }
            ]
        }
        "#;
        let file = create_test_file(content);
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(test_definition.id, "test1");
        assert_eq!(test_definition.version, 1);
        assert_eq!(test_definition.description.unwrap(), "A test definition");
        assert_eq!(test_definition.test_folder.unwrap(), "test1");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];
        assert_eq!(source.id, "source1");
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "script_files");
        assert_eq!(source.bootstrap_data_generator.script_file_list, vec!["init*.jsonl", "deploy*.jsonl"]);
        assert_eq!(source.source_change_generator.script_file_folder, "script_files");
        assert_eq!(source.source_change_generator.script_file_list, vec!["change01.jsonl", "change02.jsonl"]);
    }

    #[test]
    fn test_read_single_source_hierarchical_query() {
        let file_path = "tests/test_definitions/test_single_source_hierarchical_query.test";
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);
        let test_definition: TestDefinition = serde_json::from_reader(reader).unwrap();
        
        assert_eq!(test_definition.id, "single_source_hierarchical_query");
        assert_eq!(test_definition.version, 1);
        assert_eq!(test_definition.description.unwrap(), "A single source hierarchical query using facilities data.");
        assert_eq!(test_definition.test_folder.unwrap(), "single_source_hierarchical_query");
        assert_eq!(test_definition.sources.len(), 1);
        let source = &test_definition.sources[0];
        assert_eq!(source.id, "facilities");
        assert_eq!(source.bootstrap_data_generator.script_file_folder, "bootstrap_scripts");
        assert_eq!(source.bootstrap_data_generator.script_file_list, Vec::<String>::new());
        assert_eq!(source.source_change_generator.script_file_folder, "change_scripts");
        assert_eq!(source.source_change_generator.script_file_list, Vec::<String>::new());
    }
}
