use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize)]
pub struct IndexData {
    data_path: String,
    file_size: u64,
}

pub fn index_data(data_path: &str, file_size: u64) -> IndexData {
    IndexData {
        data_path: data_path.to_string(),
        file_size,
    }
}

pub fn get_keys_before_threshold(index_data: Vec<IndexData>, threshold: u64) -> Vec<String> {
    let mut total_size = 0;
    let mut result = Vec::new();

    for data in index_data {
        if total_size + data.file_size > threshold {
            break;
        }
        result.push(data.data_path);
        total_size += data.file_size;
    }

    result
}
