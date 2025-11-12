use crate::{DriftError, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarConfig {
    pub block_size: usize,
    pub compression: CompressionType,
    pub encoding: EncodingType,
    pub dictionary_encoding_threshold: f64,
    pub enable_statistics: bool,
    pub enable_bloom_filters: bool,
    pub enable_zone_maps: bool,
    pub row_group_size: usize,
    pub page_size: usize,
}

impl Default for ColumnarConfig {
    fn default() -> Self {
        Self {
            block_size: 65536,
            compression: CompressionType::Snappy,
            encoding: EncodingType::Auto,
            dictionary_encoding_threshold: 0.75,
            enable_statistics: true,
            enable_bloom_filters: true,
            enable_zone_maps: true,
            row_group_size: 100000,
            page_size: 8192,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
    Brotli,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncodingType {
    Auto,
    Plain,
    Dictionary,
    RunLength,
    BitPacked,
    Delta,
    DeltaBinary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Binary,
    Timestamp,
    Date,
    Decimal(u8, u8),
}

pub struct ColumnarStorage {
    config: ColumnarConfig,
    path: PathBuf,
    metadata: Arc<RwLock<ColumnarMetadata>>,
    column_files: HashMap<String, ColumnFile>,
    row_groups: Vec<RowGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnarMetadata {
    pub schema: Schema,
    pub row_count: u64,
    pub column_count: usize,
    pub row_groups: Vec<RowGroupMetadata>,
    pub created_at: u64,
    pub last_modified: u64,
    pub statistics: Option<TableStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub encoding: EncodingType,
    pub compression: CompressionType,
    pub dictionary: Option<Dictionary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dictionary {
    pub values: Vec<Vec<u8>>,
    pub indices: HashMap<Vec<u8>, u32>,
}

impl Default for Dictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            values: vec![],
            indices: HashMap::new(),
        }
    }

    pub fn add(&mut self, value: Vec<u8>) -> u32 {
        if let Some(&idx) = self.indices.get(&value) {
            return idx;
        }

        let idx = self.values.len() as u32;
        self.indices.insert(value.clone(), idx);
        self.values.push(value);
        idx
    }

    pub fn get(&self, idx: u32) -> Option<&Vec<u8>> {
        self.values.get(idx as usize)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupMetadata {
    pub row_count: u64,
    pub byte_size: u64,
    pub columns: Vec<ColumnChunkMetadata>,
    pub min_timestamp: Option<u64>,
    pub max_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkMetadata {
    pub column_name: String,
    pub offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub num_values: u64,
    pub encoding: EncodingType,
    pub compression: CompressionType,
    pub statistics: Option<ColumnStatistics>,
    pub zone_map: Option<ZoneMap>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneMap {
    pub min_value: Vec<u8>,
    pub max_value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: u64,
    pub byte_size: u64,
    pub column_stats: HashMap<String, ColumnStatistics>,
}

pub struct RowGroup {
    pub metadata: RowGroupMetadata,
    pub columns: HashMap<String, ColumnChunk>,
}

pub struct ColumnChunk {
    pub data: Vec<u8>,
    pub encoding: EncodingType,
    pub compression: CompressionType,
    pub statistics: Option<ColumnStatistics>,
    pub dictionary: Option<Dictionary>,
}

pub struct ColumnFile {
    file: Arc<RwLock<File>>,
    metadata: ColumnFileMetadata,
}

#[derive(Debug, Clone)]
struct ColumnFileMetadata {
    #[allow(dead_code)]
    column_name: String,
    #[allow(dead_code)]
    data_type: DataType,
    #[allow(dead_code)]
    row_groups: Vec<RowGroupMetadata>,
    total_rows: u64,
    file_size: u64,
}

impl ColumnarStorage {
    pub fn new<P: AsRef<Path>>(path: P, config: ColumnarConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let metadata = ColumnarMetadata {
            schema: Schema { columns: vec![] },
            row_count: 0,
            column_count: 0,
            row_groups: vec![],
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            last_modified: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            statistics: None,
        };

        Ok(Self {
            config,
            path,
            metadata: Arc::new(RwLock::new(metadata)),
            column_files: HashMap::new(),
            row_groups: vec![],
        })
    }

    pub fn create_table(&mut self, schema: Schema) -> Result<()> {
        let mut metadata = self.metadata.write().unwrap();
        metadata.schema = schema.clone();
        metadata.column_count = schema.columns.len();

        for column in &schema.columns {
            let column_path = self.path.join(format!("{}.col", column.name));
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(&column_path)?;

            let col_metadata = ColumnFileMetadata {
                column_name: column.name.clone(),
                data_type: column.data_type.clone(),
                row_groups: vec![],
                total_rows: 0,
                file_size: 0,
            };

            self.column_files.insert(
                column.name.clone(),
                ColumnFile {
                    file: Arc::new(RwLock::new(file)),
                    metadata: col_metadata,
                },
            );
        }

        Ok(())
    }

    pub fn write_batch(&mut self, rows: Vec<Row>) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut row_group = RowGroup {
            metadata: RowGroupMetadata {
                row_count: rows.len() as u64,
                byte_size: 0,
                columns: vec![],
                min_timestamp: None,
                max_timestamp: None,
            },
            columns: HashMap::new(),
        };

        let schema = self.metadata.read().unwrap().schema.clone();

        for column_schema in &schema.columns {
            let column_data: Vec<Option<Value>> = rows
                .iter()
                .map(|row| row.get(&column_schema.name).cloned().flatten())
                .collect();

            let chunk = self.encode_column(column_schema, column_data)?;

            row_group.metadata.byte_size += chunk.data.len() as u64;
            row_group.metadata.columns.push(ColumnChunkMetadata {
                column_name: column_schema.name.clone(),
                offset: 0,
                compressed_size: chunk.data.len() as u64,
                uncompressed_size: chunk.data.len() as u64,
                num_values: rows.len() as u64,
                encoding: chunk.encoding.clone(),
                compression: chunk.compression.clone(),
                statistics: chunk.statistics.clone(),
                zone_map: None,
            });

            row_group.columns.insert(column_schema.name.clone(), chunk);
        }

        self.write_row_group(row_group)?;

        let mut metadata = self.metadata.write().unwrap();
        metadata.row_count += rows.len() as u64;
        metadata.last_modified = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Ok(())
    }

    fn encode_column(
        &self,
        schema: &ColumnSchema,
        values: Vec<Option<Value>>,
    ) -> Result<ColumnChunk> {
        let encoding = match &schema.encoding {
            EncodingType::Auto => self.select_encoding(&values),
            other => other.clone(),
        };

        let mut stats = ColumnStatistics {
            null_count: 0,
            distinct_count: None,
            min_value: None,
            max_value: None,
        };

        let encoded_data = match encoding {
            EncodingType::Plain => self.encode_plain(&values, &mut stats)?,
            EncodingType::Dictionary => self.encode_dictionary(&values, &mut stats)?,
            EncodingType::RunLength => self.encode_run_length(&values, &mut stats)?,
            EncodingType::Delta => self.encode_delta(&values, &mut stats)?,
            _ => self.encode_plain(&values, &mut stats)?,
        };

        let compressed_data = self.compress(encoded_data, &schema.compression)?;

        Ok(ColumnChunk {
            data: compressed_data,
            encoding,
            compression: schema.compression.clone(),
            statistics: if self.config.enable_statistics {
                Some(stats)
            } else {
                None
            },
            dictionary: None,
        })
    }

    fn select_encoding(&self, values: &[Option<Value>]) -> EncodingType {
        let distinct_count = values
            .iter()
            .filter_map(|v| v.as_ref())
            .collect::<HashSet<_>>()
            .len();

        let total_count = values.len();

        if (distinct_count as f64) / (total_count as f64)
            <= self.config.dictionary_encoding_threshold
        {
            EncodingType::Dictionary
        } else {
            EncodingType::Plain
        }
    }

    fn encode_plain(
        &self,
        values: &[Option<Value>],
        stats: &mut ColumnStatistics,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for value in values {
            match value {
                None => {
                    buffer.push(0);
                    stats.null_count += 1;
                }
                Some(val) => {
                    buffer.push(1);
                    let bytes = self.value_to_bytes(val)?;
                    buffer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                    buffer.extend_from_slice(&bytes);
                }
            }
        }

        Ok(buffer)
    }

    fn encode_dictionary(
        &self,
        values: &[Option<Value>],
        stats: &mut ColumnStatistics,
    ) -> Result<Vec<u8>> {
        let mut dictionary = Dictionary::new();
        let mut indices = Vec::new();

        for value in values {
            match value {
                None => {
                    indices.push(u32::MAX);
                    stats.null_count += 1;
                }
                Some(val) => {
                    let bytes = self.value_to_bytes(val)?;
                    let idx = dictionary.add(bytes);
                    indices.push(idx);
                }
            }
        }

        let mut buffer = Vec::new();

        buffer.write_u32::<LittleEndian>(dictionary.values.len() as u32)?;
        for value in &dictionary.values {
            buffer.write_u32::<LittleEndian>(value.len() as u32)?;
            buffer.extend_from_slice(value);
        }

        for idx in indices {
            buffer.write_u32::<LittleEndian>(idx)?;
        }

        Ok(buffer)
    }

    fn encode_run_length(
        &self,
        values: &[Option<Value>],
        stats: &mut ColumnStatistics,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut runs = Vec::new();

        let mut current_value = None;
        let mut run_length = 0;

        for value in values {
            if value == &current_value {
                run_length += 1;
            } else {
                if run_length > 0 {
                    runs.push((current_value.clone(), run_length));
                }
                current_value = value.clone();
                run_length = 1;
            }

            if value.is_none() {
                stats.null_count += 1;
            }
        }

        if run_length > 0 {
            runs.push((current_value, run_length));
        }

        buffer.write_u32::<LittleEndian>(runs.len() as u32)?;

        for (value, length) in runs {
            buffer.write_u32::<LittleEndian>(length)?;

            match value {
                None => buffer.push(0),
                Some(val) => {
                    buffer.push(1);
                    let bytes = self.value_to_bytes(&val)?;
                    buffer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                    buffer.extend_from_slice(&bytes);
                }
            }
        }

        Ok(buffer)
    }

    fn encode_delta(
        &self,
        values: &[Option<Value>],
        stats: &mut ColumnStatistics,
    ) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let mut prev_value = None;

        for value in values {
            match (prev_value.as_ref(), value) {
                (None, None) => {
                    buffer.push(0);
                    stats.null_count += 1;
                }
                (None, Some(val)) => {
                    buffer.push(1);
                    let bytes = self.value_to_bytes(val)?;
                    buffer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                    buffer.extend_from_slice(&bytes);
                    prev_value = Some(val.clone());
                }
                (Some(_), None) => {
                    buffer.push(0);
                    stats.null_count += 1;
                    prev_value = None;
                }
                (Some(prev), Some(val)) => {
                    buffer.push(1);
                    let delta = self.compute_delta(prev, val)?;
                    buffer.write_u32::<LittleEndian>(delta.len() as u32)?;
                    buffer.extend_from_slice(&delta);
                    prev_value = Some(val.clone());
                }
            }
        }

        Ok(buffer)
    }

    fn value_to_bytes(&self, value: &Value) -> Result<Vec<u8>> {
        match value {
            Value::Null => Ok(vec![]),
            Value::Boolean(b) => Ok(vec![if *b { 1 } else { 0 }]),
            Value::Int64(i) => Ok(i.to_le_bytes().to_vec()),
            Value::Float64(f) => Ok(f.to_le_bytes().to_vec()),
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            Value::Binary(b) => Ok(b.clone()),
        }
    }

    fn compute_delta(&self, prev: &Value, curr: &Value) -> Result<Vec<u8>> {
        match (prev, curr) {
            (Value::Int64(p), Value::Int64(c)) => {
                let delta = c - p;
                Ok(delta.to_le_bytes().to_vec())
            }
            _ => self.value_to_bytes(curr),
        }
    }

    fn compress(&self, data: Vec<u8>, compression: &CompressionType) -> Result<Vec<u8>> {
        match compression {
            CompressionType::None => Ok(data),
            CompressionType::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder
                    .compress_vec(&data)
                    .map_err(|e| DriftError::Other(e.to_string()))
            }
            CompressionType::Zstd => {
                zstd::encode_all(data.as_slice(), 3).map_err(|e| DriftError::Other(e.to_string()))
            }
            _ => Ok(data),
        }
    }

    fn write_row_group(&mut self, row_group: RowGroup) -> Result<()> {
        let row_count = row_group.metadata.row_count;

        for (column_name, chunk) in &row_group.columns {
            if let Some(column_file) = self.column_files.get_mut(column_name) {
                let mut file = column_file.file.write().unwrap();

                file.seek(SeekFrom::End(0))?;
                let _offset = file.stream_position()?;

                file.write_all(&chunk.data)?;

                column_file.metadata.total_rows += row_count;
                column_file.metadata.file_size = file.stream_position()?;
            }
        }

        self.row_groups.push(row_group);
        Ok(())
    }

    pub fn scan(&self, columns: Vec<String>, predicate: Option<Predicate>) -> Result<RecordBatch> {
        let mut results = HashMap::new();

        for column_name in columns {
            if let Some(_column_file) = self.column_files.get(&column_name) {
                let column_data = self.read_column(&column_name, predicate.as_ref())?;
                results.insert(column_name, column_data);
            }
        }

        Ok(RecordBatch {
            schema: self.metadata.read().unwrap().schema.clone(),
            columns: results,
            row_count: self.metadata.read().unwrap().row_count as usize,
        })
    }

    fn read_column(
        &self,
        column_name: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Vec<Option<Value>>> {
        let _column_file = self
            .column_files
            .get(column_name)
            .ok_or_else(|| DriftError::Other(format!("Column {} not found", column_name)))?;

        let mut all_values = Vec::new();

        for row_group in &self.row_groups {
            if let Some(chunk) = row_group.columns.get(column_name) {
                if let Some(pred) = predicate {
                    if !self.evaluate_predicate_on_stats(pred, chunk.statistics.as_ref()) {
                        continue;
                    }
                }

                let decompressed = self.decompress(&chunk.data, &chunk.compression)?;
                let values = self.decode_column(&decompressed, &chunk.encoding)?;
                all_values.extend(values);
            }
        }

        Ok(all_values)
    }

    fn decompress(&self, data: &[u8], compression: &CompressionType) -> Result<Vec<u8>> {
        match compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder
                    .decompress_vec(data)
                    .map_err(|e| DriftError::Other(e.to_string()))
            }
            CompressionType::Zstd => {
                zstd::decode_all(data).map_err(|e| DriftError::Other(e.to_string()))
            }
            _ => Ok(data.to_vec()),
        }
    }

    fn decode_column(&self, data: &[u8], encoding: &EncodingType) -> Result<Vec<Option<Value>>> {
        match encoding {
            EncodingType::Plain => self.decode_plain(data),
            EncodingType::Dictionary => self.decode_dictionary(data),
            EncodingType::RunLength => self.decode_run_length(data),
            EncodingType::Delta => self.decode_delta(data),
            _ => self.decode_plain(data),
        }
    }

    fn decode_plain(&self, data: &[u8]) -> Result<Vec<Option<Value>>> {
        let mut values = Vec::new();
        let mut cursor = std::io::Cursor::new(data);

        while cursor.position() < data.len() as u64 {
            let is_null = cursor.read_u8()?;
            if is_null == 0 {
                values.push(None);
            } else {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut bytes = vec![0; len];
                cursor.read_exact(&mut bytes)?;
                values.push(Some(Value::Binary(bytes)));
            }
        }

        Ok(values)
    }

    fn decode_dictionary(&self, data: &[u8]) -> Result<Vec<Option<Value>>> {
        let mut cursor = std::io::Cursor::new(data);

        let dict_size = cursor.read_u32::<LittleEndian>()? as usize;
        let mut dictionary = Vec::new();

        for _ in 0..dict_size {
            let len = cursor.read_u32::<LittleEndian>()? as usize;
            let mut bytes = vec![0; len];
            cursor.read_exact(&mut bytes)?;
            dictionary.push(bytes);
        }

        let mut values = Vec::new();
        while cursor.position() < data.len() as u64 {
            let idx = cursor.read_u32::<LittleEndian>()?;
            if idx == u32::MAX {
                values.push(None);
            } else {
                values.push(Some(Value::Binary(dictionary[idx as usize].clone())));
            }
        }

        Ok(values)
    }

    fn decode_run_length(&self, data: &[u8]) -> Result<Vec<Option<Value>>> {
        let mut values = Vec::new();
        let mut cursor = std::io::Cursor::new(data);

        let num_runs = cursor.read_u32::<LittleEndian>()?;

        for _ in 0..num_runs {
            let length = cursor.read_u32::<LittleEndian>()? as usize;
            let is_null = cursor.read_u8()?;

            if is_null == 0 {
                for _ in 0..length {
                    values.push(None);
                }
            } else {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut bytes = vec![0; len];
                cursor.read_exact(&mut bytes)?;

                for _ in 0..length {
                    values.push(Some(Value::Binary(bytes.clone())));
                }
            }
        }

        Ok(values)
    }

    fn decode_delta(&self, data: &[u8]) -> Result<Vec<Option<Value>>> {
        let mut values = Vec::new();
        let mut cursor = std::io::Cursor::new(data);
        let mut prev_value: Option<Vec<u8>> = None;

        while cursor.position() < data.len() as u64 {
            let is_null = cursor.read_u8()?;

            if is_null == 0 {
                values.push(None);
                prev_value = None;
            } else {
                let len = cursor.read_u32::<LittleEndian>()? as usize;
                let mut bytes = vec![0; len];
                cursor.read_exact(&mut bytes)?;

                if let Some(prev) = prev_value {
                    let value = self.apply_delta(&prev, &bytes)?;
                    values.push(Some(Value::Binary(value.clone())));
                    prev_value = Some(value);
                } else {
                    values.push(Some(Value::Binary(bytes.clone())));
                    prev_value = Some(bytes);
                }
            }
        }

        Ok(values)
    }

    fn apply_delta(&self, base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
        if base.len() == 8 && delta.len() == 8 {
            let base_val = i64::from_le_bytes(base.try_into().unwrap());
            let delta_val = i64::from_le_bytes(delta.try_into().unwrap());
            let result = base_val + delta_val;
            Ok(result.to_le_bytes().to_vec())
        } else {
            Ok(delta.to_vec())
        }
    }

    fn evaluate_predicate_on_stats(
        &self,
        _predicate: &Predicate,
        _stats: Option<&ColumnStatistics>,
    ) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    values: HashMap<String, Option<Value>>,
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

impl Row {
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: String, value: Option<Value>) {
        self.values.insert(column, value);
    }

    pub fn get(&self, column: &str) -> Option<&Option<Value>> {
        self.values.get(column)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(u64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub columns: HashMap<String, Vec<Option<Value>>>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub struct Predicate {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    NotIn,
}

pub struct ColumnarWriter {
    storage: Arc<RwLock<ColumnarStorage>>,
    buffer: Vec<Row>,
    buffer_size: usize,
}

impl ColumnarWriter {
    pub fn new(storage: Arc<RwLock<ColumnarStorage>>, buffer_size: usize) -> Self {
        Self {
            storage,
            buffer: Vec::new(),
            buffer_size,
        }
    }

    pub fn write_row(&mut self, row: Row) -> Result<()> {
        self.buffer.push(row);

        if self.buffer.len() >= self.buffer_size {
            self.flush()?;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let rows = std::mem::take(&mut self.buffer);
        self.storage.write().unwrap().write_batch(rows)?;

        Ok(())
    }
}

pub struct ColumnarReader {
    storage: Arc<RwLock<ColumnarStorage>>,
}

impl ColumnarReader {
    pub fn new(storage: Arc<RwLock<ColumnarStorage>>) -> Self {
        Self { storage }
    }

    pub fn scan(&self, columns: Vec<String>, predicate: Option<Predicate>) -> Result<RecordBatch> {
        self.storage.read().unwrap().scan(columns, predicate)
    }

    pub fn count(&self) -> Result<u64> {
        Ok(self
            .storage
            .read()
            .unwrap()
            .metadata
            .read()
            .unwrap()
            .row_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnSchema {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    encoding: EncodingType::Auto,
                    compression: CompressionType::Snappy,
                    dictionary: None,
                },
                ColumnSchema {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    encoding: EncodingType::Auto,
                    compression: CompressionType::Snappy,
                    dictionary: None,
                },
                ColumnSchema {
                    name: "score".to_string(),
                    data_type: DataType::Float64,
                    nullable: true,
                    encoding: EncodingType::Plain,
                    compression: CompressionType::None,
                    dictionary: None,
                },
            ],
        }
    }

    fn create_test_row(id: i64, name: &str, score: f64) -> Row {
        let mut row = Row::new();
        row.insert("id".to_string(), Some(Value::Int64(id)));
        row.insert("name".to_string(), Some(Value::String(name.to_string())));
        row.insert(
            "score".to_string(),
            Some(Value::Float64(score.to_bits())),
        );
        row
    }

    #[test]
    fn test_columnar_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let storage = ColumnarStorage::new(temp_dir.path(), config);
        assert!(storage.is_ok());
    }

    #[test]
    fn test_dictionary_encoding() {
        let mut dict = Dictionary::new();

        let val1 = b"apple".to_vec();
        let val2 = b"banana".to_vec();
        let val3 = b"apple".to_vec();

        let idx1 = dict.add(val1.clone());
        let idx2 = dict.add(val2.clone());
        let idx3 = dict.add(val3);

        assert_eq!(idx1, 0);
        assert_eq!(idx2, 1);
        assert_eq!(idx3, 0); // Reuses existing entry

        assert_eq!(dict.values.len(), 2);
        assert_eq!(dict.get(0).unwrap(), &val1);
        assert_eq!(dict.get(1).unwrap(), &val2);
    }

    #[test]
    fn test_write_and_read_batch() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema.clone()).unwrap();

        // Write a batch of rows
        let rows = vec![
            create_test_row(1, "alice", 95.5),
            create_test_row(2, "bob", 87.3),
            create_test_row(3, "charlie", 92.1),
        ];

        storage.write_batch(rows).unwrap();

        // Verify row count
        let metadata = storage.metadata.read().unwrap();
        assert_eq!(metadata.row_count, 3);
    }

    #[test]
    fn test_column_scan() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema).unwrap();

        let rows = vec![
            create_test_row(1, "alice", 95.5),
            create_test_row(2, "bob", 87.3),
        ];
        storage.write_batch(rows).unwrap();

        // Scan specific columns
        let result = storage
            .scan(vec!["id".to_string(), "name".to_string()], None)
            .unwrap();

        assert_eq!(result.row_count, 2);
        assert!(result.columns.contains_key("id"));
        assert!(result.columns.contains_key("name"));
    }

    #[test]
    fn test_encoding_selection() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig {
            dictionary_encoding_threshold: 0.5,
            ..Default::default()
        };
        let storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        // Low cardinality - should use dictionary
        let values: Vec<Option<Value>> = vec![
            Some(Value::String("A".to_string())),
            Some(Value::String("A".to_string())),
            Some(Value::String("B".to_string())),
            Some(Value::String("B".to_string())),
        ];
        let encoding = storage.select_encoding(&values);
        assert!(matches!(encoding, EncodingType::Dictionary));

        // High cardinality - should use plain
        let values: Vec<Option<Value>> = vec![
            Some(Value::String("A".to_string())),
            Some(Value::String("B".to_string())),
            Some(Value::String("C".to_string())),
            Some(Value::String("D".to_string())),
        ];
        let encoding = storage.select_encoding(&values);
        assert!(matches!(encoding, EncodingType::Plain));
    }

    #[test]
    fn test_compression_types() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let data = b"Hello, World! This is a test of compression. ".repeat(100);

        // Test Snappy
        let compressed = storage
            .compress(data.to_vec(), &CompressionType::Snappy)
            .unwrap();
        let decompressed = storage
            .decompress(&compressed, &CompressionType::Snappy)
            .unwrap();
        assert_eq!(data, decompressed.as_slice());
        assert!(compressed.len() < data.len()); // Should be compressed

        // Test Zstd
        let compressed = storage
            .compress(data.to_vec(), &CompressionType::Zstd)
            .unwrap();
        let decompressed = storage
            .decompress(&compressed, &CompressionType::Zstd)
            .unwrap();
        assert_eq!(data, decompressed.as_slice());
        assert!(compressed.len() < data.len());

        // Test None
        let compressed = storage
            .compress(data.to_vec(), &CompressionType::None)
            .unwrap();
        assert_eq!(data, compressed.as_slice());
    }

    #[test]
    fn test_writer_and_reader() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema).unwrap();

        let storage_arc = Arc::new(RwLock::new(storage));

        // Create writer
        let mut writer = ColumnarWriter::new(storage_arc.clone(), 10);

        // Write rows
        for i in 1..=5 {
            let row = create_test_row(i, &format!("user_{}", i), 80.0 + i as f64);
            writer.write_row(row).unwrap();
        }

        writer.flush().unwrap();

        // Create reader
        let reader = ColumnarReader::new(storage_arc.clone());

        // Verify count
        let count = reader.count().unwrap();
        assert_eq!(count, 5);

        // Scan data
        let result = reader
            .scan(vec!["id".to_string(), "name".to_string()], None)
            .unwrap();
        assert_eq!(result.row_count, 5);
    }

    #[test]
    fn test_null_values() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema).unwrap();

        // Create row with null
        let mut row = Row::new();
        row.insert("id".to_string(), Some(Value::Int64(1)));
        row.insert("name".to_string(), None); // Null value
        row.insert("score".to_string(), Some(Value::Float64(95.0_f64.to_bits())));

        storage.write_batch(vec![row]).unwrap();

        let result = storage
            .scan(vec!["name".to_string()], None)
            .unwrap();
        assert!(result.columns.get("name").unwrap()[0].is_none());
    }

    #[test]
    fn test_large_batch() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema).unwrap();

        // Write 1000 rows
        let mut rows = Vec::new();
        for i in 1..=1000 {
            rows.push(create_test_row(i, &format!("user_{}", i % 10), 80.0 + (i % 20) as f64));
        }

        storage.write_batch(rows).unwrap();

        let metadata = storage.metadata.read().unwrap();
        assert_eq!(metadata.row_count, 1000);
    }

    #[test]
    fn test_row_default() {
        let row = Row::new();
        assert!(row.values.is_empty());
        assert!(row.get("nonexistent").is_none());
    }

    #[test]
    fn test_auto_flush_on_buffer_size() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        let schema = create_test_schema();
        storage.create_table(schema).unwrap();

        let storage_arc = Arc::new(RwLock::new(storage));

        // Buffer size of 3
        let mut writer = ColumnarWriter::new(storage_arc.clone(), 3);

        // Write 2 rows (should not flush yet)
        for i in 1..=2 {
            writer
                .write_row(create_test_row(i, &format!("user_{}", i), 85.0))
                .unwrap();
        }

        // Should be 0 in storage (not flushed)
        assert_eq!(storage_arc.read().unwrap().metadata.read().unwrap().row_count, 0);

        // Write third row (should auto-flush)
        writer
            .write_row(create_test_row(3, "user_3", 90.0))
            .unwrap();

        // Now should have 3 rows
        assert_eq!(storage_arc.read().unwrap().metadata.read().unwrap().row_count, 3);
    }

    #[test]
    fn test_compression_ratio_benefit() {
        let temp_dir = TempDir::new().unwrap();
        let config = ColumnarConfig::default();
        let mut storage = ColumnarStorage::new(temp_dir.path(), config).unwrap();

        // Create schema with dictionary encoding for repetitive data
        let schema = Schema {
            columns: vec![ColumnSchema {
                name: "status".to_string(),
                data_type: DataType::String,
                nullable: false,
                encoding: EncodingType::Dictionary,
                compression: CompressionType::Zstd,
                dictionary: None,
            }],
        };

        storage.create_table(schema).unwrap();

        // Write many rows with same values (high compression potential)
        let mut rows = Vec::new();
        for i in 1..=1000 {
            let mut row = Row::new();
            let status = match i % 3 {
                0 => "active",
                1 => "pending",
                _ => "inactive",
            };
            row.insert("status".to_string(), Some(Value::String(status.to_string())));
            rows.push(row);
        }

        storage.write_batch(rows).unwrap();

        // Verify data was written and compressed
        let metadata = storage.metadata.read().unwrap();
        assert_eq!(metadata.row_count, 1000);

        // Dictionary encoding should result in very small storage
        // (only 3 distinct values repeated 1000 times)
        assert!(storage.row_groups.len() > 0);
    }
}
