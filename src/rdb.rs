use bytes::Bytes;
use std::collections::HashMap;
use std::io::{self, Cursor, Read};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
#[derive(Debug, Clone)]
pub struct RdbFile {
    pub version: String,
    pub metadata: HashMap<String, String>,
    pub databases: Vec<Database>,
}

#[derive(Debug, Clone)]
pub struct Database {
    pub index: u64,
    pub key_value_hash_size: u64,
    pub expire_hash_size: u64,
    pub entries: Vec<KeyValuePair>,
}

#[derive(Debug, Clone)]
pub struct KeyValuePair {
    pub key: String,
    pub value: Value,
    pub expire: Option<Expiry>,
}

#[derive(Debug, Clone)]
pub enum Expiry {
    Seconds(u32),
    Milliseconds(u64),
}

impl Expiry {
    /// Convert RDB expiry (absolute Unix timestamp) to Instant relative to now
    pub fn to_instant(&self) -> Option<Instant> {
        let now_system = SystemTime::now();
        let now_duration = now_system.duration_since(UNIX_EPOCH).ok()?;

        let expiry_system = match self {
            Expiry::Seconds(secs) => UNIX_EPOCH + Duration::from_secs(*secs as u64),
            Expiry::Milliseconds(millis) => UNIX_EPOCH + Duration::from_millis(*millis),
        };

        // Check if already expired
        if expiry_system <= now_system {
            return None;
        }

        // Calculate time until expiry
        let time_until_expiry = expiry_system.duration_since(now_system).ok()?;
        Some(Instant::now() + time_until_expiry)
    }

    /// Check if this expiry has already passed
    pub fn is_expired(&self) -> bool {
        self.to_instant().is_none()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    // TODO: Add more types as needed (List, Set, ZSet, Hash, etc.)
}

#[derive(Debug)]
pub enum RdbError {
    IoError(io::Error),
    InvalidHeader,
    InvalidFormat(String),
    UnexpectedEof,
    ChecksumMismatch,
}

impl From<io::Error> for RdbError {
    fn from(err: io::Error) -> Self {
        RdbError::IoError(err)
    }
}

impl std::fmt::Display for RdbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbError::IoError(e) => write!(f, "IO error: {}", e),
            RdbError::InvalidHeader => write!(f, "Invalid RDB header"),
            RdbError::InvalidFormat(s) => write!(f, "Invalid format: {}", s),
            RdbError::UnexpectedEof => write!(f, "Unexpected end of file"),
            RdbError::ChecksumMismatch => write!(f, "Checksum validation failed"),
        }
    }
}

impl std::error::Error for RdbError {}

pub struct RdbParser<R: Read> {
    reader: R,
    peeked_byte: Option<u8>,
}

impl<R: Read> RdbParser<R> {
    pub fn new(reader: R) -> Self {
        RdbParser {
            reader,
            peeked_byte: None,
        }
    }

    fn read_byte(&mut self) -> Result<u8, RdbError> {
        if let Some(byte) = self.peeked_byte.take() {
            return Ok(byte);
        }

        let mut buf = [0u8; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn peek_byte(&mut self) -> Result<u8, RdbError> {
        if let Some(byte) = self.peeked_byte {
            return Ok(byte);
        }

        let byte = self.read_byte()?;
        self.peeked_byte = Some(byte);
        Ok(byte)
    }

    pub fn parse(&mut self) -> Result<RdbFile, RdbError> {
        // Parse header
        let version = self.parse_header()?;

        // Parse metadata
        let metadata = self.parse_metadata()?;

        // Parse databases
        let databases = self.parse_databases()?;

        Ok(RdbFile {
            version,
            metadata,
            databases,
        })
    }

    fn parse_header(&mut self) -> Result<String, RdbError> {
        let mut header = [0u8; 9];
        self.reader.read_exact(&mut header)?;

        // Check magic string "REDIS"
        if &header[0..5] != b"REDIS" {
            return Err(RdbError::InvalidHeader);
        }

        // Extract version
        let version = String::from_utf8_lossy(&header[5..9]).to_string();
        Ok(version)
    }

    fn parse_metadata(&mut self) -> Result<HashMap<String, String>, RdbError> {
        let mut metadata = HashMap::new();

        loop {
            let byte = self.read_byte()?;

            match byte {
                0xFA => {
                    // Metadata subsection
                    let key = self.parse_string()?;
                    let value = self.parse_string()?;
                    metadata.insert(key, value);
                }
                0xFE => {
                    // Start of database section - preserve byte for parse_databases
                    self.peeked_byte = Some(0xFE);
                    return Ok(metadata);
                }
                0xFF => {
                    // End of file - preserve byte for parse_databases
                    self.peeked_byte = Some(0xFF);
                    return Ok(metadata);
                }
                _ => {
                    return Err(RdbError::InvalidFormat(format!(
                        "Unexpected byte in metadata: 0x{:02X}",
                        byte
                    )));
                }
            }
        }
    }

    fn parse_databases(&mut self) -> Result<Vec<Database>, RdbError> {
        let mut databases = Vec::new();

        loop {
            let byte = match self.read_byte() {
                Ok(b) => b,
                Err(RdbError::IoError(e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            match byte {
                0xFE => {
                    // Database subsection
                    let db = self.parse_database()?;
                    databases.push(db);
                }
                0xFF => {
                    // End of file - read and validate checksum
                    let mut checksum = [0u8; 8];
                    match self.reader.read_exact(&mut checksum) {
                        Ok(_) => {
                            // TODO: Implement actual CRC64 validation
                            // For now, just acknowledge we read it
                        }
                        Err(_) => {
                            // Some RDB files might not have checksum
                        }
                    }
                    break;
                }
                _ => {
                    return Err(RdbError::InvalidFormat(format!(
                        "Unexpected byte in database section: 0x{:02X}",
                        byte
                    )));
                }
            }
        }

        Ok(databases)
    }

    fn parse_database(&mut self) -> Result<Database, RdbError> {
        // Parse database index
        let index = self.parse_length()?;

        let mut key_value_hash_size = 0;
        let mut expire_hash_size = 0;
        let mut entries = Vec::new();

        // Check for hash table size info (0xFB)
        let byte = self.read_byte()?;

        if byte == 0xFB {
            key_value_hash_size = self.parse_length()?;
            expire_hash_size = self.parse_length()?;
        } else {
            // Not a hash table size marker - this is the start of key-value pairs
            self.peeked_byte = Some(byte);
        }

        // Parse key-value pairs
        loop {
            let byte = match self.read_byte() {
                Ok(b) => b,
                Err(RdbError::IoError(e)) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            match byte {
                // Expiry markers
                0xFC | 0xFD => {
                    let entry = self.parse_key_value_pair(byte)?;
                    entries.push(entry);
                }
                // Value type bytes (0x00 = string, 0x01-0x0E = other types)
                0x00..=0x0E => {
                    let entry = self.parse_key_value_pair(byte)?;
                    entries.push(entry);
                }
                // End of database or file
                0xFE | 0xFF => {
                    self.peeked_byte = Some(byte);
                    break;
                }
                _ => {
                    return Err(RdbError::InvalidFormat(format!(
                        "Unexpected byte in database {}: 0x{:02X}",
                        index, byte
                    )));
                }
            }
        }

        Ok(Database {
            index,
            key_value_hash_size,
            expire_hash_size,
            entries,
        })
    }

    fn parse_key_value_pair(&mut self, first_byte: u8) -> Result<KeyValuePair, RdbError> {
        let mut expire = None;

        // Check for expiry information
        let value_type = match first_byte {
            0xFC => {
                // Expire in milliseconds (8 bytes, little-endian)
                let mut ts_bytes = [0u8; 8];
                self.reader.read_exact(&mut ts_bytes)?;
                let timestamp = u64::from_le_bytes(ts_bytes);
                expire = Some(Expiry::Milliseconds(timestamp));

                // Read actual value type
                self.read_byte()?
            }
            0xFD => {
                // Expire in seconds (4 bytes, little-endian)
                let mut ts_bytes = [0u8; 4];
                self.reader.read_exact(&mut ts_bytes)?;
                let timestamp = u32::from_le_bytes(ts_bytes);
                expire = Some(Expiry::Seconds(timestamp));

                // Read actual value type
                self.read_byte()?
            }
            _ => first_byte,
        };

        // Parse key
        let key = self.parse_string()?;

        // Parse value based on type
        let value = match value_type {
            0x00 => {
                // String type
                let val_str = self.parse_string()?;
                Value::String(val_str)
            }
            _ => {
                return Err(RdbError::InvalidFormat(format!(
                    "Unsupported value type: 0x{:02X} for key '{}'",
                    value_type, key
                )));
            }
        };

        Ok(KeyValuePair { key, value, expire })
    }

    fn parse_length(&mut self) -> Result<u64, RdbError> {
        let byte = self.read_byte()?;

        let first_two_bits = (byte & 0xC0) >> 6;

        match first_two_bits {
            0b00 => {
                // Length is in the remaining 6 bits
                Ok((byte & 0x3F) as u64)
            }
            0b01 => {
                // Length is in next 14 bits (6 bits + 8 bits)
                let next_byte = self.read_byte()?;
                let len = (((byte & 0x3F) as u64) << 8) | (next_byte as u64);
                Ok(len)
            }
            0b10 => {
                // Length is in next 4 bytes (big-endian)
                let mut len_bytes = [0u8; 4];
                self.reader.read_exact(&mut len_bytes)?;
                Ok(u32::from_be_bytes(len_bytes) as u64)
            }
            0b11 => {
                // Special encoding format indicator
                Ok(byte as u64)
            }
            _ => unreachable!(),
        }
    }

    fn parse_string(&mut self) -> Result<String, RdbError> {
        let size = self.parse_length()?;

        match size {
            0xC0 => {
                // 8-bit integer
                let byte = self.read_byte()?;
                Ok(byte.to_string())
            }
            0xC1 => {
                // 16-bit integer (little-endian)
                let mut bytes = [0u8; 2];
                self.reader.read_exact(&mut bytes)?;
                let val = u16::from_le_bytes(bytes);
                Ok(val.to_string())
            }
            0xC2 => {
                // 32-bit integer (little-endian)
                let mut bytes = [0u8; 4];
                self.reader.read_exact(&mut bytes)?;
                let val = u32::from_le_bytes(bytes);
                Ok(val.to_string())
            }
            0xC3 => Err(RdbError::InvalidFormat(
                "LZF-compressed strings not supported".to_string(),
            )),
            len => {
                // Regular string
                let mut buf = vec![0u8; len as usize];
                self.reader.read_exact(&mut buf)?;
                Ok(String::from_utf8_lossy(&buf).to_string())
            }
        }
    }
}

// Helper function to parse from bytes
pub fn parse_rdb(data: &[u8]) -> Result<RdbFile, RdbError> {
    let cursor = Cursor::new(data);
    let mut parser = RdbParser::new(cursor);
    parser.parse()
}

use crate::resp::RedisValueRef;
use tokio::sync::RwLock;
struct Set {
    value: Bytes,
    expiry: Option<Instant>,
}

pub struct RdbPath {
    dir: String,
    dbfilename: String,
}

impl RdbPath {
    pub fn new() -> Self {
        RdbPath {
            dir: String::new(),
            dbfilename: String::new(),
        }
    }
}

pub struct KeyValue {
    entries: RwLock<HashMap<Bytes, Set>>,
    path: RwLock<RdbPath>,
}

impl KeyValue {
    pub fn new() -> Self {
        KeyValue {
            entries: RwLock::new(HashMap::new()),
            path: RwLock::new(RdbPath::new()),
        }
    }

    pub async fn get_dir(&self) -> String {
        let path = self.path.read().await;
        path.dir.clone()
    }

    pub async fn get_filename(&self) -> String {
        let path = self.path.read().await;
        path.dbfilename.clone()
    }

    /// Load data from an RDB file
    pub async fn load_from_rdb(&self, rdb_data: &[u8]) -> Result<(), RdbError> {
        let rdb_file = parse_rdb(rdb_data)?;

        let mut entries = self.entries.write().await;

        // Load all databases (typically just database 0)
        for database in rdb_file.databases {
            for entry in database.entries {
                let key = Bytes::from(entry.key);

                // Extract value
                let value = match entry.value {
                    Value::String(s) => Bytes::from(s),
                };

                // Convert expiry
                let expiry = entry.expire.and_then(|exp| exp.to_instant());

                // Only insert if not already expired
                if expiry.is_none() || expiry.map_or(true, |exp| Instant::now() < exp) {
                    entries.insert(key, Set { value, expiry });
                }
            }
        }

        Ok(())
    }

    /// Load data from an RDB file path
    pub async fn load_from_rdb_file(
        &self,
        dir: String,
        dbfilename: String,
    ) -> Result<(), RdbError> {
        let mut path = self.path.write().await;
        path.dbfilename = dbfilename;
        path.dir = dir;
        let data = std::fs::read(format!("{}/{}", path.dir, path.dbfilename))
            .map_err(RdbError::IoError)?;
        self.load_from_rdb(&data).await
    }

    pub async fn insert_entry(&self, key: Bytes, value: Bytes, expiry: Option<(Bytes, i64)>) {
        let mut entries = self.entries.write().await;

        let set = if let Some((ty, time)) = expiry {
            let ty: &[u8] = &ty;
            let duration = match ty {
                b"EX" => Duration::from_secs(time as u64),
                b"PX" => Duration::from_millis(time as u64),
                b"EXAT" => {
                    let target = Instant::now() + Duration::from_secs(time as u64);
                    entries.insert(
                        key,
                        Set {
                            value,
                            expiry: Some(target),
                        },
                    );
                    return;
                }
                b"PXAT" => {
                    let target = Instant::now() + Duration::from_millis(time as u64);
                    entries.insert(
                        key,
                        Set {
                            value,
                            expiry: Some(target),
                        },
                    );
                    return;
                }
                _ => Duration::from_secs(0),
            };
            Set {
                value,
                expiry: Some(Instant::now() + duration),
            }
        } else {
            Set {
                value,
                expiry: None,
            }
        };

        entries.insert(key, set);
    }

    pub async fn get_entry(&self, key: &Bytes) -> Option<Bytes> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(key) {
            if let Some(expiry) = entry.expiry {
                if Instant::now() >= expiry {
                    entries.remove(key);
                    return None;
                }
            }
            return Some(entry.value.clone());
        }
        None
    }

    pub async fn exists(&self, key: &Bytes) -> bool {
        let entries = self.entries.read().await;

        if let Some(set) = entries.get(key) {
            if let Some(expiry) = set.expiry {
                if Instant::now() > expiry {
                    return false;
                }
            }
            return true;
        }

        false
    }

    pub async fn contains(&self, key: &Bytes) -> bool {
        let entries = self.entries.read().await;
        entries.contains_key(key)
    }

    pub async fn incr(&self, key: &Bytes) -> Result<i64, String> {
        let mut entries = self.entries.write().await;

        let set = Set {
            value: Bytes::from((1).to_string()),
            expiry: None,
        };

        if let Some(entry) = entries.get_mut(key) {
            if let Some(expiry) = entry.expiry {
                if Instant::now() >= expiry {
                    entries.remove(key);
                    entries.insert(key.clone(), set);
                    return Ok(1);
                }
            }

            let value = match std::str::from_utf8(&entry.value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(val) => val + 1,
                None => {
                    return Err("ERR value is not an integer or out of range".to_string());
                }
            };
            entry.value = Bytes::from(value.to_string());
            return Ok(value);
        } else {
            entries.insert(key.clone(), set);
        }
        Ok(1)
    }

    pub async fn get_all_keys(&self) -> Vec<Bytes> {
        let entries = self.entries.read().await;
        entries.keys().cloned().collect()
    }

    pub async fn keys(&self, pattern: Bytes) -> RedisValueRef {
        let entries = self.entries.read().await;
        let pattern_str = std::str::from_utf8(&pattern).unwrap_or("*");
        let mut res = Vec::new();
        for entry in entries.keys().filter(|key| {
            // Check if key is expired
            if let Some(set) = entries.get(*key) {
                if let Some(expiry) = set.expiry {
                    if Instant::now() >= expiry {
                        return false;
                    }
                }
            }
            // Match pattern
            let key_str = std::str::from_utf8(key).unwrap_or("");
            Self::match_pattern(pattern_str, key_str)
        }) {
            res.push(RedisValueRef::BulkString(entry.clone()));
        }
        RedisValueRef::Array(res)
    }

    fn match_pattern(pattern: &str, key: &str) -> bool {
        // Convert pattern to regex-like matching
        let mut pattern_chars = pattern.chars().peekable();
        let mut key_chars = key.chars().peekable();

        Self::match_pattern_recursive(&mut pattern_chars, &mut key_chars)
    }

    fn match_pattern_recursive(
        pattern: &mut std::iter::Peekable<std::str::Chars>,
        key: &mut std::iter::Peekable<std::str::Chars>,
    ) -> bool {
        loop {
            match pattern.peek() {
                None => return key.peek().is_none(),
                Some(&'*') => {
                    pattern.next();
                    // If * is at the end, match everything
                    if pattern.peek().is_none() {
                        return true;
                    }
                    // Try matching at each position
                    loop {
                        if Self::match_pattern_recursive(&mut pattern.clone(), &mut key.clone()) {
                            return true;
                        }
                        if key.next().is_none() {
                            return false;
                        }
                    }
                }
                Some(&'?') => {
                    pattern.next();
                    if key.next().is_none() {
                        return false;
                    }
                }
                Some(&'[') => {
                    pattern.next();
                    let key_char = match key.next() {
                        Some(c) => c,
                        None => return false,
                    };

                    let mut matched = false;
                    let mut negate = false;

                    if pattern.peek() == Some(&'^') {
                        negate = true;
                        pattern.next();
                    }

                    let mut chars_in_bracket = Vec::new();
                    loop {
                        match pattern.next() {
                            Some(']') => break,
                            Some('-') if !chars_in_bracket.is_empty() => {
                                if let Some(&end) = pattern.peek() {
                                    if end != ']' {
                                        pattern.next();
                                        let start = chars_in_bracket.pop().unwrap();
                                        if key_char >= start && key_char <= end {
                                            matched = true;
                                        }
                                    }
                                }
                            }
                            Some(c) => {
                                if c == key_char {
                                    matched = true;
                                }
                                chars_in_bracket.push(c);
                            }
                            None => return false,
                        }
                    }

                    if negate {
                        matched = !matched;
                    }

                    if !matched {
                        return false;
                    }
                }
                Some(&'\\') => {
                    pattern.next();
                    let pattern_char = match pattern.next() {
                        Some(c) => c,
                        None => return false,
                    };
                    let key_char = match key.next() {
                        Some(c) => c,
                        None => return false,
                    };
                    if pattern_char != key_char {
                        return false;
                    }
                }
                Some(&p) => {
                    pattern.next();
                    let k = match key.next() {
                        Some(c) => c,
                        None => return false,
                    };
                    if p != k {
                        return false;
                    }
                }
            }
        }
    }
}
