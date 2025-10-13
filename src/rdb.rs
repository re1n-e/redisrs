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
    /// Convert RDB expiry to Instant relative to now
    pub fn to_instant(&self) -> Option<Instant> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;

        let expiry_duration = match self {
            Expiry::Seconds(secs) => Duration::from_secs(*secs as u64),
            Expiry::Milliseconds(millis) => Duration::from_millis(*millis),
        };

        // Check if expiry is in the future
        if expiry_duration > now {
            let time_until_expiry = expiry_duration - now;
            Some(Instant::now() + time_until_expiry)
        } else {
            // Already expired
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    // Add more types as needed
}

#[derive(Debug)]
pub enum RdbError {
    IoError(io::Error),
    InvalidHeader,
    InvalidFormat(String),
    UnexpectedEof,
}

impl From<io::Error> for RdbError {
    fn from(err: io::Error) -> Self {
        RdbError::IoError(err)
    }
}

pub struct RdbParser<R: Read> {
    reader: R,
}

impl<R: Read> RdbParser<R> {
    pub fn new(reader: R) -> Self {
        RdbParser { reader }
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
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte)?;

            match byte[0] {
                0xFA => {
                    // Metadata subsection
                    let key = self.parse_string()?;
                    let value = self.parse_string()?;
                    metadata.insert(key, value);
                }
                0xFE => {
                    // Start of database section
                    return Ok(metadata);
                }
                0xFF => {
                    // End of file
                    return Ok(metadata);
                }
                _ => {
                    return Err(RdbError::InvalidFormat(format!(
                        "Unexpected byte in metadata: 0x{:02X}",
                        byte[0]
                    )));
                }
            }
        }
    }

    fn parse_databases(&mut self) -> Result<Vec<Database>, RdbError> {
        let mut databases = Vec::new();

        loop {
            let mut byte = [0u8; 1];
            match self.reader.read_exact(&mut byte) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            match byte[0] {
                0xFE => {
                    // Database subsection
                    let db = self.parse_database()?;
                    databases.push(db);
                }
                0xFF => {
                    // End of file, skip checksum
                    let mut _checksum = [0u8; 8];
                    let _ = self.reader.read_exact(&mut _checksum);
                    break;
                }
                _ => {
                    return Err(RdbError::InvalidFormat(format!(
                        "Unexpected byte in database section: 0x{:02X}",
                        byte[0]
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

        // Check for hash table size info
        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;

        if byte[0] == 0xFB {
            key_value_hash_size = self.parse_length()?;
            expire_hash_size = self.parse_length()?;
        } else {
            //start of a key-value pair
            // We'll handle this by processing it in the loop
        }

        // Parse key-value pairs
        loop {
            if byte[0] != 0xFB {
                // Process the byte we already read
                match byte[0] {
                    0xFC | 0xFD | 0x00..=0x0E => {
                        // Parse key-value pair
                        let entry = self.parse_key_value_pair(byte[0])?;
                        entries.push(entry);
                    }
                    0xFE | 0xFF => {
                        // End of database or file
                        return Ok(Database {
                            index,
                            key_value_hash_size,
                            expire_hash_size,
                            entries,
                        });
                    }
                    _ => {}
                }
            }

            // Read next byte
            match self.reader.read_exact(&mut byte) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Check if we've reached the end of this database
            if byte[0] == 0xFE || byte[0] == 0xFF {
                break;
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
                // Expire in milliseconds
                let mut ts_bytes = [0u8; 8];
                self.reader.read_exact(&mut ts_bytes)?;
                let timestamp = u64::from_le_bytes(ts_bytes);
                expire = Some(Expiry::Milliseconds(timestamp));

                // Read actual value type
                let mut vt = [0u8; 1];
                self.reader.read_exact(&mut vt)?;
                vt[0]
            }
            0xFD => {
                // Expire in seconds
                let mut ts_bytes = [0u8; 4];
                self.reader.read_exact(&mut ts_bytes)?;
                let timestamp = u32::from_le_bytes(ts_bytes);
                expire = Some(Expiry::Seconds(timestamp));

                // Read actual value type
                let mut vt = [0u8; 1];
                self.reader.read_exact(&mut vt)?;
                vt[0]
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
                    "Unsupported value type: 0x{:02X}",
                    value_type
                )));
            }
        };

        Ok(KeyValuePair { key, value, expire })
    }

    fn parse_length(&mut self) -> Result<u64, RdbError> {
        let mut byte = [0u8; 1];
        self.reader.read_exact(&mut byte)?;

        let first_two_bits = (byte[0] & 0xC0) >> 6;

        match first_two_bits {
            0b00 => {
                // Length is in the remaining 6 bits
                Ok((byte[0] & 0x3F) as u64)
            }
            0b01 => {
                // Length is in next 14 bits (6 + 8)
                let mut next_byte = [0u8; 1];
                self.reader.read_exact(&mut next_byte)?;
                let len = (((byte[0] & 0x3F) as u64) << 8) | (next_byte[0] as u64);
                Ok(len)
            }
            0b10 => {
                // Length is in next 4 bytes (big-endian)
                let mut len_bytes = [0u8; 4];
                self.reader.read_exact(&mut len_bytes)?;
                Ok(u32::from_be_bytes(len_bytes) as u64)
            }
            0b11 => {
                // Special encoding - return the format indicator
                Ok(byte[0] as u64)
            }
            _ => unreachable!(),
        }
    }

    fn parse_string(&mut self) -> Result<String, RdbError> {
        let size = self.parse_length()?;

        match size {
            0xC0 => {
                // 8-bit integer
                let mut byte = [0u8; 1];
                self.reader.read_exact(&mut byte)?;
                Ok(byte[0].to_string())
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

// Integration with KeyValue store
use tokio::sync::RwLock;

use crate::resp::RedisValueRef;

struct Set {
    value: Bytes,
    expiry: Option<Instant>,
}

pub struct KeyValue {
    entries: RwLock<HashMap<Bytes, Set>>,
}

impl KeyValue {
    pub fn new() -> Self {
        KeyValue {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Load data from an RDB file
    async fn load_from_rdb(&self, rdb_data: &[u8]) -> Result<(), RdbError> {
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
    pub async fn load_from_rdb_file(&self, path: &str) -> Result<(), RdbError> {
        let data = std::fs::read(path).map_err(RdbError::IoError)?;
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
