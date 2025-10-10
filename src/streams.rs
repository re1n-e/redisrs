use crate::resp::RedisValueRef;
use bytes::Bytes;
use memchr::memchr;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;
pub struct StreamKV {
    // (sequence num - time) -> map (key , value)
    map: BTreeMap<(Bytes, Bytes), BTreeMap<Bytes, Bytes>>,
}

impl StreamKV {
    pub fn new() -> Self {
        StreamKV {
            map: BTreeMap::new(),
        }
    }
}

pub struct Stream {
    // streamid -> StreamKV
    streams: RwLock<HashMap<Bytes, StreamKV>>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            streams: RwLock::new(HashMap::new()),
        }
    }

    pub async fn xadd(&self, stream_key: Bytes, stream_id: Bytes, kv: Vec<Bytes>) -> RedisValueRef {
        if let Some(pos) = memchr(b'-', &stream_id) {
            let ts = Bytes::copy_from_slice(&stream_id[..pos]);
            let seq = Bytes::copy_from_slice(&stream_id[pos + 1..]);

            let ts_str = std::str::from_utf8(&ts).ok().unwrap();
            let seq_str = std::str::from_utf8(&seq).ok().unwrap();

            // Determine final timestamp and sequence
            let (final_ts, final_seq) = if ts_str == "*" {
                // Full auto-generation: *
                self.generate_id(&stream_key, None).await
            } else if seq_str == "*" {
                // Partial auto-generation: <timestamp>-*
                self.generate_id(&stream_key, Some(&ts)).await
            } else {
                // Fully specified ID, validate it
                match self.validate_key(&stream_key, ts_str, seq_str).await {
                    Some(s) => return RedisValueRef::Error(Bytes::from(s)),
                    None => (ts, seq),
                }
            };

            let mut streams = self.streams.write().await;
            streams.entry(stream_key.clone()).or_insert(StreamKV::new());
            if let Some(stream) = streams.get_mut(&stream_key) {
                stream
                    .map
                    .entry((final_ts.clone(), final_seq.clone()))
                    .or_insert(BTreeMap::new());
                if let Some(map) = stream.map.get_mut(&(final_ts.clone(), final_seq.clone())) {
                    for i in (0..kv.len()).step_by(2) {
                        map.insert(kv[i].clone(), kv[i + 1].clone());
                    }
                }
            }

            let result_id = format!(
                "{}-{}",
                std::str::from_utf8(&final_ts).unwrap(),
                std::str::from_utf8(&final_seq).unwrap()
            );
            RedisValueRef::BulkString(Bytes::from(result_id))
        } else {
            // Handle special case: just "*" without a dash
            if stream_id.as_ref() == b"*" {
                let (final_ts, final_seq) = self.generate_id(&stream_key, None).await;

                let mut streams = self.streams.write().await;
                streams.entry(stream_key.clone()).or_insert(StreamKV::new());
                if let Some(stream) = streams.get_mut(&stream_key) {
                    stream
                        .map
                        .entry((final_ts.clone(), final_seq.clone()))
                        .or_insert(BTreeMap::new());
                    if let Some(map) = stream.map.get_mut(&(final_ts.clone(), final_seq.clone())) {
                        for i in (0..kv.len()).step_by(2) {
                            map.insert(kv[i].clone(), kv[i + 1].clone());
                        }
                    }
                }

                let result_id = format!(
                    "{}-{}",
                    std::str::from_utf8(&final_ts).unwrap(),
                    std::str::from_utf8(&final_seq).unwrap()
                );
                return RedisValueRef::BulkString(Bytes::from(result_id));
            }

            RedisValueRef::Error(Bytes::from(
                "ERR Invalid stream ID specified as stream command argument",
            ))
        }
    }

    pub async fn contains(&self, stream_key: &Bytes) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_key)
    }

    async fn validate_key(
        &self,
        stream_key: &Bytes,
        ts_str: &str,
        seq_str: &str,
    ) -> Option<String> {
        let streams = self.streams.read().await;

        let ts_num = ts_str.parse::<u64>().ok();
        let seq_num = seq_str.parse::<u64>().ok();

        if ts_num.is_none() || seq_num.is_none() {
            return Some("ERR Invalid stream ID specified as stream command argument".to_string());
        }

        let ts_num = ts_num.unwrap();
        let seq_num = seq_num.unwrap();

        if ts_num == 0 && seq_num == 0 {
            return Some("ERR The ID specified in XADD must be greater than 0-0".to_string());
        }

        if let Some(stream) = streams.get(stream_key) {
            if let Some(((last_ts, last_seq), _)) = stream.map.last_key_value() {
                let last_ts_str = std::str::from_utf8(last_ts).ok()?;
                let last_seq_str = std::str::from_utf8(last_seq).ok()?;

                if let (Ok(last_ts_num), Ok(last_seq_num)) =
                    (last_ts_str.parse::<u64>(), last_seq_str.parse::<u64>())
                {
                    if ts_num < last_ts_num || (ts_num == last_ts_num && seq_num <= last_seq_num) {
                        return Some(format!(
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    ));
                    }
                }
            }
        }

        None
    }

    pub async fn xrange(
        &self,
        stream_id: &Bytes,
        start: &Bytes,
        end: &Bytes,
    ) -> Vec<RedisValueRef> {
        let mut res: Vec<RedisValueRef> = Vec::new();
        let start_ts = match start.as_ref() {
            b"+" => &Bytes::from((0).to_string()),
            _ => start,
        };

        let end_ts = match end.as_ref() {
            b"-" => &Bytes::from(current_unix_timestamp_ms().to_string()),
            _ => end,
        };

        let streams = self.streams.read().await;
        if let Some(stream) = streams.get(stream_id) {
            for ((ts, seq), map) in stream.map.iter() {
                if ts >= start_ts && ts <= end_ts {
                    let result_id = format!(
                        "{}-{}",
                        std::str::from_utf8(ts).unwrap(),
                        std::str::from_utf8(seq).unwrap()
                    );
                    let mut map_vec: Vec<RedisValueRef> = Vec::new();
                    map_vec.push(RedisValueRef::BulkString(Bytes::from(result_id)));
                    for (key, val) in map.iter() {
                        map_vec.push(RedisValueRef::Array(vec![
                            RedisValueRef::String(key.clone()),
                            RedisValueRef::String(val.clone()),
                        ]));
                    }
                    res.push(RedisValueRef::Array(map_vec));
                } else if ts > end_ts {
                    break;
                }
            }
        }
        res
    }

    async fn generate_id(&self, stream_key: &Bytes, cur_ts: Option<&Bytes>) -> (Bytes, Bytes) {
        let streams = self.streams.read().await;

        if let Some(stream) = streams.get(stream_key) {
            if let Some(((last_ts, last_seq), _)) = stream.map.last_key_value() {
                match cur_ts {
                    Some(cur_ts) => {
                        // User provided timestamp, auto-generate sequence number
                        if last_ts == cur_ts {
                            // Same timestamp as last entry, increment sequence
                            let seq_str = std::str::from_utf8(last_seq).ok().unwrap();
                            let seq_num = seq_str.parse::<u64>().ok().unwrap();
                            (cur_ts.clone(), Bytes::from((seq_num + 1).to_string()))
                        } else {
                            // Different timestamp, start sequence at 0
                            (cur_ts.clone(), Bytes::from("0"))
                        }
                    }
                    None => {
                        // Auto-generate both timestamp and sequence
                        let current_timestamp = current_unix_timestamp_ms();
                        let last_ts_str = std::str::from_utf8(last_ts).ok().unwrap();
                        let last_ts_num = last_ts_str.parse::<u64>().ok().unwrap();

                        if current_timestamp > last_ts_num {
                            // Current time is ahead, use it with sequence 0
                            (Bytes::from(current_timestamp.to_string()), Bytes::from("0"))
                        } else {
                            // Current time is same or behind, use last timestamp and increment sequence
                            let last_seq_str = std::str::from_utf8(last_seq).ok().unwrap();
                            let last_seq_num = last_seq_str.parse::<u64>().ok().unwrap();
                            (last_ts.clone(), Bytes::from((last_seq_num + 1).to_string()))
                        }
                    }
                }
            } else {
                // Stream is empty, this is the first entry
                match cur_ts {
                    Some(cur_ts) => {
                        // User provided timestamp, use sequence 0 or 1
                        let ts_str = std::str::from_utf8(cur_ts).ok().unwrap();
                        let ts_num = ts_str.parse::<u64>().ok().unwrap();

                        if ts_num == 0 {
                            // Special case: if timestamp is 0, start with 0-1
                            (cur_ts.clone(), Bytes::from("1"))
                        } else {
                            (cur_ts.clone(), Bytes::from("0"))
                        }
                    }
                    None => {
                        // Auto-generate both for first entry
                        let current_timestamp = current_unix_timestamp_ms();
                        (Bytes::from(current_timestamp.to_string()), Bytes::from("0"))
                    }
                }
            }
        } else {
            // Stream doesn't exist yet, this will be the first entry
            match cur_ts {
                Some(cur_ts) => {
                    let ts_str = std::str::from_utf8(cur_ts).ok().unwrap();
                    let ts_num = ts_str.parse::<u64>().ok().unwrap();

                    if ts_num == 0 {
                        (cur_ts.clone(), Bytes::from("1"))
                    } else {
                        (cur_ts.clone(), Bytes::from("0"))
                    }
                }
                None => {
                    let current_timestamp = current_unix_timestamp_ms();
                    (Bytes::from(current_timestamp.to_string()), Bytes::from("0"))
                }
            }
        }
    }
}

fn current_unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
