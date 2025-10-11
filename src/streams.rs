use crate::resp::RedisValueRef;
use bytes::Bytes;
use memchr::memchr;
use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::sync::{oneshot, RwLock};
use tokio::time::{timeout, Duration};
type BlockedClientsMap = HashMap<Bytes, VecDeque<oneshot::Sender<bool>>>;

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
    blocked: RwLock<BlockedClientsMap>,
}

impl Stream {
    pub fn new() -> Self {
        Stream {
            streams: RwLock::new(HashMap::new()),
            blocked: RwLock::new(HashMap::new()),
        }
    }

    pub async fn xadd(&self, stream_key: Bytes, stream_id: Bytes, kv: Vec<Bytes>) -> RedisValueRef {
        let mut res = RedisValueRef::Error(Bytes::from(
            "ERR Invalid stream ID specified as stream command argument",
        ));
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
            res = RedisValueRef::BulkString(Bytes::from(result_id));
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
                res = RedisValueRef::BulkString(Bytes::from(result_id));
            }
        }

        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(&stream_key) {
            if let Some(notifier) = notifiers.pop_front() {
                let _ = notifier.send(true);
                if notifiers.is_empty() {
                    blocked_clients.remove(&stream_key);
                }
            }
        }

        res
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

        let (start_ts, start_seq) = if start.as_ref() == b"-" {
            // "-" means start from the beginning
            (Bytes::from("0"), Bytes::from("0"))
        } else if let Some(pos) = memchr(b'-', start) {
            let ts = Bytes::copy_from_slice(&start[..pos]);
            let seq = Bytes::copy_from_slice(&start[pos + 1..]);
            (ts, seq)
        } else {
            // Invalid format
            return res;
        };

        let (end_ts, end_seq) = if end.as_ref() == b"+" {
            // "+" means go to the end (use max values)
            (
                Bytes::from(u64::MAX.to_string()),
                Bytes::from(u64::MAX.to_string()),
            )
        } else if let Some(pos) = memchr(b'-', end) {
            let ts = Bytes::copy_from_slice(&end[..pos]);
            let seq = Bytes::copy_from_slice(&end[pos + 1..]);
            (ts, seq)
        } else {
            // Invalid format
            return res;
        };

        let streams = self.streams.read().await;
        if let Some(stream) = streams.get(stream_id) {
            for ((ts, seq), map) in stream.map.iter() {
                // Parse current entry's timestamp and sequence
                let ts_str = std::str::from_utf8(ts).ok().unwrap();
                let seq_str = std::str::from_utf8(seq).ok().unwrap();
                let ts_num = ts_str.parse::<u64>().ok().unwrap();
                let seq_num = seq_str.parse::<u64>().ok().unwrap();

                // Parse start and end bounds
                let start_ts_num = std::str::from_utf8(&start_ts)
                    .ok()
                    .unwrap()
                    .parse::<u64>()
                    .ok()
                    .unwrap();
                let start_seq_num = std::str::from_utf8(&start_seq)
                    .ok()
                    .unwrap()
                    .parse::<u64>()
                    .ok()
                    .unwrap();
                let end_ts_num = std::str::from_utf8(&end_ts)
                    .ok()
                    .unwrap()
                    .parse::<u64>()
                    .ok()
                    .unwrap();
                let end_seq_num = std::str::from_utf8(&end_seq)
                    .ok()
                    .unwrap()
                    .parse::<u64>()
                    .ok()
                    .unwrap();

                let after_start =
                    ts_num > start_ts_num || (ts_num == start_ts_num && seq_num >= start_seq_num);
                let before_end =
                    ts_num < end_ts_num || (ts_num == end_ts_num && seq_num <= end_seq_num);

                if after_start && before_end {
                    let result_id = format!("{}-{}", ts_str, seq_str);
                    let mut map_vec: Vec<RedisValueRef> = Vec::new();
                    map_vec.push(RedisValueRef::BulkString(Bytes::from(result_id)));

                    let mut kv_array: Vec<RedisValueRef> = Vec::new();
                    for (key, val) in map.iter() {
                        kv_array.push(RedisValueRef::BulkString(key.clone()));
                        kv_array.push(RedisValueRef::BulkString(val.clone()));
                    }
                    map_vec.push(RedisValueRef::Array(kv_array));

                    res.push(RedisValueRef::Array(map_vec));
                } else if ts_num > end_ts_num || (ts_num == end_ts_num && seq_num > end_seq_num) {
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

    pub async fn xread(&self, kv: &Vec<Bytes>) -> Vec<RedisValueRef> {
        let mut res: Vec<RedisValueRef> = Vec::new();
        let streams = self.streams.read().await;

        // Process each stream key-id pair
        for i in (0..kv.len()).step_by(2) {
            let stream_key = &kv[i];

            // Resolve the stream_id, handling the "$" special case
            let stream_id = match kv[i + 1].as_ref() {
                b"$" => {
                    // Get the last entry ID for this stream
                    if let Some(stream) = streams.get(stream_key) {
                        match stream.map.last_key_value() {
                            Some(((ts, seq), _)) => {
                                let ts_str = std::str::from_utf8(&ts).ok().unwrap();
                                let seq_str = std::str::from_utf8(&seq).ok().unwrap();
                                Bytes::from(format!("{}-{}", ts_str, seq_str))
                            }
                            None => Bytes::from("0-0"),
                        }
                    } else {
                        Bytes::from("0-0")
                    }
                }
                _ => kv[i + 1].clone(),
            };

            let mut stream_entries: Vec<RedisValueRef> = Vec::new();

            if let Some(pos) = memchr(b'-', &stream_id) {
                let cur_ts = Bytes::copy_from_slice(&stream_id[..pos]);
                let cur_seq = Bytes::copy_from_slice(&stream_id[pos + 1..]);

                let cur_ts_str = std::str::from_utf8(&cur_ts).ok().unwrap();
                let cur_seq_str = std::str::from_utf8(&cur_seq).ok().unwrap();

                let cur_ts_int = cur_ts_str.parse::<u64>().unwrap();
                let cur_seq_int = cur_seq_str.parse::<u64>().unwrap();

                if let Some(stream) = streams.get(stream_key) {
                    for ((ts, seq), map) in stream.map.iter() {
                        let ts_str = std::str::from_utf8(ts).ok().unwrap();
                        let seq_str = std::str::from_utf8(seq).ok().unwrap();

                        let ts_int = ts_str.parse::<u64>().unwrap();
                        let seq_int = seq_str.parse::<u64>().unwrap();

                        // Check if this entry is AFTER the provided ID (exclusive)
                        if ts_int > cur_ts_int || (ts_int == cur_ts_int && seq_int > cur_seq_int) {
                            let result_id = format!("{}-{}", ts_str, seq_str);
                            let mut entry_array: Vec<RedisValueRef> = Vec::new();
                            entry_array.push(RedisValueRef::BulkString(Bytes::from(result_id)));

                            let mut kv_array: Vec<RedisValueRef> = Vec::new();
                            for (key, val) in map.iter() {
                                kv_array.push(RedisValueRef::BulkString(key.clone()));
                                kv_array.push(RedisValueRef::BulkString(val.clone()));
                            }
                            entry_array.push(RedisValueRef::Array(kv_array));

                            stream_entries.push(RedisValueRef::Array(entry_array));
                        }
                    }
                }
            }

            // Only add this stream to results if it has entries
            if !stream_entries.is_empty() {
                res.push(RedisValueRef::Array(vec![
                    RedisValueRef::BulkString(stream_key.clone()),
                    RedisValueRef::Array(stream_entries),
                ]));
            }
        }

        res
    }

    pub async fn blocking_xread(&self, kv: &Vec<Bytes>, duration: Duration) -> RedisValueRef {
        // First check if there's already data
        let res = self.xread(kv).await;
        if !res.is_empty() {
            return RedisValueRef::Array(res);
        }

        // No data yet, block and wait
        let (tx, rx) = oneshot::channel::<bool>();

        // Register for the first stream key
        let stream_key = kv[0].clone();
        {
            let mut blocked_clients = self.blocked.write().await;
            blocked_clients
                .entry(stream_key.clone())
                .or_default()
                .push_back(tx);
        }

        // Wait for notification or timeout
        match timeout(duration, rx).await {
            Ok(Ok(_)) => {
                // Got notified - check for new data
                let res = self.xread(kv).await;
                if res.is_empty() {
                    RedisValueRef::NullArray
                } else {
                    RedisValueRef::Array(res)
                }
            }
            Ok(Err(_)) => RedisValueRef::NullArray,
            Err(_) => {
                let mut blocked_clients = self.blocked.write().await;
                if let Some(notifiers) = blocked_clients.get_mut(&stream_key) {
                    notifiers.retain(|sender| !sender.is_closed());
                    if notifiers.is_empty() {
                        blocked_clients.remove(&stream_key);
                    }
                }
                RedisValueRef::NullArray
            }
        }
    }
}

pub fn current_unix_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
