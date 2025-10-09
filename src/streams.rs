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

            match self.validate_key(&ts, &seq).await {
                Some(s) => return RedisValueRef::Error(Bytes::from(s)),
                None => (),
            }

            let mut streams = self.streams.write().await;
            streams.entry(stream_key.clone()).or_insert(StreamKV::new());
            if let Some(stream) = streams.get_mut(&stream_key) {
                stream
                    .map
                    .entry((ts.clone(), seq.clone()))
                    .or_insert(BTreeMap::new());
                if let Some(map) = stream.map.get_mut(&(ts, seq)) {
                    for i in (0..kv.len()).step_by(2) {
                        map.insert(kv[i].clone(), kv[i + 1].clone());
                    }
                }
            }
        }
        RedisValueRef::BulkString(stream_id)
    }

    pub async fn contains(&self, stream_key: &Bytes) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_key)
    }

    async fn validate_key(&self, ts: &Bytes, seq: &Bytes) -> Option<String> {
        let streams = self.streams.read().await;

        let ts_str = std::str::from_utf8(ts).ok()?;
        let seq_str = std::str::from_utf8(seq).ok()?;

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

        for stream_kv in streams.values() {
            if let Some(((last_ts, last_seq), _)) = stream_kv.map.last_key_value() {
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
}
