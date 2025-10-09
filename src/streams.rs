use bytes::Bytes;
use memchr::memchr;
use std::collections::{BTreeMap, HashMap};
use tokio::sync::RwLock;
pub struct StreamKV {
    // sequence num - time -> map (key , value)
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

    pub async fn xadd(&self, stream_key: Bytes, stream_id: Bytes, kv: Vec<Bytes>) -> Bytes {
        if let Some(pos) = memchr(b'-', &stream_id) {
            let ts = Bytes::copy_from_slice(&stream_id[..pos]);
            let seq = Bytes::copy_from_slice(&stream_id[pos + 1..]);

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
        stream_id
    }

    pub async fn contains(&self, stream_key: &Bytes) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(stream_key)
    }
}
