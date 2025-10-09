use crate::key_value::KeyValue;
use crate::lists::List;
use crate::streams::Stream;

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
    pub stream: Stream,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
            stream: Stream::new(),
        }
    }
}
