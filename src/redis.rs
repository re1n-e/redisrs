use crate::key_value::KeyValue;
use crate::lists::List;

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
        }
    }
}
