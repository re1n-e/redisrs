use crate::key_value::KeyValue;

pub struct Redis {
    pub kv: KeyValue,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
        }
    }
}
