pub struct Rdb {
    dir: String,
    dbfilename: String,
}

impl Rdb {
    pub fn new(dir: String, dbfilename: String) -> Self {
        Rdb { dir, dbfilename }
    }
}
