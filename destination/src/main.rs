use std::sync::Arc;

#[database]
struct Database {
    x: u32,
    y: String,
}
impl Database {
    fn add(&self) -> String {
        self.y.repeat(self.x as usize)
    }
}
#[query]
fn add(data: Arc<(u64, u64)>) -> u64 {
    *data
}

trait Datastore {
    type Data;
}

fn main() {
    println!("Hello, world!");
}
