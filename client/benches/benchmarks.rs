use client::*;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::runtime::Runtime;
const ADDRESS: &str = "127.0.0.1:8080";

fn client(c: &mut Criterion) {
    let mut group = c.benchmark_group("client");
    group.throughput(Throughput::Elements(1));
    // Creates client
    let rt = Runtime::new().unwrap();
    let mut client = rt.block_on(Client::new(ADDRESS));
    group.bench_function("sequential", |b| {
        b.iter(|| {
            rt.block_on(client.write(1, UserFilter(0)));
            let _: Person = rt.block_on(client.read());
        })
    });
}
criterion_group!(benches, client);
criterion_main!(benches);
