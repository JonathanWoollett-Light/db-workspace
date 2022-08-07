#![warn(clippy::pedantic)]
use std::time::Instant;

use log::{debug, info};
const ADDRESS: &str = "127.0.0.1:8080";
use std::{thread, time::Duration};

use client::*;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget};
use tokio::task;

#[tokio::main]
async fn main() {
    // let test = ();
    // let bytes = bincode::serialize(&test).unwrap();
    // println!("bytes: {:?}",bytes);

    // panic!("stop");

    let now = Instant::now();
    // const SAMPLES: u64 = 100_000;
    const SAMPLES: u64 = 1_000_000;
    const STEP: u64 = 100;
    const CLIENTS: u64 = 5;
    let multi_bar = MultiProgress::new();
    // multi_bar.set_draw_target(ProgressDrawTarget::stdout());
    let handles = (0..CLIENTS)
        .map(|_| {
            let bar = multi_bar.add(ProgressBar::new(SAMPLES));
            task::spawn(async move {
                let mut client = Client::new(ADDRESS).await;
                for j in 0..SAMPLES {
                    client.write(0, ()).await;
                    // println!("here? 1");
                    let _: u8 = client.read().await;
                    // println!("here? 2");
                    if j % STEP == 0 && j != 0 {
                        // println!("here?");
                        // println!("bar: {}",bar.is_hidden());
                        bar.inc(STEP);
                    }
                }
                bar.finish();
            })
        })
        .collect::<Vec<_>>();
    for handle in handles.into_iter() {
        handle.await.unwrap();
    }
    multi_bar.clear().unwrap();
    println!("elapsed: {:?}", now.elapsed());
    println!(
        "throughput: {:.3?}/s",
        (CLIENTS * SAMPLES) as f32 / now.elapsed().as_secs() as f32
    );
}
// #[tokio::main]
// async fn main() {
//     simple_logger::SimpleLogger::new()
//         .with_level(log::LevelFilter::Info)
//         .init()
//         .unwrap();
//     info!("running client...");

//     // Creates client
//     let mut client = Client::new(ADDRESS).await;

//     // Writes query calling function 0 with input `ConnectionsFilter { ... }`
//     let now = Instant::now();
//     client.write(0, ConnectionsFilter { id: 0, n: 0 }).await;
//     // Reads return value
//     let connections: Vec<HashSet<u64>> = client.read().await;
//     info!("{:?}", now.elapsed());
//     info!("connections.len(): {}", connections.len());
//     info!(
//         "connections.iter().map(|c|c.len()).collect::<Vec<_>>(): {:?}",
//         connections.iter().map(HashSet::len).collect::<Vec<_>>()
//     );
//     info!(
//         "connections.iter().map(|c|c.len()).sum::<usize>(): {}",
//         connections.iter().map(HashSet::len).sum::<usize>()
//     );
//     debug!("connections: {:?}", connections);

//     // Writes query calling function 0 with input `ConnectionsFilter { ... }`
//     let now = Instant::now();
//     client.write(0, ConnectionsFilter { id: 400, n: 2 }).await;
//     // Reads return value
//     let connections: Vec<HashSet<u64>> = client.read().await;
//     info!("{:?}", now.elapsed());
//     info!("connections.len(): {}", connections.len());
//     info!(
//         "connections.iter().map(|c|c.len()).collect::<Vec<_>>(): {:?}",
//         connections.iter().map(HashSet::len).collect::<Vec<_>>()
//     );
//     info!(
//         "connections.iter().map(|c|c.len()).sum::<usize>(): {}",
//         connections.iter().map(HashSet::len).sum::<usize>()
//     );
//     debug!("connections: {:?}", connections);
// }
