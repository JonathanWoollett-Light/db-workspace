#![warn(clippy::pedantic)]
use std::{collections::HashSet, time::Instant};

use bytes::{Buf, BytesMut};
use log::{debug, info, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, FramedRead};
const ADDRESS: &str = "127.0.0.1:8080";

#[derive(Serialize)]
struct ConnectionsFilter {
    id: u64,
    n: u8,
}

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
    info!("running client...");

    // Creates client
    let mut client = Client::new(ADDRESS).await;

    // Writes query calling function 0 with input `ConnectionsFilter { ... }`
    let now = Instant::now();
    client.write(0, ConnectionsFilter { id: 0, n: 0 }).await;
    // Reads return value
    let connections: Vec<HashSet<u64>> = client.read().await;
    info!("{:?}", now.elapsed());
    info!("connections.len(): {}", connections.len());
    info!(
        "connections.iter().map(|c|c.len()).collect::<Vec<_>>(): {:?}",
        connections.iter().map(HashSet::len).collect::<Vec<_>>()
    );
    info!(
        "connections.iter().map(|c|c.len()).sum::<usize>(): {}",
        connections.iter().map(HashSet::len).sum::<usize>()
    );
    debug!("connections: {:?}", connections);

    // Writes query calling function 0 with input `ConnectionsFilter { ... }`
    let now = Instant::now();
    client.write(0, ConnectionsFilter { id: 400, n: 2 }).await;
    // Reads return value
    let connections: Vec<HashSet<u64>> = client.read().await;
    info!("{:?}", now.elapsed());
    info!("connections.len(): {}", connections.len());
    info!(
        "connections.iter().map(|c|c.len()).collect::<Vec<_>>(): {:?}",
        connections.iter().map(HashSet::len).collect::<Vec<_>>()
    );
    info!(
        "connections.iter().map(|c|c.len()).sum::<usize>(): {}",
        connections.iter().map(HashSet::len).sum::<usize>()
    );
    debug!("connections: {:?}", connections);
}

struct Client {
    reader: FramedRead<OwnedReadHalf, MyDecoder>,
    writer: OwnedWriteHalf,
}
impl Client {
    async fn new(address: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let (read, write) = stream.into_split();
        let reader = FramedRead::new(read, MyDecoder);
        Self {
            reader,
            writer: write,
        }
    }

    async fn write<T: Serialize>(&mut self, f: u32, x: T) {
        let bytes = bincode::serialize(&x).unwrap();
        let buffer = {
            let mut b = Vec::with_capacity(8 + bytes.len());
            b.extend_from_slice(&u32::to_le_bytes(u32::try_from(bytes.len()).unwrap()));
            b.extend_from_slice(&u32::to_le_bytes(f));
            b.extend_from_slice(&bytes);
            b
        };
        trace!("sent: {:?}", buffer);
        self.writer.write_all(&buffer).await.unwrap();
    }

    async fn read<T: DeserializeOwned>(&mut self) -> T {
        let s = self.reader.next().await.unwrap().unwrap();
        trace!("received: {:?}", s);
        bincode::deserialize(&s).unwrap()
    }
}

struct MyEncoder;
impl Encoder<String> for MyEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, item: String, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let length = u32::to_le_bytes(u32::try_from(item.len()).unwrap());
        dst.reserve(4 + item.len());
        dst.extend_from_slice(&length);
        dst.extend_from_slice(item.as_bytes());
        Ok(())
    }
}
struct MyDecoder;
impl Decoder for MyDecoder {
    type Error = std::io::Error;
    type Item = Vec<u8>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // We use u32 for len marker, so need an initial 4 bytes
        if src.len() < 4 {
            return Ok(None);
        }
        // Get length
        let length = {
            let mut length_bytes = [0; 4];
            length_bytes.copy_from_slice(&src[..4]);
            u32::from_le_bytes(length_bytes) as usize
        };
        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }
        // Read data
        let data = src[4..4 + length].to_vec();
        // Advance buffer to discard data
        src.advance(4 + length);
        Ok(Some(data))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    friends: HashSet<Id>,
    posts: Vec<Id>,
    liked_posts: Vec<Id>,
}
type Id = u64;
