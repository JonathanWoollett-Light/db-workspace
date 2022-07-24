#![warn(clippy::pedantic)]
use std::collections::HashSet;

use bytes::{Buf, BytesMut};
use log::trace;
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

#[derive(Serialize)]
pub struct ConnectionsFilter {
    pub id: u64,
    pub n: u8,
}
#[derive(Serialize)]
pub struct UserFilter(pub Id);

pub struct Client {
    pub reader: FramedRead<OwnedReadHalf, MyDecoder>,
    pub writer: OwnedWriteHalf,
}
impl Client {
    pub async fn new(address: &str) -> Self {
        let stream = TcpStream::connect(address).await.unwrap();
        let (read, write) = stream.into_split();
        let reader = FramedRead::new(read, MyDecoder);
        Self {
            reader,
            writer: write,
        }
    }

    pub async fn write<T: Serialize>(&mut self, f: u32, x: T) {
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

    pub async fn read<T: DeserializeOwned>(&mut self) -> T {
        let s = self.reader.next().await.unwrap().unwrap();
        trace!("received: {:?}", s);
        bincode::deserialize(&s).unwrap()
    }
}

pub struct MyEncoder;
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
pub struct MyDecoder;
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
pub struct Person {
    pub name: String,
    pub friends: HashSet<Id>,
    pub posts: Vec<Id>,
    pub liked_posts: Vec<Id>,
}
pub type Id = u64;
