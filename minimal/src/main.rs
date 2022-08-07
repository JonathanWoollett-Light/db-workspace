#![feature(ptr_const_cast)]
#![feature(box_into_inner)]
#![warn(clippy::pedantic)]
use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use std::{mem, pin::Pin, sync::Arc};
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    select,
    sync::{Notify, RwLock},
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};
use log::info;

/// Address to communicate with clients.
const ADDRESS: &str = "127.0.0.1:8080";
/// Address to communicate with new server process for live updating.
const TRANSFER_ADDRESS: &str = "127.0.0.1:8081";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PreexistingDatabase {
    x: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Database {
    x: u8,
}
impl From<PreexistingDatabase> for Database {
    fn from(other: PreexistingDatabase) -> Self {
        Self { x: other.x }
    }
}
impl Default for Database {
    fn default() -> Self {
        Self { x: 2 }
    }
}

/// Type alias for async function pointer
type AsyncFn = fn(
    Arc<RwLock<Database>>,
    Vec<u8>,
) -> Pin<Box<dyn std::future::Future<Output = Vec<u8>> + Send>>;
/// We have N query function.
const N: u32 = 1;
/// Server query functions
static FUNCTIONS: [AsyncFn; N as usize] = [
    // Gets `x`
    |data: Arc<RwLock<Database>>, _: Vec<u8>| {
        Box::pin(async move {
            let guard = data.read().await;
            bincode::serialize(&guard.x).unwrap()
        })
    },
];

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new().with_level(log::LevelFilter::Info).init().unwrap();
    info!("started");
    let mut task_join_handles = Vec::new();
    let update_notification = Arc::new(Notify::new());

    // If there is an existing process bound to `TRANSFER_ADDRESS`.
    let data = if let Ok(existing_server_process_listener) =
        TcpStream::connect(TRANSFER_ADDRESS).await
    {
        info!("existing construction");
        // Receive data
        let (read, _write) = existing_server_process_listener.into_split();
        let mut reader = FramedRead::new(read, DatabaseTransferDecoder);
        info!("awaiting data");
        let existing_serialized_data = reader.next().await.unwrap().unwrap();
        info!("received data");
        let existing_data =
            bincode::deserialize::<PreexistingDatabase>(&existing_serialized_data).unwrap();
        let data = Arc::new(RwLock::new(Database::from(existing_data)));
        

        // Receive all tcp streams
        loop {
            info!("awaiting stream");
            let stream_ptr_buf = reader.next().await.unwrap().unwrap();
            info!("received stream");
            let stream_addr = bincode::deserialize::<usize>(&stream_ptr_buf).unwrap();
            // After sending the last stream ptr it will send 0 to indicate the end of the stream.
            if stream_addr == 0 {
                break;
            }
            // Gets the stream
            let stream_ptr = stream_addr as *const TcpStream;
            let stream_mut_ptr = stream_ptr.as_mut();
            let stream_box = unsafe { Box::from_raw(stream_mut_ptr) };
            let stream = Box::into_inner(stream_box);
            // Re-spawn `TcpStream` task
            let notification_clone = update_notification.clone();
            let data_clone = data.clone();
            let handle =
                tokio::spawn(async move { process(stream, notification_clone, data_clone).await });
            task_join_handles.push(handle);
        }

        data
    } else {
        info!("fresh construction");
        Arc::new(RwLock::new(Database::default()))
    };
    info!("constructed");

    let client_listener = TcpListener::bind(ADDRESS).await.unwrap();
    let update_listener = TcpListener::bind(TRANSFER_ADDRESS).await.unwrap();

    let res = loop {
        info!("awaiting connection");
        select! {
            Ok((stream, _socket)) = client_listener.accept() => {
                info!("received connection");
                let notification_clone = update_notification.clone();
                let data_clone = data.clone();
                let handle = tokio::spawn(async move {
                    process(stream,notification_clone,data_clone).await
                });
                task_join_handles.push(handle);
            },
            res = update_listener.accept() => { break res; }
        }
    };
    info!("received notification");

    let (stream, _socket) = res.unwrap();
    // Splits our stream into read/write
    let (_read, mut write) = stream.into_split();

    // Sends database across stream
    let guard = data.read().await;
    let data_serialized = bincode::serialize(&*guard).unwrap();
    info!("sending data");
    write_fn(&mut write, &data_serialized).await;
    info!("sent data");

    // Sends signal to threads to begin transferring `TcpStream`s.
    update_notification.notify_waiters();
    info!("sent notification");

    // We await each task returning its channel
    for handle in task_join_handles {
        info!("awaiting task");
        let stream = handle.await.unwrap();
        info!("joined task");
        // we get the address to our stream
        let stream_ptr = std::ptr::addr_of!(stream);
        let stream_addr = stream_ptr as usize;
        let serialized_stream = bincode::serialize(&stream_addr).unwrap();
        // We forget the stream
        mem::forget(stream);
        // We pass the address to the new process
        write_fn(&mut write, &serialized_stream).await;
    }
    
    let terminator = bincode::serialize(&0usize).unwrap();
    info!("sending terminator");
    write_fn(&mut write, &terminator).await;
    info!("sent terminator");
}

/// A simple function to write a byte slice to our write stream in our format (where the length is
/// at the front).
async fn write_fn(writer: &mut OwnedWriteHalf, string: &[u8]) {
    let buffer = {
        let mut b = Vec::with_capacity(4 + string.len());
        b.extend_from_slice(&u32::to_le_bytes(u32::try_from(string.len()).unwrap()));
        b.extend_from_slice(string);
        b
    };
    writer.write_all(&buffer).await.unwrap();
}

async fn process(stream: TcpStream, update: Arc<Notify>, data: Arc<RwLock<Database>>) -> TcpStream {
    // We split the bi-directional stream into its read and write halves.
    let (read, mut write) = stream.into_split();
    // From the read stream we construct a framed reader (this produces frames we define by our
    // implementation of `Decoder` on `MyDecoder`).
    let mut reader = FramedRead::new(read, MyDecoder);
    // We await for a frame to be read from our reader.
    // while let Some(next) = reader.next().await {
    loop {
        select! {
            // On receiving next input process it
            Some(next) = reader.next() => {
                // We unwrap the frame (this depends on whether the bytes read from the stream could
                // be converted into our defined frame).
                let (f, x) = next.unwrap();
                // We assert the function index given in the frame is within the range of our
                // defined function.
                assert!(f < N, "Function index out of range");
                // We call the function for the given function index with the given byte input.
                let res = FUNCTIONS[f as usize](data.clone(),x).await;
                // We write the result to our write stream.
                write_fn(&mut write, &res).await;
            },
            // On receiving signal exit loop
            _ = update.notified() => break
        }
    }
    reader.into_inner().reunite(write).unwrap()
}

/// Decoder used for receiving client data
struct MyDecoder;
/// We implement `tokio_util::codec::decoder` to allow framed reads.
impl Decoder for MyDecoder {
    type Error = std::io::Error;
    // Our frame type, consisting of a tuple of a u32 (representing the function index) and a byte
    // vector.
    type Item = (u32, Vec<u8>);

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // We use an u32 for denoting the length of a message. So before we continue we need to have
        // received 4 bytes.
        if src.len() < 4 {
            // Since we have not received a full frame we return `Ok(None)`.
            return Ok(None);
        }
        // We get our u32 representing the length in bytes of the message.
        let length = {
            let mut length_bytes = [0; 4];
            length_bytes.copy_from_slice(&src[..4]);
            u32::from_le_bytes(length_bytes) as usize
        };
        // When we have received the number of incoming bytes within this frame we need to reserve
        // the space to hold these bytes.
        // As the data we pass for each friend is formed of `(len: u32, fn_index: u32, bytes: [u8])`
        // we need to check if our `src` can contains 4 bytes for our `len`, 4 bytes for our
        // `fn_index` and `len` bytes for `bytes`. If the `src` is less than this we need to reserve
        // this additional space.
        // TODO Could this not be `src.capacity()` then also include the code in the below if?
        if src.len() < 8 + length {
            // We reserve the additional space
            src.reserve(4 + length - src.len());
            // Since we have not received a full frame we return `Ok(None)`.
            return Ok(None);
        }
        // // When our capacity is enough to contain the incoming frame but it is not currently all
        // // read.
        // if src.len() < 8 + length {
        //     // Since we have not received a full frame we return `Ok(None)`.
        //     return Ok(None);
        // }

        // Reads function index
        let index = {
            let mut i = [0; 4];
            i.copy_from_slice(&src[4..8]);
            u32::from_le_bytes(i)
        };
        // Reads data
        let read_data = src[8..8 + length].to_vec();

        // Advance buffer to discard old data
        src.advance(8 + length);
        // Since we have not received a full frame we return `Ok(None)`.
        Ok(Some((index, read_data)))
    }
}

/// Decoder used to receive database data from existing server process.
struct DatabaseTransferDecoder;
/// Implement `tokio_util::codec::decoder` for framed reads.
impl Decoder for DatabaseTransferDecoder {
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
