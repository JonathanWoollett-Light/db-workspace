#![feature(iter_intersperse)]
#![warn(clippy::pedantic)]
//! Test server database server covering a social network.
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    mem,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use bytes::{Buf, BytesMut};
use clap::Parser;
use indicatif::ProgressBar;
use log::{debug, info, trace};
#[cfg(any(metrics = "minimal", metrics = "full"))]
use metrics::{histogram, increment_counter};
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    Rng,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    select,
    sync::{Mutex, RwLock},
    time::sleep,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// When saving, do we:
    /// - `true`: acquire the read lock, clone, drop the lock, serialize from clone. Faster,
    ///   requires 2x memory of dataset to be present in system.
    /// - `false`: acquire the lock, serialize from data, drop the lock. Slower, requires no
    ///   additional memory.
    #[clap(long, default_value_t = true)]
    pub clone_save: bool,
    /// Save interval
    #[clap(long, default_value_t = 60)]
    pub save_interval: u64,
}

/// The address we spawn the server at.
const ADDRESS: &str = "127.0.0.1:8080";

const TRANSFER_ADDRESS: &str = "127.0.0.1:8081";

/// Counter we use to generate unique id's locally for our test dataset.
static UNIQUE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

lazy_static::lazy_static! {
    /// Our metrics recorder
    static ref RECORDER: recorder::MyRecorder = recorder::MyRecorder::new();
}

/// Type alias for async function pointer
type AsyncFn = fn(
    Arc<RwLock<Database>>,
    Vec<u8>,
) -> Pin<Box<dyn std::future::Future<Output = Vec<u8>> + Send>>;
/// We have N query function.
const N: u32 = 3;
// static TEST_FUNCTION: AsyncFn = |a: Vec<u8>| Box::pin(async { Vec::new()});
/// Server query functions
static FUNCTIONS: [AsyncFn; N as usize] = [
    // Gets users connections 1st, 2nd, 3rd (depending on n where 0=1st, 1=2nd, etc.)
    |data: Arc<RwLock<Database>>, bytes: Vec<u8>| {
        Box::pin(async move {
            // Increments function call count
            #[cfg(any(metrics = "minimal", metrics = "full"))]
            increment_counter!("f1");

            // Define input
            #[derive(Deserialize)]
            struct Filter {
                id: u64,
                n: u8,
            }
            let filter: Filter = bincode::deserialize(&bytes).unwrap();
            // Measures time between deserialization of input and serialization of output.
            #[cfg(metrics = "full")]
            let now = Instant::now();

            let guard = data.read().await;
            let user = guard.people.get(&filter.id).unwrap();
            let mut overall = user.friends.clone();
            let mut connections = vec![user.friends.clone()];
            for i in 0..filter.n as usize {
                let mut new_connections = HashSet::new();
                for link in &connections[i] {
                    let link = guard.people.get(link).unwrap();
                    for friend in &link.friends {
                        // if this friend not already present in overall
                        if overall.insert(*friend) {
                            new_connections.insert(*friend);
                        }
                    }
                }
                connections.push(new_connections);
            }
            // Records computation time
            #[cfg(metrics = "full")]
            histogram!("f1", now.elapsed());
            bincode::serialize(&connections).unwrap()
        })
    },
    // Gets user data by id
    |data: Arc<RwLock<Database>>, bytes: Vec<u8>| {
        Box::pin(async move {
            // Increments function call count
            #[cfg(any(metrics = "minimal", metrics = "full"))]
            increment_counter!("f2");
            // Define input
            #[derive(Deserialize)]
            struct Filter(Id);
            let filter: Filter = bincode::deserialize(&bytes).unwrap();

            // Measures time between deserialization of input and serialization of output.
            #[cfg(metrics = "full")]
            let now = Instant::now();

            let guard = data.read().await;
            let id = guard.people.get(&filter.0).unwrap();
            // Records computation time
            #[cfg(metrics = "full")]
            histogram!("f2", now.elapsed());
            bincode::serialize(&id).unwrap()
        })
    },
    // Gets `x`
    |data: Arc<RwLock<Database>>, _: Vec<u8>| {
        Box::pin(async move {
            let guard = data.read().await;
            bincode::serialize(&guard.x).unwrap()
        })
    },
];

/// The module containing our structure for recording metrics.
mod recorder;
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExistingDatabase {
    x: u8,
    people: HashMap<Id, Person>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Database {
    x: u8,
    // User Ids -> User data
    people: HashMap<Id, Person>,
}
impl Database {
    /// Each user has 300 friends.
    const NUMBER_OF_FRIENDS: usize = 10;
    /// 1 million users.
    const SIZE: usize = 10_000;
}
impl From<ExistingDatabase> for Database {
    fn from(existing: ExistingDatabase) -> Self {
        Self {
            x: existing.x,
            people: existing.people,
        }
    }
}
impl Default for Database {
    fn default() -> Self {
        const STEP: usize = 1000;
        const SAMPLE: usize = 100;
        let now = Instant::now();
        // {
        //     let mut file = File::open("./store").unwrap();
        //     let mut buffer = Vec::new();
        //     file.read_to_end(&mut buffer).unwrap();
        //     let db = bincode::deserialize(&buffer).unwrap();
        //     println!("setup database: {:?}",now.elapsed());
        //     return Arc::new(RwLock::new(db));
        // }

        let mut rng = rand::thread_rng();

        // We generate `SIZE` users.
        let mut people: HashMap<Id, Person> = {
            info!("creating users...");
            let bar = ProgressBar::new(Database::SIZE as u64);
            let people: HashMap<Id, Person> = (0..Database::SIZE)
                .map(|i| {
                    if i % STEP == 0 && i != 0 {
                        bar.inc(STEP as u64);
                        if i % (10000 * STEP) == 0 {
                            bar.println(format!("eta: {:.3?}", bar.eta()));
                        }
                    }
                    (i as u64, rng.gen())
                })
                .collect();
            bar.finish();
            // Update unique id counter to include all newly generated users
            UNIQUE_ID_COUNTER.fetch_add(Database::SIZE as u64, std::sync::atomic::Ordering::SeqCst);
            people
        };

        // Print a sample of users
        info!(
            "sample: [{}]",
            (0..SAMPLE)
                .map(|i| people.get(&(i as u64)).unwrap().name.clone())
                .intersperse(String::from(" "))
                .collect::<String>()
        );

        // For each user, we assign them `NUMBER_OF_FRIENDS` friends.
        info!("filling dataset...");
        let bar = ProgressBar::new(Database::SIZE as u64);
        for i in 0..Database::SIZE as u64 {
            // For each person create NUMBER_OF_FRIENDS semi-random friends
            {
                // We skip past this user
                let mut iter = (0..Database::SIZE as u64)
                    .cycle()
                    .skip(usize::try_from(i).unwrap());
                // The maximum step here needs to be small enough such that it never loops back
                // around and introduces duplicate friends.
                //
                // It is guaranteed that
                // `NUMBER_OF_FRIENDS * rng.gen_range(1..SIZE/NUMBER_OF_FRIENDS) < SIZE` since
                // `NUMBER_OF_FRIENDS * SIZE/NUMBER_OF_FRIENDS < SIZE`.
                let friends = (0..Database::NUMBER_OF_FRIENDS)
                    .map(|_| {
                        iter.nth(rng.gen_range(1..Database::SIZE / Database::NUMBER_OF_FRIENDS))
                            .unwrap()
                    })
                    .collect::<HashSet<_>>();
                people.get_mut(&i).unwrap().friends = friends;
                // This is way too slow, so we use the above code instead.
                // people.get_mut(&i).unwrap().friends = (0..SIZE as u64).choose_multiple(&mut rng,
                // NUMBER_OF_FRIENDS);
            }
            // print!("\\");

            if i % STEP as u64 == 0 && i != 0 {
                bar.inc(STEP as u64);
                if i % (10000 * STEP as u64) == 0 {
                    bar.println(format!("eta: {:.3?}", bar.eta()));
                }
            }
        }
        bar.finish();

        info!("setup database: {:?}", now.elapsed());

        Database { x: 2u8, people }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    friends: HashSet<Id>,
}
impl Person {
    const NAME_LEN: usize = 3;
}

impl Distribution<Person> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Person {
        Person {
            name: rng
                .sample_iter(&Alphanumeric)
                .take(Person::NAME_LEN)
                .map(char::from)
                .collect(),
            friends: HashSet::new(),
        }
    }
}
type Id = u64;
use tokio::sync::{broadcast, mpsc};

#[tokio::main]
async fn main() {
    std::env::set_var("METRICS", "minimal");
    std::env::set_var("RUST_LOG", "info");

    // Logging can have a huge impact on performance with low complexity high throughput queries.
    simple_logger::SimpleLogger::new().env().init().unwrap();
    #[cfg(any(metrics = "minimal", metrics = "full"))]
    metrics::set_recorder(&*RECORDER).unwrap();
    trace!("testing this>");
    let args = Args::parse();

    // We check if an existing server is running by attempting to connection to TRANSFER_ADDRESS
    let data = if let Ok(existing) = TcpStream::connect(TRANSFER_ADDRESS).await {
        // As soon as this process connects the existing process sends the database data across the
        // stream, we then want to read this data.
        let (read, mut write) = existing.into_split();
        let mut reader = FramedRead::new(read, MyDataDecoder);
        let existing_data = reader.next().await.unwrap().unwrap();

        // y: Do this concurrently with x
        let existing_database: ExistingDatabase = bincode::deserialize(&existing_data).unwrap();
        let data = Arc::new(RwLock::new(Database::from(existing_database)));

        // x: Do this concurrently with y
        // Following sending the database the old server will gradually send the pointers to the TCP
        // client streams across
        loop {
            let stream_ptr_buf = reader.next().await.unwrap().unwrap();
            let stream_ptr = usize::from_ne_bytes(stream_ptr_buf.try_into().unwrap());
            // After sending the last stream ptr it will send 0 to indicate the end of the stream.
            if stream_ptr == 0 {
                break;
            }

            let stream_ptr = stream_ptr as *const TcpStream;
            let stream_arc = unsafe { Arc::from_raw(stream_ptr) };
        }
        data
    } else {
        Arc::new(RwLock::new(Database::default()))
    };

    info!("running...");
    let listener = TcpListener::bind(ADDRESS).await.unwrap();
    let (end_stream_sender, _) = broadcast::channel(16);
    let (stream_channel_sender, stream_channel_receiver) = mpsc::channel::<usize>(100);

    let save_data = data.clone();
    tokio::spawn(async move {
        save(
            save_data,
            Duration::from_secs(args.save_interval),
            args.clone_save,
        )
        .await
    });
    let transfer_data = data.clone();
    let transfer_end_stream_sender = end_stream_sender.clone();
    tokio::spawn(async move {
        transfer(
            transfer_data,
            transfer_end_stream_sender,
            stream_channel_receiver,
        )
        .await
    });
    loop {
        let (stream, socket) = listener.accept().await.unwrap();
        info!("new client: {}", socket);

        let process_data = data.clone();
        let process_end_stream_receiver = end_stream_sender.subscribe();
        let process_stream_channel_sender = stream_channel_sender.clone();
        tokio::spawn(async move {
            process(
                process_data,
                stream,
                process_end_stream_receiver,
                process_stream_channel_sender,
            )
            .await
        });
    }
}

mod my_metrics {
    pub const SAVE_LOCKED: &str = "save: locked";
    pub const SAVE_UNLOCKED: &str = "save: unlocked";
    pub const SAVE_SERIALIZED: &str = "save: serialized";
    pub const SAVE_SAVED: &str = "save: saved";
}

/// Awaits the command to transfer the entire database through the transfer channel.
///
/// Used for updating the server. A read lock can be held on the database while it is transferred to
/// the new server process.
async fn transfer(
    data: Arc<RwLock<Database>>,
    end_stream_sender: broadcast::Sender<()>,
    mut stream_channel_receiver: mpsc::Receiver<usize>,
) {
    let listener = TcpListener::bind(TRANSFER_ADDRESS).await.unwrap();
    // When we receive the first connection we send the database data
    let (stream, _socket) = listener.accept().await.unwrap();
    // Splits our stream into read/write
    let (read, mut write) = stream.into_split();

    // Write the all of `data` to the stream
    let guard = data.read().await;
    let vec_data = bincode::serialize(&*guard).unwrap();
    write_fn(&mut write, vec_data).await;

    // Sends signal to begin transferring tcp streams
    end_stream_sender.send(()).unwrap();
    // Await receiving all streams from tasks then forgets each stream and sends their address to
    // the new process.
    while let Some(stream) = stream_channel_receiver.recv().await {
        // When we receive the TcpStream from the thread we pass it through to the other process
        let vec_data = bincode::serialize(&stream).unwrap();
        write_fn(&mut write, vec_data).await;
    }

    // Considering the new process may need to spend some time mutating the database before being
    // ready we await a message before ending this process.
    let mut reader = FramedRead::new(read, TransferDecoder);
    // A `true` response indicate the new process did not encounter an error setting up, at this
    // point the new process is ready to take over.
    // At this point this process needs to await
    if let Some(Ok(0)) = reader.next().await {}
}

async fn save(data: Arc<RwLock<Database>>, interval: Duration, clone_save: bool) {
    loop {
        info!("save: started");
        #[cfg(any(metrics = "minimal", metrics = "full"))]
        let now = Instant::now();

        // Acquires read lock
        let guard = {
            let guard = loop {
                if let Ok(g) = data.try_read() {
                    break g;
                }
            };
            info!("{}", my_metrics::SAVE_LOCKED);
            #[cfg(any(metrics = "minimal", metrics = "full"))]
            histogram!(my_metrics::SAVE_LOCKED, now.elapsed());
            guard
        };

        // Serializes data
        let serialized = {
            let serialized = if clone_save {
                let clone = guard.clone();
                drop(guard);
                info!("{}", my_metrics::SAVE_UNLOCKED);
                #[cfg(any(metrics = "minimal", metrics = "full"))]
                histogram!(my_metrics::SAVE_UNLOCKED, now.elapsed());
                bincode::serialize(&clone).unwrap()
            } else {
                let s = bincode::serialize(&*guard).unwrap();
                drop(guard);
                info!("{}", my_metrics::SAVE_UNLOCKED);
                #[cfg(any(metrics = "minimal", metrics = "full"))]
                histogram!(my_metrics::SAVE_UNLOCKED, now.elapsed());
                s
            };
            info!("{}", my_metrics::SAVE_SERIALIZED);
            #[cfg(any(metrics = "minimal", metrics = "full"))]
            histogram!(my_metrics::SAVE_SERIALIZED, now.elapsed());
            serialized
        };
        // Saves data
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open("./store")
                .unwrap();
            file.write_all(&serialized).unwrap();

            info!("{}", my_metrics::SAVE_SAVED);
            #[cfg(any(metrics = "minimal", metrics = "full"))]
            histogram!(my_metrics::SAVE_SAVED, now.elapsed());
        }
        info!("save: finished");
        // Print metrics every save.
        info!("metrics: {:?}", &*RECORDER);
        // Sleep for interval between saves.
        sleep(interval).await;
    }
}

/// When a stream is connected we call `process` on it
async fn process(
    data: Arc<RwLock<Database>>,
    stream: TcpStream,
    mut end_stream_receiver: broadcast::Receiver<()>,
    stream_channel_sender: mpsc::Sender<usize>,
) {
    // We split the bi-directional stream into its read and write halves.
    let (read, mut write) = stream.into_split();
    // From the read stream we construct a framed reader (this produces frames we define by our
    // implementation of `Decoder` on `MyDecoder`).
    let mut reader = FramedRead::new(read, MyDecoder);
    // We await for a frame to be read from our reader.
    // while let Some(next) = reader.next().await {
    loop {
        select! {
            next_opt = reader.next() => {
                let next = next_opt.unwrap();
                // We log the received frame option.
                trace!("received: {:?}", next);
                // We unwrap the frame (this depends on whether the bytes read from the stream could be
                // converted into our defined frame).
                let (f, x) = next.unwrap();
                // We assert the function index given in the frame is within the range of our defined
                // function.
                assert!(f < N, "Function index out of range");
                // We call the function for the given function index with the given byte input.
                let res = FUNCTIONS[f as usize](data.clone(),x).await;
                // We write the result to our write stream.
                write_fn(&mut write, res).await;
                debug!("sent");
            },
            _ = end_stream_receiver.recv() => break
        }
    }
    let stream = reader.into_inner().reunite(write).unwrap();
    let stream_ptr = &stream as *const TcpStream;
    let stream_addr = stream_ptr as usize;
    drop(stream_ptr);
    mem::forget(stream);
    stream_channel_sender.send(stream_addr).await.unwrap();
}

/// A simple function to write a byte slice to our write stream in our format (where the length is
/// at the front).
async fn write_fn(writer: &mut OwnedWriteHalf, string: Vec<u8>) {
    let buffer = {
        let mut b = Vec::with_capacity(4 + string.len());
        b.extend_from_slice(&u32::to_le_bytes(u32::try_from(string.len()).unwrap()));
        b.extend_from_slice(&string);
        b
    };
    trace!("sending: {:?}", buffer);
    writer.write_all(&buffer).await.unwrap();
}

/// Decoder used for receiving client data
struct MyDecoder;
/// We implement `tokio_util::codec::decoder` to allow framed reads.
impl Decoder for MyDecoder {
    type Error = std::io::Error;
    // Our frame type, consisting of a tuple of a u32 representing the function index and a byte
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

/// A simple decoder with no internal state for process transfer
struct TransferDecoder;
/// We implement `tokio_util::codec::decoder` to allow framed reads.
impl Decoder for TransferDecoder {
    type Error = std::io::Error;
    // Our frame type, consisting of a tuple of a u32 representing the function index and a byte
    // vector.
    type Item = u8;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // We use an u32 for denoting the length of a message. So before we continue we need to have
        // received 4 bytes.
        if src.len() == 1 {
            let x = src[0];
            src.advance(1);
            return Ok(Some(x));
        } else {
            Ok(None)
        }
    }
}

/// A simple decoder with no internal state
struct MyDataDecoder;
/// We implement `tokio_util::codec::decoder` to allow framed reads.
impl Decoder for MyDataDecoder {
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
