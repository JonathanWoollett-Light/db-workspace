#![feature(iter_intersperse)]
#![warn(clippy::pedantic)]
//! Test server database server covering a social network.
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io::Write,
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use bytes::{Buf, BytesMut};
use clap::Parser;
use indicatif::ProgressBar;
use log::{info, trace};
use metrics::histogram;
use rand::{
    distributions::{Alphanumeric, Distribution, Standard},
    seq::IteratorRandom,
    Rng,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    net::{tcp::OwnedWriteHalf, TcpListener, TcpStream},
    sync::RwLock,
    time::sleep,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};

/// The address we spawn the server at.
const ADDRESS: &str = "127.0.0.1:8080";


const SAVE_INTERVAL: Duration = Duration::from_secs(30);

const SPLIT_INTERVAL: Duration = Duration::from_secs(30);

/// Counter we use to generate unique id's locally for our test dataset.
static UNIQUE_ID_COUNTER: AtomicU64 = AtomicU64::new(0);

lazy_static::lazy_static! {
    static ref SPLIT: bool = {
        let args = Args::parse();
        info!("args: {:?}", args);
        args.split
    };
    /// Our metrics recorder
    static ref RECORDER: recorder::MyRecorder = recorder::MyRecorder::new();
    /// Our database
    ///
    /// For testing we simulate a social network,
    static ref DATA: Arc<RwLock<Vec<RwLock<Database>>>> = {
        const STEP: usize = 1000;
        const SAMPLE:usize = 100;
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
        let mut people: HashMap<Id,Person> = {
            info!("creating users...");
            let bar = ProgressBar::new(Database::SIZE as u64);
            let people: HashMap<Id,Person> = (0..Database::SIZE).map(|i|{
                if i % STEP == 0 && i!=0 {
                    bar.inc(STEP as u64);
                    if i % (10000 * STEP) == 0 {
                        bar.println(format!("eta: {:.3?}",bar.eta()));
                    }
                }
                (i as u64,rng.gen())
            }).collect();
            bar.finish();
            // Update unique id counter to include all newly generated users
            UNIQUE_ID_COUNTER.fetch_add(Database::SIZE as u64, std::sync::atomic::Ordering::SeqCst);
            people
        };

        // Print a sample of users

        info!("sample: [{}]",(0..SAMPLE).map(|i|people.get(&(i as u64)).unwrap().name.clone()).intersperse(String::from(" ")).collect::<String>());

        // For each user, we create `NUMBER_OF_POSTS` posts and assign them `NUMBER_OF_FRIENDS` friends.
        let posts = {
            // The map of post id's to the post data.
            let mut posts: HashMap<Id,Post> = HashMap::new();

            info!("filling dataset...");
            let bar = ProgressBar::new(Database::SIZE as u64);
            for i in 0..Database::SIZE as u64 {

                // For each person create NUMBER_OF_FRIENDS semi-random friends
                {
                    // We skip past this user
                    let mut iter = (0..Database::SIZE as u64).cycle().skip(usize::try_from(i).unwrap());
                    // The maximum step here needs to be small enough such that it never loops back
                    // around and introduces duplicate friends.
                    //
                    // It is guaranteed that
                    // `NUMBER_OF_FRIENDS * rng.gen_range(1..SIZE/NUMBER_OF_FRIENDS) < SIZE` since
                    // `NUMBER_OF_FRIENDS * SIZE/NUMBER_OF_FRIENDS < SIZE`.
                    let friends = (0..Database::NUMBER_OF_FRIENDS).map(|_|iter.nth(rng.gen_range(1..Database::SIZE/Database::NUMBER_OF_FRIENDS)).unwrap()).collect::<HashSet<_>>();
                    people.get_mut(&i).unwrap().friends = friends;
                    // This is way too slow, so we use the above code instead.
                    // people.get_mut(&i).unwrap().friends = (0..SIZE as u64).choose_multiple(&mut rng, NUMBER_OF_FRIENDS);
                }
                // print!("\\");

                // For each person create `NUMBER_OF_POSTS` random posts (where each post is liked by `NUMBER_OF_LIKES` of their friends)
                {
                    for i in 0..Database::NUMBER_OF_POSTS {
                        // Samples `NUMBER_OF_LIKES` friends from their friend list to like their post.
                        let friend_set = people.get(&(i as u64)).unwrap().friends.iter().copied().choose_multiple(&mut rng, Database::NUMBER_OF_LIKES);

                        // Adds post id
                        let n = UNIQUE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                        // Adds post to liked posts of the friends who liked this post.
                        for friend in &friend_set {
                            people.get_mut(friend).unwrap().liked_posts.push(n);
                        }
                        // Generates post
                        let p = Post::new(&mut rng,i as u64,friend_set);
                        // Inserts post
                        posts.insert(n,p);
                        // Adds post id to this user
                        people.get_mut(&(i as u64)).unwrap().posts.push(n);
                    }
                }
                if i % STEP as u64 == 0 && i!=0 {
                    bar.inc(STEP as u64);
                    if i % (10000 * STEP as u64) == 0 {
                        bar.println(format!("eta: {:.3?}",bar.eta()));
                    }
                }
            }
            bar.finish();
            posts
        };

        info!("splitting");
        
        let mut data_split = vec![Database {
            people, posts,
        }];
        if *SPLIT {
            loop {
                let split_opt = data_split.iter().enumerate().find_map(|(i,f)|if f.should_slit() { Some(i) } else { None });
                if let Some(split) = split_opt {
                    let [a,b] = data_split.remove(split).split();
                    data_split.push(a);
                    data_split.push(b);
                } else {
                    break
                }
            }
        }

        info!("split");
        info!("setup database: {:?}",now.elapsed());

        Arc::new(RwLock::new(data_split.into_iter().map(RwLock::new).collect()))
    };
}

/// The module containing our structure for recording metrics.
mod recorder;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Do we split our database into sections to speed up lock acquisition.
    #[clap(long, default_value_t = true)]
    split: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Database {
    // User Ids -> User data
    people: HashMap<Id, Person>,
    // Post Ids -> Post data
    posts: HashMap<Id, Post>,
}
impl Database {
    /// Each user has 300 friends.
    const NUMBER_OF_FRIENDS: usize = 100;
    /// Each post has 5 likes from among a users friends.
    const NUMBER_OF_LIKES: usize = 5;
    /// Each user has 30 posts.
    const NUMBER_OF_POSTS: usize = 30;
    /// 1 million users.
    const SIZE: usize = 1_000_000;

    fn should_slit(&self) -> bool {
        self.people.len() > 100000
    }

    // Splits `self` into 2 approximately equal sized sections
    fn split(self) -> [Self; 2] {
        let [first_people, second_people] = split_hashmap(self.people);
        let [first_posts, second_posts] = split_hashmap(self.posts);
        [
            Self {
                people: first_people,
                posts: first_posts,
            },
            Self {
                people: second_people,
                posts: second_posts,
            },
        ]
    }
}
fn split_hashmap<K: Hash + Eq, V>(map: HashMap<K, V>) -> [HashMap<K, V>; 2] {
    let [mut first, mut second] = [HashMap::new(), HashMap::new()];
    let len = map.len();
    let mut iter = map.into_iter();
    for _ in 0..len / 2 {
        let (key, value) = iter.next().unwrap();
        first.insert(key, value);
    }
    for _ in len / 2..len {
        let (key, value) = iter.next().unwrap();
        second.insert(key, value);
    }
    [first, second]
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Person {
    name: String,
    friends: HashSet<Id>,
    posts: Vec<Id>,
    liked_posts: Vec<Id>,
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
            posts: Vec::new(),
            liked_posts: Vec::new(),
        }
    }
}
type Id = u64;
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Post {
    poster: Id,
    text: String,
    likes: Vec<Id>,
    // References/Quotes specific text from a specific post
    references: Vec<PostReference>,
}
const TEXT_LEN: usize = 200;
impl Post {
    fn new<R: Rng + ?Sized>(rng: &mut R, poster: Id, likes: Vec<Id>) -> Self {
        // let rng = rand::thread_rng();
        Self {
            poster,
            text: rng
                .sample_iter(&Alphanumeric)
                .take(TEXT_LEN)
                .map(char::from)
                .collect(),
            likes,
            references: Vec::new(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PostReference {
    post: Id,
    range: std::ops::Range<usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Group {
    name: String,
    members: Vec<Id>,
    endorsements: Vec<Id>,
}

/// Type alias for async function pointer
type AsyncFn = fn(Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Vec<u8>> + Send>>;
/// We have N query function.
const N: u32 = 2;
// static TEST_FUNCTION: AsyncFn = |a: Vec<u8>| Box::pin(async { Vec::new()});
/// Server query functions
static FUNCTIONS: [AsyncFn; N as usize] = [
    // Gets users connections 1st, 2nd, 3rd (depending on n where 0=1st, 1=2nd, etc.)
    |bytes: Vec<u8>| {
        Box::pin(async move {
            #[derive(Deserialize)]
            struct Filter {
                id: u64,
                n: u8,
            }
            let filter: Filter = bincode::deserialize(&bytes).unwrap();

            let guard = DATA.read().await;
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
            bincode::serialize(&connections).unwrap()
        })
    },
    // Gets user data by id
    |bytes: Vec<u8>| {
        Box::pin(async move {
            #[derive(Deserialize)]
            struct Filter(Id);
            let filter: Filter = bincode::deserialize(&bytes).unwrap();

            let guard = DATA.read().await;
            let id = guard.people.get(&filter.0).unwrap();

            bincode::serialize(&id).unwrap()
        })
    },
];

#[tokio::main]
async fn main() {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
    metrics::set_recorder(&*RECORDER).unwrap();

    info!("running...");
    tokio::spawn(async { save().await });
    tokio::spawn(async { split().await });
    loop {
        let listener = TcpListener::bind(ADDRESS).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async { process(stream).await });
    }
}

mod my_metrics {
    pub const SPLIT_LOCKED: &str = "split: locked";
    pub const SPLIT_SPLIT: &str = "split: split";
    pub const SAVE_READ_LOCKED: &str = "save: read locked";
    pub const SAVE_SERIALIZED: &str = "save: serialized";
    pub const SAVE_SAVED: &str = "save: saved";
}

// Does split checks and splits db if needed
async fn split() {
    loop {
        info!("split: started");
        let now = Instant::now();

        // Acquires write lock on all database sections
        let mut guard = {
            let guard = loop {
                if let Ok(g) = DATA.try_write() {
                    break g;
                }
            };
            info!("{}: {}", my_metrics::SPLIT_LOCKED, guard.len());
            histogram!(my_metrics::SPLIT_LOCKED, now.elapsed());
            guard
        };
        // Splits database sections until none return `true` for `should_split()`
        info!("split: splitting");
        loop {
            let split_opt = guard.iter().enumerate().find_map(|(i,f)|if f.blocking_read().should_slit() { Some(i) } else { None });
            if let Some(split) = split_opt {
                let [a,b] = guard.remove(split).into_inner().split();
                guard.push(RwLock::new(a));
                guard.push(RwLock::new(b));
            } else {
                break
            }
        }
        info!("{}: {}",my_metrics::SPLIT_SPLIT, guard.len());
        histogram!(my_metrics::SPLIT_SPLIT, now.elapsed());

        // TODO Is this needed?
        drop(guard);

        sleep(SPLIT_INTERVAL).await;
    }
}

async fn save() {
    loop {
        info!("save: started");
        let now = Instant::now();

        // Acquires read lock
        let guard = {
            let guard = loop {
                if let Ok(g) = DATA.try_read() {
                    break g;
                }
            };
            info!("{}", my_metrics::SAVE_READ_LOCKED);
            histogram!(my_metrics::SAVE_READ_LOCKED, now.elapsed());
            guard
        };
        let clones = guard.iter().map(|g|g.blocking_read().clone()).collect::<Vec<_>>();

        // Serializes data
        let serialized = {
            let serialized = bincode::serialize(&clones).unwrap();
            info!("{}", my_metrics::SAVE_SERIALIZED);
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
            histogram!(my_metrics::SAVE_SAVED, now.elapsed());
        }
        info!("save: finished");
        // Print metrics every save.
        info!("metrics: {:?}", &*RECORDER);
        // Sleep for interval between saves.
        sleep(SAVE_INTERVAL).await;
    }
}
/// When a stream is connected we call `process` on it
async fn process(stream: TcpStream) {
    // We split the bi-directional stream into its read and write halves.
    let (read, mut write) = stream.into_split();
    // From the read stream we construct a framed reader (this produces frames we define by our
    // implementation of `Decoder` on `MyDecoder`).
    let mut reader = FramedRead::new(read, MyDecoder);
    // We await for a frame to be read from our reader.
    while let Some(next) = reader.next().await {
        // We log the received frame option.
        info!("received: {:?}", next);
        // We unwrap the frame (this depends on whether the bytes read from the stream could be
        // converted into our defined frame).
        let (f, x) = next.unwrap();
        // We assert the function index given in the frame is within the range of our defined
        // function.
        assert!(f < N, "Function index out of range");
        // We call the function for the given function index with the given byte input.
        let res = FUNCTIONS[f as usize](x).await;
        // We write the result to our write stream.
        write_fn(&mut write, res).await;
        info!("sent");
    }
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

/// A simple decoder with no internal state
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
