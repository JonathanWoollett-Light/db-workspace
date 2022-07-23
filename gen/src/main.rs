use std::io::Write;
fn main() {
    // TODO In application it would pull this from the local crate root
    const CRATE_NAME: &str = "destination";

    // These are config params
    const SERVER_SUFFIX: &str = "server";
    const SAVE_FILE: &str = "store";
    const SAVE_INTERVAL_SECONDS: u64 = 60;
    const ADDRESS: &str = "127.0.0.1:8080";

    let server_name = format!("{}-{}", CRATE_NAME, SERVER_SUFFIX);
    let str = std::fs::read_to_string(format!("../{}/src/main.rs", CRATE_NAME)).unwrap();
    let ast: syn::File = syn::parse_str(&str).unwrap();

    println!("ast: {:#?}", ast);
    let db = ast.items.iter().find(|x| match x {
        syn::Item::Struct(db) if db.ident == "Database" => true,
        _ => false,
    });
    println!("db: {:#?}", db);
    let out = std::process::Command::new("cargo")
        .args(["new", &format!("../{}", server_name), "--bin"])
        .output()
        .unwrap();
    println!("creation: {:?}", out);

    let mut src_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(format!("../{}/src/main.rs", server_name))
        .unwrap();
    let src = format!(
        "
use std::io::Write;
use tokio_stream::StreamExt;
use bytes::Buf;
use tokio::io::AsyncWriteExt;

lazy_static::lazy_static! {{
    static ref DATA: std::sync::Arc<tokio::sync::RwLock<Vec<u8>>> = \
         std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new()));
}}
static FUNCTIONS: [fn(Vec<u8>)->Vec<u8>; NUMBER_OF_FUNCTIONS as usize] = [
    |bytes: Vec<u8>| {{ 
        // TODO: Placeholder, handle unwraps.
        let _filter: Filter = bincode::deserialize(&bytes).unwrap();
        let x = vec![1];
        bincode::serialize(&x).unwrap()
    }},
    |bytes: Vec<u8>| {{ 
        // TODO: Placeholder, handle unwraps.
        let _filter: Filter = bincode::deserialize(&bytes).unwrap();
        let x = vec![1];
        bincode::serialize(&x).unwrap()
    }}
];

#[derive(serde::Deserialize)]
struct Filter {{
    name: String
}}

const NUMBER_OF_FUNCTIONS: u32 = 2;
const ADDRESS: &str = \"{}\";
const SAVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs({});

#[tokio::main]
async fn main() {{
    tokio::spawn(async {{ save().await }});
    loop {{
        let listener = tokio::net::TcpListener::bind(ADDRESS).await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async {{ process(stream).await }});
    }}
}}
async fn save() {{ loop {{
        let serialized = bincode::serialize(&*DATA.read().await).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(\"./{}\")
            .unwrap();
        file.write_all(&serialized).unwrap();
        tokio::time::sleep(SAVE_INTERVAL).await;
}} }}
async fn process(stream: tokio::net::TcpStream) {{
    let (read, mut writer) = stream.into_split();
    let mut reader = tokio_util::codec::FramedRead::new(read, SimpleDecoder());
    while let Some(next) = reader.next().await {{
        let (f, x) = next.expect(\"TODO What message should go here?\");
        // Asserts the given function index is within range.
        assert!(f < NUMBER_OF_FUNCTIONS, \"Function index out of range\");
        // Calls the respective function getting the result
        let result = FUNCTIONS[f as usize](x);
        // Creates the buffer to write to the stream
        let buffer = {{
            let mut buf = Vec::with_capacity(4 + result.len());
            buf.extend_from_slice(&u32::to_le_bytes(result.len() as u32));
            buf.extend_from_slice(&result);
            buf
        }};
        // Writes to stream
        writer.write_all(&buffer).await.unwrap();
    }}
}}
struct SimpleDecoder();
impl tokio_util::codec::Decoder for SimpleDecoder {{
    type Item = (u32, Vec<u8>);
    type Error = std::io::Error;
    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {{
        // We use u32 for len marker, so need an initial 4 bytes
        if src.len() < 4 {{
            return Ok(None);
        }}
        // Reads length of data
        let length = {{
            let mut length_bytes = [0; 4];
            length_bytes.copy_from_slice(&src[..4]);
            u32::from_le_bytes(length_bytes) as usize
        }};
        // TODO Explain this better
        if src.len() < 8 + length {{
            src.reserve(4 + length - src.len());
            return Ok(None);
        }}
        // Reads function index
        let index = {{
            let mut i = [0; 4];
            i.copy_from_slice(&src[4..8]);
            u32::from_le_bytes(i)
        }};
        // Reads data
        let read_data = src[8..8 + length].to_vec();
        // Advance buffer to discard read data
        src.advance(8 + length);
        Ok(Some((index, read_data)))
    }}
}}
    ",
        ADDRESS, SAVE_INTERVAL_SECONDS, SAVE_FILE
    );
    src_file.write_all(src.as_bytes()).unwrap();

    let mut toml_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(format!("../{}/Cargo.toml", server_name))
        .unwrap();

    let toml = format!(
        "
[package]
name = \"{}\"
version = \"0.1.0\"
edition = \"2021\"

# See more keys and their definitions \
         at https://doc.rust-lang.org/cargo/reference/manifest.html

[dependencies]
tokio = {{ version=\"1.18.0\", features=[\"full\"] }}
tokio-stream = \"0.1.8\"
tokio-util = {{ version=\"0.7.1\",features=[\"codec\",\"io\"] }}
bytes = \"1.1.0\"
serde = {{ version=\"1.0.137\", features=[\"derive\"] }}
lazy_static = \"1.4.0\"
bincode = \"1.3.3\"
rand = \"0.8.5\"
indicatif = \"0.16.2\"
    ",
        server_name
    );
    toml_file.write_all(toml.as_bytes()).unwrap();
}
