#![feature(let_chains)]
use std::io::Write;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum Metrics {
    None,
    Minimal,
    Full,
}
use quote::quote;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// When saving, do we:
    /// - `true`: acquire the read lock, clone, drop the lock, serialize from clone. Faster,
    ///   requires 2x memory of dataset to be present in system.
    /// - `false`: acquire the lock, serialize from data, drop the lock. Slower, requires no
    ///   additional memory.
    #[clap(short, long, value_parser,value_enum, default_value_t = Metrics::Minimal)]
    pub metrics: Metrics,
}

fn main() {
    let args = Args::parse();
    println!("args: {:?}", args);
    let schema =
        std::fs::read_to_string("multiplex.rs").expect("Can't find schema (`multiplex.rs`)");
    let ast: syn::File = syn::parse_str(&schema).unwrap();
    println!("ast: \n{:#?}", ast);

    let data_struct = match &ast.items[0] {
        syn::Item::Struct(data_struct) if data_struct.ident.to_string() == "Data" => data_struct,
        _ => panic!("The 1st item in the schema should be a struct with the identifier `Data`"),
    };
    let outer_impl_block = &ast.items[1];
    let impl_items =
        if let syn::Item::Impl(impl_block) = outer_impl_block
            && let syn::Type::Path(type_path) = &*impl_block.self_ty
            && type_path.path.get_ident().unwrap() == "Data" 
        {
            &impl_block.items
        }
        else {
            panic!("The 2nd item in the schema should be an impl for the `Data` struct")
        };

    // let generated: proc_macro2::TokenStream = some_import.parse().unwrap();
    let mut n = 0u32;
    let functions = impl_items.iter().map(|func|{
        let (function_ident,function_input_type,(guard_mut,function_self_mutability)) = if let syn::ImplItem::Method(method_item) = func
            && method_item.sig.asyncness.is_some()
            {
            let mut iter = method_item.sig.inputs.iter();
            // Must have self
            let lock_type = if let Some(syn::FnArg::Receiver(self_token)) = iter.next() {
                let (a,b) = match self_token.mutability {
                    Some(_) => ("mut","write()"),
                    None => ("","read()")
                };
                (a.parse::<proc_macro2::TokenStream>().unwrap(),b.parse::<proc_macro2::TokenStream>().unwrap())
            }
            else {
                panic!("self must be an input")
            };
            // Gets input other than self
            let input = if let Some(input) = iter.next() {
                if let syn::FnArg::Typed(test) = input {if let syn::Type::Path(type_path) = &*test.ty {
                    let ident = type_path.path.get_ident().unwrap();
                    ident.to_string()
                }else { String::from("()") }.parse::<proc_macro2::TokenStream>().unwrap()}
                else {
                    panic!("here1")
                }
            }else {
                panic!("Every function requires an input, if you don't need an use the unit type `()`")
            };
            // Cannot have 2 inputs (excluding self)
            assert!(iter.next().is_none(),"Cannot have 2 inputs (excluding self)");
            (&method_item.sig.ident,input,lock_type)
        }
        else {
            panic!("All items within impl must `async fn`s")
        };
        n += 1;
        quote!{
            |bytes: Vec<u8>| { Box::pin( async move {
                type Input = #function_input_type;
                let input: Input = bincode::deserialize(&bytes).unwrap();

                let #guard_mut guard = DATA.#function_self_mutability.await;
                let result = guard.#function_ident(input).await;

                let output = bincode::serialize(&result).unwrap();
                output
            } ) },
        }
    }).collect::<proc_macro2::TokenStream>();

    // TODO Check this is default
    let default_block = &ast.items[2];

    println!("functions: {}", functions);
    let src = quote! {
        use std::{sync::Arc,pin::Pin};
        use tokio::sync::RwLock;
        use serde::Deserialize;

        #data_struct
        #outer_impl_block
        #default_block

        type DataStore = Arc<RwLock<Data>>;

        lazy_static::lazy_static! {
            static ref DATA: DataStore = Arc::new(RwLock::new(Data::new()));
        }

        type AsyncFn = fn(Vec<u8>) -> Pin<Box<dyn std::future::Future<Output = Vec<u8>> + Send>>;
        const N: u32 = #n;
        static FUNCTIONS: [AsyncFn; N as usize] = [
            #functions
        ];
    };
    let mut output = std::fs::File::create("temp.rs").unwrap();
    output.write_all(src.to_string().as_bytes()).unwrap();
    std::process::Command::new("rustfmt")
        .arg("temp.rs")
        .output()
        .expect("failed to execute process");
    // eprintln!("src:\n{}",src)
}

// fn main() {
//     // TODO In application it would pull this from the local crate root
//     const CRATE_NAME: &str = "destination";

//     // These are config params
//     const SERVER_SUFFIX: &str = "server";
//     const SAVE_FILE: &str = "store";
//     const SAVE_INTERVAL_SECONDS: u64 = 60;
//     const ADDRESS: &str = "127.0.0.1:8080";

//     let server_name = format!("{}-{}", CRATE_NAME, SERVER_SUFFIX);
//     let str = std::fs::read_to_string(format!("../{}/src/main.rs", CRATE_NAME)).unwrap();
//     let ast: syn::File = syn::parse_str(&str).unwrap();

//     println!("ast: {:#?}", ast);
//     let db = ast.items.iter().find(|x| match x {
//         syn::Item::Struct(db) if db.ident == "Database" => true,
//         _ => false,
//     });
//     println!("db: {:#?}", db);
//     let out = std::process::Command::new("cargo")
//         .args(["new", &format!("../{}", server_name), "--bin"])
//         .output()
//         .unwrap();
//     println!("creation: {:?}", out);

//     let mut src_file = std::fs::OpenOptions::new()
//         .write(true)
//         .truncate(true)
//         .open(format!("../{}/src/main.rs", server_name))
//         .unwrap();
//     let src = format!(
//         "
// use std::io::Write;
// use tokio_stream::StreamExt;
// use bytes::Buf;
// use tokio::io::AsyncWriteExt;

// lazy_static::lazy_static! {{
//     static ref DATA: std::sync::Arc<tokio::sync::RwLock<Vec<u8>>> = \
//          std::sync::Arc::new(tokio::sync::RwLock::new(Vec::new()));
// }}
// static FUNCTIONS: [fn(Vec<u8>)->Vec<u8>; NUMBER_OF_FUNCTIONS as usize] = [
//     |bytes: Vec<u8>| {{
//         // TODO: Placeholder, handle unwraps.
//         let _filter: Filter = bincode::deserialize(&bytes).unwrap();
//         let x = vec![1];
//         bincode::serialize(&x).unwrap()
//     }},
//     |bytes: Vec<u8>| {{
//         // TODO: Placeholder, handle unwraps.
//         let _filter: Filter = bincode::deserialize(&bytes).unwrap();
//         let x = vec![1];
//         bincode::serialize(&x).unwrap()
//     }}
// ];

// #[derive(serde::Deserialize)]
// struct Filter {{
//     name: String
// }}

// const NUMBER_OF_FUNCTIONS: u32 = 2;
// const ADDRESS: &str = \"{}\";
// const SAVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs({});

// #[tokio::main]
// async fn main() {{
//     tokio::spawn(async {{ save().await }});
//     loop {{
//         let listener = tokio::net::TcpListener::bind(ADDRESS).await.unwrap();
//         let (stream, _) = listener.accept().await.unwrap();
//         tokio::spawn(async {{ process(stream).await }});
//     }}
// }}
// async fn save() {{ loop {{
//         let serialized = bincode::serialize(&*DATA.read().await).unwrap();
//         let mut file = std::fs::OpenOptions::new()
//             .create(true)
//             .write(true)
//             .open(\"./{}\")
//             .unwrap();
//         file.write_all(&serialized).unwrap();
//         tokio::time::sleep(SAVE_INTERVAL).await;
// }} }}
// async fn process(stream: tokio::net::TcpStream) {{
//     let (read, mut writer) = stream.into_split();
//     let mut reader = tokio_util::codec::FramedRead::new(read, SimpleDecoder());
//     while let Some(next) = reader.next().await {{
//         let (f, x) = next.expect(\"TODO What message should go here?\");
//         // Asserts the given function index is within range.
//         assert!(f < NUMBER_OF_FUNCTIONS, \"Function index out of range\");
//         // Calls the respective function getting the result
//         let result = FUNCTIONS[f as usize](x);
//         // Creates the buffer to write to the stream
//         let buffer = {{
//             let mut buf = Vec::with_capacity(4 + result.len());
//             buf.extend_from_slice(&u32::to_le_bytes(result.len() as u32));
//             buf.extend_from_slice(&result);
//             buf
//         }};
//         // Writes to stream
//         writer.write_all(&buffer).await.unwrap();
//     }}
// }}
// struct SimpleDecoder();
// impl tokio_util::codec::Decoder for SimpleDecoder {{
//     type Item = (u32, Vec<u8>);
//     type Error = std::io::Error;
//     fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {{
//         // We use u32 for len marker, so need an initial 4 bytes
//         if src.len() < 4 {{
//             return Ok(None);
//         }}
//         // Reads length of data
//         let length = {{
//             let mut length_bytes = [0; 4];
//             length_bytes.copy_from_slice(&src[..4]);
//             u32::from_le_bytes(length_bytes) as usize
//         }};
//         // TODO Explain this better
//         if src.len() < 8 + length {{
//             src.reserve(4 + length - src.len());
//             return Ok(None);
//         }}
//         // Reads function index
//         let index = {{
//             let mut i = [0; 4];
//             i.copy_from_slice(&src[4..8]);
//             u32::from_le_bytes(i)
//         }};
//         // Reads data
//         let read_data = src[8..8 + length].to_vec();
//         // Advance buffer to discard read data
//         src.advance(8 + length);
//         Ok(Some((index, read_data)))
//     }}
// }}
//     ",
//         ADDRESS, SAVE_INTERVAL_SECONDS, SAVE_FILE
//     );
//     src_file.write_all(src.as_bytes()).unwrap();

//     let mut toml_file = std::fs::OpenOptions::new()
//         .write(true)
//         .truncate(true)
//         .open(format!("../{}/Cargo.toml", server_name))
//         .unwrap();

//     let toml = format!(
//         "
// [package]
// name = \"{}\"
// version = \"0.1.0\"
// edition = \"2021\"

// # See more keys and their definitions \
//          at https://doc.rust-lang.org/cargo/reference/manifest.html

// [dependencies]
// tokio = {{ version=\"1.18.0\", features=[\"full\"] }}
// tokio-stream = \"0.1.8\"
// tokio-util = {{ version=\"0.7.1\",features=[\"codec\",\"io\"] }}
// bytes = \"1.1.0\"
// serde = {{ version=\"1.0.137\", features=[\"derive\"] }}
// lazy_static = \"1.4.0\"
// bincode = \"1.3.3\"
// rand = \"0.8.5\"
// indicatif = \"0.16.2\"
//     ",
//         server_name
//     );
//     toml_file.write_all(toml.as_bytes()).unwrap();
// }
