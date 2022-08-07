use std :: { sync :: Arc , pin :: Pin } ; use tokio :: sync :: RwLock ; use serde :: Deserialize ; struct Data { x : u32 , y : String , } impl Data { async fn repeat (& self , _ : ()) -> String { self . y . repeat (self . x as usize) } async fn add (& mut self , a : u32) { self . x += a ; } } impl Default for Data { fn default () -> Self { Self { x : 2 , y : String :: from ("Hello World!") } } } type DataStore = Arc < RwLock < Data >> ; lazy_static :: lazy_static ! { static ref DATA : DataStore = Arc :: new (RwLock :: new (Data :: new ())) ; } type AsyncFn = fn (Vec < u8 >) -> Pin < Box < dyn std :: future :: Future < Output = Vec < u8 >> + Send >> ; const N : u32 = 2u32 ; static FUNCTIONS : [AsyncFn ; N as usize] = [| bytes : Vec < u8 > | { Box :: pin (async move { type Input = () ; let input : Input = bincode :: deserialize (& bytes) . unwrap () ; let guard = DATA . read () . await ; let result = guard . repeat (input) . await ; let output = bincode :: serialize (& result) . unwrap () ; output }) } , | bytes : Vec < u8 > | { Box :: pin (async move { type Input = u32 ; let input : Input = bincode :: deserialize (& bytes) . unwrap () ; let mut guard = DATA . write () . await ; let result = guard . add (input) . await ; let output = bincode :: serialize (& result) . unwrap () ; output }) } ,] ;