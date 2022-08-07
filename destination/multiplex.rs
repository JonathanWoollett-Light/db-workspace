// struct Data {
//     x: u32,
//     y: String,
// }
// impl Data {
//     async fn repeat(&self,_:()) -> String {
//         self.y.repeat(self.x as usize)
//     }
//     async fn add(&mut self, a: u32) {
//         self.x += a;
//     }
// }
// impl Default for Data {
//     fn default() -> Self {
//         Self {
//             x: 2,
//             y: String::from("Hello World!")
//         }
//     }
// }

struct Data {
    x: u8,
}
impl Data {
    async fn add(&mut self, a: u8) -> u8 {
        let y = x;
        self.x += 1;
        y
    }
}
impl Default for Data {
    fn default() -> Self {
        Self {
            x: 2,
        }
    }
}