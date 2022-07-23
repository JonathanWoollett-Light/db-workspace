use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use metrics::{Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Unit};

// #[derive(Debug)]
struct MyHistogram(Mutex<Vec<f64>>);
impl MyHistogram {
    fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }
}
impl HistogramFn for MyHistogram {
    fn record(&self, value: f64) {
        self.0.lock().unwrap().push(value);
    }
}
impl fmt::Debug for MyHistogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const MAX: usize = 5;
        let out = if self.0.lock().unwrap().len() > MAX {
            " and more ... "
        } else {
            ""
        };
        write!(
            f,
            "{:?}{}",
            self.0.lock().unwrap().iter().take(MAX).collect::<Vec<_>>(),
            out
        )
    }
}
#[allow(clippy::module_name_repetitions)]
pub struct MyRecorder {
    counters: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    gauges: Arc<Mutex<HashMap<String, Arc<AtomicU64>>>>,
    histograms: Arc<Mutex<HashMap<String, Arc<MyHistogram>>>>,
}
impl MyRecorder {
    pub fn new() -> Self {
        Self {
            counters: Arc::new(Mutex::new(HashMap::new())),
            gauges: Arc::new(Mutex::new(HashMap::new())),
            histograms: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
impl fmt::Debug for MyRecorder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let gauges = self
            .gauges
            .lock()
            .unwrap()
            .iter()
            .map(|(key, bits)| (key.clone(), f64::from_bits(bits.load(Ordering::SeqCst))))
            .collect::<HashMap<String, f64>>();
        f.debug_struct("MyRecorder")
            .field("counters", &self.counters.lock().unwrap())
            .field("gauges", &gauges)
            .field("histograms", &self.histograms.lock().unwrap())
            .finish()
    }
}

impl metrics::Recorder for MyRecorder {
    fn describe_counter(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn describe_gauge(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn describe_histogram(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: metrics::SharedString,
    ) {
        unimplemented!()
    }

    fn register_counter(&self, key: &Key) -> Counter {
        let mut guard = self.counters.lock().unwrap();
        Counter::from_arc(if let Some(cur) = guard.get(key.name()) {
            cur.clone()
        } else {
            let new = Arc::new(AtomicU64::new(0));
            guard.insert(String::from(key.name()), new.clone());
            new
        })
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        let mut guard = self.gauges.lock().unwrap();
        Gauge::from_arc(if let Some(cur) = guard.get(key.name()) {
            cur.clone()
        } else {
            let new = Arc::new(AtomicU64::new(0));
            guard.insert(String::from(key.name()), new.clone());
            new
        })
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        let mut guard = self.histograms.lock().unwrap();
        Histogram::from_arc(if let Some(cur) = guard.get(key.name()) {
            cur.clone()
        } else {
            let new = Arc::new(MyHistogram::new());
            guard.insert(String::from(key.name()), new.clone());
            new
        })
    }
}

// fn main() {
//     env_logger::builder().filter_level(LevelFilter::Debug).init();
//     metrics::set_recorder(&*RECORDER).unwrap();

//     error!("started");
//     let mut rng = thread_rng();
//     for _ in 0..10000 {
//         if rng.gen() {
//             one::add(rng.gen(), rng.gen());
//         } else {
//             two::sub(rng.gen(), rng.gen());
//         }
//     }
//     debug!("MyRecorder: {:?}", *RECORDER);
//     info!("finished");
// }
