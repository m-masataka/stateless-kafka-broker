use rand::{rng, Rng};

pub fn jittered_delay(base: u64) -> u64 {
    let mut rng = rng();
    let jitter: f64 = rng.random_range(0.5..=1.5); // ±50% jitter
    (base as f64 * jitter) as u64
}
