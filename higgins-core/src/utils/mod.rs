#[cfg(test)]
pub mod test;

pub mod consumption;
pub mod request_response;

pub fn epoch() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let start = SystemTime::now();
    let since_the_epoch = match start.duration_since(UNIX_EPOCH) {
        Ok(v) => v,
        Err(_) => unreachable!(), // time should go forward
    };

    since_the_epoch.as_secs()
}
