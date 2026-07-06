use std::{
    net::TcpListener,
    sync::atomic::{AtomicU16, Ordering},
};

use get_port::{Ops, Range, tcp::TcpPort};

static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

pub fn get_random_port() -> u16 {
    if NEXT_PORT.load(Ordering::Relaxed) == 0 {
        let seed = TcpPort::in_range(
            "127.0.0.1",
            Range {
                min: 2000,
                max: 25000,
            },
        )
        .unwrap();

        let _ = NEXT_PORT.compare_exchange(0, seed, Ordering::Relaxed, Ordering::Relaxed);
    }

    loop {
        let candidate = NEXT_PORT.fetch_add(1, Ordering::Relaxed);
        if candidate == 0 {
            continue;
        }
        if TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            return candidate;
        }
    }
}
