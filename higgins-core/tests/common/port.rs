use get_port::{Ops, Range, tcp::TcpPort};

pub fn get_random_port() -> u16 {
    let port = TcpPort::in_range(
        "127.0.0.1",
        Range {
            min: 2000,
            max: 25000,
        },
    )
    .unwrap();

    port
}
