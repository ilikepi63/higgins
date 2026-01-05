use colored::Color;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ByteInterval(pub usize, pub usize);

#[derive(PartialEq, Eq, Debug)]
pub struct Interval(pub ByteInterval, pub Color, pub String);

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

pub fn print_bytes_coloured(bytes: &[u8], colours: &mut [Interval]) {
    use colored::Colorize;

    colours.sort();

    // ensure that the intervals don't overlap.
    for window in colours.windows(3) {
        let first = window.first().unwrap();
        let second = window.get(1).unwrap();
        let third = window.get(2).unwrap();

        assert!(first.0.1 <= second.0.0);
        assert!(second.0.1 <= third.0.0);
    }

    let mut i = 0;

    println!("[");

    for colour in colours {
        // For each colour, we want to iterate over the bytes basically
        while i < colour.0.0 {
            println!(" {}", bytes.get(i).unwrap());
            i += 1;
        }

        while i < colour.0.1 {
            if i == colour.0.0 {
                println!(
                    " {} {}",
                    format!("{}", bytes.get(i).unwrap())
                        .to_string()
                        .color(colour.1),
                    colour.2
                );
            } else {
                println!(
                    " {}",
                    format!("{}", bytes.get(i).unwrap())
                        .to_string()
                        .color(colour.1)
                );
            }

            i += 1;
        }
    }

    while i < bytes.len() {
        println!(" {}", bytes.get(i).unwrap());
        i += 1;
    }

    print!("]");
}
