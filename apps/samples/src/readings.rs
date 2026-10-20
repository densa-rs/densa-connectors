//! A local WebSocket server of sensor readings, for the apps' streaming demos.

use std::io;
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Sensors whose readings the server sends.
pub const SENSORS: u64 = 500;

/// Where the sensors are.
const SITES: [&str; 6] = ["north", "south", "east", "west", "harbor", "airfield"];

/// Starts a server on a free local port sending readings of [`SENSORS`] sensors, a hundred
/// every 20 ms, as text messages of JSON objects (one per line): `sensor` (`s-0042`), `site`,
/// `celsius`, `humidity`, `ok` and `time` (milliseconds since the epoch). Returns its URL;
/// the server runs until the process ends, each connection on a thread of its own.
///
/// # Errors
/// If no local port can be had.
pub fn readings_server() -> io::Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let url = format!("ws://{}/readings", listener.local_addr()?);
    thread::spawn(move || {
        for connection in listener.incoming().flatten() {
            thread::spawn(move || {
                let Ok(mut socket) = tungstenite::accept(connection) else {
                    return;
                };
                let mut next = random(0x5EED);
                loop {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis());
                    let rows: Vec<String> = (0..100).map(|_| reading(&mut next, now)).collect();
                    if socket
                        .send(tungstenite::Message::text(rows.join("\n")))
                        .is_err()
                    {
                        return;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
            });
        }
    });
    Ok(url)
}

/// Numbers in `0..1`, the same from the same seed.
fn random(mut seed: u64) -> impl FnMut() -> f64 {
    move || {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        // Lossless: the high bits of a 64-bit state.
        (seed >> 11) as f64 / (1_u64 << 53) as f64
    }
}

/// One sensor's reading at `now`, as a JSON object.
fn reading(next: &mut impl FnMut() -> f64, now: u128) -> String {
    // Lossless: an index below 500.
    let sensor = (next() * SENSORS as f64) as u64;
    // Lossless: an index below 6.
    let site = SITES[(sensor % 6) as usize];
    let base = 15.0 + (sensor % 17) as f64;
    let celsius = base + (next() - 0.5) * 4.0;
    let humidity = 30.0 + next() * 50.0;
    format!(
        r#"{{"sensor":"s-{sensor:04}","site":"{site}","celsius":{celsius:.2},"humidity":{humidity:.1},"ok":{},"time":{now}}}"#,
        celsius < base + 1.8
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readings_are_json_objects_of_known_sensors() {
        let mut next = random(1);
        for _ in 0..1_000 {
            let row = reading(&mut next, 7);
            assert!(row.starts_with(r#"{"sensor":"s-0"#), "{row}");
            assert!(row.ends_with(r#""time":7}"#), "{row}");
            let digits = &row[13..17];
            let sensor: u64 = digits.parse().unwrap();
            assert!(sensor < SENSORS);
        }
    }

    #[test]
    fn the_server_answers_with_readings() {
        let url = readings_server().unwrap();
        let (mut socket, _) = tungstenite::connect(&url).unwrap();
        let message = socket.read().unwrap().into_text().unwrap();
        assert_eq!(message.lines().count(), 100);
        assert!(message.lines().all(|line| line.contains(r#""site":""#)));
    }
}
