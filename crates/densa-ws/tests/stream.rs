//! Streams from a local WebSocket server: JSON and Arrow IPC rows in batches, bad messages
//! skipped, reconnection with growing waits, and a stream that stops when dropped.

use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use densa_ws::{Stream, StreamError, StreamEvent, StreamOptions};
use tungstenite::{Message, WebSocket};

const WAIT: Duration = Duration::from_secs(8);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("sensor", DataType::Utf8, false),
        Field::new("celsius", DataType::Float64, true),
    ]))
}

/// A server on a free local port running `script`, and its URL.
fn server(script: impl FnOnce(TcpListener) + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}/readings", listener.local_addr().unwrap());
    thread::spawn(move || script(listener));
    url
}

fn accept(listener: &TcpListener) -> WebSocket<TcpStream> {
    tungstenite::accept(listener.accept().unwrap().0).unwrap()
}

/// Waits for the client to go away, answering pings.
fn hold(mut socket: WebSocket<TcpStream>) {
    while socket.read().is_ok() {}
}

/// The next batch, skipping other events.
fn next_batch(stream: &mut Stream) -> RecordBatch {
    let until = Instant::now() + WAIT;
    while Instant::now() < until {
        if let Some(StreamEvent::Batch(batch)) = stream.wait_next(WAIT) {
            return batch;
        }
    }
    panic!("no batch within {WAIT:?}");
}

fn celsius(batch: &RecordBatch) -> Vec<f64> {
    batch
        .column(1)
        .as_primitive::<Float64Type>()
        .values()
        .to_vec()
}

fn row(sensor: &str, celsius: f64) -> String {
    format!(r#"{{"sensor":"{sensor}","celsius":{celsius}}}"#)
}

#[test]
fn json_rows_arrive_soon_even_when_a_batch_is_not_full() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket
            .send(Message::text(format!(
                "{}\n{}",
                row("a", 21.5),
                row("b", 22.0)
            )))
            .unwrap();
        socket.send(Message::text(row("c", 19.25))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(30))
        .connect(|| {})
        .unwrap();
    assert!(matches!(
        stream.wait_next(WAIT),
        Some(StreamEvent::Connected)
    ));
    let mut seen = Vec::new();
    while seen.len() < 3 {
        seen.extend(celsius(&next_batch(&mut stream)));
    }
    assert_eq!(seen, [21.5, 22.0, 19.25]);
}

#[test]
fn full_batches_are_handed_over_at_once() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        let rows: Vec<String> = (0..5).map(|i| row("s", f64::from(i))).collect();
        socket.send(Message::text(rows.join("\n"))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .batch_rows(2)
        .max_delay(Duration::from_secs(3))
        .connect(|| {})
        .unwrap();
    let first = next_batch(&mut stream);
    let second = next_batch(&mut stream);
    assert_eq!(
        (first.num_rows(), second.num_rows()),
        (2, 2),
        "full ones without waiting"
    );
    let started = Instant::now();
    assert_eq!(next_batch(&mut stream).num_rows(), 1);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "the rest after the delay"
    );
}

#[test]
fn an_arrow_ipc_stream_split_across_messages_reads_back_whole() {
    let batches: Vec<RecordBatch> = [(0..3), (3..7)]
        .into_iter()
        .map(|range| {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from_iter_values(
                        range.clone().map(|i| format!("s{i}")),
                    )) as ArrayRef,
                    Arc::new(Float64Array::from_iter_values(range.map(f64::from))),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut bytes = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema()).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();
    }
    let url = server(move |listener| {
        let mut socket = accept(&listener);
        // Cut wherever: the decoder must put the pieces back together.
        for chunk in bytes.chunks(37) {
            socket.send(Message::binary(chunk.to_vec())).unwrap();
        }
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url).connect(|| {}).unwrap();
    let mut seen = Vec::new();
    while seen.len() < 7 {
        seen.extend(celsius(&next_batch(&mut stream)));
    }
    assert_eq!(seen, (0..7).map(f64::from).collect::<Vec<_>>());
}

#[test]
fn a_message_that_cannot_be_read_is_skipped_and_the_stream_goes_on() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text("{not json at all")).unwrap();
        socket.send(Message::binary(vec![1, 2, 3])).unwrap();
        socket.send(Message::text(row("ok", 1.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(20))
        .connect(|| {})
        .unwrap();
    let mut reasons = Vec::new();
    let batch = loop {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Invalid { reason, .. }) => reasons.push(reason),
            Some(StreamEvent::Batch(batch)) => break batch,
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    };
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    assert!(reasons[1].contains("binary message in a JSON stream"));
    assert_eq!(celsius(&batch), [1.0]);
}

#[test]
fn text_in_an_arrow_ipc_stream_is_skipped() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text("hello")).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url).connect(|| {}).unwrap();
    let reason = loop {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Invalid { reason, rows_lost }) => {
                assert_eq!(rows_lost, 0);
                break reason;
            }
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    };
    assert!(reason.contains("text message in an Arrow IPC stream"));
}

#[test]
fn a_lost_connection_is_opened_again_and_subscribed_again() {
    let subscriptions = Arc::new(AtomicUsize::new(0));
    let counted = subscriptions.clone();
    let url = server(move |listener| {
        for round in 0..2 {
            let mut socket = accept(&listener);
            if let Ok(Message::Text(text)) = socket.read() {
                if text.as_str() == "subscribe" {
                    counted.fetch_add(1, Ordering::Relaxed);
                }
            }
            socket
                .send(Message::text(row("r", f64::from(round))))
                .unwrap();
            if round == 0 {
                socket.close(None).unwrap();
                while socket.read().is_ok() {}
            } else {
                hold(socket);
            }
        }
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(10))
        .retry(Duration::from_millis(30), Duration::from_secs(1))
        .on_connect("subscribe")
        .connect(|| {})
        .unwrap();
    let mut events = Vec::new();
    let until = Instant::now() + WAIT;
    while events.iter().filter(|e| **e == "batch").count() < 2 && Instant::now() < until {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Connected) => events.push("connected"),
            Some(StreamEvent::Batch(_)) => events.push("batch"),
            Some(StreamEvent::Disconnected { retry_in, .. }) => {
                assert_eq!(retry_in, Duration::from_millis(30), "a first wait");
                events.push("disconnected");
            }
            _ => {}
        }
    }
    assert_eq!(
        events,
        ["connected", "batch", "disconnected", "connected", "batch"]
    );
    assert_eq!(subscriptions.load(Ordering::Relaxed), 2);
}

#[test]
fn a_server_that_is_not_there_is_tried_again_with_growing_waits() {
    // A port that was free a moment ago: nothing listens on it.
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut stream = StreamOptions::json(format!("ws://127.0.0.1:{port}/"), schema())
        .retry(Duration::from_millis(10), Duration::from_millis(40))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 4 {
        if let Some(StreamEvent::Disconnected { retry_in, reason }) = stream.wait_next(WAIT) {
            assert!(reason.contains("cannot connect"), "{reason}");
            waits.push(retry_in.as_millis());
        }
    }
    assert_eq!(waits, [10, 20, 40, 40]);
}

#[test]
fn urls_that_are_not_websocket_urls_are_refused() {
    for url in [
        "http://example.com/",
        "not a url",
        "ws:///nohost",
        "ws://:80/",
    ] {
        let error = StreamOptions::json(url, schema())
            .connect(|| {})
            .unwrap_err();
        assert!(
            matches!(error, StreamError::InvalidUrl(_)),
            "{url}: {error}"
        );
        assert!(error.to_string().starts_with("invalid WebSocket URL"));
    }
    #[cfg(not(feature = "tls"))]
    {
        let error = StreamOptions::json("wss://example.com/", schema())
            .connect(|| {})
            .unwrap_err();
        assert!(matches!(error, StreamError::TlsDisabled));
    }
}

#[test]
fn dropping_the_stream_closes_the_connection() {
    let (closed_tx, closed_rx) = std::sync::mpsc::channel();
    let url = server(move |listener| {
        let socket = accept(&listener);
        let started = Instant::now();
        hold(socket);
        let _ = closed_tx.send(started.elapsed());
    });
    let wakes = Arc::new(AtomicUsize::new(0));
    let counted = wakes.clone();
    let mut stream = StreamOptions::json(url, schema())
        .connect(move || {
            counted.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    assert!(matches!(
        stream.wait_next(WAIT),
        Some(StreamEvent::Connected)
    ));
    // The wake follows the event it announces, so it may still be on its way.
    let deadline = Instant::now() + WAIT;
    while wakes.load(Ordering::Relaxed) == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        wakes.load(Ordering::Relaxed) >= 1,
        "woken for the connection"
    );
    drop(stream);
    let held = closed_rx
        .recv_timeout(WAIT)
        .expect("the server saw the connection close");
    assert!(held < WAIT);
}

#[test]
fn settings_are_kept_within_bounds() {
    // Out-of-range settings still make a working stream.
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("x", 5.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .batch_rows(0)
        .max_delay(Duration::ZERO)
        .retry(Duration::ZERO, Duration::ZERO)
        .max_message_bytes(0)
        .connect(|| {})
        .unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [5.0]);
    assert_eq!(densa_ws::MAX_BATCH_ROWS, 1 << 20);

    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("y", 6.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .batch_rows(usize::MAX)
        .max_delay(Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [6.0]);
}

#[test]
fn a_longest_wait_shorter_than_the_first_is_the_first() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut stream = StreamOptions::json(format!("ws://127.0.0.1:{port}/"), schema())
        .retry(Duration::from_millis(30), Duration::from_millis(5))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 2 {
        if let Some(StreamEvent::Disconnected { retry_in, .. }) = stream.wait_next(WAIT) {
            waits.push(retry_in.as_millis());
        }
    }
    assert_eq!(waits, [30, 30]);
}

fn ipc_bytes(range: std::ops::Range<i32>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema()).unwrap();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(
                range.clone().map(|i| format!("s{i}")),
            )) as ArrayRef,
            Arc::new(Float64Array::from_iter_values(range.map(f64::from))),
        ],
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

#[test]
fn a_host_name_reaches_a_server_on_any_of_its_addresses() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("v4", 4.0))).unwrap();
        hold(socket);
    });
    // localhost may resolve to ::1 first, where nothing listens.
    let mut stream = StreamOptions::json(format!("ws://localhost:{port}/"), schema())
        .max_delay(Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [4.0]);
    assert!(stream.is_connected());
}

#[test]
fn text_between_arrow_ipc_messages_leaves_the_stream_readable() {
    let bytes = ipc_bytes(0..3);
    let url = server(move |listener| {
        let mut socket = accept(&listener);
        let (head, tail) = bytes.split_at(bytes.len() / 2);
        socket.send(Message::binary(head.to_vec())).unwrap();
        socket.send(Message::text("heartbeat")).unwrap();
        socket.send(Message::binary(tail.to_vec())).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url).connect(|| {}).unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [0.0, 1.0, 2.0]);
}

#[test]
fn an_unreadable_arrow_ipc_stream_starts_over_on_a_new_connection() {
    let good = ipc_bytes(5..7);
    let url = server(move |listener| {
        let mut socket = accept(&listener);
        // Not an IPC stream at all: the connection cannot go on from here.
        // A message marker and a length, then a body that is no IPC message.
        let mut junk = vec![0xFF, 0xFF, 0xFF, 0xFF, 16, 0, 0, 0];
        junk.extend([0xAB; 16]);
        socket.send(Message::binary(junk)).unwrap();
        while socket.read().is_ok() {}
        let mut socket = accept(&listener);
        socket.send(Message::binary(good)).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url)
        .retry(Duration::from_millis(10), Duration::from_millis(10))
        .connect(|| {})
        .unwrap();
    let mut disconnected = None;
    let batch = loop {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Disconnected { reason, .. }) => disconnected = Some(reason),
            Some(StreamEvent::Batch(batch)) => break batch,
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    };
    assert!(disconnected.is_some_and(|r| r.contains("Arrow IPC")));
    assert_eq!(celsius(&batch), [5.0, 6.0]);
}

#[test]
fn a_stray_binary_message_keeps_the_rows_gathered_so_far() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("a", 1.0))).unwrap();
        socket.send(Message::binary(vec![9, 9, 9])).unwrap();
        socket.send(Message::text(row("b", 2.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(400))
        .connect(|| {})
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 2 {
        seen.extend(celsius(&next_batch(&mut stream)));
    }
    assert_eq!(seen, [1.0, 2.0]);
}

#[test]
fn a_message_cut_off_mid_row_is_skipped_alone() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("a", 1.0))).unwrap();
        let cut = format!("{}\n{{\"sensor\":\"cut", row("b", 2.0));
        socket.send(Message::text(cut)).unwrap();
        socket.send(Message::text(row("c", 3.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(400))
        .connect(|| {})
        .unwrap();
    let mut seen = Vec::new();
    let mut lost = None;
    let until = Instant::now() + WAIT;
    while seen.len() < 2 && Instant::now() < until {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Batch(batch)) => seen.extend(celsius(&batch)),
            Some(StreamEvent::Invalid { rows_lost, .. }) => lost = Some(rows_lost),
            _ => {}
        }
    }
    assert_eq!(seen, [1.0, 3.0], "the rows of the other messages");
    assert_eq!(
        lost,
        Some(1),
        "the cut message's whole row, not the cut one"
    );
}

#[test]
fn headers_go_with_the_request_that_opens_the_connection() {
    let (seen_tx, seen_rx) = std::sync::mpsc::channel();
    let url = server(move |listener| {
        let tcp = listener.accept().unwrap().0;
        // tungstenite's handshake callback returns its (large) error response by value.
        #[allow(clippy::result_large_err)]
        let callback = |request: &tungstenite::handshake::server::Request,
                        response: tungstenite::handshake::server::Response| {
            let token = request
                .headers()
                .get("authorization")
                .map(|v| v.to_str().unwrap_or_default().to_owned());
            let _ = seen_tx.send(token);
            Ok(response)
        };
        let socket = tungstenite::accept_hdr(tcp, callback).unwrap();
        hold(socket);
    });
    let _stream = StreamOptions::json(url, schema())
        .header("Authorization", "Bearer secret")
        .connect(|| {})
        .unwrap();
    let token = seen_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(token.as_deref(), Some("Bearer secret"));

    let error = StreamOptions::json("ws://localhost/", schema())
        .header("bad header", "x")
        .connect(|| {})
        .unwrap_err();
    assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
}

#[test]
fn a_message_over_the_limit_ends_the_connection() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        let rows: Vec<String> = (0..100).map(|i| row("big", f64::from(i))).collect();
        socket.send(Message::text(rows.join("\n"))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_message_bytes(1_024)
        .retry(Duration::from_secs(5), Duration::from_secs(5))
        .connect(|| {})
        .unwrap();
    let reason = loop {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Disconnected { reason, .. }) => break reason,
            Some(StreamEvent::Batch(_)) => panic!("the message was over the limit"),
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    };
    let lower = reason.to_lowercase();
    assert!(
        lower.contains("size") || lower.contains("too long") || lower.contains("capacity"),
        "{reason}"
    );
    assert!(!stream.is_connected());
}

#[test]
fn a_server_that_hangs_up_at_once_is_tried_again_with_growing_waits() {
    let url = server(|listener| {
        for connection in listener.incoming().flatten() {
            if let Ok(mut socket) = tungstenite::accept(connection) {
                let _ = socket.close(None);
                while socket.read().is_ok() {}
            }
        }
    });
    let mut stream = StreamOptions::json(url, schema())
        .retry(Duration::from_millis(10), Duration::from_millis(80))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 4 {
        if let Some(StreamEvent::Disconnected { retry_in, .. }) = stream.wait_next(WAIT) {
            waits.push(retry_in.as_millis());
        }
    }
    assert_eq!(
        waits,
        [10, 20, 40, 80],
        "a handshake alone does not reset the waits"
    );
}

#[test]
fn an_arrow_ipc_body_that_never_ends_is_cut_off() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        // A message that declares itself very long, fed in pieces that never finish it.
        let mut start = vec![0xFF, 0xFF, 0xFF, 0xFF];
        start.extend(1_000_000_u32.to_le_bytes());
        socket.send(Message::binary(start)).unwrap();
        for _ in 0..8 {
            socket.send(Message::binary(vec![0; 1_000])).unwrap();
        }
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url)
        .max_message_bytes(1_024)
        .retry(Duration::from_secs(5), Duration::from_secs(5))
        .connect(|| {})
        .unwrap();
    let reason = loop {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Disconnected { reason, .. }) => break reason,
            Some(_) => {}
            None => panic!("nothing within {WAIT:?}"),
        }
    };
    assert!(reason.contains("over 4096 bytes"), "{reason}");
}

#[test]
fn a_message_that_fills_a_batch_exactly_is_handed_over_at_once() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        let rows = format!("{}\n{}", row("a", 1.0), row("b", 2.0));
        socket.send(Message::text(rows)).unwrap();
        hold(socket);
    });
    let started = Instant::now();
    let mut stream = StreamOptions::json(url, schema())
        .batch_rows(2)
        .max_delay(Duration::from_secs(5))
        .connect(|| {})
        .unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [1.0, 2.0]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "without waiting for the delay"
    );
}

#[test]
fn the_delay_runs_from_the_first_row_still_waiting() {
    let url = server(|listener| {
        let mut socket = accept(&listener);
        socket.send(Message::text(row("a", 1.0))).unwrap();
        socket.send(Message::text(row("b", 2.0))).unwrap();
        thread::sleep(Duration::from_millis(1_000));
        socket.send(Message::text(row("c", 3.0))).unwrap();
        hold(socket);
    });
    let mut stream = StreamOptions::json(url, schema())
        .batch_rows(2)
        .max_delay(Duration::from_millis(400))
        .connect(|| {})
        .unwrap();
    assert_eq!(celsius(&next_batch(&mut stream)), [1.0, 2.0]);
    let full = Instant::now();
    assert_eq!(celsius(&next_batch(&mut stream)), [3.0]);
    let waited = full.elapsed();
    // c arrives a second after the full batch, then waits 0.4 s of its own.
    assert!(
        waited >= Duration::from_millis(1_250),
        "c waited its own delay, not the one a started: {waited:?}"
    );
    assert!(
        waited < Duration::from_millis(2_500),
        "c was handed over without more messages: {waited:?}"
    );
}

#[test]
fn a_connection_that_delivered_rows_starts_the_waits_over() {
    let url = server(|listener| {
        for connection in listener.incoming().flatten() {
            if let Ok(mut socket) = tungstenite::accept(connection) {
                let _ = socket.send(Message::text(row("a", 1.0)));
                // Long enough for the row to be handed over after its delay.
                thread::sleep(Duration::from_millis(300));
                let _ = socket.close(None);
                while socket.read().is_ok() {}
            }
        }
    });
    let mut stream = StreamOptions::json(url, schema())
        .max_delay(Duration::from_millis(20))
        .retry(Duration::from_millis(10), Duration::from_secs(1))
        .connect(|| {})
        .unwrap();
    let mut waits = Vec::new();
    while waits.len() < 3 {
        if let Some(StreamEvent::Disconnected { retry_in, .. }) = stream.wait_next(WAIT) {
            waits.push(retry_in.as_millis());
        }
    }
    assert_eq!(waits, [10, 10, 10]);
}

#[test]
fn a_close_from_the_server_is_answered() {
    let (ended_tx, ended_rx) = std::sync::mpsc::channel();
    let url = server(move |listener| {
        let mut socket = accept(&listener);
        socket.close(None).unwrap();
        let end = loop {
            if let Err(error) = socket.read() {
                break error;
            }
        };
        let _ = ended_tx.send(end.to_string());
    });
    let _stream = StreamOptions::json(url, schema())
        .retry(Duration::from_secs(5), Duration::from_secs(5))
        .connect(|| {})
        .unwrap();
    let end = ended_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        end,
        tungstenite::Error::ConnectionClosed.to_string(),
        "a clean closing handshake"
    );
}

#[test]
fn a_panic_on_the_reading_thread_stops_the_stream() {
    let url = server(|listener| hold(accept(&listener)));
    let mut stream = StreamOptions::json(url, schema())
        .connect(|| panic!("a waker that fails"))
        .unwrap();
    let mut stopped = None;
    while let Some(event) = stream.wait_next(WAIT) {
        if let StreamEvent::Stopped { reason } = event {
            stopped = Some(reason);
        }
    }
    assert!(stopped.is_some_and(|r| r.contains("panicked")));
    assert!(stream.is_finished());
    assert!(!stream.is_connected());
}

#[cfg(feature = "tls")]
#[test]
fn a_wss_url_is_opened_on_port_443() {
    let mut stream = StreamOptions::json("wss://127.0.0.1/", schema())
        .retry(Duration::from_secs(5), Duration::from_secs(5))
        .connect(|| {})
        .unwrap();
    match stream.wait_next(WAIT) {
        Some(StreamEvent::Disconnected { reason, .. }) => {
            assert!(reason.contains("127.0.0.1:443"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_long_arrow_ipc_stream_of_small_messages_is_never_cut_off() {
    // A schema of about 3 KB and ten batches of about 3 KB, in messages of at most 1,000
    // bytes: each under the 4 KiB an unfinished IPC message may reach, the schema and a
    // batch together over it.
    let schema = Arc::new(Schema::new_with_metadata(
        schema().fields().clone(),
        [("notes".to_owned(), "x".repeat(3_000))],
    ));
    let mut bytes = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut bytes, &schema).unwrap();
    let schema_bytes = writer.get_ref().len();
    let mut batch_bytes = 0;
    for b in 0..10 {
        let range = b * 100..(b + 1) * 100;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from_iter_values(
                    range.clone().map(|i| format!("sensor-{i:06}")),
                )) as ArrayRef,
                Arc::new(Float64Array::from_iter_values(range.map(f64::from))),
            ],
        )
        .unwrap();
        let before = writer.get_ref().len();
        writer.write(&batch).unwrap();
        batch_bytes = writer.get_ref().len() - before;
    }
    writer.finish().unwrap();
    drop(writer);
    assert!((3_000..3_500).contains(&schema_bytes), "{schema_bytes}");
    assert!((2_500..3_500).contains(&batch_bytes), "{batch_bytes}");
    let url = server(move |listener| {
        let mut socket = accept(&listener);
        for chunk in bytes.chunks(1_000) {
            socket.send(Message::binary(chunk.to_vec())).unwrap();
        }
        hold(socket);
    });
    let mut stream = StreamOptions::arrow_ipc(url)
        .max_message_bytes(1_024)
        .connect(|| {})
        .unwrap();
    let mut rows = 0;
    while rows < 1_000 {
        match stream.wait_next(WAIT) {
            Some(StreamEvent::Batch(batch)) => rows += batch.num_rows(),
            Some(StreamEvent::Disconnected { reason, .. }) => panic!("{reason}"),
            Some(_) => {}
            None => panic!("{rows} rows within {WAIT:?}"),
        }
    }
}

#[test]
fn header_values_are_not_printed() {
    let options =
        StreamOptions::json("ws://localhost/", schema()).header("Authorization", "Bearer secret");
    let printed = format!("{options:?}");
    assert!(
        printed.contains("Authorization") && !printed.contains("secret"),
        "{printed}"
    );
}
