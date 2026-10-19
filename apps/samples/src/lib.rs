//! Sample data the connector apps show: a service's web requests, the same rows whichever
//! app makes them, so the demos agree.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::ArrowError;

/// How many requests the demos show.
pub const REQUESTS: i64 = 1_000_000;

/// Requests `start..end` as a batch: request id, time, service, method, path, status, ms
/// and bytes.
///
/// # Errors
/// None in practice: the columns always agree (Arrow checks it).
pub fn requests(start: i64, end: i64) -> Result<RecordBatch, ArrowError> {
    const SERVICES: [&str; 6] = [
        "gateway", "auth", "catalog", "checkout", "search", "storage",
    ];
    const PATHS: [&str; 6] = ["/", "/login", "/items", "/cart", "/search", "/files"];
    const METHODS: [&str; 4] = ["GET", "GET", "POST", "PUT"];
    let ids: Vec<i64> = (start..end).collect();
    // Lossless: small indices.
    let pick = |i: i64, n: i64| (i * 7_919 % n) as usize;
    let status = |i: i64| match i % 97 {
        0 => 500,
        1..=3 => 404,
        _ => 200,
    };
    // Whole hundredths, so values print short.
    let millis = |i: i64| (200 + (i * 7_919) % 16_000) as f64 / 100.0;
    RecordBatch::try_from_iter([
        (
            "request",
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
        ),
        (
            "time",
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    ids.iter().map(|&i| 1_792_368_000_000 + i * 37),
                )
                .with_timezone("UTC"),
            ),
        ),
        (
            "service",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| SERVICES[pick(i, 6)]),
            )),
        ),
        (
            "method",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| METHODS[pick(i, 4)]),
            )),
        ),
        (
            "path",
            Arc::new(StringArray::from_iter_values(
                ids.iter().map(|&i| PATHS[pick(i + 3, 6)]),
            )),
        ),
        (
            "status",
            Arc::new(Int64Array::from_iter_values(ids.iter().map(|&i| status(i)))),
        ),
        (
            "ms",
            Arc::new(Float64Array::from_iter_values(
                ids.iter().map(|&i| millis(i)),
            )),
        ),
        (
            "bytes",
            Arc::new(Int64Array::from_iter_values(
                ids.iter().map(|&i| 200 + i * 31 % 40_000),
            )),
        ),
    ])
}
