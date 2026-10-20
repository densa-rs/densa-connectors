//! Shows a WebSocket stream of JSON rows in densa's live grid: each row is upserted by its
//! key as it arrives, so the grid holds the latest row of every key, sorted by a click.
//!
//! `cargo run --release -p stream-viewer -- --demo` starts a local server sending readings
//! from 500 sensors and shows them. For another stream:
//! `cargo run --release -p stream-viewer -- ws://host/path --columns id:string,value:float --key id`
//! (types: `string`, `int`, `float`, `bool`, `time` (milliseconds since the epoch)).
//! `--subscribe TEXT` sends a message when the connection opens; `--light` for the light
//! theme, `--seconds N` to close after N seconds.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_ui::{Action, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{DataGrid, Label, Panel};
use densa_window::{App, Host, WindowOptions};
use densa_ws::{Stream, StreamEvent, StreamOptions};

type Grid = DataGrid<LiveModel>;

/// The longest a frame spends taking in batches.
const FRAME_BUDGET: Duration = Duration::from_millis(8);
/// How often the status bar is written again.
const STATUS_EVERY: Duration = Duration::from_millis(250);

struct Viewer {
    url: String,
    schema: SchemaRef,
    key: String,
    subscribe: Option<String>,
    light: bool,
    close_at: Option<Instant>,
    stream: Option<Stream>,
    ids: Option<Ids>,
    /// The connection's state.
    state: String,
    rows: u64,
    /// Messages skipped, and the last reason.
    skipped: (u64, String),
    /// Rows the table refused (a batch whose key is null, say), and the last reason.
    refused: (u64, String),
    rate: (Instant, u64, f64),
    status_at: Instant,
}

struct Ids {
    grid: WidgetId,
    status: WidgetId,
}

impl Viewer {
    /// Takes in what has arrived, for at most a frame's budget. Whether some is left.
    fn take(&mut self, ui: &mut Ui) -> bool {
        let (Some(stream), Some(ids)) = (self.stream.as_mut(), &self.ids) else {
            return false;
        };
        let started = Instant::now();
        while started.elapsed() < FRAME_BUDGET {
            let Some(event) = stream.try_next() else {
                return false;
            };
            match event {
                StreamEvent::Connected => self.state = "connected".to_owned(),
                StreamEvent::Batch(batch) => {
                    // Lossless: a batch's row count.
                    let rows = batch.num_rows() as u64;
                    self.rows += rows;
                    let mut refused = None;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        refused = grid.model_mut(cx).table_mut().upsert(batch).err();
                    });
                    if let Some(error) = refused {
                        self.refused = (self.refused.0 + rows, error.to_string());
                    }
                }
                StreamEvent::Invalid { reason, .. } => {
                    self.skipped = (self.skipped.0 + 1, reason);
                }
                StreamEvent::Stopped { reason } => self.state = format!("stopped: {reason}"),
                StreamEvent::Disconnected { reason, retry_in } => {
                    self.state =
                        format!("{reason}; trying again in {:.1} s", retry_in.as_secs_f64());
                }
                _ => {}
            }
        }
        true
    }

    fn show_status(&mut self, ui: &mut Ui) {
        let Some(ids) = &self.ids else {
            return;
        };
        let (since, rows_then, rate) = self.rate;
        let elapsed = since.elapsed().as_secs_f64();
        if elapsed >= 1.0 {
            // Lossless enough: a rate to show.
            self.rate = (
                Instant::now(),
                self.rows,
                (self.rows - rows_then) as f64 / elapsed,
            );
        }
        let keys = ui
            .widget::<Grid>(ids.grid)
            .map_or(0, |grid| grid.model().table().len());
        let mut text = format!(
            "{}: {}. {keys} keys, {} rows received, {:.0} rows a second",
            self.url, self.state, self.rows, rate
        );
        if self.skipped.0 > 0 {
            text.push_str(&format!(
                ". {} messages skipped ({})",
                self.skipped.0, self.skipped.1
            ));
        }
        if self.refused.0 > 0 {
            text.push_str(&format!(
                ". {} rows refused ({})",
                self.refused.0, self.refused.1
            ));
        }
        ui.update::<Label, _>(ids.status, |label, cx| label.set_text(cx, text));
    }
}

impl App for Viewer {
    fn build(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> Result<(), Box<dyn Error>> {
        if self.light {
            ui.set_theme(Theme::light());
        }
        let model = LiveModel::new(LiveTable::new(self.schema.clone(), &self.key)?)?;
        let mut options = StreamOptions::json(self.url.clone(), self.schema.clone());
        if let Some(message) = &self.subscribe {
            options = options.on_connect(message.clone());
        }
        let waker = host.waker();
        self.stream = Some(options.connect(move || {
            waker.wake();
        })?);

        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        ui.add(toolbar, Label::new("Stream").with_size(15.0), Style::row())?;
        let hint = "The latest row of every key. Click a title to sort";
        ui.add(toolbar, Label::new(hint).dim(), Style::row())?;
        let grid = ui.add(root, Grid::new(model).with_frozen_columns(1), Grid::style())?;
        let bar = ui.add(root, Panel::new(), Style::row().padding(6.0).shrink(0.0))?;
        let status = ui.add(bar, Label::new("").dim(), Style::row())?;
        self.ids = Some(Ids { grid, status });
        self.show_status(ui);
        Ok(())
    }

    fn action(&mut self, _: &mut Ui, _: &mut Host<'_>, _: Action) {}

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        let more = self.take(ui);
        if self.status_at.elapsed() >= STATUS_EVERY {
            self.status_at = Instant::now();
            self.show_status(ui);
        }
        if self.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        // The stream wakes the window as rows come; the status bar's rate wants a refresh.
        more || self.close_at.is_some()
    }
}

/// A schema from `name:type,name:type`.
fn columns(spec: &str) -> Result<SchemaRef, String> {
    let fields = spec
        .split(',')
        .map(|column| {
            let (name, kind) = column
                .split_once(':')
                .ok_or_else(|| format!("{column:?} is not name:type"))?;
            let data_type = match kind.trim() {
                "string" => DataType::Utf8,
                "int" => DataType::Int64,
                "float" => DataType::Float64,
                "bool" => DataType::Boolean,
                // An offset, not "UTC": JSON reads named time zones only with chrono-tz.
                "time" => DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                other => return Err(format!("unknown type {other:?}")),
            };
            Ok(Field::new(name.trim(), data_type, true))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let value = |flag: &str| -> Result<Option<String>, String> {
        match args.iter().position(|a| a == flag) {
            None => Ok(None),
            Some(at) => match args.get(at + 1) {
                Some(value) if !value.starts_with("--") => Ok(Some(value.clone())),
                _ => Err(format!("{flag} needs a value")),
            },
        }
    };
    let (url, schema, key) = if args.iter().any(|a| a == "--demo") {
        let spec = "sensor:string,site:string,celsius:float,humidity:float,ok:bool,time:time";
        (
            samples::readings_server()?,
            columns(spec)?,
            "sensor".to_owned(),
        )
    } else {
        let Some(url) = args
            .iter()
            .enumerate()
            .find(|(i, a)| {
                let after_flag = *i > 0
                    && args[i - 1].starts_with("--")
                    && args[i - 1] != "--demo"
                    && args[i - 1] != "--light";
                (a.starts_with("ws://") || a.starts_with("wss://")) && !after_flag
            })
            .map(|(_, a)| a)
        else {
            eprintln!("usage: stream-viewer URL --columns name:type,... --key COLUMN, or --demo");
            std::process::exit(2);
        };
        let spec = value("--columns")?.ok_or("--columns is needed with a URL")?;
        let key = value("--key")?.ok_or("--key is needed with a URL")?;
        (url.clone(), columns(&spec)?, key)
    };
    let seconds = value("--seconds")?.map(|s| s.parse::<u64>()).transpose()?;
    let viewer = Viewer {
        url,
        schema,
        key,
        subscribe: value("--subscribe")?,
        light: args.iter().any(|a| a == "--light"),
        close_at: seconds.map(|s| Instant::now() + Duration::from_secs(s)),
        stream: None,
        ids: None,
        state: "connecting".to_owned(),
        rows: 0,
        skipped: (0, String::new()),
        refused: (0, String::new()),
        rate: (Instant::now(), 0, 0.0),
        status_at: Instant::now(),
    };
    densa_window::run(
        WindowOptions::new("densa stream viewer").with_size(1000.0, 680.0),
        viewer,
    )?;
    Ok(())
}
