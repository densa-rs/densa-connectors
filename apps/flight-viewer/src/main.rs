//! Shows an Arrow Flight or Flight SQL result in densa's live grid, rows appearing as they
//! arrive; a click on a title sorts them at any time.
//!
//! `cargo run --release -p flight-viewer -- URL --sql QUERY` (or `--ticket T`, `--path a/b`,
//! `--command C`), with `--key COLUMN` to key rows by a column (rows with the same key replace
//! each other; by default rows are numbered), `--header name:value` for each header (a token,
//! say; `name:$VAR` reads it from an environment variable), `--light` for the light theme and
//! `--seconds N` to close after N seconds.
//! `--demo` starts a local Flight service whose answer to any query is a million web
//! requests, in four endpoints.

mod demo;

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_schema::{DataType, Field, Schema};
use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_flight::{FlightEvent, FlightOptions, FlightStream};
use densa_ui::{Action, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{DataGrid, Label, Panel};
use densa_window::{App, Host, WindowOptions};

/// The key column added when none is given.
const ROW: &str = "row";
/// How often the status bar is written again.
const STATUS_EVERY: Duration = Duration::from_millis(200);
/// The longest a frame spends taking in batches: the rest wait for the next frame.
const FRAME_BUDGET: Duration = Duration::from_millis(8);

type Grid = DataGrid<LiveModel>;

struct Viewer {
    /// What is asked, for the title bar.
    title: String,
    options: Option<FlightOptions>,
    key: String,
    light: bool,
    close_at: Option<Instant>,
    stream: Option<FlightStream>,
    ids: Option<Ids>,
    /// Where the result stands.
    state: String,
    /// Rows the table refused, and why the last ones were.
    refused: (u64, String),
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
                FlightEvent::Schema(schema) => {
                    self.state = "reading".to_owned();
                    let key = self.key.clone();
                    let mut error = None;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        let table = grid.model_mut(cx).table_mut();
                        // A retry announces the same columns: keep the rows read so far.
                        if table.schema() != &schema {
                            match LiveTable::new(schema, &key) {
                                Ok(fresh) => *table = fresh,
                                Err(e) => error = Some(e.to_string()),
                            }
                        }
                    });
                    if let Some(error) = error {
                        self.state = format!("cannot show the result: {error}");
                    }
                }
                FlightEvent::Batch(batch) => {
                    // Lossless: a batch's row count.
                    let rows = batch.num_rows() as u64;
                    let mut refused = None;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        refused = grid.model_mut(cx).table_mut().upsert(batch).err();
                    });
                    if let Some(error) = refused {
                        self.refused = (self.refused.0 + rows, error.to_string());
                    }
                }
                FlightEvent::Finished => self.state = "done".to_owned(),
                FlightEvent::Retrying { reason, retry_in } => {
                    self.state =
                        format!("{reason}; asking again in {:.1} s", retry_in.as_secs_f64());
                }
                FlightEvent::Failed { reason } => self.state = format!("failed: {reason}"),
                _ => {}
            }
        }
        true
    }

    fn show_status(&self, ui: &mut Ui) {
        let Some(ids) = &self.ids else {
            return;
        };
        // The table's own count: rows sharing a key are one row.
        let rows = ui
            .widget::<Grid>(ids.grid)
            .map_or(0, |grid| grid.model().table().len());
        // Lossless: a row count.
        let mut text = format!("{}: {} rows", self.state, grouped(rows as u64));
        if self.refused.0 > 0 {
            text.push_str(&format!(
                ", {} refused ({})",
                grouped(self.refused.0),
                self.refused.1
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
        // Columns come with the result: until then, an empty table of the key alone.
        let waiting = Schema::new(vec![Field::new(&self.key, DataType::UInt64, false)]);
        let model = LiveModel::new(LiveTable::new(Arc::new(waiting), &self.key)?)?;
        let waker = host.waker();
        if let Some(options) = self.options.take() {
            self.stream = Some(options.connect(move || {
                waker.wake();
            })?);
        }

        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        ui.add(
            toolbar,
            Label::new(self.title.clone()).with_size(15.0),
            Style::row(),
        )?;
        ui.add(
            toolbar,
            Label::new("Click a title to sort").dim(),
            Style::row(),
        )?;
        let grid = ui.add(root, Grid::new(model), Grid::style())?;
        let bar = ui.add(root, Panel::new(), Style::row().padding(6.0).shrink(0.0))?;
        let status = ui.add(bar, Label::new("").dim(), Style::row())?;
        self.ids = Some(Ids { grid, status });
        self.show_status(ui);
        Ok(())
    }

    fn action(&mut self, _: &mut Ui, _: &mut Host<'_>, _: Action) {}

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        let more = self.take(ui);
        if self.status_at.elapsed() >= STATUS_EVERY || !more {
            self.status_at = Instant::now();
            self.show_status(ui);
        }
        if self.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        // The stream wakes the window as events come; events left over need another frame.
        more || self.close_at.is_some()
    }
}

/// `n` with a comma between groups of three digits.
fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut text = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            text.push(',');
        }
        text.push(digit);
    }
    text
}

/// Flags followed by a value.
const VALUE_FLAGS: [&str; 7] = [
    "--sql",
    "--ticket",
    "--path",
    "--command",
    "--key",
    "--header",
    "--seconds",
];

/// The value after `flag`, if it is given: any text but another flag (an SQL comment may
/// start with `--`).
fn value(args: &[String], flag: &str) -> Result<Option<String>, String> {
    match args.iter().position(|a| a == flag) {
        None => Ok(None),
        Some(at) => match args.get(at + 1) {
            Some(value) if !VALUE_FLAGS.contains(&value.as_str()) && value != "--light" => {
                Ok(Some(value.clone()))
            }
            _ => Err(format!("{flag} needs a value")),
        },
    }
}

/// The request the arguments name for `url`, and a title for it.
fn request(url: &str, args: &[String]) -> Result<Option<(FlightOptions, String)>, String> {
    if let Some(query) = value(args, "--sql")? {
        return Ok(Some((FlightOptions::sql(url, &query), query)));
    }
    if let Some(ticket) = value(args, "--ticket")? {
        let title = format!("ticket {ticket}");
        return Ok(Some((FlightOptions::ticket(url, ticket), title)));
    }
    if let Some(path) = value(args, "--path")? {
        let title = format!("path {path}");
        return Ok(Some((FlightOptions::path(url, path.split('/')), title)));
    }
    Ok(value(args, "--command")?.map(|command| {
        let title = format!("command {command}");
        (FlightOptions::command(url, command), title)
    }))
}

/// A header's value: `$NAME` reads the environment variable, so a token need not appear
/// in the command line.
fn header_value(given: &str) -> Result<String, String> {
    match given.strip_prefix('$') {
        Some(name) => std::env::var(name).map_err(|_| format!("${name} is not set")),
        None => Ok(given.to_owned()),
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (options, title) = if args.iter().any(|a| a == "--demo") {
        let url = demo::serve()?;
        let query = "SELECT * FROM requests";
        (FlightOptions::sql(url, query), query.to_owned())
    } else {
        let url = args.iter().enumerate().find(|(i, a)| {
            !a.starts_with("--") && (*i == 0 || !VALUE_FLAGS.contains(&args[i - 1].as_str()))
        });
        let named = match url {
            Some((_, url)) => request(url, &args)?,
            None => None,
        };
        let Some(named) = named else {
            eprintln!(
                "usage: flight-viewer URL (--sql QUERY | --ticket T | --path a/b | --command C) \
                 [--key COLUMN] [--header name:value|name:$VAR]... [--light] [--seconds N], \
                 or --demo"
            );
            std::process::exit(2);
        };
        named
    };
    let key = value(&args, "--key")?;
    let mut options = match &key {
        Some(_) => options,
        None => options.row_numbers(ROW),
    };
    for (at, flag) in args.iter().enumerate() {
        if flag == "--header" {
            let header = args.get(at + 1).ok_or("--header needs a value")?;
            let (name, value) = header.split_once(':').ok_or("--header takes name:value")?;
            options = options.header(name.trim(), header_value(value.trim())?);
        }
    }
    let seconds = value(&args, "--seconds")?
        .map(|s| s.parse::<u64>())
        .transpose()?;
    let viewer = Viewer {
        title,
        options: Some(options),
        key: key.unwrap_or_else(|| ROW.to_owned()),
        light: args.iter().any(|a| a == "--light"),
        close_at: seconds.map(|s| Instant::now() + Duration::from_secs(s)),
        stream: None,
        ids: None,
        state: "asking".to_owned(),
        refused: (0, String::new()),
        status_at: Instant::now(),
    };
    densa_window::run(
        WindowOptions::new("densa flight viewer").with_size(1100.0, 700.0),
        viewer,
    )?;
    Ok(())
}
