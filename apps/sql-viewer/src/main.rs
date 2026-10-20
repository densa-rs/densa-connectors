//! Runs SQL with DataFusion over Parquet and CSV files and shows the result in densa's live
//! grid as it is computed. Type a query and press Enter to run it; a click on a title sorts
//! the result.
//!
//! `cargo run --release -p sql-viewer -- --table requests=requests.parquet [QUERY]`
//! (`--table name=path` for each file, Parquet or CSV by its extension; `--query Q` for a
//! query that starts with `--`), `--max-rows N` to cut results off (a million by default),
//! `--light` for the light theme, `--seconds N` to close after N seconds. `--demo` registers
//! a million web requests as `requests` and runs a query over them. `--live` streams requests
//! into a live table instead (the last 200,000 kept) and runs the query again each second
//! over a snapshot of it. Queries only read.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::UInt64Array;
use arrow_schema::{DataType, Field, Schema};
use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_datafusion::{Engine, QueryEvent, QueryStream};
use densa_ui::{Action, ActionKind, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{DataGrid, Label, Panel, TextInput};
use densa_window::{App, Host, Waker, WindowOptions};

/// The key column every result gets: rows are numbered as they arrive.
const ROW: &str = "row";
/// How often the status bar is written again.
const STATUS_EVERY: Duration = Duration::from_millis(200);
/// The longest a frame spends taking in batches: the rest wait for the next frame.
const FRAME_BUDGET: Duration = Duration::from_millis(8);
/// Rows shown at most: a result past this is cut off (its query cancelled), so a `SELECT *`
/// over a large table does not fill memory.
const DEFAULT_MAX_ROWS: usize = 1_000_000;
/// The demo's query.
const DEMO_QUERY: &str = "SELECT service, status, count(*) AS requests, \
    round(avg(ms), 2) AS avg_ms, max(ms) AS max_ms \
    FROM requests GROUP BY service, status ORDER BY requests DESC";
/// The live demo's query: the latest requests per service.
const LIVE_QUERY: &str = "SELECT service, count(*) AS requests, \
    round(avg(ms), 2) AS avg_ms, sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors, \
    max(time) AS latest FROM requests GROUP BY service ORDER BY service";
/// Requests the live demo adds each frame.
const LIVE_PER_FRAME: i64 = 2_000;
/// Requests the live table keeps: older ones are deleted.
const LIVE_WINDOW: i64 = 200_000;
/// How often the live demo's query runs again.
const LIVE_REFRESH: Duration = Duration::from_secs(1);

type Grid = DataGrid<LiveModel>;

struct Viewer {
    engine: Engine,
    /// The tables registered, for the title bar.
    tables: String,
    query: String,
    max_rows: usize,
    light: bool,
    close_at: Option<Instant>,
    waker: Option<Waker>,
    stream: Option<QueryStream>,
    ids: Option<Ids>,
    /// Where the query stands, and when it started.
    state: String,
    started: Instant,
    status_at: Instant,
    live: Option<Live>,
    /// The result's rows so far: a query run again updates them in place, then drops those
    /// past its own count.
    rows: u64,
    /// Whether the query running is the last one run again (its rows replace the last
    /// result's in place, so the grid keeps its scroll and selection).
    again: bool,
}

/// The live demo's requests, and when the query last ran over them.
struct Live {
    table: LiveTable,
    next: i64,
    ran: Instant,
}

struct Ids {
    input: WidgetId,
    grid: WidgetId,
    status: WidgetId,
}

impl Viewer {
    /// Runs the query in the input, replacing the one running (and its rows).
    fn run(&mut self, ui: &mut Ui) {
        let Some(ids) = &self.ids else {
            return;
        };
        if let Some(input) = ui.widget::<TextInput>(ids.input) {
            self.query = input.text().trim().to_owned();
        }
        self.again = false;
        self.start();
    }

    /// Starts the query in `self.query`.
    fn start(&mut self) {
        // Dropping the stream cancels the query running.
        self.stream = None;
        if self.query.is_empty() {
            self.state = "type a query and press Enter".to_owned();
            return;
        }
        let waker = self.waker.clone();
        let stream = self
            .engine
            .sql(self.query.clone())
            .row_numbers(ROW)
            .run(move || {
                if let Some(waker) = &waker {
                    waker.wake();
                }
            });
        self.stream = Some(stream);
        self.state = "running".to_owned();
        self.started = Instant::now();
        self.rows = 0;
    }

    /// Adds the live demo's next requests, drops the oldest, and runs the query again over a
    /// snapshot each `LIVE_REFRESH`, once the last run is done.
    fn feed(&mut self) {
        let Some(live) = &mut self.live else {
            return;
        };
        let (from, to) = (live.next, live.next + LIVE_PER_FRAME);
        live.next = to;
        if let Ok(batch) = samples::requests(from, to) {
            if let Err(error) = live.table.upsert(batch) {
                self.state = format!("requests refused: {error}");
            }
        }
        let gone = (from - LIVE_WINDOW).max(0)..(to - LIVE_WINDOW).max(0);
        if !gone.is_empty() {
            let _ = live
                .table
                .delete(&arrow_array::Int64Array::from_iter_values(gone));
        }
        if self.stream.is_some() || live.ran.elapsed() < LIVE_REFRESH {
            return;
        }
        live.ran = Instant::now();
        let registered = live
            .table
            .snapshot()
            .map_err(|e| e.to_string())
            .and_then(|batches| {
                let _ = self.engine.deregister("requests");
                let schema = Arc::clone(live.table.schema());
                self.engine
                    .register_batches("requests", &schema, &batches)
                    .map_err(|e| e.to_string())
            });
        match registered {
            Ok(()) => {
                self.again = true;
                self.start();
            }
            Err(error) => self.state = format!("snapshot not registered: {error}"),
        }
    }

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
                QueryEvent::Schema(schema) => {
                    let mut error = None;
                    let again = self.again;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        // The same query again, with the same columns: its rows replace the
                        // last ones in place. Otherwise a new table: the last rows go.
                        let same = |table: &LiveTable| {
                            let names = |s: &Schema| -> Vec<(String, DataType)> {
                                let fields = s.fields().iter();
                                fields
                                    .map(|f| (f.name().clone(), f.data_type().clone()))
                                    .collect()
                            };
                            names(table.schema()) == names(&schema)
                        };
                        if again && same(grid.model().table()) {
                            return;
                        }
                        match LiveTable::new(schema, ROW) {
                            Ok(fresh) => *grid.model_mut(cx).table_mut() = fresh,
                            Err(e) => error = Some(e.to_string()),
                        }
                    });
                    if let Some(error) = error {
                        self.state = format!("cannot show the result: {error}");
                    }
                }
                QueryEvent::Batch(batch) => {
                    self.rows += batch.num_rows() as u64;
                    let mut refused = None;
                    let mut shown = 0;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        let table = grid.model_mut(cx).table_mut();
                        refused = table.upsert(batch).err();
                        shown = table.len();
                    });
                    if let Some(error) = refused {
                        self.state = format!("rows refused: {error}");
                    }
                    if shown >= self.max_rows {
                        // Dropping the stream cancels the query.
                        self.stream = None;
                        self.state = format!("cut off at {} rows (--max-rows)", self.max_rows);
                        return false;
                    }
                }
                QueryEvent::Finished => {
                    // Rows past this result's count are the last result's.
                    let rows = self.rows;
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        let table = grid.model_mut(cx).table_mut();
                        let stale = rows..table.len() as u64;
                        if !stale.is_empty() {
                            let _ = table.delete(&UInt64Array::from_iter_values(stale));
                        }
                    });
                    let seconds = self.started.elapsed().as_secs_f64();
                    self.state = format!("done in {seconds:.2} s");
                    self.stream = None;
                    return false;
                }
                QueryEvent::Failed { reason } => {
                    // The rows shown were the last result's: they go, so they are not taken
                    // for this one's.
                    ui.update::<Grid, _>(ids.grid, |grid, cx| {
                        if let Ok(empty) = LiveTable::new(waiting(), ROW) {
                            *grid.model_mut(cx).table_mut() = empty;
                        }
                    });
                    let reason = reason.split_whitespace().collect::<Vec<_>>().join(" ");
                    self.state = format!("failed: {reason}");
                    // Done: the live demo may run it again.
                    self.stream = None;
                    return false;
                }
                _ => {}
            }
        }
        true
    }

    fn show_status(&self, ui: &mut Ui) {
        let Some(ids) = &self.ids else {
            return;
        };
        let rows = ui
            .widget::<Grid>(ids.grid)
            .map_or(0, |grid| grid.model().table().len());
        let text = format!("{}: {rows} rows", self.state);
        ui.update::<Label, _>(ids.status, |label, cx| label.set_text(cx, text));
    }
}

/// Columns come with a result: until then, an empty table of the key alone.
fn waiting() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(ROW, DataType::UInt64, false)]))
}

impl App for Viewer {
    fn build(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> Result<(), Box<dyn Error>> {
        if self.light {
            ui.set_theme(Theme::light());
        }
        self.waker = Some(host.waker());
        let model = LiveModel::new(LiveTable::new(waiting(), ROW)?)?;

        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        ui.add(toolbar, Label::new("SQL").with_size(15.0), Style::row())?;
        let input = TextInput::new(self.query.clone()).with_placeholder("SELECT ... and Enter");
        let input = ui.add(toolbar, input, Style::row().grow(1.0))?;
        ui.add(toolbar, Label::new(self.tables.clone()).dim(), Style::row())?;
        let grid = ui.add(root, Grid::new(model), Grid::style())?;
        let bar = ui.add(root, Panel::new(), Style::row().padding(6.0).shrink(0.0))?;
        let status = ui.add(bar, Label::new("").dim(), Style::row())?;
        self.ids = Some(Ids {
            input,
            grid,
            status,
        });
        self.run(ui);
        self.show_status(ui);
        Ok(())
    }

    fn action(&mut self, ui: &mut Ui, _: &mut Host<'_>, action: Action) {
        let submitted = self.ids.as_ref().is_some_and(|ids| {
            action.source == ids.input && matches!(action.kind, ActionKind::Submitted)
        });
        if submitted {
            self.run(ui);
            self.show_status(ui);
        }
    }

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        self.feed();
        let more = self.take(ui);
        if self.status_at.elapsed() >= STATUS_EVERY || !more {
            self.status_at = Instant::now();
            self.show_status(ui);
        }
        if self.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        // The query wakes the window as events come; events left over need another frame,
        // and the live demo's requests keep coming.
        more || self.close_at.is_some() || self.live.is_some()
    }
}

/// Registers the demo's requests as `requests`.
fn register_demo(engine: &Engine) -> Result<(), Box<dyn Error>> {
    const BATCH: i64 = 65_536;
    let mut batches = Vec::new();
    for start in (0..samples::REQUESTS).step_by(65_536) {
        batches.push(samples::requests(
            start,
            (start + BATCH).min(samples::REQUESTS),
        )?);
    }
    let schema = batches[0].schema();
    engine.register_batches("requests", &schema, &batches)?;
    Ok(())
}

/// What the command line asks for.
struct Args {
    tables: Vec<(String, String)>,
    query: Option<String>,
    demo: bool,
    live: bool,
    light: bool,
    seconds: Option<u64>,
    max_rows: usize,
}

const USAGE: &str = "usage: sql-viewer --table name=path... [QUERY] [--max-rows N] [--light] \
                     [--seconds N], or --demo, or --live";

/// Reads the command line. A query may start with `--` (an SQL comment): anything after
/// `--query` is taken whole.
fn parse(args: &[String]) -> Result<Args, String> {
    let mut parsed = Args {
        tables: Vec::new(),
        query: None,
        demo: false,
        live: false,
        light: false,
        seconds: None,
        max_rows: DEFAULT_MAX_ROWS,
    };
    let mut at = 0;
    let value = |at: usize, flag: &str| {
        args.get(at + 1)
            .cloned()
            .ok_or_else(|| format!("{flag} needs a value"))
    };
    while at < args.len() {
        match args[at].as_str() {
            "--table" => {
                let spec = value(at, "--table")?;
                let (name, path) = spec.split_once('=').ok_or("--table takes name=path")?;
                parsed.tables.push((name.to_owned(), path.to_owned()));
                at += 2;
            }
            "--query" => {
                parsed.query = Some(value(at, "--query")?);
                at += 2;
            }
            "--seconds" => {
                let text = value(at, "--seconds")?;
                parsed.seconds = Some(text.parse().map_err(|_| format!("--seconds {text}?"))?);
                at += 2;
            }
            "--max-rows" => {
                let text = value(at, "--max-rows")?;
                parsed.max_rows = text.parse().map_err(|_| format!("--max-rows {text}?"))?;
                at += 2;
            }
            "--demo" => {
                parsed.demo = true;
                at += 1;
            }
            "--live" => {
                parsed.live = true;
                at += 1;
            }
            "--light" => {
                parsed.light = true;
                at += 1;
            }
            flag if flag.starts_with("--") => return Err(format!("unknown option {flag}")),
            text => {
                if parsed.query.replace(text.to_owned()).is_some() {
                    return Err("one query at a time (quote it as one argument)".to_owned());
                }
                at += 1;
            }
        }
    }
    Ok(parsed)
}

/// Registers `path` as `name`, by its extension.
fn register(engine: &Engine, name: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".csv") {
        engine.register_csv(name, path)?;
    } else if lower.ends_with(".parquet")
        || lower.ends_with(".parq")
        || lower.ends_with(".pq")
        || std::path::Path::new(path).is_dir()
    {
        engine.register_parquet(name, path)?;
    } else {
        return Err(
            format!("{path}: give a .csv or .parquet file (or a folder of Parquet)").into(),
        );
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args = match parse(&args) {
        Ok(args) if args.demo || args.live || !args.tables.is_empty() => args,
        Ok(_) => return Err(USAGE.into()),
        Err(error) => return Err(format!("{error}\n{USAGE}").into()),
    };
    let engine = Engine::new()?;
    let mut names = Vec::new();
    let mut query = args.query;
    if args.demo {
        register_demo(&engine)?;
        names.push("requests".to_owned());
        query.get_or_insert_with(|| DEMO_QUERY.to_owned());
    }
    let mut live = None;
    if args.live {
        let schema = samples::requests(0, 0)?.schema();
        let table = LiveTable::new(schema, "request")?;
        live = Some(Live {
            table,
            next: 0,
            ran: Instant::now() - LIVE_REFRESH,
        });
        names.push("requests (live)".to_owned());
        query.get_or_insert_with(|| LIVE_QUERY.to_owned());
    }
    for (name, path) in &args.tables {
        register(&engine, name, path)?;
        names.push(name.clone());
    }
    let viewer = Viewer {
        engine,
        tables: format!("tables: {}", names.join(", ")),
        query: query.unwrap_or_default(),
        max_rows: args.max_rows,
        light: args.light,
        close_at: args
            .seconds
            .map(|s| Instant::now() + Duration::from_secs(s)),
        waker: None,
        stream: None,
        ids: None,
        state: String::new(),
        started: Instant::now(),
        status_at: Instant::now(),
        live,
        rows: 0,
        again: false,
    };
    densa_window::run(
        WindowOptions::new("densa sql viewer").with_size(1100.0, 700.0),
        viewer,
    )?;
    Ok(())
}
