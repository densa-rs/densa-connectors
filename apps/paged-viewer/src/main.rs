//! Pages through a SQL result too large to hold, in densa's grid: only the rows on screen are
//! fetched, a page at a time as you scroll, from DataFusion over files or from a Flight SQL
//! service, and a click on a title sorts at the engine.
//!
//! `cargo run --release -p paged-viewer -- --table trips=trips.parquet --key id QUERY` runs the
//! query with DataFusion (`--table name=path` for each Parquet or CSV file, a folder of Parquet
//! files too); `cargo run --release -p paged-viewer -- grpc://HOST:PORT --key id QUERY` asks a
//! Flight SQL service (`--header name:value` for each header; `name:$VAR` reads the value from
//! an environment variable). `--key COLUMN`, once per column, names columns that tell rows
//! apart, so pages keep one order. The query is one SELECT without an ORDER BY of its own:
//! click a title to sort. `--demo` pages a million web requests with DataFusion,
//! `--demo-flight` the same requests from a local Flight SQL service. `--light` for the light
//! theme, `--seconds N` to close after N seconds. Refresh counts and fetches again.

use std::error::Error;
use std::ops::Range;
use std::time::{Duration, Instant};

use densa_arrow::{FetchError, PagedModel};
use densa_datafusion::{Engine, SqlPages};
use densa_flight::{FlightOptions, FlightPages};
use densa_grid::{Column, GridModel, SortOrder};
use densa_ui::{Action, ActionKind, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{Button, DataGrid, Label, Panel};
use densa_window::{App, Host, WindowOptions};

type Grid = DataGrid<PagedModel>;

/// The query the demos page through, and the column that tells its rows apart.
const DEMO_QUERY: &str = "SELECT * FROM requests";
const DEMO_KEY: &str = "request";

/// Where the rows come from.
enum Source {
    /// DataFusion over these tables: a name and a path each.
    Files(Vec<(String, String)>),
    /// DataFusion over the sample requests.
    Requests,
    /// A Flight SQL service, asked with these headers.
    Flight {
        url: String,
        headers: Vec<(String, String)>,
    },
}

/// The query's pages, from one engine or the other.
enum Pages {
    Sql(SqlPages),
    Flight(FlightPages),
}

impl Pages {
    /// Plans the query (or asks the service for its columns): the slow part, done before the
    /// window shows.
    fn open(source: Source, query: &str, keys: &[String]) -> Result<Self, Box<dyn Error>> {
        let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
        if let Source::Flight { url, headers } = source {
            let mut options = FlightOptions::sql(url, query);
            for (name, value) in headers {
                options = options.header(name, value);
            }
            return Ok(Self::Flight(options.paged(&keys)?));
        }
        let engine = Engine::new()?;
        match source {
            Source::Files(tables) => {
                for (name, path) in &tables {
                    register(&engine, name, path)?;
                }
            }
            Source::Requests => register_requests(&engine)?,
            Source::Flight { .. } => {}
        }
        Ok(Self::Sql(engine.pages(query, &keys)?))
    }

    /// A model of the pages, `wake` called on its thread as they arrive.
    fn model(self, wake: impl Fn() + Send + 'static) -> std::io::Result<PagedModel> {
        match self {
            Self::Sql(pages) => PagedModel::new(pages.schema(), pages, wake),
            Self::Flight(pages) => PagedModel::new(pages.schema(), pages, wake),
        }
    }
}

struct Viewer {
    pages: Option<Pages>,
    query: String,
    light: bool,
    close_at: Option<Instant>,
    ids: Option<Ids>,
    /// What the status bar tells now.
    status: Status,
}

#[derive(Clone, Copy)]
struct Ids {
    grid: WidgetId,
    refresh: WidgetId,
    status: WidgetId,
}

/// What the status bar tells: how many rows there are, how many pages are held, which rows are
/// shown, in what order, and why rows are missing, if they are. Written again only when it
/// changes, so a frame allocates nothing otherwise.
#[derive(Debug, Default)]
struct Status {
    rows: Option<u64>,
    pages: usize,
    shown: Range<usize>,
    sort: Option<(usize, SortOrder)>,
    error: Option<FetchError>,
}

impl Status {
    fn of(grid: &Grid) -> Self {
        let model = grid.model();
        Self {
            rows: model.row_count(),
            pages: model.pages_held(),
            shown: shown(grid),
            sort: grid.sort_order(),
            error: model.error().cloned(),
        }
    }

    /// Whether `grid` would tell what this tells (comparing, not copying).
    fn tells(&self, grid: &Grid) -> bool {
        let model = grid.model();
        self.rows == model.row_count()
            && self.pages == model.pages_held()
            && self.shown == shown(grid)
            && self.sort == grid.sort_order()
            && self.error.as_ref() == model.error()
    }

    /// The text, with the model's `columns` and `page_rows`.
    fn text(&self, columns: &[Column], page_rows: usize) -> String {
        if let Some(error) = &self.error {
            return error.to_string();
        }
        let Some(rows) = self.rows else {
            return "Counting the rows...".to_owned();
        };
        let shown = match self.shown.clone() {
            shown if shown.is_empty() => "no rows shown".to_owned(),
            shown => format!(
                "rows {} to {} shown",
                grouped(shown.start as u64),
                grouped(shown.end as u64 - 1)
            ),
        };
        let sorted = self
            .sort
            .and_then(|(column, order)| {
                let name = &columns.get(column)?.title;
                let way = match order {
                    SortOrder::Descending => "largest first",
                    _ => "smallest first",
                };
                Some(format!(" · sorted by {name}, {way}"))
            })
            .unwrap_or_default();
        format!(
            "{} rows · {} pages of {page_rows} held · {shown}{sorted}",
            grouped(rows),
            self.pages
        )
    }
}

/// The rows on screen.
fn shown(grid: &Grid) -> Range<usize> {
    let first = grid.first_row();
    first..(first + grid.visible_rows()).min(grid.model().rows())
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

impl App for Viewer {
    fn build(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> Result<(), Box<dyn Error>> {
        if self.light {
            ui.set_theme(Theme::light());
        }
        // The rows arrive on the model's thread: its wakes bring the frames that show them.
        let waker = host.waker();
        let pages = self.pages.take().ok_or("the pages were taken")?;
        let model = pages.model(move || {
            waker.wake();
        })?;
        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        ui.add(
            toolbar,
            Label::new("A page at a time").with_size(15.0),
            Style::row(),
        )?;
        let refresh = ui.add(toolbar, Button::new("Refresh"), Style::row())?;
        ui.add(toolbar, Label::new(self.query.as_str()).dim(), Style::row())?;
        let grid = ui.add(root, Grid::new(model), Grid::style())?;
        let bar = Style::row().padding(6.0).shrink(0.0);
        let bar = ui.add(root, Panel::new(), bar)?;
        let text = self.status.text(&[], 0);
        let status = ui.add(bar, Label::new(text).dim(), Style::row())?;
        self.ids = Some(Ids {
            grid,
            refresh,
            status,
        });
        Ok(())
    }

    fn action(&mut self, ui: &mut Ui, _: &mut Host<'_>, action: Action) {
        let Some(ids) = self.ids else {
            return;
        };
        if matches!(action.kind, ActionKind::Clicked) && action.source == ids.refresh {
            // Counts and fetches again; what failed is asked for again too.
            ui.update::<Grid, _>(ids.grid, |grid, cx| grid.model_mut(cx).refresh());
        }
    }

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        if self.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        let Some(ids) = self.ids else {
            return false;
        };
        // What arrived since the last frame, so the status bar tells it in this one.
        ui.update::<Grid, _>(ids.grid, |grid, cx| grid.model_mut(cx).prepare());
        let text = match ui.widget::<Grid>(ids.grid) {
            Some(grid) if !self.status.tells(grid) => {
                self.status = Status::of(grid);
                let model = grid.model();
                Some(self.status.text(model.columns(), model.page_rows()))
            }
            _ => None,
        };
        if let Some(text) = text {
            ui.update::<Label, _>(ids.status, |label, cx| label.set_text(cx, text));
        }
        // Frames come when pages arrive (their wakes) or the user acts; continuously only to
        // close by the clock.
        self.close_at.is_some()
    }
}

/// Registers the sample requests as table `requests`.
fn register_requests(engine: &Engine) -> Result<(), Box<dyn Error>> {
    const BATCH: i64 = 65_536;
    let mut batches = Vec::new();
    for start in (0..samples::REQUESTS).step_by(BATCH as usize) {
        batches.push(samples::requests(
            start,
            (start + BATCH).min(samples::REQUESTS),
        )?);
    }
    let schema = samples::requests(0, 0)?.schema();
    engine.register_batches("requests", &schema, &batches)?;
    Ok(())
}

/// Registers the file at `path` as table `name`, Parquet or CSV by its extension.
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

/// Flags followed by a value.
const VALUE_FLAGS: [&str; 4] = ["--table", "--key", "--header", "--seconds"];

/// What the command line asks for.
struct Args {
    source: Source,
    query: String,
    keys: Vec<String>,
    light: bool,
    seconds: Option<u64>,
}

const USAGE: &str = "usage: paged-viewer --table name=path... [--key COLUMN]... QUERY, or \
                     paged-viewer URL [--header name:value|name:$VAR]... [--key COLUMN]... QUERY, \
                     or --demo, or --demo-flight; [--light] [--seconds N]";

fn parse(args: &[String]) -> Result<Args, Box<dyn Error>> {
    let mut tables = Vec::new();
    let mut keys = Vec::new();
    let mut headers = Vec::new();
    let mut seconds = None;
    let mut free = Vec::new();
    let mut at = 0;
    while at < args.len() {
        let arg = args[at].as_str();
        if VALUE_FLAGS.contains(&arg) {
            let value = args
                .get(at + 1)
                .ok_or_else(|| format!("{arg} needs a value"))?;
            match arg {
                "--table" => {
                    let (name, path) = value.split_once('=').ok_or("--table takes name=path")?;
                    tables.push((name.to_owned(), path.to_owned()));
                }
                "--key" => keys.push(value.clone()),
                "--header" => {
                    let (name, given) = value.split_once(':').ok_or("--header takes name:value")?;
                    headers.push((name.trim().to_owned(), header_value(given.trim())?));
                }
                _ => seconds = Some(value.parse::<u64>()?),
            }
            at += 2;
        } else {
            free.push(arg);
            at += 1;
        }
    }
    let light = free.contains(&"--light");
    free.retain(|arg| *arg != "--light");
    let demo = |source: Source| Args {
        source,
        query: DEMO_QUERY.to_owned(),
        keys: vec![DEMO_KEY.to_owned()],
        light,
        seconds,
    };
    match free.as_slice() {
        ["--demo"] => return Ok(demo(Source::Requests)),
        ["--demo-flight"] => {
            let url = samples::flight_sql_server()?;
            return Ok(demo(Source::Flight {
                url,
                headers: Vec::new(),
            }));
        }
        _ => {}
    }
    let source = match free.as_slice() {
        [url, _] if !tables.is_empty() => {
            return Err(format!("{url}: give tables or a Flight SQL service, not both").into())
        }
        [url, _] => Source::Flight {
            url: (*url).to_owned(),
            headers,
        },
        [_] if !tables.is_empty() => Source::Files(tables),
        _ => return Err(USAGE.into()),
    };
    let query = free.last().ok_or(USAGE)?.to_string();
    Ok(Args {
        source,
        query,
        keys,
        light,
        seconds,
    })
}

/// A header's value: `$NAME` reads the environment variable, so a token need not appear in the
/// command line.
fn header_value(given: &str) -> Result<String, String> {
    match given.strip_prefix('$') {
        Some(name) => std::env::var(name).map_err(|_| format!("${name} is not set")),
        None => Ok(given.to_owned()),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(error) = run(&args) {
        eprintln!("paged-viewer: {error}");
        std::process::exit(2);
    }
}

fn run(args: &[String]) -> Result<(), Box<dyn Error>> {
    let args = parse(args)?;
    let pages = Pages::open(args.source, &args.query, &args.keys)?;
    let viewer = Viewer {
        pages: Some(pages),
        query: args.query,
        light: args.light,
        close_at: args
            .seconds
            .map(|s| Instant::now() + Duration::from_secs(s)),
        ids: None,
        status: Status::default(),
    };
    densa_window::run(
        WindowOptions::new("densa paged viewer").with_size(1100.0, 700.0),
        viewer,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(given: &[&str]) -> Vec<String> {
        given.iter().map(|&arg| arg.to_owned()).collect()
    }

    #[test]
    fn the_command_line_names_files_or_a_service_and_the_query() {
        let files = parse(&args(&[
            "--table",
            "t=t.parquet",
            "--key",
            "id",
            "SELECT * FROM t",
        ]))
        .unwrap();
        assert!(matches!(&files.source, Source::Files(tables) if tables[0].0 == "t"));
        assert_eq!(files.query, "SELECT * FROM t");
        assert_eq!(files.keys, ["id"]);
        let flight = parse(&args(&[
            "grpc://h:1",
            "--header",
            "a: b",
            "--light",
            "SELECT 1",
        ]))
        .unwrap();
        assert!(matches!(&flight.source, Source::Flight { url, headers }
            if url == "grpc://h:1" && headers[0] == ("a".to_owned(), "b".to_owned())));
        assert!(flight.light && flight.keys.is_empty());
        let demo = parse(&args(&["--demo", "--seconds", "3"])).unwrap();
        assert!(matches!(demo.source, Source::Requests));
        assert_eq!((demo.query.as_str(), demo.seconds), (DEMO_QUERY, Some(3)));
        assert_eq!(demo.keys, [DEMO_KEY]);
        for wrong in [
            &["SELECT 1"][..],
            &["--table", "t=t.csv", "grpc://h:1", "SELECT 1"][..],
            &["--key"][..],
            &["grpc://h:1", "--header", "a:$DENSA_NOT_SET", "SELECT 1"][..],
        ] {
            assert!(parse(&args(wrong)).is_err(), "{wrong:?}");
        }
    }

    /// Takes in what arrived and asks for rows 0..20, as the grid does, until `done`.
    fn frames(model: &mut PagedModel, done: impl Fn(&PagedModel) -> bool) {
        let start = Instant::now();
        while !done(model) {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "{:?}",
                model.error()
            );
            model.prepare();
            model.showing(0..20);
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    fn first_id(model: &PagedModel) -> String {
        let mut text = densa_grid::CellText::new();
        model.cell(0, 0, &mut text, &densa_grid::Palette::new());
        text.as_str().to_owned()
    }

    #[test]
    fn the_demos_page_through_the_requests_at_datafusion_and_a_flight_sql_service() {
        let flight = samples::flight_sql_server_of(10_000).unwrap();
        for (source, rows) in [
            (Source::Requests, samples::REQUESTS as u64),
            (
                Source::Flight {
                    url: flight,
                    headers: Vec::new(),
                },
                10_000,
            ),
        ] {
            let keys = [DEMO_KEY.to_owned()];
            let pages = Pages::open(source, DEMO_QUERY, &keys).unwrap();
            let mut model = pages.model(|| {}).unwrap();
            frames(&mut model, |m| m.is_fetched(19));
            assert_eq!(model.row_count(), Some(rows));
            assert_eq!(first_id(&model), "0");
            assert!(model.sort(0, SortOrder::Descending));
            frames(&mut model, |m| m.is_fetched(0));
            assert_eq!(first_id(&model), (rows - 1).to_string());
        }
    }

    #[test]
    fn a_query_the_engine_cannot_page_is_refused_before_the_window() {
        let keys = [DEMO_KEY.to_owned()];
        let ordered = "SELECT * FROM requests ORDER BY ms DESC";
        let error = Pages::open(Source::Requests, ordered, &keys)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("sort the model"), "{error}");
    }
}
