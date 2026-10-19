//! Opens a CSV or Parquet file in densa's live grid, loading it on a background thread
//! while the window draws: rows appear as they are read, and the grid sorts by a click at
//! any time.
//!
//! `cargo run --release -p file-viewer -- PATH [--key COLUMN]`: rows are keyed by `COLUMN`
//! (rows with the same key replace each other), or by their row number. `--sample` writes a
//! million-row Parquet file of web requests to the temporary folder and opens it; `--light` for the
//! light theme, `--seconds N` to close after N seconds.

use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use densa_arrow::LiveModel;
use densa_data::LiveTable;
use densa_files::{Loader, OpenOptions};
use densa_ui::{Action, Align, Style, Theme, Ui, WidgetId};
use densa_widgets::{DataGrid, Label, Panel};
use densa_window::{App, Host, WindowOptions};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// The key column added when none is given.
const ROW: &str = "row";
/// How often the status bar is written again while loading.
const STATUS_EVERY: Duration = Duration::from_millis(200);
/// The longest a frame spends taking in batches: the rest wait for the next frame, so the
/// window keeps drawing however fast the file reads.
const FRAME_BUDGET: Duration = Duration::from_millis(8);

type Grid = DataGrid<LiveModel>;

struct Viewer {
    path: PathBuf,
    key: Option<String>,
    light: bool,
    close_at: Option<Instant>,
    loader: Option<Loader>,
    ids: Option<Ids>,
    /// Whether the whole file has been read and taken in.
    done: bool,
    /// Rows in batches the table refused, and why the last one was.
    skipped: u64,
    refused: Option<String>,
    /// Why reading stopped early.
    error: Option<String>,
    status_at: Instant,
}

struct Ids {
    grid: WidgetId,
    status: WidgetId,
}

impl Viewer {
    /// Takes in the batches read so far, for at most a frame's budget. Whether some are
    /// left for the next frame.
    fn take_batches(&mut self, ui: &mut Ui) -> bool {
        let (Some(loader), Some(ids)) = (self.loader.as_mut(), &self.ids) else {
            return false;
        };
        let started = Instant::now();
        while started.elapsed() < FRAME_BUDGET {
            let Some(batch) = loader.try_next() else {
                self.done = loader.is_finished();
                return false;
            };
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    // Nothing comes after an error.
                    self.error = Some(error.to_string());
                    continue;
                }
            };
            // Lossless: a batch's row count.
            let rows = batch.num_rows() as u64;
            let mut refused = None;
            ui.update::<Grid, _>(ids.grid, |grid, cx| {
                refused = grid.model_mut(cx).table_mut().upsert(batch).err();
            });
            if let Some(error) = refused {
                self.skipped += rows;
                self.refused = Some(error.to_string());
            }
        }
        true
    }

    fn show_status(&self, ui: &mut Ui) {
        let Some(ids) = &self.ids else {
            return;
        };
        let total = self.loader.as_ref().and_then(Loader::total_rows);
        // The table's own count: rows sharing a key are one row.
        let rows = ui
            .widget::<Grid>(ids.grid)
            .map_or(0, |grid| grid.model().table().len());
        // Lossless: a row count.
        let rows = grouped(rows as u64);
        let mut text = match (self.done, total) {
            (true, _) => format!("{rows} rows"),
            (false, Some(total)) => format!("Reading: {rows} of {} rows", grouped(total)),
            (false, None) => format!("Reading: {rows} rows"),
        };
        if let Some(refused) = &self.refused {
            text.push_str(&format!(", {} skipped: {refused}", grouped(self.skipped)));
        }
        if let Some(error) = &self.error {
            text.push_str(". Reading stopped: ");
            text.push_str(error);
        }
        ui.update::<Label, _>(ids.status, |label, cx| label.set_text(cx, text));
    }
}

impl App for Viewer {
    fn build(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> Result<(), Box<dyn Error>> {
        if self.light {
            ui.set_theme(Theme::light());
        }
        let options = match &self.key {
            Some(_) => OpenOptions::new(),
            None => OpenOptions::new().row_numbers(ROW),
        };
        let reader = options.open(&self.path)?;
        let key = self.key.as_deref().unwrap_or(ROW);
        let model = LiveModel::new(LiveTable::new(reader.schema(), key)?)?;
        let waker = host.waker();
        self.loader = Some(Loader::spawn(reader, move || {
            waker.wake();
        })?);

        let root = ui.root();
        let bar = Style::row()
            .padding(8.0)
            .gap(12.0)
            .shrink(0.0)
            .align_items(Align::Center);
        let toolbar = ui.add(root, Panel::new(), bar)?;
        let name = self
            .path
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
        ui.add(toolbar, Label::new(name).with_size(15.0), Style::row())?;
        ui.add(
            toolbar,
            Label::new("Click a title to sort").dim(),
            Style::row(),
        )?;
        let grid = ui.add(root, Grid::new(model).with_frozen_columns(1), Grid::style())?;
        let bar = ui.add(root, Panel::new(), Style::row().padding(6.0).shrink(0.0))?;
        let status = ui.add(bar, Label::new("").dim(), Style::row())?;
        self.ids = Some(Ids { grid, status });
        self.show_status(ui);
        Ok(())
    }

    fn action(&mut self, _: &mut Ui, _: &mut Host<'_>, _: Action) {}

    fn frame(&mut self, ui: &mut Ui, host: &mut Host<'_>) -> bool {
        let more = self.take_batches(ui);
        let done = self.done;
        if done || self.status_at.elapsed() >= STATUS_EVERY {
            self.status_at = Instant::now();
            self.show_status(ui);
        }
        if self.close_at.is_some_and(|at| Instant::now() >= at) {
            host.close();
            return false;
        }
        // The loader wakes the window as batches come; batches left over need another frame.
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

/// Writes a million web requests (a service log) to a Parquet file in the temporary folder,
/// returning its path.
fn write_sample() -> Result<PathBuf, Box<dyn Error>> {
    const BATCH: i64 = 65_536;
    let name = format!("densa-sample-requests-{}.parquet", std::process::id());
    let path = std::env::temp_dir().join(name);
    let first = samples::requests(0, BATCH)?;
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(File::create(&path)?, first.schema(), Some(properties))?;
    writer.write(&first)?;
    for start in (BATCH..samples::REQUESTS).step_by(65_536) {
        writer.write(&samples::requests(
            start,
            (start + BATCH).min(samples::REQUESTS),
        )?)?;
    }
    writer.close()?;
    Ok(path)
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
    let flags = ["--key", "--seconds"];
    let path = if args.iter().any(|a| a == "--sample") {
        write_sample()?
    } else {
        let positional = args.iter().enumerate().find(|(i, a)| {
            !a.starts_with("--") && (*i == 0 || !flags.contains(&args[i - 1].as_str()))
        });
        match positional {
            Some((_, path)) => Path::new(path).to_owned(),
            None => {
                eprintln!(
                    "usage: file-viewer PATH [--key COLUMN] [--light] [--seconds N], or --sample"
                );
                std::process::exit(2);
            }
        }
    };
    let seconds = value("--seconds")?.map(|s| s.parse::<u64>()).transpose()?;
    let viewer = Viewer {
        path,
        key: value("--key")?,
        light: args.iter().any(|a| a == "--light"),
        close_at: seconds.map(|s| Instant::now() + Duration::from_secs(s)),
        loader: None,
        ids: None,
        done: false,
        skipped: 0,
        refused: None,
        error: None,
        status_at: Instant::now(),
    };
    densa_window::run(
        WindowOptions::new("densa file viewer").with_size(1100.0, 700.0),
        viewer,
    )?;
    Ok(())
}
