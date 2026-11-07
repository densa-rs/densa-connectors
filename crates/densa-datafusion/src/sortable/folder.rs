//! The folders sorted copies are made in: one each, private to the user, holding a lock file
//! locked while its copy is held, so a folder left behind (by a process that is gone, or a
//! write given up whose files could not be removed yet) is found and removed by a later sort.

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// What copies' folders are named after (then the process and a number of its own): what a
/// sweep looks for.
pub(super) const COPY_NAME: &str = "densa-sorted";

/// The file in a copy's folder its owner holds locked, and the copy itself.
const LOCK: &str = "lock";
const COPY: &str = "copy.parquet";

/// A folder held by no process is removed only once its lock is this old: its owner may be
/// between making the folder and locking it.
const SWEEP_AFTER: Duration = Duration::from_secs(60);

/// How long removing a folder is tried for after a write is given up: a thread of the write
/// may hold a file in it for a moment.
const REMOVE_TRIES: u32 = 40;
const REMOVE_WAIT: Duration = Duration::from_millis(50);

/// The next folder's number, in this process.
static NEXT: AtomicU64 = AtomicU64::new(0);

/// A sorted copy's folder, removed with all in it when dropped.
pub(super) struct CopyFolder {
    path: PathBuf,
    /// Held locked while the folder is.
    lock: Option<File>,
}

impl CopyFolder {
    /// A new folder in `parent`, private to the user, its lock held.
    pub(super) fn new(parent: &Path) -> io::Result<Self> {
        let path = loop {
            // Relaxed: only that no two folders of this process get one number matters.
            let number = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = parent.join(format!("{COPY_NAME}-{}-{number}", std::process::id()));
            match private_folder(&path) {
                Ok(()) => break path,
                // Left by a process of this id that is gone: another number.
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        };
        // From here a failure removes the folder (dropped).
        let mut folder = Self { path, lock: None };
        let lock = File::create_new(folder.path.join(LOCK))?;
        lock.lock()?;
        folder.lock = Some(lock);
        Ok(folder)
    }

    /// Where the copy goes.
    pub(super) fn copy(&self) -> PathBuf {
        self.path.join(COPY)
    }

    /// Removes it now, trying for a while (a write given up may still hold a file in it); left
    /// for a later sweep if it still cannot be.
    pub(super) fn remove(mut self) {
        self.lock = None;
        for _ in 0..REMOVE_TRIES {
            if remove_folder(&self.path).is_ok() || !self.path.exists() {
                return;
            }
            std::thread::sleep(REMOVE_WAIT);
        }
    }
}

impl Drop for CopyFolder {
    fn drop(&mut self) {
        // Unlocked first: a folder that cannot be removed now is swept later.
        self.lock = None;
        let _ = remove_folder(&self.path);
    }
}

/// Removes the copies' folders in `parent` that no process holds: left by one that is gone, or
/// by a removal that failed. Only folders named as copies' are (`densa-sorted-1234-5`), holding
/// nothing but a copy's files; what cannot be looked at or removed is left as it is.
pub(super) fn sweep(parent: &Path) {
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let named = entry.file_name().to_str().is_some_and(is_copy_folder);
        if named && entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            let path = entry.path();
            if holds_only_a_copy(&path) && is_free(&path.join(LOCK)) {
                let _ = remove_folder(&path);
            }
        }
    }
}

/// Whether `name` is a copy's folder's: `densa-sorted-`, a process and a number.
fn is_copy_folder(name: &str) -> bool {
    let Some(rest) = name
        .strip_prefix(COPY_NAME)
        .and_then(|rest| rest.strip_prefix('-'))
    else {
        return false;
    };
    let number = |part: Option<&str>| {
        part.is_some_and(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
    };
    let mut parts = rest.split('-');
    number(parts.next()) && number(parts.next()) && parts.next().is_none()
}

/// Whether `name` is of a copy's own files: the copy, or what DataFusion stages it in
/// (`copy.parquet#1`).
fn is_copy_file(name: &str) -> bool {
    name.starts_with(COPY)
}

/// Whether the folder at `path` holds nothing but a copy's files and lock.
fn holds_only_a_copy(path: &Path) -> bool {
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    entries.into_iter().all(|entry| {
        entry.is_ok_and(|entry| {
            let named = entry
                .file_name()
                .to_str()
                .is_some_and(|name| name == LOCK || is_copy_file(name));
            named && entry.file_type().is_ok_and(|kind| kind.is_file())
        })
    })
}

/// Removes a copy's folder: its copy's files first and its lock last, so that a folder that
/// cannot be removed wholly (a file in it held) keeps its lock for a later sweep to find.
fn remove_folder(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name().to_str().is_some_and(is_copy_file) {
            fs::remove_file(entry.path())?;
        }
    }
    match fs::remove_file(path.join(LOCK)) {
        // Never made: the folder failed before it was locked.
        Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
        _ => {}
    }
    fs::remove_dir(path)
}

/// Whether the lock file at `path` is old enough and no process holds it (a lock is let go when
/// its process ends, however it ends).
fn is_free(path: &Path) -> bool {
    let Ok(lock) = File::open(path) else {
        return false;
    };
    let old = lock
        .metadata()
        .and_then(|metadata| metadata.modified())
        .is_ok_and(|at| at.elapsed().is_ok_and(|age| age >= SWEEP_AFTER));
    old && lock.try_lock().is_ok()
}

/// Makes the folder at `path`, which must not be there yet, that only its owner may enter.
#[cfg(unix)]
fn private_folder(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    fs::DirBuilder::new().mode(0o700).create(path)
}

/// Makes the folder at `path`, which must not be there yet (the user's temporary folder already
/// keeps others out).
#[cfg(not(unix))]
fn private_folder(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_folders_named_as_copies_are_taken_for_them() {
        assert!(is_copy_folder("densa-sorted-1234-0"));
        assert!(is_copy_folder("densa-sorted-1-99"));
        for name in [
            "densa-sorted-results",
            "densa-sorted-12",
            "densa-sorted-12-",
            "densa-sorted--3",
            "densa-sorted-1-2-3",
            "densa-sorted-1-x",
            "densa-sorted1-2",
            "other-1-2",
        ] {
            assert!(!is_copy_folder(name), "{name}");
        }
    }

    /// A copy held by another (a write that did not end yet, an indexer) cannot be removed: its
    /// folder keeps its lock, and once free and old enough a sweep removes it.
    #[cfg(windows)]
    #[test]
    fn a_folder_whose_copy_is_held_keeps_its_lock_for_a_later_sweep() {
        use std::os::windows::fs::OpenOptionsExt;
        use std::time::SystemTime;

        let name = format!("densa-folder-held-{}", std::process::id());
        let parent = std::env::temp_dir().join(name);
        let _ = fs::remove_dir_all(&parent);
        fs::create_dir_all(&parent).unwrap();
        let folder = CopyFolder::new(&parent).unwrap();
        let path = folder.path.clone();
        fs::write(folder.copy(), b"copy").unwrap();
        // Shared with no one: it cannot be removed while open.
        let held = fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(folder.copy())
            .unwrap();
        drop(folder);
        assert!(
            path.join(LOCK).exists(),
            "the lock stays while the copy cannot go"
        );
        drop(held);
        let lock = File::options().write(true).open(path.join(LOCK)).unwrap();
        lock.set_modified(SystemTime::now() - SWEEP_AFTER * 2)
            .unwrap();
        drop(lock);
        sweep(&parent);
        assert!(!path.exists(), "swept");
        fs::remove_dir_all(&parent).unwrap();
    }
}
