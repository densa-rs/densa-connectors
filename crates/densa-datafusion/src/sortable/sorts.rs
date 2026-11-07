//! What the sorts of a [`SortablePages`](super::SortablePages) are doing, and which order the
//! app's grid shows: so a status line can tell a sort being made, and a sort the grid no longer
//! shows is given up.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// How long a sort that began while the app said its grid showed another order waits for the
/// app to say this one (its frame may come late), before it is given up.
const GRACE: Duration = Duration::from_secs(2);

/// What the sorts of a [`SortablePages`](super::SortablePages) are doing, and which order its
/// grid shows: from [`SortablePages::sorts`](super::SortablePages::sorts), for the app (any
/// thread).
#[derive(Clone, Default)]
pub struct Sorts(Arc<Mutex<State>>);

/// A sorted copy being made: by which column, since when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sorting {
    column: String,
    since: Instant,
}

impl Sorting {
    /// The column the copy is sorted by.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// When making it began.
    pub fn since(&self) -> Instant {
        self.since
    }
}

#[derive(Default)]
struct State {
    making: Option<Sorting>,
    /// The column the grid is sorted by, as the app last said, and how many times it said
    /// another.
    shown: Option<String>,
    said: u64,
}

/// What the app had said when a sort began, and when that was.
pub(super) struct Began {
    said: u64,
    at: Instant,
}

impl Sorts {
    /// The sorted copy being made, if one is: the grid's rows in its order come once it is.
    pub fn making(&self) -> Option<Sorting> {
        self.state().making.clone()
    }

    /// Says which column the grid is sorted by now (`None`: none), so that a copy being made
    /// by another one is given up: the grid no longer needs it, and its own order is made
    /// sooner. Say it each frame (only a change counts): once an app has said anything, a copy
    /// begun while it says another order is given up too, unless it says this one within two
    /// seconds. An app that never says keeps every sort to its end.
    pub fn showing(&self, column: Option<&str>) {
        let mut state = self.state();
        if state.shown.as_deref() != column {
            state.shown = column.map(str::to_owned);
            state.said += 1;
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A copy by `column` is being made.
    pub(super) fn start(&self, column: &str) -> Began {
        let mut state = self.state();
        let since = Instant::now();
        state.making = Some(Sorting {
            column: column.to_owned(),
            since,
        });
        Began {
            said: state.said,
            at: since,
        }
    }

    pub(super) fn finish(&self) {
        self.state().making = None;
    }

    /// Whether the grid shows another order than `column`'s: said since the sort `began`, or
    /// said before it and not taken back within the grace a late frame needs.
    pub(super) fn given_up(&self, began: &Began, column: &str) -> bool {
        let state = self.state();
        if state.shown.as_deref() == Some(column) {
            return false;
        }
        state.said != began.said || (state.said > 0 && began.at.elapsed() >= GRACE)
    }
}

impl fmt::Debug for Sorts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        f.debug_struct("Sorts")
            .field("making", &state.making)
            .field("shown", &state.shown)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sort of `column` begun `ago`.
    fn begun(sorts: &Sorts, column: &str, ago: Duration) -> Began {
        let began = sorts.start(column);
        Began {
            at: began.at.checked_sub(ago).unwrap(),
            ..began
        }
    }

    #[test]
    fn a_sort_is_given_up_when_told_of_another_order_since_it_began() {
        let sorts = Sorts::default();
        sorts.showing(Some("before"));
        let began = sorts.start("value");
        // What the app said before the sort began does not count yet: its frame may come late.
        assert!(!sorts.given_up(&began, "value"));
        // Nor does saying it again.
        sorts.showing(Some("before"));
        assert!(!sorts.given_up(&began, "value"));
        assert_eq!(sorts.making().unwrap().column(), "value");
        sorts.showing(Some("value"));
        assert!(!sorts.given_up(&began, "value"));
        // Said each frame: only a change counts.
        sorts.showing(Some("value"));
        assert!(!sorts.given_up(&began, "value"));
        sorts.showing(Some("other"));
        assert!(sorts.given_up(&began, "value"));
        sorts.showing(None);
        assert!(sorts.given_up(&began, "value"));
        // Back to its order before the sort ended: kept.
        sorts.showing(Some("value"));
        assert!(!sorts.given_up(&began, "value"));
        sorts.finish();
        assert!(sorts.making().is_none());
    }

    #[test]
    fn a_sort_begun_after_the_app_moved_on_is_given_up_once_its_grace_is_over() {
        let sorts = Sorts::default();
        sorts.showing(Some("later"));
        let fresh = begun(&sorts, "value", Duration::ZERO);
        assert!(!sorts.given_up(&fresh, "value"), "its frame may come late");
        let old = begun(&sorts, "value", GRACE);
        assert!(sorts.given_up(&old, "value"));
        // Said in time, it is kept.
        sorts.showing(Some("value"));
        assert!(!sorts.given_up(&old, "value"));
    }

    #[test]
    fn an_app_that_never_says_keeps_every_sort() {
        let sorts = Sorts::default();
        let old = begun(&sorts, "value", GRACE * 2);
        assert!(!sorts.given_up(&old, "value"));
        assert!(!sorts.given_up(&old, "other"));
    }
}
