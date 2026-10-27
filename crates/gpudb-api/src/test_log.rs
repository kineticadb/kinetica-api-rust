//! The log records a test's own thread writes, to check what the client
//! logs and at what level.

use std::cell::RefCell;
use std::sync::Once;

thread_local! {
    static RECORDS: RefCell<Option<Vec<(log::Level, String)>>> = const { RefCell::new(None) };
}

struct Capture;

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        RECORDS.with(|records| {
            if let Some(records) = records.borrow_mut().as_mut() {
                records.push((record.level(), record.args().to_string()));
            }
        });
    }

    fn flush(&self) {}
}

/// Runs `f`, returning what it returned and the records logged on this
/// thread meanwhile, at every level.
pub(crate) fn capture<T>(f: impl FnOnce() -> T) -> (T, Vec<(log::Level, String)>) {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        log::set_boxed_logger(Box::new(Capture)).expect("no other logger in the unit tests");
        log::set_max_level(log::LevelFilter::Trace);
    });
    RECORDS.with(|records| *records.borrow_mut() = Some(Vec::new()));
    let result = f();
    let records = RECORDS.with(|records| records.borrow_mut().take().unwrap_or_default());
    (result, records)
}
