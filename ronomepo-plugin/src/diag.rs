//! Persistent diagnostics log and main-thread stall detection.
//!
//! Lines go to `$XDG_STATE_HOME/ronomepo/ronomepo.log` (default
//! `~/.local/state/ronomepo/ronomepo.log`), rotated to `ronomepo.log.1` once the
//! file exceeds `MAX_LOG_BYTES`.

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use gtk::glib;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;
const HEARTBEAT_INTERVAL_MILLIS: u64 = 100;
const STALL_CHECK_INTERVAL_MILLIS: u64 = 200;
const STALL_THRESHOLD_MILLIS: u64 = 1000;
const SLOW_SECTION_MILLIS: u128 = 100;

struct LogFile {
    path: PathBuf,
    file: Option<File>,
    /// Current size, tracked so logging a line needs no fstat.
    len: u64,
}

static LOG_FILE: OnceLock<Mutex<LogFile>> = OnceLock::new();
static EPOCH: OnceLock<Instant> = OnceLock::new();
static MAIN_HEARTBEAT_MILLIS: AtomicU64 = AtomicU64::new(0);
static WATCHDOG_STARTED: AtomicBool = AtomicBool::new(false);
static MAIN_ACTIVITY: Mutex<Option<&'static str>> = Mutex::new(None);

fn log_path() -> PathBuf {
    let mut path = if let Ok(dir) = env::var("XDG_STATE_HOME") {
        PathBuf::from(dir)
    } else if let Ok(home) = env::var("HOME") {
        PathBuf::from(home).join(".local").join("state")
    } else {
        env::temp_dir()
    };
    path.push("ronomepo");
    path.push("ronomepo.log");
    path
}

/// Opens the log for appending and returns it with its current size.
fn open_log(path: &PathBuf) -> (Option<File>, u64) {
    // Unit tests must not write into the user's real diagnostics log.
    if cfg!(test) {
        return (None, 0);
    }
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let file = OpenOptions::new().create(true).append(true).open(path).ok();
    let len = file
        .as_ref()
        .and_then(|file| file.metadata().ok())
        .map_or(0, |meta| meta.len());
    (file, len)
}

fn log_file() -> &'static Mutex<LogFile> {
    LOG_FILE.get_or_init(|| {
        let path = log_path();
        let (file, len) = open_log(&path);
        Mutex::new(LogFile { path, file, len })
    })
}

fn timestamp() -> String {
    glib::DateTime::now_local()
        .and_then(|now| now.format("%F %T.%f"))
        .map(|stamp| stamp.to_string())
        .unwrap_or_else(|_| "----".to_string())
}

/// Appends one line to the persistent diagnostics log. Never panics.
pub fn log(message: impl AsRef<str>) {
    let thread = thread::current();
    let line = format!(
        "{} [{}] {}\n",
        timestamp(),
        thread.name().unwrap_or("?"),
        message.as_ref()
    );
    let Ok(mut log) = log_file().lock() else {
        return;
    };
    if log.file.is_some() && log.len > MAX_LOG_BYTES {
        let rotated = log.path.with_extension("log.1");
        let _ = fs::rename(&log.path, rotated);
        (log.file, log.len) = open_log(&log.path);
    }
    if let Some(file) = log.file.as_mut() {
        if file.write_all(line.as_bytes()).is_ok() {
            log.len += line.len() as u64;
        }
    }
}

fn now_millis() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

fn current_activity() -> &'static str {
    MAIN_ACTIVITY
        .lock()
        .ok()
        .and_then(|activity| *activity)
        .unwrap_or("idle/gtk")
}

/// Starts a main-loop heartbeat plus a watchdog thread that logs whenever the
/// GTK main thread stops servicing its loop for longer than the threshold,
/// naming the instrumented section it was in (if any).
pub fn start_main_thread_watchdog() {
    if WATCHDOG_STARTED.swap(true, Ordering::SeqCst) {
        return;
    }
    log(format!(
        "diagnostics started pid={} log={}",
        std::process::id(),
        log_path().display()
    ));
    MAIN_HEARTBEAT_MILLIS.store(now_millis(), Ordering::Relaxed);
    glib::timeout_add_local(Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS), || {
        MAIN_HEARTBEAT_MILLIS.store(now_millis(), Ordering::Relaxed);
        glib::ControlFlow::Continue
    });
    let _ = thread::Builder::new()
        .name("ronomepo-watchdog".to_string())
        .spawn(|| {
            let mut stalled_since: Option<u64> = None;
            let mut stall_activity = "";
            loop {
                thread::sleep(Duration::from_millis(STALL_CHECK_INTERVAL_MILLIS));
                let now = now_millis();
                let last = MAIN_HEARTBEAT_MILLIS.load(Ordering::Relaxed);
                let age = now.saturating_sub(last);
                match stalled_since {
                    None if age > STALL_THRESHOLD_MILLIS => {
                        stalled_since = Some(last);
                        stall_activity = current_activity();
                        log(format!(
                            "MAIN THREAD STALL started ({age}ms without heartbeat) activity={stall_activity}"
                        ));
                    }
                    Some(since) if age <= STALL_THRESHOLD_MILLIS => {
                        log(format!(
                            "MAIN THREAD STALL ended after ~{}ms activity_at_start={stall_activity}",
                            last.saturating_sub(since)
                        ));
                        stalled_since = None;
                    }
                    Some(since) => {
                        // Re-report long stalls every ~5s so a hang is visible in the log.
                        let duration = now.saturating_sub(since);
                        if duration / 5000 != duration.saturating_sub(STALL_CHECK_INTERVAL_MILLIS) / 5000 {
                            log(format!(
                                "MAIN THREAD STALL ongoing {duration}ms activity={}",
                                current_activity()
                            ));
                        }
                    }
                    None => {}
                }
            }
        });
}

/// Times a section; logs it when slow. On the main thread it also labels the
/// section so the watchdog can attribute stalls to it.
pub struct Timed {
    label: &'static str,
    detail: String,
    start: Instant,
    previous_activity: Option<Option<&'static str>>,
}

pub fn timed(label: &'static str) -> Timed {
    timed_with(label, String::new())
}

pub fn timed_with(label: &'static str, detail: String) -> Timed {
    let previous_activity = if glib::MainContext::default().is_owner() {
        MAIN_ACTIVITY
            .lock()
            .ok()
            .map(|mut activity| activity.replace(label))
    } else {
        None
    };
    Timed {
        label,
        detail,
        start: Instant::now(),
        previous_activity,
    }
}

impl Timed {
    pub fn set_detail(&mut self, detail: String) {
        self.detail = detail;
    }
}

impl Drop for Timed {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_millis();
        if let Some(previous) = self.previous_activity.take() {
            if let Ok(mut activity) = MAIN_ACTIVITY.lock() {
                *activity = previous;
            }
        }
        if elapsed >= SLOW_SECTION_MILLIS {
            log(format!(
                "slow {} took {elapsed}ms {}",
                self.label, self.detail
            ));
        }
    }
}
