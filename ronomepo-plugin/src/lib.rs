use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime};

use gtk::gdk::{Rectangle, RGBA};
use gtk::gio;
use gtk::glib::{
    self,
    translate::{FromGlibPtrFull, IntoGlibPtr},
    BoxedAnyObject,
};
use gtk::pango::EllipsizeMode;
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, CheckButton, CustomFilter, CustomSorter, Dialog, DropDown,
    Entry, EventControllerMotion, FilterChange, GestureClick, Image, Label, ListBox, ListBoxRow,
    Orientation, Paned, PolicyType, Popover, PositionType, ResponseType, ScrolledWindow,
    SelectionMode, Separator, SortListModel, SorterChange, TextBuffer, TextView, ToggleButton,
    Window, WrapMode,
};
use maruzzella_sdk::{
    attach_text_tooltip, button_css_class, export_plugin, input_css_class, surface_css_class,
    text_css_class,
    CommandSpec, HostApi, MzLogLevel, MzStatusCode, MzToolbarDisplayMode, MzViewOpenDisposition,
    MzViewPlacement, OpenViewRequest, Plugin, PluginDependency, PluginDescriptor,
    SurfaceContributionSpec, ToolbarWidgetSpec, Version, ViewFactorySpec,
};
use notify::{Config as NotifyConfig, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use ronomepo_core::{
    build_repository_list, collect_commit_check_report, collect_repository_details,
    collect_workspace_line_stats, default_commit_check_rules, default_manifest_path,
    derive_dir_name, ensure_commit_check_rules_initialized, format_sync_label, import_repos_txt,
    list_repo_artifacts, load_manifest, load_repo_manifest, normalize_workspace_root,
    plan_repo_action, run_workspace_operation, save_manifest, scan_repo_manifest,
    verify_repo_dependencies_freshness, workspace_summary, CommitCheckRule, CommitCheckRuleEffect,
    CommitCheckRuleMatcher, CommitCheckRuleScope, OperationEvent, OperationEventKind,
    OperationKind, PlannedCommand, RepoActionExecutor, RepoManifestScan, RepoManifestScanState,
    RepositoryDetails, RepositoryEntry, RepositoryListItem, RepositoryStatus, StandardActionName,
    WorkspaceManifest, MANIFEST_FILE_NAME,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "embedded-terminal")]
use vte4::prelude::*;

const PLUGIN_ID: &str = "com.lelloman.ronomepo";
const VIEW_REPO_MONITOR: &str = "com.lelloman.ronomepo.repo_monitor";
const VIEW_MONOREPO_OVERVIEW: &str = "com.lelloman.ronomepo.monorepo_overview";
const VIEW_REPO_OVERVIEW: &str = "com.lelloman.ronomepo.repo_overview";
const VIEW_COMMIT_CHECK: &str = "com.lelloman.ronomepo.commit_check";
const VIEW_WORKSPACE_SETTINGS: &str = "com.lelloman.ronomepo.workspace_settings";
const VIEW_TEXT_EDITOR: &str = "com.lelloman.ronomepo.text_editor";
const VIEW_OPERATIONS: &str = "com.lelloman.ronomepo.operations";
const CMD_REFRESH: &str = "ronomepo.workspace.refresh";
const CMD_PULL: &str = "ronomepo.workspace.pull";
const CMD_PUSH: &str = "ronomepo.workspace.push";
const CMD_OPEN_OVERVIEW: &str = "ronomepo.workspace.open_overview";
const CMD_OPEN_COMMIT_CHECK: &str = "ronomepo.workspace.open_commit_check";
const CMD_FILTER: &str = "ronomepo.workspace.filter";
const CMD_ADD_REPO: &str = "ronomepo.workspace.add_repo";
const CMD_EXIT: &str = "ronomepo.workspace.exit";
const CMD_REFRESH_LOGS: &str = "ronomepo.logs.refresh";
const CMD_CLEAR_LOGS: &str = "ronomepo.logs.clear";
const MONITOR_NAME_COL_CHARS: i32 = 28;
const MONITOR_BRANCH_COL_CHARS: i32 = 14;
const MONITOR_MANIFEST_COL_CHARS: i32 = 12;
const MONITOR_STATE_COL_CHARS: i32 = 12;
const MONITOR_NAME_COL_WIDTH: i32 = 300;
const MONITOR_BRANCH_COL_WIDTH: i32 = 120;
const MONITOR_MANIFEST_COL_WIDTH: i32 = 120;
const MONITOR_STATE_COL_WIDTH: i32 = 120;
const WORKER_POOL_SIZE: usize = 4;
const LOCAL_RESCAN_INTERVAL_SECS: u32 = 5 * 60;
const REMOTE_FETCH_TICK_SECS: u32 = 30;
const REMOTE_FETCH_INTERVAL_SECS: u64 = 60 * 60;
const REMOTE_FETCH_JITTER_SECS: u64 = 30 * 60;
const REMOTE_FETCH_CONCURRENCY: usize = 1;
const WATCH_POLL_FALLBACK_SECS: u64 = 15;
const MAX_PENDING_WATCH_PATHS: usize = 4_096;
const UI_REFRESH_DEBOUNCE_MILLIS: u64 = 75;
const MAX_LOG_ENTRIES: usize = 500;
const RUNTIME_PROFILE_ENV: &str = "RONOMEPO_PROFILE";

pub struct RonomepoPlugin;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RonomepoPluginConfig {
    last_workspace_path: Option<String>,
    import_banner_dismissed: bool,
    #[serde(default)]
    monitor_filter_mode: MonitorFilterMode,
    #[serde(default)]
    monitor_sort_mode: MonitorSortMode,
    #[serde(default)]
    monitor_sort_descending: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MonitorFilterMode {
    #[default]
    All,
    Dirty,
    ToSync,
    Issues,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MonitorSortMode {
    #[default]
    Name,
    SyncState,
    RecentActivity,
}

#[derive(Clone, Debug)]
struct AppState {
    workspace_root: PathBuf,
    manifest_path: Option<PathBuf>,
    manifest: Option<WorkspaceManifest>,
    workspace_status: RepositoryStatus,
    repository_items: Vec<RepositoryListItem>,
    repository_items_loading: bool,
    repository_items_refresh_pending: bool,
    repo_details_cache: HashMap<String, RepositoryDetails>,
    repo_details_loading: HashSet<String>,
    monitor_filter: String,
    monitor_filter_mode: MonitorFilterMode,
    monitor_sort_mode: MonitorSortMode,
    monitor_sort_descending: bool,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    next_operation_batch: usize,
    history_report: Vec<String>,
    history_report_loading: bool,
    line_stats_report: Vec<String>,
    line_stats_loading: bool,
    line_stats_since: String,
    repo_runtime: HashMap<String, RepoRuntimeState>,
    watch_manager_sync_in_flight: bool,
    watch_manager_sync_pending: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            workspace_root: PathBuf::new(),
            manifest_path: None,
            manifest: None,
            workspace_status: empty_repository_status(PathBuf::new()),
            repository_items: Vec::new(),
            repository_items_loading: false,
            repository_items_refresh_pending: false,
            repo_details_cache: HashMap::new(),
            repo_details_loading: HashSet::new(),
            monitor_filter: String::new(),
            monitor_filter_mode: MonitorFilterMode::default(),
            monitor_sort_mode: MonitorSortMode::default(),
            monitor_sort_descending: false,
            selected_repo_ids: Vec::new(),
            active_repo_id: None,
            logs: Vec::new(),
            next_operation_batch: 0,
            history_report: Vec::new(),
            history_report_loading: false,
            line_stats_report: Vec::new(),
            line_stats_loading: false,
            line_stats_since: String::new(),
            repo_runtime: HashMap::new(),
            watch_manager_sync_in_flight: false,
            watch_manager_sync_pending: false,
        }
    }
}

#[derive(Clone, Debug)]
struct RepoRuntimeState {
    invalidation_seq: u64,
    scheduled_scan_seq: u64,
    last_scanned_seq: u64,
    local_refresh_in_flight: bool,
    remote_fetch_in_flight: bool,
    last_local_scan_at: Option<SystemTime>,
    last_fetch_at: Option<SystemTime>,
    next_fetch_due_at: SystemTime,
}

impl RepoRuntimeState {
    fn new(now: SystemTime, repo_id: &str) -> Self {
        Self {
            invalidation_seq: 0,
            scheduled_scan_seq: 0,
            last_scanned_seq: 0,
            local_refresh_in_flight: false,
            remote_fetch_in_flight: false,
            last_local_scan_at: None,
            last_fetch_at: None,
            next_fetch_due_at: next_remote_fetch_due_at(now, repo_id),
        }
    }

    fn needs_rescan(&self) -> bool {
        self.invalidation_seq > self.last_scanned_seq
    }
}

enum WatchBackend {
    Recommended(RecommendedWatcher),
    Poll(PollWatcher),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatchBackendKind {
    None,
    Recommended,
    Poll,
}

struct WatchManager {
    _backend: WatchBackend,
    backend_kind: WatchBackendKind,
    watched_paths: usize,
}

#[derive(Default)]
struct PendingWatchEvents {
    paths: HashSet<PathBuf>,
    flush_scheduled: bool,
    overflowed: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum JobKey {
    WorkspaceScan,
    WorkspaceRootStatus,
    RepoStatus(String),
    RepoFetch(String),
    RepoDetails(String),
    WatchManagerSync,
    HistoryReport,
    LineStats(Option<String>),
}

struct ExecutorState {
    sender: mpsc::Sender<QueuedJob>,
    in_flight: Mutex<HashSet<JobKey>>,
}

struct QueuedJob {
    key: Option<JobKey>,
    job: WorkerJob,
}

enum WorkerJob {
    OperationBatch {
        manifest: WorkspaceManifest,
        selected_repo_ids: Vec<String>,
        kind: OperationKind,
        batch_id: usize,
    },
    WorkspaceScan {
        workspace_root: PathBuf,
        manifest: Option<WorkspaceManifest>,
    },
    WorkspaceRootStatusRefresh {
        workspace_root: PathBuf,
    },
    RepositoryStatusRefresh {
        item: RepositoryListItem,
    },
    RepositoryRemoteFetch {
        repo_id: String,
        repo_name: String,
        repo_path: PathBuf,
    },
    RepoDetailsLoad {
        repo_id: String,
        repo_path: PathBuf,
    },
    WatchManagerSync {
        manifest: Option<WorkspaceManifest>,
        sync_seq: usize,
    },
    RefreshWorkspace {
        workspace_root: PathBuf,
        status_sender: Option<mpsc::Sender<String>>,
    },
    ImportWorkspaceFromReposTxt {
        workspace_root: PathBuf,
        status_sender: Option<mpsc::Sender<String>>,
    },
    HistoryReport {
        manifest: WorkspaceManifest,
        selected_repo_ids: Vec<String>,
        num_commits: usize,
    },
    LineStats {
        manifest: WorkspaceManifest,
        since_date: Option<String>,
    },
    SaveManifestFromEditor {
        host_ptr: usize,
        workspace_name: String,
        workspace_root: String,
        shared_hooks_path: String,
        repo_rows: Vec<RepoEditorRowInput>,
        selected_repo_id: Option<String>,
        clone_after_save: bool,
        status_sender: mpsc::Sender<String>,
    },
    EditorLoad {
        path: PathBuf,
        reply: mpsc::Sender<EditorLoadMessage>,
    },
    EditorSave {
        path: PathBuf,
        content: String,
        reply: mpsc::Sender<EditorSaveMessage>,
    },
}

enum WorkerResult {
    WorkspaceScanCompleted {
        workspace_status: RepositoryStatus,
        repository_items: Vec<RepositoryListItem>,
    },
    WorkspaceRootStatusRefreshed {
        workspace_status: RepositoryStatus,
    },
    RepositoryStatusRefreshed {
        item: RepositoryListItem,
    },
    RepositoryRemoteFetchCompleted {
        repo_id: String,
        repo_name: String,
        result: Result<(), String>,
    },
    RepoDetailsLoaded {
        repo_id: String,
        details: RepositoryDetails,
    },
    WatchManagerSyncCompleted {
        sync_seq: usize,
        result: Result<Option<WatchManager>, String>,
    },
    RefreshWorkspaceCompleted {
        result: Result<RefreshWorkspaceResult, String>,
        status_sender: Option<mpsc::Sender<String>>,
    },
    ImportWorkspaceCompleted {
        result: Result<ImportWorkspaceResult, String>,
        status_sender: Option<mpsc::Sender<String>>,
    },
    HistoryReportCompleted {
        result: Result<HistoryReportResult, String>,
    },
    LineStatsCompleted {
        result: Result<LineStatsResult, String>,
    },
    SaveManifestCompleted {
        result: Result<SaveManifestResult, String>,
        status_sender: mpsc::Sender<String>,
    },
}

struct RefreshWorkspaceResult {
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    manifest: Option<WorkspaceManifest>,
    message: String,
}

struct ImportWorkspaceResult {
    manifest_path: PathBuf,
    manifest: WorkspaceManifest,
    message: String,
}

struct HistoryReportResult {
    lines: Vec<String>,
    message: String,
}

struct LineStatsResult {
    lines: Vec<String>,
    message: String,
}

struct SaveManifestResult {
    host_ptr: usize,
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    manifest: WorkspaceManifest,
    selected_repo_id: Option<String>,
    clone_after_save: bool,
    message: String,
}

#[derive(Clone)]
struct RepoEditorRowInput {
    enabled: bool,
    name: String,
    dir_name: String,
    remote_url: String,
}

#[derive(Clone)]
struct CommitCheckRuleRowHandle {
    enabled: CheckButton,
    allow: CheckButton,
    hash_matcher: CheckButton,
    name: Entry,
    priority: Entry,
    value: Entry,
    repository_ids: Entry,
    rule_id: String,
}

struct EditorLoadMessage {
    path: PathBuf,
    result: Result<String, String>,
}

struct EditorSaveMessage {
    path: PathBuf,
    result: Result<(), String>,
}

struct RepositoryViewHandle {
    list: glib::WeakRef<ListBox>,
    scroller: glib::WeakRef<ScrolledWindow>,
    store: gio::ListStore,
    filter: CustomFilter,
    sorter: CustomSorter,
}

#[derive(Default)]
struct ContainerViewHandle {
    root: glib::WeakRef<GtkBox>,
    auxiliary_root: glib::WeakRef<GtkBox>,
    instance_key: Option<String>,
    host_ptr: usize,
}

struct OperationFollowHandle {
    scroller: glib::WeakRef<ScrolledWindow>,
    toggle: glib::WeakRef<ToggleButton>,
    follow_enabled: Rc<Cell<bool>>,
    suppress_scroll_events: Rc<Cell<bool>>,
}

thread_local! {
    static REPOSITORY_VIEWS: RefCell<Vec<RepositoryViewHandle>> = const { RefCell::new(Vec::new()) };
    static MONOREPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static REPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static COMMIT_CHECK_VIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static WORKSPACE_SETTINGS_VIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_BUFFERS: RefCell<Vec<glib::WeakRef<TextBuffer>>> = const { RefCell::new(Vec::new()) };
    static OPERATION_SUMMARIES: RefCell<Vec<glib::WeakRef<Label>>> = const { RefCell::new(Vec::new()) };
    static OPERATION_FOLLOWERS: RefCell<Vec<OperationFollowHandle>> = const { RefCell::new(Vec::new()) };
}

static STATE: OnceLock<Mutex<AppState>> = OnceLock::new();
static EXECUTOR: OnceLock<ExecutorState> = OnceLock::new();
static WATCH_MANAGER: OnceLock<Mutex<Option<WatchManager>>> = OnceLock::new();
static PENDING_WATCH_EVENTS: OnceLock<Mutex<PendingWatchEvents>> = OnceLock::new();
static LAST_HOST_PTR: AtomicUsize = AtomicUsize::new(0);
static BACKGROUND_LOOPS_STARTED: AtomicUsize = AtomicUsize::new(0);
static LOG_REFRESH_SCHEDULED: AtomicUsize = AtomicUsize::new(0);
static VIEW_REFRESH_SCHEDULED: AtomicUsize = AtomicUsize::new(0);
static WATCH_MANAGER_SYNC_SEQ: AtomicUsize = AtomicUsize::new(0);
static RUNTIME_PROFILE_ENABLED: OnceLock<bool> = OnceLock::new();
static WATCH_BACKEND_CODE: AtomicUsize = AtomicUsize::new(0);
static WATCHED_PATH_COUNT: AtomicUsize = AtomicUsize::new(0);
static WATCH_SYNC_REQUESTS: AtomicUsize = AtomicUsize::new(0);
static WATCH_SYNC_COMPLETIONS: AtomicUsize = AtomicUsize::new(0);
static WATCH_SYNC_FAILURES: AtomicUsize = AtomicUsize::new(0);
static WATCH_SYNC_PENDING_COLLAPSES: AtomicUsize = AtomicUsize::new(0);
static WATCH_EVENTS_RECEIVED: AtomicUsize = AtomicUsize::new(0);
static WATCH_EVENT_FLUSHES: AtomicUsize = AtomicUsize::new(0);
static PENDING_WATCH_PATHS: AtomicUsize = AtomicUsize::new(0);
static PENDING_WATCH_PATHS_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);
static LOCAL_RESCAN_TICKS: AtomicUsize = AtomicUsize::new(0);
static REMOTE_FETCH_TICKS: AtomicUsize = AtomicUsize::new(0);
static DISPATCHED_JOBS: AtomicUsize = AtomicUsize::new(0);
static LOG_REFRESH_COUNT: AtomicUsize = AtomicUsize::new(0);
static VIEW_REFRESH_COUNT: AtomicUsize = AtomicUsize::new(0);
static LAST_WATCH_SYNC_SEQ_COMPLETED: AtomicUsize = AtomicUsize::new(0);

fn state() -> &'static Mutex<AppState> {
    STATE.get_or_init(|| Mutex::new(AppState::default()))
}

fn executor() -> &'static ExecutorState {
    EXECUTOR.get_or_init(|| {
        let (sender, receiver) = mpsc::channel::<QueuedJob>();
        let receiver = Arc::new(Mutex::new(receiver));
        for index in 0..WORKER_POOL_SIZE {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("ronomepo-worker-{index}"))
                .spawn(move || worker_loop(receiver))
                .expect("failed to spawn ronomepo worker");
        }
        ExecutorState {
            sender,
            in_flight: Mutex::new(HashSet::new()),
        }
    })
}

fn watch_manager() -> &'static Mutex<Option<WatchManager>> {
    WATCH_MANAGER.get_or_init(|| Mutex::new(None))
}

fn pending_watch_events() -> &'static Mutex<PendingWatchEvents> {
    PENDING_WATCH_EVENTS.get_or_init(|| Mutex::new(PendingWatchEvents::default()))
}

fn runtime_profile_enabled() -> bool {
    *RUNTIME_PROFILE_ENABLED.get_or_init(|| {
        env::var_os(RUNTIME_PROFILE_ENV).is_some_and(|value| !value.is_empty() && value != "0")
    })
}

fn watch_backend_kind_code(kind: WatchBackendKind) -> usize {
    match kind {
        WatchBackendKind::None => 0,
        WatchBackendKind::Recommended => 1,
        WatchBackendKind::Poll => 2,
    }
}

fn watch_backend_kind_label(kind: WatchBackendKind) -> &'static str {
    match kind {
        WatchBackendKind::None => "none",
        WatchBackendKind::Recommended => "recommended",
        WatchBackendKind::Poll => "poll",
    }
}

fn current_watch_backend_kind() -> WatchBackendKind {
    match WATCH_BACKEND_CODE.load(Ordering::Relaxed) {
        1 => WatchBackendKind::Recommended,
        2 => WatchBackendKind::Poll,
        _ => WatchBackendKind::None,
    }
}

fn update_max_atomic(target: &AtomicUsize, value: usize) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn current_view_registry_counts() -> (usize, usize, usize, usize, usize, usize, usize) {
    let repository_views = REPOSITORY_VIEWS.with(|views| views.borrow().len());
    let monorepo_views = MONOREPO_OVERVIEWS.with(|views| views.borrow().len());
    let repo_views = REPO_OVERVIEWS.with(|views| views.borrow().len());
    let commit_check_views = COMMIT_CHECK_VIEWS.with(|views| views.borrow().len());
    let workspace_settings_views = WORKSPACE_SETTINGS_VIEWS.with(|views| views.borrow().len());
    let operation_buffers = OPERATION_BUFFERS.with(|buffers| buffers.borrow().len());
    let operation_summaries = OPERATION_SUMMARIES.with(|labels| labels.borrow().len());
    (
        repository_views,
        monorepo_views,
        repo_views,
        commit_check_views,
        workspace_settings_views,
        operation_buffers,
        operation_summaries,
    )
}

fn runtime_profile_summary(reason: &str) -> Option<String> {
    if !runtime_profile_enabled() {
        return None;
    }

    let app_state = state().lock().expect("state mutex poisoned");
    let pending_watch_paths = PENDING_WATCH_PATHS.load(Ordering::Relaxed);
    let in_flight_jobs = executor()
        .in_flight
        .lock()
        .expect("executor mutex poisoned")
        .len();
    let (
        repository_views,
        monorepo_views,
        repo_views,
        commit_check_views,
        workspace_settings_views,
        operation_buffers,
        operation_summaries,
    ) = current_view_registry_counts();
    let mut line = format!(
        "[runtime:{reason}] watch_backend={} watched_paths={} repo_runtime={} repo_details_cache={} repo_details_loading={} pending_watch_paths={} pending_watch_paths_high_water={} watch_sync_requests={} watch_sync_completions={} watch_sync_failures={} watch_sync_pending_collapses={} watch_events={} watch_event_flushes={} local_rescan_ticks={} remote_fetch_ticks={} dispatched_jobs={} in_flight_jobs={} view_refreshes={} log_refreshes={} repository_views={} monorepo_views={} repo_views={} commit_check_views={} workspace_settings_views={} operation_buffers={} operation_summaries={}",
        watch_backend_kind_label(current_watch_backend_kind()),
        WATCHED_PATH_COUNT.load(Ordering::Relaxed),
        app_state.repo_runtime.len(),
        app_state.repo_details_cache.len(),
        app_state.repo_details_loading.len(),
        pending_watch_paths,
        PENDING_WATCH_PATHS_HIGH_WATER.load(Ordering::Relaxed),
        WATCH_SYNC_REQUESTS.load(Ordering::Relaxed),
        WATCH_SYNC_COMPLETIONS.load(Ordering::Relaxed),
        WATCH_SYNC_FAILURES.load(Ordering::Relaxed),
        WATCH_SYNC_PENDING_COLLAPSES.load(Ordering::Relaxed),
        WATCH_EVENTS_RECEIVED.load(Ordering::Relaxed),
        WATCH_EVENT_FLUSHES.load(Ordering::Relaxed),
        LOCAL_RESCAN_TICKS.load(Ordering::Relaxed),
        REMOTE_FETCH_TICKS.load(Ordering::Relaxed),
        DISPATCHED_JOBS.load(Ordering::Relaxed),
        in_flight_jobs,
        VIEW_REFRESH_COUNT.load(Ordering::Relaxed),
        LOG_REFRESH_COUNT.load(Ordering::Relaxed),
        repository_views,
        monorepo_views,
        repo_views,
        commit_check_views,
        workspace_settings_views,
        operation_buffers,
        operation_summaries,
    );
    if app_state.watch_manager_sync_in_flight {
        let _ = write!(line, " watch_sync_in_flight=1");
    }
    if app_state.watch_manager_sync_pending {
        let _ = write!(line, " watch_sync_pending=1");
    }
    Some(line)
}

fn emit_runtime_profile(reason: &str) {
    let Some(line) = runtime_profile_summary(reason) else {
        return;
    };
    eprintln!("{line}");
}

fn record_watch_manager_metrics(manager: Option<&WatchManager>, reason: &str) {
    let next_kind = manager
        .map(|manager| manager.backend_kind)
        .unwrap_or(WatchBackendKind::None);
    let next_watched_paths = manager.map(|manager| manager.watched_paths).unwrap_or(0);
    let previous = WATCH_BACKEND_CODE.swap(watch_backend_kind_code(next_kind), Ordering::Relaxed);
    WATCHED_PATH_COUNT.store(next_watched_paths, Ordering::Relaxed);
    if runtime_profile_enabled() && previous != watch_backend_kind_code(next_kind) {
        emit_runtime_profile(reason);
    }
}

fn submit_job(job: WorkerJob) -> Result<(), String> {
    submit_queued_job(QueuedJob { key: None, job })
}

fn submit_coalesced_job(key: JobKey, job: WorkerJob) -> Result<bool, String> {
    let executor = executor();
    {
        let mut in_flight = executor.in_flight.lock().expect("executor mutex poisoned");
        if !in_flight.insert(key.clone()) {
            return Ok(false);
        }
    }
    if let Err(error) = submit_queued_job(QueuedJob {
        key: Some(key.clone()),
        job,
    }) {
        let mut in_flight = executor.in_flight.lock().expect("executor mutex poisoned");
        in_flight.remove(&key);
        return Err(error);
    }
    Ok(true)
}

fn submit_queued_job(queued_job: QueuedJob) -> Result<(), String> {
    DISPATCHED_JOBS.fetch_add(1, Ordering::Relaxed);
    executor()
        .sender
        .send(queued_job)
        .map_err(|_| "worker pool is unavailable".to_string())
}

fn worker_loop(receiver: Arc<Mutex<mpsc::Receiver<QueuedJob>>>) {
    loop {
        let queued_job = {
            let receiver = receiver.lock().expect("worker receiver poisoned");
            receiver.recv()
        };
        let Ok(queued_job) = queued_job else {
            break;
        };
        run_worker_job(queued_job);
    }
}

fn run_worker_job(queued_job: QueuedJob) {
    let key = queued_job.key.clone();
    match queued_job.job {
        WorkerJob::OperationBatch {
            manifest,
            selected_repo_ids,
            kind,
            batch_id,
        } => {
            let main_context = glib::MainContext::default();
            let operation = operation_kind_title(kind);
            run_workspace_operation(&manifest, &selected_repo_ids, kind, |event| {
                let event = event.clone();
                let manifest = manifest.clone();
                main_context
                    .invoke(move || handle_operation_event(batch_id, operation, manifest, event));
            });
        }
        WorkerJob::WorkspaceScan {
            workspace_root,
            manifest,
        } => {
            let workspace_status = ronomepo_core::collect_repository_status(&workspace_root);
            let repository_items = manifest
                .as_ref()
                .map(build_repository_list)
                .unwrap_or_default();
            dispatch_worker_result(WorkerResult::WorkspaceScanCompleted {
                workspace_status,
                repository_items,
            });
        }
        WorkerJob::WorkspaceRootStatusRefresh { workspace_root } => {
            let workspace_status = ronomepo_core::collect_repository_status(&workspace_root);
            dispatch_worker_result(WorkerResult::WorkspaceRootStatusRefreshed { workspace_status });
        }
        WorkerJob::RepositoryStatusRefresh { mut item } => {
            item.status = ronomepo_core::collect_repository_status(&item.status.repo_path);
            item.repo_manifest = Some(scan_repo_manifest(&item.status.repo_path));
            dispatch_worker_result(WorkerResult::RepositoryStatusRefreshed { item });
        }
        WorkerJob::RepositoryRemoteFetch {
            repo_id,
            repo_name,
            repo_path,
        } => {
            let result = fetch_repository_remote(&repo_path);
            dispatch_worker_result(WorkerResult::RepositoryRemoteFetchCompleted {
                repo_id,
                repo_name,
                result,
            });
        }
        WorkerJob::RepoDetailsLoad { repo_id, repo_path } => {
            let details = collect_repository_details(&repo_path);
            dispatch_worker_result(WorkerResult::RepoDetailsLoaded { repo_id, details });
        }
        WorkerJob::WatchManagerSync { manifest, sync_seq } => {
            let result = manifest
                .map(|manifest| build_watch_manager(&manifest))
                .transpose();
            dispatch_worker_result(WorkerResult::WatchManagerSyncCompleted { sync_seq, result });
        }
        WorkerJob::RefreshWorkspace {
            workspace_root,
            status_sender,
        } => {
            let result = load_workspace_manifest(&workspace_root);
            dispatch_worker_result(WorkerResult::RefreshWorkspaceCompleted {
                result,
                status_sender,
            });
        }
        WorkerJob::ImportWorkspaceFromReposTxt {
            workspace_root,
            status_sender,
        } => {
            let result = import_workspace_manifest_from_repos_txt(&workspace_root);
            dispatch_worker_result(WorkerResult::ImportWorkspaceCompleted {
                result,
                status_sender,
            });
        }
        WorkerJob::HistoryReport {
            manifest,
            selected_repo_ids,
            num_commits,
        } => {
            let result = build_history_report(&manifest, &selected_repo_ids, num_commits);
            dispatch_worker_result(WorkerResult::HistoryReportCompleted { result });
        }
        WorkerJob::LineStats {
            manifest,
            since_date,
        } => {
            let result = build_line_stats_report(&manifest, since_date.as_deref());
            dispatch_worker_result(WorkerResult::LineStatsCompleted { result });
        }
        WorkerJob::SaveManifestFromEditor {
            host_ptr,
            workspace_name,
            workspace_root,
            shared_hooks_path,
            repo_rows,
            selected_repo_id,
            clone_after_save,
            status_sender,
        } => {
            let result = save_workspace_manifest_from_inputs(
                host_ptr,
                &workspace_name,
                &workspace_root,
                &shared_hooks_path,
                &repo_rows,
                selected_repo_id,
                clone_after_save,
            );
            dispatch_worker_result(WorkerResult::SaveManifestCompleted {
                result,
                status_sender,
            });
        }
        WorkerJob::EditorLoad { path, reply } => {
            let result = fs::read_to_string(&path).map_err(|error| error.to_string());
            let _ = reply.send(EditorLoadMessage { path, result });
        }
        WorkerJob::EditorSave {
            path,
            content,
            reply,
        } => {
            let result = fs::write(&path, content).map_err(|error| error.to_string());
            let _ = reply.send(EditorSaveMessage { path, result });
        }
    }

    if let Some(key) = key {
        let mut in_flight = executor()
            .in_flight
            .lock()
            .expect("executor mutex poisoned");
        in_flight.remove(&key);
    }
}

fn dispatch_worker_result(result: WorkerResult) {
    let main_context = glib::MainContext::default();
    main_context.invoke(move || handle_worker_result(result));
}

fn empty_repository_status(repo_path: PathBuf) -> ronomepo_core::RepositoryStatus {
    ronomepo_core::RepositoryStatus {
        state: ronomepo_core::RepositoryState::Unknown,
        branch: None,
        sync: ronomepo_core::RepositorySync::Unknown,
        repo_path,
    }
}

fn next_remote_fetch_due_at(now: SystemTime, repo_id: &str) -> SystemTime {
    let jitter = Duration::from_secs(repo_fetch_jitter_secs(repo_id));
    now + Duration::from_secs(REMOTE_FETCH_INTERVAL_SECS) + jitter
}

fn retry_remote_fetch_due_at(now: SystemTime, repo_id: &str) -> SystemTime {
    let capped = repo_fetch_jitter_secs(repo_id).min(REMOTE_FETCH_INTERVAL_SECS / 4);
    now + Duration::from_secs((REMOTE_FETCH_INTERVAL_SECS / 4) + capped)
}

fn repo_fetch_jitter_secs(repo_id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    repo_id.hash(&mut hasher);
    hasher.finish() % (REMOTE_FETCH_JITTER_SECS + 1)
}

fn ensure_background_loops_started() {
    if BACKGROUND_LOOPS_STARTED
        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }

    glib::timeout_add_seconds_local(LOCAL_RESCAN_INTERVAL_SECS, || {
        LOCAL_RESCAN_TICKS.fetch_add(1, Ordering::Relaxed);
        mark_all_repos_stale();
        schedule_pending_local_rescans();
        glib::ControlFlow::Continue
    });
    glib::timeout_add_seconds_local(REMOTE_FETCH_TICK_SECS, || {
        REMOTE_FETCH_TICKS.fetch_add(1, Ordering::Relaxed);
        schedule_due_remote_fetches();
        glib::ControlFlow::Continue
    });
}

impl Plugin for RonomepoPlugin {
    fn descriptor() -> PluginDescriptor {
        static DEPENDENCIES: &[PluginDependency] = &[PluginDependency::required(
            "maruzzella.base",
            Version::new(1, 0, 0),
            Version::new(2, 0, 0),
        )];

        PluginDescriptor::new(PLUGIN_ID, "Ronomepo", Version::new(0, 1, 0))
            .with_description("Desktop workspace manager for multiple sibling Git repositories")
            .with_dependencies(DEPENDENCIES)
    }

    fn register(host: &HostApi<'_>) -> Result<(), MzStatusCode> {
        host.log(MzLogLevel::Info, "Registering Ronomepo plugin");

        let config = ensure_config(host)?;
        initialize_state(&config);

        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_REFRESH, "Refresh Workspace")
                .with_handler(command_refresh_workspace),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_PULL, "Pull").with_handler(command_pull),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_PUSH, "Push").with_handler(command_push),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_OPEN_OVERVIEW, "Monorepo Overview")
                .with_handler(command_open_overview),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_OPEN_COMMIT_CHECK, "Commit Check")
                .with_handler(command_open_commit_check),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_FILTER, "Filter Repositories")
                .with_handler(command_filter),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_ADD_REPO, "Add Repo").with_handler(command_add_repo),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_EXIT, "Exit").with_handler(command_exit),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_REFRESH_LOGS, "Refresh Logs")
                .with_handler(command_refresh_logs),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_CLEAR_LOGS, "Clear Logs")
                .with_handler(command_clear_logs),
        )?;

        host.register_surface_contribution(SurfaceContributionSpec::about_section(
            PLUGIN_ID,
            "ronomepo-about",
            "Ronomepo",
            "Desktop-first multi-repository workspace manager hosted inside Maruzzella.",
        ))?;

        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_REPO_MONITOR,
            "Repository Monitor",
            MzViewPlacement::SidePanel,
            create_repo_monitor_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_MONOREPO_OVERVIEW,
            "Monorepo Overview",
            MzViewPlacement::Workbench,
            create_monorepo_overview_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_REPO_OVERVIEW,
            "Repo Overview",
            MzViewPlacement::Workbench,
            create_repo_overview_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_COMMIT_CHECK,
            "Commit Check",
            MzViewPlacement::Workbench,
            create_commit_check_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_WORKSPACE_SETTINGS,
            "Workspace Settings",
            MzViewPlacement::Workbench,
            create_workspace_settings_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_TEXT_EDITOR,
            "Text Editor",
            MzViewPlacement::Workbench,
            create_text_editor_view,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_OPERATIONS,
            "Operations",
            MzViewPlacement::BottomPanel,
            create_operations_view,
        ))?;

        Ok(())
    }
}

fn ensure_config(host: &HostApi<'_>) -> Result<RonomepoPluginConfig, MzStatusCode> {
    let bytes = host.read_config()?;
    let mut config = if bytes.is_empty() {
        RonomepoPluginConfig::default()
    } else {
        serde_json::from_slice(&bytes).unwrap_or_default()
    };

    if config.last_workspace_path.is_none() {
        let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        config.last_workspace_path = Some(current_dir.to_string_lossy().to_string());
        let payload = serde_json::to_vec(&config).map_err(|_| MzStatusCode::InternalError)?;
        host.write_config(&payload)?;
    }

    Ok(config)
}

fn initialize_state(config: &RonomepoPluginConfig) {
    ensure_background_loops_started();

    let workspace_root = env::var_os("RONOMEPO_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .or_else(|| config.last_workspace_path.as_deref().map(PathBuf::from))
        .map(normalize_workspace_root)
        .unwrap_or_else(|| PathBuf::from("."));
    let manifest_path = default_manifest_path(&workspace_root);
    let manifest = load_manifest_if_present(&manifest_path);

    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.workspace_root = workspace_root.clone();
    app_state.manifest_path = manifest.as_ref().map(|_| manifest_path.clone());
    app_state.manifest = manifest;
    app_state.workspace_status = empty_repository_status(workspace_root.clone());
    app_state.repository_items.clear();
    app_state.repository_items_loading = false;
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    app_state.monitor_filter_mode = config.monitor_filter_mode;
    app_state.monitor_sort_mode = config.monitor_sort_mode;
    app_state.monitor_sort_descending = config.monitor_sort_descending;
    sync_repo_runtime_state(&mut app_state);
    if app_state.logs.is_empty() {
        app_state.logs.push(format!(
            "Ronomepo initialized for workspace {}",
            workspace_root.display()
        ));
    }
    drop(app_state);
    emit_runtime_profile("startup");
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

extern "C" fn command_refresh_workspace(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match queue_refresh_workspace(None) {
        Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(format!("Refresh failed: {message}"));
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_clone_missing(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::CloneMissing);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_pull(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::Pull);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_push(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::Push);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_push_force(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::PushForce);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_apply_hooks(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::ApplyHooks);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_open_overview(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    let host_ptr = current_host_ptr();
    if host_ptr.is_null() {
        append_log(
            "Cannot focus Monorepo Overview because the Maruzzella host handle is unavailable."
                .to_string(),
        );
        refresh_views();
        return maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError);
    }

    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let request = OpenViewRequest::new(
        PLUGIN_ID,
        VIEW_MONOREPO_OVERVIEW,
        MzViewPlacement::Workbench,
    );

    match host.open_view(&request) {
        Ok(MzViewOpenDisposition::Opened) => {
            append_log("Opened Monorepo Overview.".to_string());
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Ok(MzViewOpenDisposition::FocusedExisting) => {
            append_log("Focused existing Monorepo Overview.".to_string());
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Err(status) => {
            append_log(format!("Failed to open Monorepo Overview: {status:?}"));
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

fn open_commit_check_view() -> Result<MzViewOpenDisposition, String> {
    let host_ptr = current_host_ptr();
    if host_ptr.is_null() {
        return Err(
            "Cannot focus Commit Check because the Maruzzella host handle is unavailable."
                .to_string(),
        );
    }

    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let request = OpenViewRequest::new(PLUGIN_ID, VIEW_COMMIT_CHECK, MzViewPlacement::Workbench);
    host.open_view(&request)
        .map_err(|status| format!("Failed to open Commit Check: {status:?}"))
}

extern "C" fn command_open_commit_check(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match open_commit_check_view() {
        Ok(MzViewOpenDisposition::Opened) => {
            append_log("Opened Commit Check.".to_string());
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Ok(MzViewOpenDisposition::FocusedExisting) => {
            append_log("Focused existing Commit Check.".to_string());
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Err(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_add_repo(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match open_workspace_settings_tab() {
        Ok(()) => {
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Err(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_exit(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    if let Some(application) = gio::Application::default() {
        application.quit();
        maruzzella_sdk::ffi::MzStatus::OK
    } else {
        append_log("Cannot exit because no GTK application is active.".to_string());
        refresh_views();
        maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
    }
}

extern "C" fn command_check_history(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match queue_history_report(25) {
        Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_line_stats(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match queue_line_stats_report_from_state() {
        Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_refresh_logs(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    refresh_log_surfaces();
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_clear_logs(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.logs.clear();
        app_state.logs.push("Operations log cleared.".to_string());
    }
    refresh_views();
    maruzzella_sdk::ffi::MzStatus::OK
}

fn queue_refresh_workspace(status_sender: Option<mpsc::Sender<String>>) -> Result<(), String> {
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    submit_job(WorkerJob::RefreshWorkspace {
        workspace_root,
        status_sender,
    })
}

fn queue_import_workspace_from_repos_txt(
    status_sender: Option<mpsc::Sender<String>>,
) -> Result<(), String> {
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    submit_job(WorkerJob::ImportWorkspaceFromReposTxt {
        workspace_root,
        status_sender,
    })
}

fn queue_history_report(num_commits: usize) -> Result<(), String> {
    let (manifest, selected_repo_ids) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.clone() else {
            return Err(format!(
                "Check History skipped because no {} is loaded.",
                MANIFEST_FILE_NAME
            ));
        };
        app_state.history_report_loading = true;
        (manifest, app_state.selected_repo_ids.clone())
    };

    match submit_coalesced_job(
        JobKey::HistoryReport,
        WorkerJob::HistoryReport {
            manifest,
            selected_repo_ids,
            num_commits,
        },
    ) {
        Ok(true) => {
            refresh_views();
            Ok(())
        }
        Ok(false) => {
            append_log("History check is already running.".to_string());
            refresh_views();
            Ok(())
        }
        Err(error) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            app_state.history_report_loading = false;
            Err(error)
        }
    }
}

fn queue_line_stats_report_from_state() -> Result<(), String> {
    let (manifest, since_date) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.clone() else {
            return Err(format!(
                "Line Stats skipped because no {} is loaded.",
                MANIFEST_FILE_NAME
            ));
        };
        let trimmed = app_state.line_stats_since.trim().to_string();
        let since_date = (!trimmed.is_empty()).then_some(trimmed);
        app_state.line_stats_loading = true;
        (manifest, since_date)
    };

    match submit_coalesced_job(
        JobKey::LineStats(since_date.clone()),
        WorkerJob::LineStats {
            manifest,
            since_date,
        },
    ) {
        Ok(true) => {
            refresh_views();
            Ok(())
        }
        Ok(false) => {
            append_log("Line stats refresh is already running.".to_string());
            refresh_views();
            Ok(())
        }
        Err(error) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            app_state.line_stats_loading = false;
            Err(error)
        }
    }
}

fn append_log(message: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.logs.push(message);
    if app_state.logs.len() > MAX_LOG_ENTRIES {
        let excess = app_state.logs.len() - MAX_LOG_ENTRIES;
        app_state.logs.drain(0..excess);
    }
    drop(app_state);
    schedule_log_surface_refresh();
}

fn schedule_log_surface_refresh() {
    if LOG_REFRESH_SCHEDULED
        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    glib::timeout_add_local(Duration::from_millis(UI_REFRESH_DEBOUNCE_MILLIS), || {
        LOG_REFRESH_SCHEDULED.store(0, Ordering::SeqCst);
        LOG_REFRESH_COUNT.fetch_add(1, Ordering::Relaxed);
        refresh_log_surfaces();
        glib::ControlFlow::Break
    });
}

fn launch_operation(kind: OperationKind) {
    let (manifest, selected_repo_ids, batch_id) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let batch_id = app_state.next_operation_batch.max(1);
        app_state.next_operation_batch = batch_id + 1;
        (
            app_state.manifest.clone(),
            app_state.selected_repo_ids.clone(),
            batch_id,
        )
    };

    let Some(manifest) = manifest else {
        append_log(format!(
            "[run {batch_id}] {} skipped because no {} is loaded.",
            operation_kind_title(kind),
            MANIFEST_FILE_NAME
        ));
        refresh_views();
        return;
    };

    if let Err(message) = submit_job(WorkerJob::OperationBatch {
        manifest,
        selected_repo_ids,
        kind,
        batch_id,
    }) {
        append_log(format!(
            "[run {batch_id}] {} failed to start: {message}",
            operation_kind_title(kind)
        ));
        refresh_views();
    }
}

fn operation_kind_title(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::CloneMissing => "Clone Missing",
        OperationKind::Pull => "Pull",
        OperationKind::Push => "Push",
        OperationKind::PushForce => "Push Force",
        OperationKind::ApplyHooks => "Apply Hooks",
    }
}

fn format_operation_event(event: &OperationEvent) -> String {
    let prefix = match event.kind {
        OperationEventKind::Started => "START",
        OperationEventKind::Success => "OK",
        OperationEventKind::Skipped => "SKIP",
        OperationEventKind::Failed => "FAIL",
        OperationEventKind::Finished => "DONE",
    };

    match event.repository_name.as_deref() {
        Some(repo_name) => format!("[{prefix}] {repo_name}: {}", event.message),
        None => format!("[{prefix}] {}", event.message),
    }
}

fn active_window() -> Option<Window> {
    gio::Application::default()
        .and_then(|app| app.downcast::<gtk::Application>().ok())
        .and_then(|app| app.active_window())
}

fn present_operation_failure_dialog(
    _batch_id: usize,
    operation: &'static str,
    event: &OperationEvent,
) {
    let is_generated_commit_failure =
        operation == "Push" && event.message.contains("commit check rules blocked commits");
    let dialog = Dialog::builder()
        .modal(true)
        .title(format!("{operation} failed"))
        .build();
    if let Some(parent) = active_window().as_ref() {
        dialog.set_transient_for(Some(parent));
    }
    if is_generated_commit_failure {
        dialog.add_button("Open Commit Check", ResponseType::Accept);
    }
    dialog.add_button("Close", ResponseType::Close);
    dialog.set_default_response(ResponseType::Close);

    let content = dialog.content_area();
    content.set_margin_top(16);
    content.set_margin_bottom(16);
    content.set_margin_start(16);
    content.set_margin_end(16);
    content.set_spacing(12);

    let body = GtkBox::new(Orientation::Vertical, 8);

    let header = GtkBox::new(Orientation::Horizontal, 10);
    let icon = Image::from_icon_name("dialog-error-symbolic");
    icon.set_icon_size(gtk::IconSize::Large);
    icon.add_css_class("error");

    let title_text = event.repository_name.as_deref().map_or_else(
        || format!("{operation} failed."),
        |repo_name| format!("{operation} failed for {repo_name}."),
    );
    let title = Label::new(Some(&title_text));
    title.set_xalign(0.0);
    title.add_css_class("title-4");
    title.add_css_class("error");
    title.set_wrap(true);

    let message = Label::new(Some(&event.message));
    message.set_xalign(0.0);
    message.set_wrap(true);

    header.append(&icon);
    header.append(&title);
    body.append(&header);
    body.append(&message);
    content.append(&body);

    dialog.connect_response(move |dialog, response| {
        if response == ResponseType::Accept {
            let _ = command_open_commit_check(maruzzella_sdk::ffi::MzBytes::empty());
            let _ = queue_history_report(25);
        }
        dialog.close();
    });
    dialog.present();
}

fn handle_operation_event(
    batch_id: usize,
    operation: &'static str,
    manifest: WorkspaceManifest,
    event: OperationEvent,
) {
    if matches!(event.kind, OperationEventKind::Failed)
        && matches!(operation, "Pull" | "Push" | "Push Force")
    {
        present_operation_failure_dialog(batch_id, operation, &event);
    }
    append_log(format!(
        "[run {batch_id}] {}",
        format_operation_event(&event)
    ));
    match event.kind {
        OperationEventKind::Success | OperationEventKind::Skipped | OperationEventKind::Failed => {
            schedule_refresh_for_operation_event(&manifest, &event);
        }
        OperationEventKind::Finished => {
            sync_watch_manager_from_state();
            refresh_views();
        }
        OperationEventKind::Started => {}
    }
}

fn handle_worker_result(result: WorkerResult) {
    match result {
        WorkerResult::WorkspaceScanCompleted {
            workspace_status,
            repository_items,
        } => {
            let rerun = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                mark_full_workspace_scan_completed(&mut app_state, &repository_items);
                app_state.workspace_status = workspace_status;
                app_state.repository_items = repository_items;
                app_state.repository_items_loading = false;
                let rerun = app_state.repository_items_refresh_pending;
                app_state.repository_items_refresh_pending = false;
                rerun
            };
            sync_watch_manager_from_state();
            if rerun {
                schedule_workspace_scan();
            }
            refresh_views();
        }
        WorkerResult::WorkspaceRootStatusRefreshed { workspace_status } => {
            let changed = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                if app_state.workspace_status == workspace_status {
                    false
                } else {
                    app_state.workspace_status = workspace_status;
                    true
                }
            };
            if changed {
                refresh_views();
            }
        }
        WorkerResult::RepositoryStatusRefreshed { item } => {
            let (follow_up_refresh, changed) = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                let repo_id = item.id.clone();
                let follow_up_refresh = finalize_repo_status_refresh(&mut app_state, &repo_id);
                let changed = if let Some(existing_item) = app_state
                    .repository_items
                    .iter_mut()
                    .find(|existing_item| existing_item.id == repo_id)
                {
                    let changed = *existing_item != item;
                    if changed {
                        *existing_item = item.clone();
                    }
                    changed
                } else {
                    false
                };
                (follow_up_refresh, changed)
            };
            let has_follow_up_refresh = follow_up_refresh.is_some();
            if let Some(repo_path) = follow_up_refresh {
                if let Some(item) = repository_item_from_state(&item.id) {
                    schedule_repository_status_refresh(&item);
                } else {
                    let mut fallback = item.clone();
                    fallback.status.repo_path = repo_path;
                    schedule_repository_status_refresh(&fallback);
                }
            }
            if has_follow_up_refresh || changed {
                refresh_views();
            }
        }
        WorkerResult::RepositoryRemoteFetchCompleted {
            repo_id,
            repo_name,
            result,
        } => {
            let mut refresh_path = None;
            let message = match result {
                Ok(()) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    mark_remote_fetch_completed(&mut app_state, &repo_id, true);
                    refresh_path = app_state
                        .repository_items
                        .iter()
                        .find(|item| item.id == repo_id)
                        .map(|item| item.status.repo_path.clone());
                    format!("Remote sync refreshed for {repo_name}.")
                }
                Err(error) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    mark_remote_fetch_completed(&mut app_state, &repo_id, false);
                    format!("Remote sync refresh failed for {repo_name}: {error}")
                }
            };
            if refresh_path.is_some() {
                if let Some(item) = repository_item_from_state(&repo_id) {
                    schedule_repository_status_refresh(&item);
                }
            }
            append_log(message);
            refresh_views();
        }
        WorkerResult::RepoDetailsLoaded { repo_id, details } => {
            let changed = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.repo_details_loading.remove(&repo_id);
                let changed = app_state.repo_details_cache.get(&repo_id) != Some(&details);
                app_state.repo_details_cache.insert(repo_id, details);
                changed
            };
            if changed {
                if snapshot().monitor_sort_mode == MonitorSortMode::RecentActivity {
                    let snapshot = snapshot();
                    refresh_repository_views(&snapshot);
                }
                refresh_views();
            }
        }
        WorkerResult::WatchManagerSyncCompleted { sync_seq, result } => {
            let rerun = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.watch_manager_sync_in_flight = false;
                let rerun = app_state.watch_manager_sync_pending;
                app_state.watch_manager_sync_pending = false;
                rerun
            };

            match result {
                Ok(manager) => {
                    LAST_WATCH_SYNC_SEQ_COMPLETED.store(sync_seq, Ordering::Relaxed);
                    WATCH_SYNC_COMPLETIONS.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut slot = watch_manager()
                            .lock()
                            .expect("watch manager mutex poisoned");
                        *slot = manager;
                        record_watch_manager_metrics(slot.as_ref(), "watch-backend-changed");
                    }
                    emit_runtime_profile("watch-sync");
                }
                Err(message) => {
                    LAST_WATCH_SYNC_SEQ_COMPLETED.store(sync_seq, Ordering::Relaxed);
                    WATCH_SYNC_FAILURES.fetch_add(1, Ordering::Relaxed);
                    {
                        let mut slot = watch_manager()
                            .lock()
                            .expect("watch manager mutex poisoned");
                        *slot = None;
                        record_watch_manager_metrics(None, "watch-backend-changed");
                    }
                    append_log(format!("Repository watcher setup failed: {message}"));
                    emit_runtime_profile("watch-sync-failed");
                }
            }

            if rerun {
                WATCH_SYNC_PENDING_COLLAPSES.fetch_add(1, Ordering::Relaxed);
                sync_watch_manager_from_state();
            }
        }
        WorkerResult::RefreshWorkspaceCompleted {
            result,
            status_sender,
        } => {
            let message = match result {
                Ok(result) => {
                    apply_loaded_manifest(
                        result.workspace_root,
                        result.manifest_path,
                        result.manifest,
                    );
                    result.message
                }
                Err(message) => format!("Refresh failed: {message}"),
            };
            if let Some(sender) = status_sender {
                let _ = sender.send(message.clone());
            }
            append_log(message);
            refresh_views();
        }
        WorkerResult::ImportWorkspaceCompleted {
            result,
            status_sender,
        } => {
            let message = match result {
                Ok(result) => {
                    apply_imported_manifest(result.manifest_path, result.manifest);
                    result.message
                }
                Err(message) => format!("Import failed: {message}"),
            };
            if let Some(sender) = status_sender {
                let _ = sender.send(message.clone());
            }
            append_log(message);
            refresh_views();
        }
        WorkerResult::HistoryReportCompleted { result } => {
            let message = match result {
                Ok(result) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    app_state.history_report = result.lines;
                    app_state.history_report_loading = false;
                    result.message
                }
                Err(message) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    app_state.history_report_loading = false;
                    message
                }
            };
            append_log(message);
            refresh_views();
        }
        WorkerResult::LineStatsCompleted { result } => {
            let message = match result {
                Ok(result) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    app_state.line_stats_report = result.lines;
                    app_state.line_stats_loading = false;
                    result.message
                }
                Err(message) => {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    app_state.line_stats_loading = false;
                    message
                }
            };
            append_log(message);
            refresh_views();
        }
        WorkerResult::SaveManifestCompleted {
            result,
            status_sender,
        } => {
            let message = match result {
                Ok(result) => {
                    let selected_repo_id = result.selected_repo_id.clone();
                    let clone_after_save = result.clone_after_save;
                    apply_saved_manifest(
                        result.host_ptr,
                        result.workspace_root,
                        result.manifest_path,
                        result.manifest,
                    );
                    if let Some(repo_id) = selected_repo_id {
                        update_selected_repo_ids(vec![repo_id]);
                    }
                    if clone_after_save {
                        launch_operation(OperationKind::CloneMissing);
                    }
                    result.message
                }
                Err(message) => message,
            };
            let _ = status_sender.send(message.clone());
            append_log(message);
            refresh_views();
        }
    }
}

fn schedule_refresh_for_operation_event(manifest: &WorkspaceManifest, event: &OperationEvent) {
    if event.repository_name.as_deref() == Some("(monorepo)") {
        schedule_workspace_root_status_refresh(manifest.root.clone());
        return;
    }

    let Some(repo_id) = event.repository_id.as_deref() else {
        return;
    };
    let Some(repo) = manifest.repos.iter().find(|repo| repo.id == repo_id) else {
        return;
    };

    invalidate_repo_details(repo_id);
    let item = RepositoryListItem {
        id: repo.id.clone(),
        name: repo.name.clone(),
        dir_name: repo.dir_name.clone(),
        remote_url: repo.remote_url.clone(),
        status: RepositoryStatus {
            repo_path: manifest.root.join(&repo.dir_name),
            ..empty_repository_status(manifest.root.join(&repo.dir_name))
        },
        repo_manifest: Some(scan_repo_manifest(&manifest.root.join(&repo.dir_name))),
    };
    schedule_repository_status_refresh(&item);
}

fn refresh_views() {
    if VIEW_REFRESH_SCHEDULED
        .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    glib::timeout_add_local(Duration::from_millis(UI_REFRESH_DEBOUNCE_MILLIS), || {
        VIEW_REFRESH_SCHEDULED.store(0, Ordering::SeqCst);
        VIEW_REFRESH_COUNT.fetch_add(1, Ordering::Relaxed);
        refresh_views_now();
        glib::ControlFlow::Break
    });
}

fn refresh_views_now() {
    let snapshot = snapshot();
    refresh_repository_views(&snapshot);
    refresh_overview_views(&snapshot);
    refresh_workspace_settings_views(&snapshot);
    refresh_operation_views(&snapshot);
}

fn refresh_repository_views(snapshot: &StateSnapshot) {
    REPOSITORY_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| {
            let Some(list) = handle.list.upgrade() else {
                return false;
            };
            let Some(scroller) = handle.scroller.upgrade() else {
                return false;
            };
            refresh_repository_view_handle(handle, &list, &scroller, &snapshot);
            true
        });
    });
}

fn refresh_overview_views(snapshot: &StateSnapshot) {
    MONOREPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_monorepo_overview_into(&root, &snapshot, handle.host_ptr as *const _);
                true
            }
            None => false,
        });
    });

    COMMIT_CHECK_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_commit_check_into(&root, &snapshot);
                true
            }
            None => false,
        });
    });

    REPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_repo_overview_into(
                    &root,
                    &snapshot,
                    handle.instance_key.as_deref(),
                    handle.host_ptr as *const _,
                );
                if let Some(panel) = handle.auxiliary_root.upgrade() {
                    sync_repo_terminal_panel(&panel, &snapshot, handle.instance_key.as_deref());
                }
                true
            }
            None => false,
        });
    });
}

fn refresh_commit_check_views_now() {
    let snapshot = snapshot();
    COMMIT_CHECK_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_commit_check_into(&root, &snapshot);
                true
            }
            None => false,
        });
    });
}

fn refresh_workspace_settings_views(snapshot: &StateSnapshot) {
    WORKSPACE_SETTINGS_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_workspace_settings_into(&root, &snapshot, handle.host_ptr as *const _);
                true
            }
            None => false,
        });
    });
}

fn refresh_operation_views(snapshot: &StateSnapshot) {
    OPERATION_BUFFERS.with(|buffers| {
        let mut buffers = buffers.borrow_mut();
        buffers.retain(|buffer_ref| match buffer_ref.upgrade() {
            Some(buffer) => {
                buffer.set_text(&snapshot.logs.join("\n"));
                true
            }
            None => false,
        });
    });

    OPERATION_SUMMARIES.with(|labels| {
        let mut labels = labels.borrow_mut();
        labels.retain(|label_ref| match label_ref.upgrade() {
            Some(label) => {
                label.set_text(&operation_summary_text(&snapshot.logs));
                true
            }
            None => false,
        });
    });

    OPERATION_FOLLOWERS.with(|followers| {
        let mut followers = followers.borrow_mut();
        followers.retain(|handle| {
            let Some(scroller) = handle.scroller.upgrade() else {
                return false;
            };
            let Some(toggle) = handle.toggle.upgrade() else {
                return false;
            };

            if handle.follow_enabled.get() {
                if !toggle.is_active() {
                    toggle.set_active(true);
                }
                handle.suppress_scroll_events.set(true);
                schedule_scroll_to_bottom(&scroller, handle.suppress_scroll_events.clone());
            }

            true
        });
    });
}

fn refresh_log_surfaces() {
    let snapshot = snapshot();
    refresh_operation_views(&snapshot);
}

fn adjustment_bottom_value(adjustment: &gtk::Adjustment) -> f64 {
    (adjustment.upper() - adjustment.page_size()).max(adjustment.lower())
}

fn adjustment_is_at_bottom(adjustment: &gtk::Adjustment) -> bool {
    (adjustment.value() - adjustment_bottom_value(adjustment)).abs() <= 1.0
}

fn schedule_scroll_to_bottom(scroller: &ScrolledWindow, suppress_scroll_events: Rc<Cell<bool>>) {
    let scroller = scroller.clone();
    glib::idle_add_local_once(move || {
        let adjustment = scroller.vadjustment();
        adjustment.set_value(adjustment_bottom_value(&adjustment));
        suppress_scroll_events.set(false);
    });
}

fn operation_follow_button() -> ToggleButton {
    let button = ToggleButton::new();
    button.add_css_class("toolbar-button");
    button.add_css_class("toolbar-icon-button");
    button.add_css_class(&button_css_class("ronomepo-toolbar-ghost"));
    button.set_focus_on_click(false);
    button.set_tooltip_text(Some("Follow scroll to bottom"));

    let icon = Image::from_icon_name("document-save-symbolic");
    icon.set_icon_size(gtk::IconSize::Normal);
    button.set_child(Some(&icon));
    button
}

fn remember_host_ptr(host: *const maruzzella_sdk::ffi::MzHostApi) {
    if !host.is_null() {
        LAST_HOST_PTR.store(host as usize, Ordering::Relaxed);
    }
}

fn current_host_ptr() -> *const maruzzella_sdk::ffi::MzHostApi {
    LAST_HOST_PTR.load(Ordering::Relaxed) as *const _
}

fn open_workspace_settings_tab() -> Result<(), String> {
    let host_ptr = current_host_ptr();
    if host_ptr.is_null() {
        return Err(
            "Cannot open Workspace Settings because the Maruzzella host handle is unavailable."
                .to_string(),
        );
    }

    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let request = OpenViewRequest::new(
        PLUGIN_ID,
        VIEW_WORKSPACE_SETTINGS,
        MzViewPlacement::Workbench,
    );
    match host.open_view(&request) {
        Ok(MzViewOpenDisposition::Opened) => {
            append_log("Opened Workspace Settings.".to_string());
            Ok(())
        }
        Ok(MzViewOpenDisposition::FocusedExisting) => {
            append_log("Focused existing Workspace Settings tab.".to_string());
            Ok(())
        }
        Err(status) => Err(format!("Failed to open Workspace Settings: {status:?}")),
    }
}

#[derive(Clone)]
struct StateSnapshot {
    workspace_root: PathBuf,
    manifest_path: Option<PathBuf>,
    manifest: Option<WorkspaceManifest>,
    workspace_status: RepositoryStatus,
    repository_items: Vec<RepositoryListItem>,
    repo_details_cache: HashMap<String, RepositoryDetails>,
    repo_details_loading: HashSet<String>,
    monitor_filter: String,
    monitor_filter_mode: MonitorFilterMode,
    monitor_sort_mode: MonitorSortMode,
    monitor_sort_descending: bool,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    history_report: Vec<String>,
    history_report_loading: bool,
    line_stats_report: Vec<String>,
    line_stats_loading: bool,
    line_stats_since: String,
}

fn snapshot() -> StateSnapshot {
    let app_state = state().lock().expect("state mutex poisoned");
    StateSnapshot {
        workspace_root: app_state.workspace_root.clone(),
        manifest_path: app_state.manifest_path.clone(),
        manifest: app_state.manifest.clone(),
        workspace_status: app_state.workspace_status.clone(),
        repository_items: app_state.repository_items.clone(),
        repo_details_cache: app_state.repo_details_cache.clone(),
        repo_details_loading: app_state.repo_details_loading.clone(),
        monitor_filter: app_state.monitor_filter.clone(),
        monitor_filter_mode: app_state.monitor_filter_mode,
        monitor_sort_mode: app_state.monitor_sort_mode,
        monitor_sort_descending: app_state.monitor_sort_descending,
        selected_repo_ids: app_state.selected_repo_ids.clone(),
        active_repo_id: app_state.active_repo_id.clone(),
        logs: app_state.logs.clone(),
        history_report: app_state.history_report.clone(),
        history_report_loading: app_state.history_report_loading,
        line_stats_report: app_state.line_stats_report.clone(),
        line_stats_loading: app_state.line_stats_loading,
        line_stats_since: app_state.line_stats_since.clone(),
    }
}

fn refresh_repository_view_handle(
    handle: &RepositoryViewHandle,
    list: &ListBox,
    _scroller: &ScrolledWindow,
    snapshot: &StateSnapshot,
) {
    sync_repository_monitor_store(&handle.store, &all_monitor_items(snapshot));
    handle.filter.changed(FilterChange::Different);
    handle.sorter.changed(SorterChange::Different);
    sync_repository_monitor_selection(list, &snapshot.selected_repo_ids);

    let filtered_items = visible_monitor_items(snapshot);
    update_repository_monitor_empty_state(list, snapshot, filtered_items.is_empty());
}

fn status_label(state: &ronomepo_core::RepositoryState) -> &'static str {
    match state {
        ronomepo_core::RepositoryState::Unknown => "Unknown",
        ronomepo_core::RepositoryState::Missing => "Missing",
        ronomepo_core::RepositoryState::Clean => "Clean",
        ronomepo_core::RepositoryState::Dirty => "Dirty",
        ronomepo_core::RepositoryState::Untracked => "Untracked",
    }
}

fn manifest_presence_label(scan: Option<&RepoManifestScan>) -> &'static str {
    match scan.map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(_)) => "Manifest",
        Some(RepoManifestScanState::Invalid { .. }) => "Manifest!",
        Some(RepoManifestScanState::Missing) => "No Manifest",
        None => "",
    }
}

fn manifest_presence_search_text(scan: Option<&RepoManifestScan>) -> &'static str {
    match scan.map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(_)) => "manifest valid",
        Some(RepoManifestScanState::Invalid { .. }) => "manifest invalid",
        Some(RepoManifestScanState::Missing) => "manifest missing",
        None => "",
    }
}

fn manifest_presence_tooltip(scan: Option<&RepoManifestScan>) -> Option<String> {
    match scan.map(|scan| (&scan.path, &scan.state)) {
        Some((path, RepoManifestScanState::Valid(summary))) => Some(format!(
            "{}\n{} items | types: {} | actions: {}",
            path.display(),
            summary.item_count,
            summary.item_types.join(", "),
            summary
                .supported_actions
                .iter()
                .map(|action| standard_action_label(*action).to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Some((path, RepoManifestScanState::Invalid { message })) => {
            Some(format!("{}\nInvalid manifest: {}", path.display(), message))
        }
        Some((path, RepoManifestScanState::Missing)) => {
            Some(format!("{} not found.", path.display()))
        }
        None => None,
    }
}

fn standard_action_label(action: ronomepo_core::StandardActionName) -> &'static str {
    match action {
        ronomepo_core::StandardActionName::ListArtifacts => "list_artifacts",
        ronomepo_core::StandardActionName::Build => "build",
        ronomepo_core::StandardActionName::Test => "test",
        ronomepo_core::StandardActionName::Clean => "clean",
        ronomepo_core::StandardActionName::VerifyDependenciesFreshness => {
            "verify_dependencies_freshness"
        }
        ronomepo_core::StandardActionName::Deploy => "deploy",
    }
}

fn branch_label(item: &RepositoryListItem) -> &str {
    item.status.branch.as_deref().unwrap_or("detached")
}

fn all_monitor_items(snapshot: &StateSnapshot) -> Vec<RepositoryListItem> {
    let mut items = vec![monorepo_monitor_item(snapshot)];
    items.extend(repository_items(snapshot));
    items
}

fn sync_repository_monitor_store(store: &gio::ListStore, next_items: &[RepositoryListItem]) {
    let mut index = 0usize;
    while index < next_items.len() {
        let next_item = &next_items[index];
        let current = store
            .item(index as u32)
            .and_then(|item| item.downcast::<BoxedAnyObject>().ok())
            .map(|boxed| boxed.borrow::<RepositoryListItem>().clone());

        match current {
            Some(current_item) if current_item.id == next_item.id => {
                if current_item != *next_item {
                    store.splice(index as u32, 1, &[BoxedAnyObject::new(next_item.clone())]);
                }
                index += 1;
            }
            Some(_) => {
                store.splice(index as u32, 1, &[BoxedAnyObject::new(next_item.clone())]);
                index += 1;
            }
            None => {
                store.append(&BoxedAnyObject::new(next_item.clone()));
                index += 1;
            }
        }
    }

    while store.n_items() > next_items.len() as u32 {
        store.remove(store.n_items() - 1);
    }
}

fn update_repository_monitor_empty_state(list: &ListBox, snapshot: &StateSnapshot, is_empty: bool) {
    if is_empty {
        list.add_css_class("repo-monitor-empty");
        list.set_placeholder(Some(&repository_monitor_empty_placeholder(snapshot)));
    } else {
        list.remove_css_class("repo-monitor-empty");
        list.set_placeholder(Option::<&gtk::Widget>::None);
    }
}

fn repository_monitor_empty_placeholder(snapshot: &StateSnapshot) -> GtkBox {
    let empty = GtkBox::new(Orientation::Vertical, 6);
    empty.set_margin_top(18);
    empty.set_margin_bottom(18);
    empty.set_margin_start(12);
    empty.set_margin_end(12);

    let title = Label::new(Some(if snapshot.manifest.is_some() {
        "No repositories match the current filter"
    } else {
        "No workspace manifest loaded"
    }));
    title.set_xalign(0.0);
    title.add_css_class("title-4");

    let body = Label::new(Some(if snapshot.manifest.is_some() {
        "Try a broader search term or clear the filter to see the full workspace."
    } else {
        "Ronomepo is running, but no ronomepo.json was found. Import repos.txt from the current workspace root to bootstrap the manifest."
    }));
    body.set_xalign(0.0);
    body.set_wrap(true);

    empty.append(&title);
    empty.append(&body);
    empty
}

fn sync_repository_monitor_selection(list: &ListBox, selected_repo_ids: &[String]) {
    list.unselect_all();
    let mut index = 0;
    while let Some(row) = list.row_at_index(index) {
        if repo_id_from_list_box_row(&row)
            .is_some_and(|id| selected_repo_ids.iter().any(|selected| selected == &id))
        {
            list.select_row(Some(&row));
        }
        index += 1;
    }
    sync_selection_css(list);
}

fn sync_selection_css(list: &ListBox) {
    let mut index = 0;
    while let Some(row) = list.row_at_index(index) {
        if row.is_selected() {
            row.add_css_class("repo-selected");
        } else {
            row.remove_css_class("repo-selected");
        }
        index += 1;
    }
}

fn repo_id_from_list_box_row(row: &ListBoxRow) -> Option<String> {
    row.child()
        .map(|child| child.widget_name().to_string())
        .filter(|id| !id.is_empty())
}

fn repo_item_from_object(object: &glib::Object) -> Option<RepositoryListItem> {
    object
        .downcast_ref::<BoxedAnyObject>()
        .map(|boxed| boxed.borrow::<RepositoryListItem>().clone())
}

fn repo_monitor_filter_matches(item: &RepositoryListItem, snapshot: &StateSnapshot) -> bool {
    if !matches_filter_mode(item, snapshot.monitor_filter_mode) {
        return false;
    }
    let filter = snapshot.monitor_filter.trim().to_ascii_lowercase();
    if filter.is_empty() {
        return true;
    }
    let branch = branch_label(item).to_ascii_lowercase();
    let sync = format_sync_label(&item.status.sync).to_ascii_lowercase();
    let state = status_label(&item.status.state).to_ascii_lowercase();
    let manifest = manifest_presence_search_text(item.repo_manifest.as_ref()).to_ascii_lowercase();
    item.name.to_ascii_lowercase().contains(&filter)
        || item.dir_name.to_ascii_lowercase().contains(&filter)
        || item.remote_url.to_ascii_lowercase().contains(&filter)
        || branch.contains(&filter)
        || sync.contains(&filter)
        || state.contains(&filter)
        || manifest.contains(&filter)
}

fn build_repo_monitor_row(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let content = GtkBox::new(Orientation::Horizontal, 10);
    content.set_margin_top(8);
    content.set_margin_bottom(8);
    content.set_margin_start(10);
    content.set_margin_end(10);
    content.set_widget_name(&item.id);
    content.set_tooltip_text(Some(&format!(
        "{}\n{}\n{}",
        item.status.repo_path.display(),
        item.remote_url,
        item.dir_name
    )));

    let name = monitor_text_cell(
        &item.name,
        MONITOR_NAME_COL_CHARS,
        MONITOR_NAME_COL_WIDTH,
        false,
    );
    let branch = monitor_text_cell(
        branch_label(item),
        MONITOR_BRANCH_COL_CHARS,
        MONITOR_BRANCH_COL_WIDTH,
        false,
    );
    let manifest = monitor_manifest_cell(item.repo_manifest.as_ref());
    let status = monitor_state_cell(&item.status.state);

    let sync = monitor_sync_cell(&item.status.sync);

    content.append(&name);
    content.append(&branch);
    content.append(&manifest);
    content.append(&status);
    content.append(&sync);
    attach_repo_monitor_context_menu(&content, host_ptr);
    content
}

fn repository_items(snapshot: &StateSnapshot) -> Vec<RepositoryListItem> {
    snapshot.repository_items.clone()
}

const MONOREPO_ROW_ID: &str = "__ronomepo_monorepo__";

fn monorepo_monitor_item(snapshot: &StateSnapshot) -> RepositoryListItem {
    RepositoryListItem {
        id: MONOREPO_ROW_ID.to_string(),
        name: "(monorepo)".to_string(),
        dir_name: ".".to_string(),
        remote_url: snapshot.workspace_root.display().to_string(),
        status: snapshot.workspace_status.clone(),
        repo_manifest: None,
    }
}

fn visible_monitor_items(snapshot: &StateSnapshot) -> Vec<RepositoryListItem> {
    filtered_repository_items(snapshot, all_monitor_items(snapshot))
}

fn filtered_repository_items(
    snapshot: &StateSnapshot,
    mut items: Vec<RepositoryListItem>,
) -> Vec<RepositoryListItem> {
    items.sort_by(|left, right| repo_monitor_sort_cmp(snapshot, left, right));

    let mode = snapshot.monitor_filter_mode;
    items.retain(|item| matches_filter_mode(item, mode));

    let filter = snapshot.monitor_filter.trim().to_ascii_lowercase();
    if filter.is_empty() {
        return items;
    }

    items
        .into_iter()
        .filter(|item| repo_monitor_filter_matches(item, snapshot))
        .collect()
}

fn repo_monitor_sort_key(item: &RepositoryListItem) -> (u8, String) {
    (
        u8::from(item.id == MONOREPO_ROW_ID),
        item.name.to_ascii_lowercase(),
    )
}

fn monitor_sort_mode_label(mode: MonitorSortMode) -> &'static str {
    match mode {
        MonitorSortMode::Name => "Name",
        MonitorSortMode::SyncState => "Sync state",
        MonitorSortMode::RecentActivity => "Recent activity",
    }
}

fn monitor_sort_mode_from_index(index: u32) -> MonitorSortMode {
    match index {
        1 => MonitorSortMode::SyncState,
        2 => MonitorSortMode::RecentActivity,
        _ => MonitorSortMode::Name,
    }
}

fn monitor_sort_mode_index(mode: MonitorSortMode) -> u32 {
    match mode {
        MonitorSortMode::Name => 0,
        MonitorSortMode::SyncState => 1,
        MonitorSortMode::RecentActivity => 2,
    }
}

fn monitor_sort_direction_label(descending: bool) -> &'static str {
    if descending {
        "↓"
    } else {
        "↑"
    }
}

fn repo_monitor_sort_cmp(
    snapshot: &StateSnapshot,
    left: &RepositoryListItem,
    right: &RepositoryListItem,
) -> std::cmp::Ordering {
    let monorepo_rank = u8::from(left.id == MONOREPO_ROW_ID).cmp(&u8::from(right.id == MONOREPO_ROW_ID));
    if monorepo_rank != std::cmp::Ordering::Equal {
        return monorepo_rank;
    }

    let ordering = match snapshot.monitor_sort_mode {
        MonitorSortMode::Name => left
            .name
            .to_ascii_lowercase()
            .cmp(&right.name.to_ascii_lowercase()),
        MonitorSortMode::SyncState => repo_attention_rank(left)
            .cmp(&repo_attention_rank(right))
            .then_with(|| left.name.to_ascii_lowercase().cmp(&right.name.to_ascii_lowercase())),
        MonitorSortMode::RecentActivity => repo_last_activity_sort_key(snapshot, left)
            .cmp(&repo_last_activity_sort_key(snapshot, right))
            .then_with(|| left.name.to_ascii_lowercase().cmp(&right.name.to_ascii_lowercase())),
    };

    if snapshot.monitor_sort_descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn repo_last_activity_sort_key(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> (i64, u8, String) {
    let committed_at_epoch_secs = snapshot
        .repo_details_cache
        .get(&item.id)
        .and_then(|details| details.last_commit.as_ref())
        .map(|commit| commit.committed_at_epoch_secs)
        .unwrap_or(i64::MIN);
    (
        committed_at_epoch_secs,
        repo_attention_rank(item),
        item.name.to_ascii_lowercase(),
    )
}

fn matches_filter_mode(item: &RepositoryListItem, mode: MonitorFilterMode) -> bool {
    use ronomepo_core::{RepositoryState, RepositorySync};

    match mode {
        MonitorFilterMode::All => true,
        MonitorFilterMode::Dirty => {
            !matches!(item.status.state, RepositoryState::Clean)
                || !matches!(
                    item.status.sync,
                    RepositorySync::UpToDate | RepositorySync::NoUpstream
                )
        }
        MonitorFilterMode::ToSync => !matches!(
            item.status.sync,
            RepositorySync::UpToDate | RepositorySync::NoUpstream
        ),
        MonitorFilterMode::Issues => {
            matches!(
                item.status.state,
                RepositoryState::Missing | RepositoryState::Unknown
            ) || matches!(
                item.status.sync,
                RepositorySync::Diverged { .. }
                    | RepositorySync::NoUpstream
                    | RepositorySync::Unknown
            )
        }
    }
}

fn repo_attention_rank(item: &RepositoryListItem) -> u8 {
    use ronomepo_core::{RepositoryState, RepositorySync};

    match (&item.status.state, &item.status.sync) {
        (RepositoryState::Missing, _) => 0,
        (RepositoryState::Dirty, _) | (RepositoryState::Untracked, _) => 1,
        (_, RepositorySync::Diverged { .. }) => 2,
        (_, RepositorySync::Behind(_)) => 3,
        (_, RepositorySync::Ahead(_)) => 4,
        (RepositoryState::Unknown, _) | (_, RepositorySync::Unknown) => 5,
        (_, RepositorySync::NoUpstream) => 6,
        _ => 7,
    }
}

fn selection_ids_from_list(list: &ListBox) -> Vec<String> {
    let mut selected = list
        .selected_rows()
        .into_iter()
        .filter_map(|row| repo_id_from_list_box_row(&row))
        .filter(|id| !id.is_empty())
        .filter(|id| id != MONOREPO_ROW_ID)
        .collect::<Vec<_>>();
    selected.sort();
    selected.dedup();
    selected
}

fn update_selected_repo_ids(ids: Vec<String>) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.selected_repo_ids = ids;
}

fn update_monitor_filter(filter: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.monitor_filter = filter;
}

extern "C" fn command_filter(
    payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    let text = if payload.ptr.is_null() || payload.len == 0 {
        String::new()
    } else {
        let bytes = unsafe { std::slice::from_raw_parts(payload.ptr, payload.len) };
        String::from_utf8_lossy(bytes).into_owned()
    };
    update_monitor_filter(text);
    refresh_views();
    maruzzella_sdk::ffi::MzStatus::OK
}

fn update_monitor_filter_mode(mode: MonitorFilterMode) {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.monitor_filter_mode = mode;
    }
    persist_monitor_filter_mode(mode);
}

fn update_monitor_sort_mode(mode: MonitorSortMode) {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.monitor_sort_mode = mode;
    }
    if mode == MonitorSortMode::RecentActivity {
        prefetch_monitor_sort_details();
    }
    persist_monitor_sort_mode(mode);
}

fn update_monitor_sort_descending(descending: bool) {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.monitor_sort_descending = descending;
    }
    persist_monitor_sort_descending(descending);
}

fn prefetch_monitor_sort_details() {
    let snapshot = snapshot();
    for item in snapshot.repository_items {
        schedule_repo_details_load(&item.id, &item.status.repo_path);
    }
}

fn update_line_stats_since(value: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.line_stats_since = value;
}

fn open_repo_overviews(host_ptr: *const maruzzella_sdk::ffi::MzHostApi, repo_ids: &[String]) {
    let snapshot = snapshot();
    let items = repository_items(&snapshot);
    let selected = items
        .iter()
        .filter(|item| repo_ids.iter().any(|id| id == &item.id))
        .collect::<Vec<_>>();

    if selected.is_empty() {
        append_log("No repository selected.".to_string());
        refresh_views();
        return;
    }

    if host_ptr.is_null() {
        append_log(
            "Cannot open repo overview because the Maruzzella host handle is unavailable."
                .to_string(),
        );
        refresh_views();
        return;
    }

    for item in selected {
        if let Err(message) = open_repo_overview_for_item(host_ptr, item) {
            append_log(message);
            continue;
        }
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.active_repo_id = Some(item.id.clone());
    }
    refresh_views();
}

fn sync_repo_runtime_state(app_state: &mut AppState) {
    let now = SystemTime::now();
    let expected_ids = app_state
        .manifest
        .as_ref()
        .map(|manifest| {
            manifest
                .repos
                .iter()
                .map(|repo| repo.id.clone())
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();

    app_state
        .repo_details_cache
        .retain(|repo_id, _| expected_ids.contains(repo_id));
    app_state
        .repo_details_loading
        .retain(|repo_id| expected_ids.contains(repo_id));
    app_state
        .repo_runtime
        .retain(|repo_id, _| expected_ids.contains(repo_id));

    for repo_id in expected_ids {
        app_state
            .repo_runtime
            .entry(repo_id.clone())
            .or_insert_with(|| RepoRuntimeState::new(now, &repo_id));
    }
}

fn mark_full_workspace_scan_completed(
    app_state: &mut AppState,
    repository_items: &[RepositoryListItem],
) {
    let now = SystemTime::now();
    for item in repository_items {
        let runtime = app_state
            .repo_runtime
            .entry(item.id.clone())
            .or_insert_with(|| RepoRuntimeState::new(now, &item.id));
        runtime.last_scanned_seq = runtime.invalidation_seq;
        runtime.scheduled_scan_seq = runtime.invalidation_seq;
        runtime.local_refresh_in_flight = false;
        runtime.last_local_scan_at = Some(now);
    }
}

fn mark_repo_scan_completed(app_state: &mut AppState, repo_id: &str) {
    let now = SystemTime::now();
    let runtime = app_state
        .repo_runtime
        .entry(repo_id.to_string())
        .or_insert_with(|| RepoRuntimeState::new(now, repo_id));
    runtime.local_refresh_in_flight = false;
    runtime.last_scanned_seq = runtime.last_scanned_seq.max(runtime.scheduled_scan_seq);
    runtime.last_local_scan_at = Some(now);
}

fn finalize_repo_status_refresh(app_state: &mut AppState, repo_id: &str) -> Option<PathBuf> {
    mark_repo_scan_completed(app_state, repo_id);

    let needs_follow_up = app_state
        .repo_runtime
        .get(repo_id)
        .is_some_and(RepoRuntimeState::needs_rescan);
    if !needs_follow_up {
        return None;
    }

    let repo_path = app_state
        .repository_items
        .iter()
        .find(|item| item.id == repo_id)
        .map(|item| item.status.repo_path.clone())?;

    if let Some(runtime) = app_state.repo_runtime.get_mut(repo_id) {
        runtime.local_refresh_in_flight = true;
        runtime.scheduled_scan_seq = runtime.invalidation_seq;
    }

    Some(repo_path)
}

fn mark_remote_fetch_completed(app_state: &mut AppState, repo_id: &str, success: bool) {
    let now = SystemTime::now();
    let runtime = app_state
        .repo_runtime
        .entry(repo_id.to_string())
        .or_insert_with(|| RepoRuntimeState::new(now, repo_id));
    runtime.remote_fetch_in_flight = false;
    if success {
        runtime.last_fetch_at = Some(now);
        runtime.next_fetch_due_at = next_remote_fetch_due_at(now, repo_id);
    } else {
        runtime.next_fetch_due_at = retry_remote_fetch_due_at(now, repo_id);
    }
}

fn mark_repo_stale(app_state: &mut AppState, repo_id: &str) -> bool {
    let now = SystemTime::now();
    let runtime = app_state
        .repo_runtime
        .entry(repo_id.to_string())
        .or_insert_with(|| RepoRuntimeState::new(now, repo_id));
    let was_stale = runtime.needs_rescan();
    runtime.invalidation_seq = runtime.invalidation_seq.saturating_add(1);
    !was_stale
}

fn schedule_pending_local_rescans() {
    let scheduled = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state
            .repository_items
            .iter()
            .filter_map(|item| {
                let runtime = app_state.repo_runtime.get(&item.id)?;
                if runtime.local_refresh_in_flight || !runtime.needs_rescan() {
                    return None;
                }
                Some(item.clone())
            })
            .collect::<Vec<_>>()
    };

    if !scheduled.is_empty() {
        let mut app_state = state().lock().expect("state mutex poisoned");
        for item in &scheduled {
            if let Some(runtime) = app_state.repo_runtime.get_mut(&item.id) {
                runtime.local_refresh_in_flight = true;
                runtime.scheduled_scan_seq = runtime.invalidation_seq;
            }
        }
    }

    for item in scheduled {
        schedule_repository_status_refresh(&item);
    }
}

fn mark_all_repos_stale() {
    let mut app_state = state().lock().expect("state mutex poisoned");
    for runtime in app_state.repo_runtime.values_mut() {
        runtime.invalidation_seq = runtime.invalidation_seq.saturating_add(1);
    }
}

fn schedule_due_remote_fetches() {
    let scheduled = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let inflight = app_state
            .repo_runtime
            .values()
            .filter(|runtime| runtime.remote_fetch_in_flight)
            .count();
        let capacity = REMOTE_FETCH_CONCURRENCY.saturating_sub(inflight);
        if capacity == 0 {
            Vec::new()
        } else {
            let now = SystemTime::now();
            let items = app_state
                .repository_items
                .iter()
                .map(|item| {
                    (
                        item.id.clone(),
                        item.name.clone(),
                        item.status.repo_path.clone(),
                        item.status.state.clone(),
                        item.status.sync.clone(),
                    )
                })
                .collect::<Vec<_>>();
            let mut due = items
                .into_iter()
                .filter_map(|(repo_id, repo_name, repo_path, state, sync)| {
                    if matches!(state, ronomepo_core::RepositoryState::Missing)
                        || matches!(
                            sync,
                            ronomepo_core::RepositorySync::NoUpstream
                                | ronomepo_core::RepositorySync::Unknown
                        )
                    {
                        return None;
                    }
                    let runtime = app_state.repo_runtime.get_mut(&repo_id)?;
                    if runtime.remote_fetch_in_flight || runtime.next_fetch_due_at > now {
                        return None;
                    }
                    runtime.remote_fetch_in_flight = true;
                    Some((repo_id, repo_name, repo_path, runtime.next_fetch_due_at))
                })
                .collect::<Vec<_>>();
            due.sort_by_key(|(_, _, _, due_at)| *due_at);
            due.truncate(capacity);
            due.into_iter()
                .map(|(repo_id, repo_name, repo_path, _)| (repo_id, repo_name, repo_path))
                .collect()
        }
    };

    for (repo_id, repo_name, repo_path) in scheduled {
        schedule_repository_remote_fetch(&repo_id, &repo_name, repo_path);
    }
}

fn schedule_workspace_scan() {
    let (workspace_root, manifest) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.repository_items_loading {
            app_state.repository_items_refresh_pending = true;
            return;
        }
        app_state.repository_items_loading = true;
        app_state.repository_items_refresh_pending = false;
        (app_state.workspace_root.clone(), app_state.manifest.clone())
    };

    if let Err(message) = submit_coalesced_job(
        JobKey::WorkspaceScan,
        WorkerJob::WorkspaceScan {
            workspace_root,
            manifest,
        },
    ) {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.repository_items_loading = false;
        append_log(format!("Workspace scan failed to start: {message}"));
        refresh_views();
    }
}

fn schedule_workspace_root_status_refresh(workspace_root: PathBuf) {
    if let Err(message) = submit_coalesced_job(
        JobKey::WorkspaceRootStatus,
        WorkerJob::WorkspaceRootStatusRefresh { workspace_root },
    ) {
        append_log(format!("Workspace root refresh failed to start: {message}"));
        refresh_views();
    }
}

fn schedule_repository_status_refresh(item: &RepositoryListItem) {
    let repo_id = item.id.clone();
    match submit_coalesced_job(
        JobKey::RepoStatus(repo_id.clone()),
        WorkerJob::RepositoryStatusRefresh { item: item.clone() },
    ) {
        Ok(true) => {}
        Ok(false) => {}
        Err(message) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            if let Some(runtime) = app_state.repo_runtime.get_mut(&repo_id) {
                runtime.local_refresh_in_flight = false;
            }
            drop(app_state);
            append_log(format!(
                "Repository status refresh failed to start: {message}"
            ));
            refresh_views();
        }
    }
}

fn repository_item_from_state(repo_id: &str) -> Option<RepositoryListItem> {
    state()
        .lock()
        .expect("state mutex poisoned")
        .repository_items
        .iter()
        .find(|item| item.id == repo_id)
        .cloned()
}

fn schedule_repository_remote_fetch(repo_id: &str, repo_name: &str, repo_path: PathBuf) {
    let repo_id = repo_id.to_string();
    let repo_name = repo_name.to_string();
    match submit_coalesced_job(
        JobKey::RepoFetch(repo_id.clone()),
        WorkerJob::RepositoryRemoteFetch {
            repo_id: repo_id.clone(),
            repo_name,
            repo_path,
        },
    ) {
        Ok(true) => {}
        Ok(false) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            if let Some(runtime) = app_state.repo_runtime.get_mut(&repo_id) {
                runtime.remote_fetch_in_flight = false;
            }
        }
        Err(message) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            if let Some(runtime) = app_state.repo_runtime.get_mut(&repo_id) {
                runtime.remote_fetch_in_flight = false;
            }
            drop(app_state);
            append_log(format!("Remote sync refresh failed to start: {message}"));
            refresh_views();
        }
    }
}

fn invalidate_repo_details(repo_id: &str) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.repo_details_cache.remove(repo_id);
    app_state.repo_details_loading.remove(repo_id);
}

fn schedule_repo_details_load(repo_id: &str, repo_path: &Path) {
    let repo_id = repo_id.to_string();
    let repo_path = repo_path.to_path_buf();
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.repo_details_cache.contains_key(&repo_id)
            || !app_state.repo_details_loading.insert(repo_id.clone())
        {
            return;
        }
    }

    let job_repo_id = repo_id.clone();
    if let Err(message) = submit_coalesced_job(
        JobKey::RepoDetails(repo_id.clone()),
        WorkerJob::RepoDetailsLoad {
            repo_id: job_repo_id,
            repo_path,
        },
    ) {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.repo_details_loading.remove(&repo_id);
        append_log(format!(
            "Repository details load failed to start: {message}"
        ));
        refresh_views();
    }
}

fn sync_watch_manager_from_state() {
    let manifest = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.watch_manager_sync_in_flight {
            app_state.watch_manager_sync_pending = true;
            WATCH_SYNC_PENDING_COLLAPSES.fetch_add(1, Ordering::Relaxed);
            return;
        }
        app_state.watch_manager_sync_in_flight = true;
        app_state.watch_manager_sync_pending = false;
        app_state.manifest.clone()
    };

    WATCH_SYNC_REQUESTS.fetch_add(1, Ordering::Relaxed);
    let sync_seq = WATCH_MANAGER_SYNC_SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    if let Err(error) = submit_coalesced_job(
        JobKey::WatchManagerSync,
        WorkerJob::WatchManagerSync { manifest, sync_seq },
    ) {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.watch_manager_sync_in_flight = false;
        app_state.watch_manager_sync_pending = false;
        WATCH_SYNC_FAILURES.fetch_add(1, Ordering::Relaxed);
        append_log(format!("Repository watcher setup failed to start: {error}"));
        emit_runtime_profile("watch-sync-submit-failed");
    }
}

fn build_watch_manager(manifest: &WorkspaceManifest) -> Result<WatchManager, String> {
    let workspace_root = normalized_watch_path(&manifest.root);
    let repos = manifest
        .repos
        .iter()
        .map(|repo| (repo.id.clone(), workspace_root.join(&repo.dir_name)))
        .filter(|(_, path)| path.exists())
        .collect::<Vec<_>>();

    let (mut backend, backend_kind) = create_watch_backend()?;
    let workspace_git_dir = workspace_root.join(".git");
    let mut watched_paths = 0;

    if workspace_root.exists() {
        watch_backend_mut(&mut backend)
            .watch(&workspace_root, RecursiveMode::NonRecursive)
            .map_err(|error| format!("{}: {error}", workspace_root.display()))?;
        watched_paths += 1;
    }
    if workspace_git_dir.exists() {
        watch_backend_mut(&mut backend)
            .watch(&workspace_git_dir, RecursiveMode::Recursive)
            .map_err(|error| format!("{}: {error}", workspace_git_dir.display()))?;
        watched_paths += 1;
    }

    for (_, path) in &repos {
        watch_backend_mut(&mut backend)
            .watch(path, RecursiveMode::Recursive)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        watched_paths += 1;
    }

    if watched_paths == 0 {
        return Err("no local repositories are available to watch".to_string());
    }

    Ok(WatchManager {
        _backend: backend,
        backend_kind,
        watched_paths,
    })
}

fn create_watch_backend() -> Result<(WatchBackend, WatchBackendKind), String> {
    let config = NotifyConfig::default();
    match RecommendedWatcher::new(dispatch_watch_event_result, config) {
        Ok(watcher) => Ok((
            WatchBackend::Recommended(watcher),
            WatchBackendKind::Recommended,
        )),
        Err(_) => PollWatcher::new(
            dispatch_watch_event_result,
            config.with_poll_interval(Duration::from_secs(WATCH_POLL_FALLBACK_SECS)),
        )
        .map(|watcher| (WatchBackend::Poll(watcher), WatchBackendKind::Poll))
        .map_err(|error| error.to_string()),
    }
}

fn watch_backend_mut(backend: &mut WatchBackend) -> &mut dyn Watcher {
    match backend {
        WatchBackend::Recommended(watcher) => watcher,
        WatchBackend::Poll(watcher) => watcher,
    }
}

fn dispatch_watch_event_result(event: notify::Result<notify::Event>) {
    let main_context = glib::MainContext::default();
    match event {
        Ok(event) => {
            if event.paths.is_empty() {
                return;
            }
            WATCH_EVENTS_RECEIVED.fetch_add(1, Ordering::Relaxed);
            let should_schedule = {
                let mut pending = pending_watch_events()
                    .lock()
                    .expect("pending watch events mutex poisoned");
                if !pending.overflowed {
                    pending.paths.extend(event.paths);
                    if pending.paths.len() > MAX_PENDING_WATCH_PATHS {
                        pending.paths.clear();
                        pending.overflowed = true;
                    }
                }
                let pending_len = if pending.overflowed {
                    MAX_PENDING_WATCH_PATHS
                } else {
                    pending.paths.len()
                };
                PENDING_WATCH_PATHS.store(pending_len, Ordering::Relaxed);
                update_max_atomic(&PENDING_WATCH_PATHS_HIGH_WATER, pending_len);
                if pending.flush_scheduled {
                    false
                } else {
                    pending.flush_scheduled = true;
                    true
                }
            };
            if should_schedule {
                main_context.invoke(schedule_watch_event_flush);
            }
        }
        Err(error) => {
            let message = error.to_string();
            main_context.invoke(move || append_log(format!("Repository watcher error: {message}")));
        }
    }
}

fn schedule_watch_event_flush() {
    glib::timeout_add_local(Duration::from_millis(UI_REFRESH_DEBOUNCE_MILLIS), || {
        flush_pending_watch_events();
        glib::ControlFlow::Break
    });
}

fn flush_pending_watch_events() {
    WATCH_EVENT_FLUSHES.fetch_add(1, Ordering::Relaxed);
    let (paths, overflowed) = {
        let mut pending = pending_watch_events()
            .lock()
            .expect("pending watch events mutex poisoned");
        pending.flush_scheduled = false;
        let overflowed = pending.overflowed;
        pending.overflowed = false;
        PENDING_WATCH_PATHS.store(0, Ordering::Relaxed);
        (std::mem::take(&mut pending.paths), overflowed)
    };

    if overflowed {
        handle_overflow_watch_paths();
        return;
    }

    if paths.is_empty() {
        return;
    }

    handle_watch_paths(paths.into_iter().collect());
}

fn handle_watch_paths(paths: Vec<PathBuf>) {
    let (workspace_touched, any_marked) = {
        let mut touched = HashSet::new();
        let mut app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.clone() else {
            return;
        };
        let mut workspace_touched = false;

        for path in paths {
            if workspace_watch_path_matches(&manifest, &path) {
                workspace_touched = true;
            }
            if let Some(repo_id) = repo_id_for_watch_path(&manifest, &path) {
                touched.insert(repo_id);
            }
        }

        let any_marked = touched
            .iter()
            .any(|repo_id| mark_repo_stale(&mut app_state, repo_id));
        (workspace_touched, any_marked)
    };

    if workspace_touched {
        let workspace_root = {
            let app_state = state().lock().expect("state mutex poisoned");
            app_state.workspace_root.clone()
        };
        schedule_workspace_root_status_refresh(workspace_root);
    }
    if any_marked {
        schedule_pending_local_rescans();
    }
}

fn handle_overflow_watch_paths() {
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    mark_all_repos_stale();
    append_log(
        "Repository watcher overflowed pending events; forcing a full local refresh.".to_string(),
    );
    schedule_workspace_root_status_refresh(workspace_root);
    schedule_pending_local_rescans();
}

fn workspace_watch_path_matches(manifest: &WorkspaceManifest, path: &Path) -> bool {
    let workspace_root = normalized_watch_path(&manifest.root);
    let path = normalized_watch_path(path);

    if let Ok(relative) = path.strip_prefix(&workspace_root) {
        if relative.components().count() == 0 {
            return false;
        }
        if relative
            .iter()
            .next()
            .is_some_and(|component| component == ".git")
        {
            return watch_path_is_relevant(relative);
        }
        return relative.components().count() == 1 && watch_path_is_relevant(relative);
    }

    path.strip_prefix(workspace_root.join(".git"))
        .ok()
        .is_some_and(watch_path_is_relevant)
}

fn repo_id_for_watch_path(manifest: &WorkspaceManifest, path: &Path) -> Option<String> {
    let workspace_root = normalized_watch_path(&manifest.root);
    let path = normalized_watch_path(path);
    let mut matches = manifest
        .repos
        .iter()
        .filter_map(|repo| {
            let repo_root = workspace_root.join(&repo.dir_name);
            let relative = path.strip_prefix(&repo_root).ok()?;
            if !watch_path_is_relevant(relative) {
                return None;
            }
            Some((repo.id.clone(), repo_root.components().count()))
        })
        .collect::<Vec<_>>();
    matches.sort_by_key(|(_, depth)| *depth);
    matches.pop().map(|(repo_id, _)| repo_id)
}

fn watch_path_is_relevant(relative: &Path) -> bool {
    let path_text = relative.to_string_lossy();
    if path_text.starts_with(".git/objects") || path_text.starts_with(".git/lfs") {
        return false;
    }
    if path_text.ends_with(".swp")
        || path_text.ends_with(".swx")
        || path_text.ends_with('~')
        || path_text.ends_with(".tmp")
    {
        return false;
    }
    true
}

fn fetch_repository_remote(repo_path: &Path) -> Result<(), String> {
    if !repo_path.exists() {
        return Err(format!("{} is missing locally", repo_path.display()));
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(["fetch", "--quiet", "--all", "--prune"])
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn open_repo_overview_for_item(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    item: &RepositoryListItem,
) -> Result<(), String> {
    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let mut request =
        OpenViewRequest::new(PLUGIN_ID, VIEW_REPO_OVERVIEW, MzViewPlacement::Workbench);
    request.instance_key = Some(&item.id);
    request.requested_title = Some(&item.name);
    request.payload = item.id.as_bytes();

    match host.open_view(&request) {
        Ok(MzViewOpenDisposition::Opened) => {
            append_log(format!("Opened repo overview for {}.", item.name));
            Ok(())
        }
        Ok(MzViewOpenDisposition::FocusedExisting) => {
            append_log(format!("Focused existing repo overview for {}.", item.name));
            Ok(())
        }
        Err(status) => Err(format!(
            "Failed to open repo overview for {}: {status:?}",
            item.name
        )),
    }
}

fn attach_repo_monitor_context_menu(
    relative_to: &impl IsA<gtk::Widget>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    let popover = build_repo_context_menu(relative_to, host_ptr);
    let gesture = GestureClick::new();
    gesture.set_button(3);
    gesture.connect_pressed({
        let relative_to = relative_to.clone();
        let popover = popover.clone();
        move |_, _, x, y| {
            if let Some(row) = relative_to.parent().and_downcast::<ListBoxRow>() {
                if let Some(list) = row.parent().and_downcast::<ListBox>() {
                    update_repo_context_selection(&list, &row);
                    refresh_repo_context_menu(&popover, host_ptr);
                }
            }
            popover.set_pointing_to(Some(&Rectangle::new(x as i32, y as i32, 1, 1)));
            popover.popup();
        }
    });
    relative_to.add_controller(gesture);
}

fn build_repo_context_menu(
    relative_to: &impl IsA<gtk::Widget>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> Popover {
    let popover = Popover::new();
    popover.set_autohide(true);
    popover.set_has_arrow(false);
    popover.set_position(PositionType::Bottom);
    popover.set_parent(relative_to);

    refresh_repo_context_menu(&popover, host_ptr);
    popover
}

fn attach_root_context_menu_close(
    menu: &GtkBox,
    popover: &Popover,
    root_hovered: &Rc<Cell<bool>>,
    schedule_close: &Rc<dyn Fn()>,
) {
    let root_hovered_for_enter = Rc::clone(root_hovered);
    let motion = EventControllerMotion::new();
    motion.connect_enter(move |_, _, _| {
        root_hovered_for_enter.set(true);
    });
    let root_hovered_for_leave = Rc::clone(root_hovered);
    let schedule_close_for_root = Rc::clone(schedule_close);
    motion.connect_leave(move |_| {
        root_hovered_for_leave.set(false);
        schedule_close_for_root();
    });
    menu.add_controller(motion);

    popover.connect_show({
        let root_hovered = Rc::clone(root_hovered);
        move |_| {
            root_hovered.set(false);
        }
    });
}

fn update_repo_context_selection(list: &ListBox, row: &ListBoxRow) {
    let keep_existing_selection = row.is_selected() && list.selected_rows().len() > 1;
    if !keep_existing_selection {
        list.unselect_all();
        list.select_row(Some(row));
    }
    sync_selection_css(list);
    update_selected_repo_ids(selection_ids_from_list(list));
}

fn refresh_repo_context_menu(popover: &Popover, host_ptr: *const maruzzella_sdk::ffi::MzHostApi) {
    let menu = GtkBox::new(Orientation::Vertical, 0);
    menu.set_margin_top(4);
    menu.set_margin_bottom(4);
    menu.set_margin_start(4);
    menu.set_margin_end(4);

    let selection = selected_repository_items_from_state();
    let root_hovered = Rc::new(Cell::new(false));
    let child_hovered = Rc::new(Cell::new(false));
    let schedule_close: Rc<dyn Fn()> = Rc::new({
        let popover = popover.clone();
        let root_hovered = Rc::clone(&root_hovered);
        let child_hovered = Rc::clone(&child_hovered);
        move || {
            let popover = popover.clone();
            let root_hovered = Rc::clone(&root_hovered);
            let child_hovered = Rc::clone(&child_hovered);
            glib::timeout_add_local(Duration::from_millis(140), move || {
                if !root_hovered.get() && !child_hovered.get() {
                    popover.popdown();
                }
                glib::ControlFlow::Break
            });
        }
    });

    let mut has_section = false;
    if append_repo_context_open_section(
        &menu,
        popover,
        host_ptr,
        &selection,
        &child_hovered,
        &schedule_close,
    ) {
        has_section = true;
    }
    if append_repo_context_git_section(&menu, popover, &selection, &child_hovered, &schedule_close)
    {
        has_section = true;
    }
    if append_repo_context_ronomepo_section(
        &menu,
        popover,
        &selection,
        &child_hovered,
        &schedule_close,
    ) {
        has_section = true;
    }
    if !has_section {
        let empty = Label::new(Some("No actions available for this selection."));
        empty.set_xalign(0.0);
        empty.add_css_class("dim-label");
        empty.set_margin_top(6);
        empty.set_margin_bottom(6);
        empty.set_margin_start(8);
        empty.set_margin_end(8);
        menu.append(&empty);
    }

    attach_root_context_menu_close(&menu, popover, &root_hovered, &schedule_close);
    popover.set_child(Some(&menu));
}

fn append_repo_context_ronomepo_section(
    menu: &GtkBox,
    popover: &Popover,
    selection: &[RepositoryListItem],
    root_child_hovered: &Rc<Cell<bool>>,
    root_schedule_close: &Rc<dyn Fn()>,
) -> bool {
    let supported_actions = repo_context_supported_manifest_actions(selection);
    if supported_actions.is_empty() {
        return false;
    }

    let submenu = GtkBox::new(Orientation::Vertical, 0);
    for action in supported_actions {
        let label = context_action_label(action);
        append_context_button(&submenu, popover, &label, move || {
            run_selected_repo_manifest_action(action);
        });
    }

    append_context_submenu(
        menu,
        popover,
        "Ronomepo",
        &submenu,
        root_child_hovered,
        root_schedule_close,
    );
    true
}

fn append_repo_context_open_section(
    menu: &GtkBox,
    popover: &Popover,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    selection: &[RepositoryListItem],
    root_child_hovered: &Rc<Cell<bool>>,
    root_schedule_close: &Rc<dyn Fn()>,
) -> bool {
    let can_open_overview = !selection.is_empty();
    let can_open_folder = selection.iter().any(|item| item.status.repo_path.exists());
    let can_open_terminal = selection
        .iter()
        .filter(|item| item.status.repo_path.exists())
        .nth(1)
        .is_none()
        && selection.iter().any(|item| item.status.repo_path.exists());
    let can_open_in_editor = can_open_terminal;

    let has_actions =
        can_open_overview || can_open_folder || can_open_terminal || can_open_in_editor;
    if !has_actions {
        return false;
    }

    let submenu = GtkBox::new(Orientation::Vertical, 0);
    if can_open_overview {
        append_context_button(&submenu, popover, "Overview", move || {
            let repo_ids = {
                let app_state = state().lock().expect("state mutex poisoned");
                app_state.selected_repo_ids.clone()
            };
            open_repo_overviews(host_ptr, &repo_ids);
        });
    }
    if can_open_folder {
        append_context_button(&submenu, popover, "Folder", || {
            open_selected_repo_folders();
        });
    }
    if can_open_terminal {
        append_context_button(&submenu, popover, "Terminal", || {
            open_selected_repo_terminal();
        });
    }
    if can_open_in_editor {
        append_context_button(&submenu, popover, "In Editor", || {
            open_selected_repo_in_editor();
        });
    }
    append_context_submenu(
        menu,
        popover,
        "Open",
        &submenu,
        root_child_hovered,
        root_schedule_close,
    );
    true
}

fn append_repo_context_git_section(
    menu: &GtkBox,
    popover: &Popover,
    selection: &[RepositoryListItem],
    root_child_hovered: &Rc<Cell<bool>>,
    root_schedule_close: &Rc<dyn Fn()>,
) -> bool {
    let can_pull = selection.iter().any(repo_can_pull);
    let can_push = selection.iter().any(repo_can_push);
    let can_push_force = selection.iter().any(repo_can_push_force);
    let can_clone_missing = selection.iter().any(repo_can_clone_missing);
    let can_apply_hooks = !selection.is_empty();
    let has_actions =
        can_pull || can_push || can_push_force || can_clone_missing || can_apply_hooks;
    if !has_actions {
        return false;
    }

    let submenu = GtkBox::new(Orientation::Vertical, 0);
    if can_pull {
        append_context_button(&submenu, popover, "Pull", || {
            let _ = command_pull(maruzzella_sdk::ffi::MzBytes::empty());
        });
    }
    if can_push {
        append_context_button(&submenu, popover, "Push", || {
            let _ = command_push(maruzzella_sdk::ffi::MzBytes::empty());
        });
    }
    if can_push_force {
        append_context_button(&submenu, popover, "Push Force", || {
            let _ = command_push_force(maruzzella_sdk::ffi::MzBytes::empty());
        });
    }
    if can_clone_missing {
        append_context_button(&submenu, popover, "Clone Missing", || {
            let _ = command_clone_missing(maruzzella_sdk::ffi::MzBytes::empty());
        });
    }
    if can_apply_hooks {
        append_context_button(&submenu, popover, "Apply Hooks", || {
            let _ = command_apply_hooks(maruzzella_sdk::ffi::MzBytes::empty());
        });
    }
    append_context_submenu(
        menu,
        popover,
        "Git",
        &submenu,
        root_child_hovered,
        root_schedule_close,
    );
    true
}

fn append_context_submenu(
    menu: &GtkBox,
    parent_popover: &Popover,
    title: &str,
    submenu: &GtkBox,
    root_child_hovered: &Rc<Cell<bool>>,
    root_schedule_close: &Rc<dyn Fn()>,
) {
    let header = Button::new();
    header.set_halign(Align::Fill);
    header.set_hexpand(true);
    header.set_margin_top(2);
    header.set_margin_bottom(2);
    header.add_css_class("flat");
    header.add_css_class("menu-heading");
    header.set_margin_start(4);
    header.set_margin_end(4);

    let row = GtkBox::new(Orientation::Horizontal, 8);
    row.set_halign(Align::Fill);
    row.set_hexpand(true);

    let label = Label::new(Some(title));
    label.set_xalign(0.0);
    label.set_hexpand(true);
    label.set_halign(Align::Start);

    let chevron = Label::new(Some("›"));
    chevron.set_xalign(1.0);
    chevron.set_halign(Align::End);

    row.append(&label);
    row.append(&chevron);
    header.set_child(Some(&row));

    let submenu_popover = Popover::new();
    submenu_popover.set_autohide(true);
    submenu_popover.set_has_arrow(false);
    submenu_popover.set_position(PositionType::Right);
    submenu_popover.set_parent(&header);
    submenu_popover.set_child(Some(submenu));

    let header_hovered = Rc::new(Cell::new(false));
    let submenu_hovered = Rc::new(Cell::new(false));

    let schedule_close: Rc<dyn Fn()> = Rc::new({
        let submenu_popover = submenu_popover.clone();
        let header_hovered = Rc::clone(&header_hovered);
        let submenu_hovered = Rc::clone(&submenu_hovered);
        move || {
            let submenu_popover = submenu_popover.clone();
            let header_hovered = Rc::clone(&header_hovered);
            let submenu_hovered = Rc::clone(&submenu_hovered);
            glib::timeout_add_local(Duration::from_millis(120), move || {
                if !header_hovered.get() && !submenu_hovered.get() {
                    submenu_popover.popdown();
                }
                glib::ControlFlow::Break
            });
        }
    });

    let popup = submenu_popover.clone();
    let header_hovered_for_enter = Rc::clone(&header_hovered);
    let motion = EventControllerMotion::new();
    motion.connect_enter(move |_, _, _| {
        header_hovered_for_enter.set(true);
        popup.popup();
    });
    let header_hovered_for_leave = Rc::clone(&header_hovered);
    let schedule_close_for_header = Rc::clone(&schedule_close);
    motion.connect_leave(move |_| {
        header_hovered_for_leave.set(false);
        schedule_close_for_header();
    });
    header.add_controller(motion);

    let submenu_hovered_for_enter = Rc::clone(&submenu_hovered);
    let root_child_hovered_for_enter = Rc::clone(root_child_hovered);
    let popup = submenu_popover.clone();
    let motion = EventControllerMotion::new();
    motion.connect_enter(move |_, _, _| {
        submenu_hovered_for_enter.set(true);
        root_child_hovered_for_enter.set(true);
        popup.popup();
    });
    let submenu_hovered_for_leave = Rc::clone(&submenu_hovered);
    let root_child_hovered_for_leave = Rc::clone(root_child_hovered);
    let root_schedule_close_for_submenu = Rc::clone(root_schedule_close);
    let schedule_close_for_submenu = Rc::clone(&schedule_close);
    motion.connect_leave(move |_| {
        submenu_hovered_for_leave.set(false);
        root_child_hovered_for_leave.set(false);
        schedule_close_for_submenu();
        root_schedule_close_for_submenu();
    });
    submenu.add_controller(motion);

    header.connect_clicked({
        let submenu_popover = submenu_popover.clone();
        move |_| {
            if submenu_popover.is_visible() {
                submenu_popover.popdown();
            } else {
                submenu_popover.popup();
            }
        }
    });

    header.connect_unrealize({
        let submenu_popover = submenu_popover.clone();
        let root_child_hovered = Rc::clone(root_child_hovered);
        move |_| {
            root_child_hovered.set(false);
            submenu_popover.popdown();
            if submenu_popover.parent().is_some() {
                submenu_popover.unparent();
            }
        }
    });

    parent_popover.connect_closed({
        let submenu_popover = submenu_popover.clone();
        let root_child_hovered = Rc::clone(root_child_hovered);
        move |_| {
            root_child_hovered.set(false);
            submenu_popover.popdown();
        }
    });

    menu.append(&header);
}

fn repo_can_pull(item: &RepositoryListItem) -> bool {
    !matches!(
        item.status.state,
        ronomepo_core::RepositoryState::Missing | ronomepo_core::RepositoryState::Dirty
    )
}

fn repo_can_push(item: &RepositoryListItem) -> bool {
    matches!(
        item.status.sync,
        ronomepo_core::RepositorySync::Ahead(_) | ronomepo_core::RepositorySync::Diverged { .. }
    )
}

fn repo_can_push_force(item: &RepositoryListItem) -> bool {
    repo_can_push(item)
}

fn repo_can_clone_missing(item: &RepositoryListItem) -> bool {
    matches!(item.status.state, ronomepo_core::RepositoryState::Missing)
}

fn repo_context_supported_manifest_actions(
    selection: &[RepositoryListItem],
) -> Vec<StandardActionName> {
    let mut actions = selection
        .iter()
        .filter_map(|item| item.repo_manifest.as_ref())
        .filter_map(|scan| match &scan.state {
            RepoManifestScanState::Valid(summary) => Some(summary.supported_actions.clone()),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    actions.sort();
    actions.dedup();
    actions
}

fn context_action_label(action: StandardActionName) -> String {
    match action {
        StandardActionName::ListArtifacts => "List Artifacts".to_string(),
        StandardActionName::Build => "Build".to_string(),
        StandardActionName::Test => "Test".to_string(),
        StandardActionName::Clean => "Clean".to_string(),
        StandardActionName::VerifyDependenciesFreshness => {
            "Verify Dependencies Freshness".to_string()
        }
        StandardActionName::Deploy => "Deploy".to_string(),
    }
}

fn selected_repo_manifest_targets(
    action: StandardActionName,
) -> Vec<(RepositoryListItem, ronomepo_core::RepoManifest)> {
    selected_repository_items_from_state()
        .into_iter()
        .filter_map(|item| {
            let scan = item.repo_manifest.as_ref()?;
            let RepoManifestScanState::Valid(summary) = &scan.state else {
                return None;
            };
            if !summary.supported_actions.contains(&action) {
                return None;
            }
            let manifest = load_repo_manifest(&scan.path).ok()?;
            Some((item, manifest))
        })
        .collect()
}

fn run_selected_repo_manifest_action(action: StandardActionName) {
    let targets = selected_repo_manifest_targets(action);
    if targets.is_empty() {
        append_log(format!(
            "Ronomepo action {} skipped because no selected repo exposes it.",
            standard_action_label(action)
        ));
        return;
    }

    for (item, manifest) in targets {
        match action {
            StandardActionName::ListArtifacts => {
                match list_repo_artifacts(&item.status.repo_path, &manifest) {
                    Ok(artifacts) => {
                        if artifacts.is_empty() {
                            append_log(format!(
                                "Ronomepo list_artifacts for {} found no artifacts.",
                                item.name
                            ));
                        } else {
                            append_log(format!(
                                "Ronomepo list_artifacts for {}: {} artifact(s).",
                                item.name,
                                artifacts.len()
                            ));
                            for artifact in artifacts {
                                append_log(format!(
                                    "  [{}] {} {}",
                                    artifact.item_id,
                                    artifact.name,
                                    artifact
                                        .path
                                        .as_ref()
                                        .map(|path| path.display().to_string())
                                        .or(artifact.pattern.clone())
                                        .unwrap_or_else(|| "<no-path>".to_string())
                                ));
                            }
                        }
                    }
                    Err(error) => append_log(format!(
                        "Ronomepo list_artifacts failed for {}: {error}",
                        item.name
                    )),
                }
            }
            StandardActionName::VerifyDependenciesFreshness => {
                match verify_repo_dependencies_freshness(&item.status.repo_path, &manifest) {
                    Ok(reports) => {
                        let findings = reports
                            .iter()
                            .map(|report| report.findings.len())
                            .sum::<usize>();
                        append_log(format!(
                            "Ronomepo verify_dependencies_freshness for {}: {} report(s), {} finding(s).",
                            item.name,
                            reports.len(),
                            findings
                        ));
                        for report in reports {
                            if report.findings.is_empty() {
                                append_log(format!("  [{}] no issues found.", report.item_id));
                            } else {
                                for finding in report.findings {
                                    append_log(format!(
                                        "  [{}] {}",
                                        report.item_id, finding.message
                                    ));
                                }
                            }
                        }
                    }
                    Err(error) => append_log(format!(
                        "Ronomepo verify_dependencies_freshness failed for {}: {error}",
                        item.name
                    )),
                }
            }
            _ => match plan_repo_action(&item.status.repo_path, &manifest, action) {
                Ok(plan) => {
                    append_log(format!(
                        "Running Ronomepo {} for {} ({} step(s)).",
                        standard_action_label(action),
                        item.name,
                        plan.steps.len()
                    ));
                    for step in plan.steps {
                        match step.executor {
                            RepoActionExecutor::Command(command) => {
                                run_planned_repo_command(&item.name, action, command);
                            }
                            RepoActionExecutor::BuiltInInspector => {
                                append_log(format!(
                                    "Ronomepo {} for {} uses a built-in inspector and has no terminal command to launch.",
                                    standard_action_label(action),
                                    item.name
                                ));
                            }
                        }
                    }
                }
                Err(error) => append_log(format!(
                    "Ronomepo {} failed for {}: {error}",
                    standard_action_label(action),
                    item.name
                )),
            },
        }
    }
}

fn run_planned_repo_command(repo_name: &str, action: StandardActionName, command: PlannedCommand) {
    let mut process = Command::new(&command.program);
    process.args(&command.args);
    process.current_dir(&command.workdir);
    for (key, value) in &command.env {
        process.env(key, value);
    }

    let command_text = if command.args.is_empty() {
        command.program.clone()
    } else {
        format!("{} {}", command.program, command.args.join(" "))
    };

    match process.spawn() {
        Ok(_) => append_log(format!(
            "Started Ronomepo {} for {}: {} (cwd: {}).",
            standard_action_label(action),
            repo_name,
            command_text,
            command.workdir.display()
        )),
        Err(error) => append_log(format!(
            "Failed to start Ronomepo {} for {}: {} ({error})",
            standard_action_label(action),
            repo_name,
            command_text
        )),
    }
}

fn append_context_button<F>(menu: &GtkBox, popover: &Popover, label: &str, action: F)
where
    F: Fn() + 'static,
{
    let button = Button::new();
    button.set_halign(Align::Fill);
    button.set_hexpand(true);
    button.add_css_class("flat");
    let text = Label::new(Some(label));
    text.set_xalign(0.0);
    text.set_halign(Align::Start);
    text.set_hexpand(true);
    button.set_child(Some(&text));
    let popover = popover.clone();
    button.connect_clicked(move |_| {
        popover.popdown();
        action();
    });
    menu.append(&button);
}

extern "C" fn create_repo_monitor_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let content = GtkBox::new(Orientation::Vertical, 12);
    content.set_hexpand(true);
    content.set_valign(Align::Fill);
    content.set_vexpand(true);
    content.set_margin_top(8);
    content.set_margin_bottom(8);
    content.set_margin_start(8);
    content.set_margin_end(8);

    let filter_box = GtkBox::new(Orientation::Horizontal, 0);
    filter_box.add_css_class("linked");

    let btn_all = ToggleButton::with_label("All");
    let btn_dirty = ToggleButton::with_label("Dirty");
    let btn_to_sync = ToggleButton::with_label("To sync");
    let btn_issues = ToggleButton::with_label("Issues");
    btn_dirty.set_group(Some(&btn_all));
    btn_to_sync.set_group(Some(&btn_all));
    btn_issues.set_group(Some(&btn_all));

    match snapshot().monitor_filter_mode {
        MonitorFilterMode::All => btn_all.set_active(true),
        MonitorFilterMode::Dirty => btn_dirty.set_active(true),
        MonitorFilterMode::ToSync => btn_to_sync.set_active(true),
        MonitorFilterMode::Issues => btn_issues.set_active(true),
    }

    btn_all.connect_toggled(|button| {
        if button.is_active() {
            update_monitor_filter_mode(MonitorFilterMode::All);
            refresh_views();
        }
    });
    btn_dirty.connect_toggled(|button| {
        if button.is_active() {
            update_monitor_filter_mode(MonitorFilterMode::Dirty);
            refresh_views();
        }
    });
    btn_to_sync.connect_toggled(|button| {
        if button.is_active() {
            update_monitor_filter_mode(MonitorFilterMode::ToSync);
            refresh_views();
        }
    });
    btn_issues.connect_toggled(|button| {
        if button.is_active() {
            update_monitor_filter_mode(MonitorFilterMode::Issues);
            refresh_views();
        }
    });

    filter_box.append(&btn_all);
    filter_box.append(&btn_dirty);
    filter_box.append(&btn_to_sync);
    filter_box.append(&btn_issues);

    let sort_dropdown = DropDown::from_strings(&[
        monitor_sort_mode_label(MonitorSortMode::Name),
        monitor_sort_mode_label(MonitorSortMode::SyncState),
        monitor_sort_mode_label(MonitorSortMode::RecentActivity),
    ]);
    let initial_snapshot = snapshot();
    sort_dropdown.set_selected(monitor_sort_mode_index(initial_snapshot.monitor_sort_mode));
    sort_dropdown.set_tooltip_text(Some("Sort repositories by the selected criterion"));
    sort_dropdown.connect_selected_notify(|dropdown| {
        let mode = monitor_sort_mode_from_index(dropdown.selected());
        update_monitor_sort_mode(mode);
        let snapshot = snapshot();
        refresh_repository_views(&snapshot);
        refresh_views();
    });

    let sort_box = GtkBox::new(Orientation::Horizontal, 6);
    sort_box.set_halign(Align::End);
    sort_box.append(&sort_dropdown);
    let sort_direction = Rc::new(Cell::new(initial_snapshot.monitor_sort_descending));
    let sort_indicator = Button::new();
    sort_indicator.add_css_class("flat");
    sort_indicator.set_tooltip_text(Some("Toggle sort direction"));
    let sort_indicator_label =
        Label::new(Some(monitor_sort_direction_label(sort_direction.get())));
    sort_indicator_label.add_css_class("title-4");
    sort_indicator_label.add_css_class("dim-label");
    sort_indicator.set_child(Some(&sort_indicator_label));
    sort_indicator.connect_clicked({
        let sort_direction = Rc::clone(&sort_direction);
        let sort_indicator_label = sort_indicator_label.clone();
        move |_| {
            let next = !sort_direction.get();
            sort_direction.set(next);
            sort_indicator_label.set_text(monitor_sort_direction_label(next));
            update_monitor_sort_descending(next);
            let snapshot = snapshot();
            refresh_repository_views(&snapshot);
            refresh_views();
        }
    });
    sort_box.append(&sort_indicator);

    let controls = GtkBox::new(Orientation::Horizontal, 10);
    controls.set_hexpand(true);
    controls.append(&filter_box);
    let spacer = GtkBox::new(Orientation::Horizontal, 0);
    spacer.set_hexpand(true);
    controls.append(&spacer);
    controls.append(&sort_box);

    let store = gio::ListStore::new::<BoxedAnyObject>();
    let filter = CustomFilter::new(|object| {
        let snapshot = snapshot();
        repo_item_from_object(object)
            .is_some_and(|item| repo_monitor_filter_matches(&item, &snapshot))
    });
    let filter_model = gtk::FilterListModel::new(Some(store.clone()), Some(filter.clone()));
    let sorter = CustomSorter::new(|left, right| {
        let snapshot = snapshot();
        let left = repo_item_from_object(left);
        let right = repo_item_from_object(right);
        match (left, right) {
            (Some(left), Some(right)) => repo_monitor_sort_cmp(&snapshot, &left, &right).into(),
            _ => gtk::Ordering::Equal,
        }
    });
    let sort_model = SortListModel::new(Some(filter_model.clone()), Some(sorter.clone()));

    let list = ListBox::new();
    list.add_css_class("boxed-list");
    list.set_hexpand(true);
    list.set_valign(Align::Start);
    list.set_selection_mode(SelectionMode::Multiple);
    list.bind_model(Some(&sort_model), move |object| {
        repo_item_from_object(object)
            .map(|item| build_repo_monitor_row(&item, host))
            .unwrap_or_else(|| GtkBox::new(Orientation::Horizontal, 0))
            .upcast()
    });
    {
        let click = GestureClick::new();
        click.set_button(1);
        click.set_propagation_phase(gtk::PropagationPhase::Capture);
        let list_ref = list.clone();
        click.connect_pressed(move |gesture, n_press, _x, y| {
            gesture.set_state(gtk::EventSequenceState::Claimed);
            let Some(row) = list_ref.row_at_y(y as i32) else {
                return;
            };
            let modifiers = gesture
                .current_event()
                .map(|e| e.modifier_state())
                .unwrap_or_else(gtk::gdk::ModifierType::empty);
            let ctrl = modifiers.contains(gtk::gdk::ModifierType::CONTROL_MASK);
            let shift = modifiers.contains(gtk::gdk::ModifierType::SHIFT_MASK);

            if n_press >= 2 && !ctrl && !shift {
                let repo_id = repo_id_from_list_box_row(&row).unwrap_or_default();
                if repo_id == MONOREPO_ROW_ID {
                    let _ = command_open_overview(maruzzella_sdk::ffi::MzBytes::empty());
                } else if !repo_id.is_empty() {
                    open_repo_overviews(host, std::slice::from_ref(&repo_id));
                }
                return;
            }

            if ctrl {
                if row.is_selected() {
                    list_ref.unselect_row(&row);
                } else {
                    list_ref.select_row(Some(&row));
                }
            } else if shift {
                let clicked_index = row.index();
                let mut anchor = clicked_index;
                let mut i = 0;
                while let Some(r) = list_ref.row_at_index(i) {
                    if r.is_selected() {
                        anchor = i;
                        break;
                    }
                    i += 1;
                }
                let lo = anchor.min(clicked_index);
                let hi = anchor.max(clicked_index);
                list_ref.unselect_all();
                for idx in lo..=hi {
                    if let Some(r) = list_ref.row_at_index(idx) {
                        list_ref.select_row(Some(&r));
                    }
                }
            } else {
                if row.is_selected() {
                    list_ref.unselect_all();
                } else {
                    list_ref.unselect_all();
                    list_ref.select_row(Some(&row));
                }
            }
            sync_selection_css(&list_ref);
            update_selected_repo_ids(selection_ids_from_list(&list_ref));
        });
        list.add_controller(click);
    }
    list.connect_row_activated(move |_, row| {
        let repo_id = repo_id_from_list_box_row(row).unwrap_or_default();
        if repo_id.is_empty() {
            return;
        }
        if repo_id == MONOREPO_ROW_ID {
            let _ = command_open_overview(maruzzella_sdk::ffi::MzBytes::empty());
            return;
        }
        open_repo_overviews(host, std::slice::from_ref(&repo_id));
    });

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .min_content_height(0)
        .child(&list)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);

    content.append(&controls);
    content.append(&repo_monitor_header());
    content.append(&scroller);

    let snapshot = snapshot();
    sync_repository_monitor_store(&store, &all_monitor_items(&snapshot));
    filter.changed(FilterChange::Different);
    sorter.changed(SorterChange::Different);
    refresh_repository_view_handle(
        &RepositoryViewHandle {
            list: glib::WeakRef::new(),
            scroller: glib::WeakRef::new(),
            store: store.clone(),
            filter: filter.clone(),
            sorter: sorter.clone(),
        },
        &list,
        &scroller,
        &snapshot,
    );

    let list_ref = glib::WeakRef::new();
    list_ref.set(Some(&list));
    let scroller_ref = glib::WeakRef::new();
    scroller_ref.set(Some(&scroller));
    REPOSITORY_VIEWS.with(|views| {
        views.borrow_mut().push(RepositoryViewHandle {
            list: list_ref,
            scroller: scroller_ref,
            store,
            filter,
            sorter,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(content.upcast())
            as *mut std::ffi::c_void
    }
}

fn repo_monitor_header() -> GtkBox {
    let header = GtkBox::new(Orientation::Horizontal, 10);
    header.add_css_class("mono");
    header.set_margin_bottom(4);
    let name = monitor_text_cell(
        "Name",
        MONITOR_NAME_COL_CHARS,
        MONITOR_NAME_COL_WIDTH,
        false,
    );
    let branch = monitor_text_cell(
        "Branch",
        MONITOR_BRANCH_COL_CHARS,
        MONITOR_BRANCH_COL_WIDTH,
        false,
    );
    let manifest = monitor_text_cell(
        "Manifest",
        MONITOR_MANIFEST_COL_CHARS,
        MONITOR_MANIFEST_COL_WIDTH,
        false,
    );
    let state = monitor_text_cell(
        "State",
        MONITOR_STATE_COL_CHARS,
        MONITOR_STATE_COL_WIDTH,
        false,
    );
    let sync = Label::new(Some("Sync"));
    sync.set_xalign(0.0);
    sync.set_hexpand(true);
    sync.set_ellipsize(EllipsizeMode::End);

    for label in [&name, &branch, &manifest, &state, &sync] {
        label.add_css_class("dim-label");
        header.append(label);
    }

    header
}

fn monitor_text_cell(text: &str, width_chars: i32, width_px: i32, expand: bool) -> Label {
    let label = Label::new(Some(text));
    label.set_xalign(0.0);
    label.add_css_class("mono");
    label.set_width_chars(width_chars);
    label.set_max_width_chars(width_chars);
    label.set_size_request(width_px, -1);
    label.set_ellipsize(EllipsizeMode::End);
    label.set_hexpand(expand);
    label
}

fn monitor_manifest_cell(scan: Option<&RepoManifestScan>) -> Label {
    let label = monitor_text_cell(
        manifest_presence_label(scan),
        MONITOR_MANIFEST_COL_CHARS,
        MONITOR_MANIFEST_COL_WIDTH,
        false,
    );
    if let Some(tooltip) = manifest_presence_tooltip(scan) {
        attach_text_tooltip(&label, tooltip);
    }
    let escaped = glib::markup_escape_text(manifest_presence_label(scan));
    label.set_markup(&format!(
        "<span foreground=\"{}\">{escaped}</span>",
        manifest_presence_color(scan)
    ));
    label
}

fn manifest_presence_color(scan: Option<&RepoManifestScan>) -> &'static str {
    match scan.map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(_)) => "#7fdc8a",
        Some(RepoManifestScanState::Invalid { .. }) => "#ff6b6b",
        Some(RepoManifestScanState::Missing) => "#8f96a3",
        None => "#8f96a3",
    }
}

fn monitor_state_cell(state: &ronomepo_core::RepositoryState) -> Label {
    let label = monitor_text_cell(
        status_label(state),
        MONITOR_STATE_COL_CHARS,
        MONITOR_STATE_COL_WIDTH,
        false,
    );
    let escaped = glib::markup_escape_text(status_label(state));
    label.set_markup(&format!(
        "<span foreground=\"{}\">{escaped}</span>",
        state_color(state)
    ));
    label
}

fn state_color(state: &ronomepo_core::RepositoryState) -> &'static str {
    match state {
        ronomepo_core::RepositoryState::Clean => "#7fdc8a",
        ronomepo_core::RepositoryState::Missing => "#ff6b6b",
        ronomepo_core::RepositoryState::Dirty
        | ronomepo_core::RepositoryState::Untracked
        | ronomepo_core::RepositoryState::Unknown => "#ff8e5f",
    }
}

fn monitor_sync_cell(sync: &ronomepo_core::RepositorySync) -> Label {
    let text = format_sync_label(sync);
    let label = Label::new(Some(&text));
    label.set_xalign(0.0);
    label.add_css_class("mono");
    label.set_hexpand(true);
    label.set_ellipsize(EllipsizeMode::End);
    let escaped = glib::markup_escape_text(&text);
    label.set_markup(&format!(
        "<span foreground=\"{}\">{escaped}</span>",
        sync_color(sync)
    ));
    label
}

fn sync_color(sync: &ronomepo_core::RepositorySync) -> &'static str {
    match sync {
        ronomepo_core::RepositorySync::UpToDate => "#7fdc8a",
        ronomepo_core::RepositorySync::Ahead(_) | ronomepo_core::RepositorySync::Behind(_) => {
            "#ff8e5f"
        }
        ronomepo_core::RepositorySync::Diverged { .. }
        | ronomepo_core::RepositorySync::Unknown
        | ronomepo_core::RepositorySync::NoUpstream => "#ff6b6b",
    }
}

extern "C" fn create_monorepo_overview_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .min_content_height(0)
        .child(&root)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);

    let snapshot = snapshot();
    render_monorepo_overview_into(&root, &snapshot, host);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    MONOREPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            auxiliary_root: glib::WeakRef::new(),
            instance_key: None,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(scroller.upcast())
            as *mut std::ffi::c_void
    }
}

extern "C" fn create_commit_check_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .min_content_height(0)
        .child(&root)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);

    let snapshot = snapshot();
    render_commit_check_into(&root, &snapshot);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    COMMIT_CHECK_VIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            auxiliary_root: glib::WeakRef::new(),
            instance_key: None,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(scroller.upcast())
            as *mut std::ffi::c_void
    }
}

fn render_monorepo_overview_into(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    clear_box(root);

    let summary = workspace_summary(
        snapshot.manifest.as_ref(),
        snapshot.manifest_path.as_deref(),
        &snapshot.workspace_root,
    );
    let items = repository_items(snapshot);
    let missing = items
        .iter()
        .filter(|item| matches!(item.status.state, ronomepo_core::RepositoryState::Missing))
        .count();
    let dirty = items
        .iter()
        .filter(|item| {
            matches!(
                item.status.state,
                ronomepo_core::RepositoryState::Dirty | ronomepo_core::RepositoryState::Untracked
            )
        })
        .count();
    let ahead = items
        .iter()
        .filter(|item| matches!(item.status.sync, ronomepo_core::RepositorySync::Ahead(_)))
        .count();
    let behind = items
        .iter()
        .filter(|item| matches!(item.status.sync, ronomepo_core::RepositorySync::Behind(_)))
        .count();
    let diverged = items
        .iter()
        .filter(|item| {
            matches!(
                item.status.sync,
                ronomepo_core::RepositorySync::Diverged { .. }
            )
        })
        .count();
    let no_upstream = items
        .iter()
        .filter(|item| matches!(item.status.sync, ronomepo_core::RepositorySync::NoUpstream))
        .count();
    let attention = items
        .iter()
        .filter(|item| repo_attention_rank(item) < 7)
        .count();
    let selected = selected_repository_items(snapshot, &items);

    let hero = GtkBox::new(Orientation::Vertical, 8);
    let title = Label::new(Some("Monorepo Overview"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");
    let subtitle = Label::new(Some(&format!(
        "{} repositories tracked in {}",
        summary.repo_count, summary.workspace_name
    )));
    subtitle.set_xalign(0.0);
    subtitle.add_css_class("muted");
    subtitle.set_wrap(true);
    hero.append(&title);
    hero.append(&subtitle);
    let stats = GtkBox::new(Orientation::Horizontal, 12);
    for (label, value) in [
        ("Attention", attention),
        ("Selected", snapshot.selected_repo_ids.len()),
        ("Dirty", dirty),
        ("Missing", missing),
        ("Ahead", ahead),
        ("Behind", behind),
        ("Diverged", diverged),
        ("No Upstream", no_upstream),
    ] {
        stats.append(&stat_card(label, &value.to_string()));
    }

    let actions = overview_actions(false);
    let selection_actions = monorepo_selection_actions(host_ptr, &items);
    let report_actions = monorepo_report_actions(snapshot);
    let file_actions = overview_file_actions(snapshot, host_ptr);
    let command_area = GtkBox::new(Orientation::Vertical, 8);
    command_area.append(&overview_command_group("Workspace", actions));
    command_area.append(&overview_command_group("Selection", selection_actions));
    command_area.append(&overview_command_group("Reports", report_actions));
    command_area.append(&overview_command_group("Files", file_actions));

    let sections = GtkBox::new(Orientation::Vertical, 12);
    let selection_scope = if selected.is_empty() {
        "No repos selected. Toolbar and overview actions apply to the whole workspace.".to_string()
    } else {
        format!(
            "{} repos selected. Toolbar and overview actions target the current selection first.",
            selected.len()
        )
    };
    let repo_focus = snapshot
        .active_repo_id
        .as_ref()
        .map(|repo_id| format!("Active repo overview target: {repo_id}"))
        .unwrap_or_else(|| "No active repo overview target yet".to_string());
    append_facts_section(
        &sections,
        "Workspace Context",
        &[
            ("Root", snapshot.workspace_root.display().to_string()),
            (
                "Manifest",
                snapshot
                    .manifest_path
                    .as_ref()
                    .map(|path| format!("Loaded from {}", path.display()))
                    .unwrap_or_else(|| format!("No {MANIFEST_FILE_NAME} loaded yet")),
            ),
            ("Selection", selection_scope),
            ("Repo Focus", repo_focus),
        ],
    );
    append_repo_group_section(
        &sections,
        "Needs Attention",
        "Repos that are missing, dirty, behind, diverged, ahead, or missing an upstream.",
        &attention_items(&items),
        Some(8),
        host_ptr,
    );
    append_repo_group_section(
        &sections,
        "Current Selection",
        "The repos currently selected in the left monitor.",
        &selected,
        Some(8),
        host_ptr,
    );
    append_lines_section(
        &sections,
        "History Check",
        &if snapshot.history_report_loading {
            vec!["History check is running...".to_string()]
        } else {
            snapshot.history_report.clone()
        },
        "Run Check History to scan recent commits with workspace commit check rules.",
    );
    append_lines_section(
        &sections,
        "Line Stats",
        &if snapshot.line_stats_loading {
            vec!["Line stats refresh is running...".to_string()]
        } else {
            snapshot.line_stats_report.clone()
        },
        "Run Line Stats to inspect additions and deletions across the workspace.",
    );
    append_log_section(&sections, "Recent Operations", &snapshot.logs, 8);

    root.append(&hero);
    root.append(&stats);
    root.append(&command_area);
    root.append(&sections);
}

fn render_commit_check_into(root: &GtkBox, snapshot: &StateSnapshot) {
    clear_box(root);

    let hero = GtkBox::new(Orientation::Vertical, 4);
    let title = Label::new(Some("Commit Check"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");
    let subtitle = Label::new(Some(
        "Scans recent commits with workspace rules before push.",
    ));
    subtitle.set_xalign(0.0);
    subtitle.add_css_class("muted");
    subtitle.set_wrap(true);
    hero.append(&title);
    hero.append(&subtitle);

    let actions = GtkBox::new(Orientation::Horizontal, 8);
    let rerun = Button::with_label("Run Check");
    rerun.connect_clicked(|_| {
        let _ = command_check_history(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&rerun);

    let open_overview = Button::with_label("Open Overview");
    open_overview.connect_clicked(|_| {
        let _ = command_open_overview(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&open_overview);

    let rules_header = Label::new(Some("Rules"));
    rules_header.set_xalign(0.0);
    rules_header.add_css_class("title-4");

    let rules_help = Label::new(Some(
        "Rules run from the lowest priority to the highest. Later allow rules can unblock commits matched by earlier block rules.",
    ));
    rules_help.set_xalign(0.0);
    rules_help.set_wrap(true);
    rules_help.add_css_class("muted");

    let rules_box = GtkBox::new(Orientation::Vertical, 8);
    let rule_rows = Rc::new(RefCell::new(Vec::<CommitCheckRuleRowHandle>::new()));
    let manifest_rules = snapshot
        .manifest
        .as_ref()
        .and_then(|manifest| manifest.commit_check_rules.clone())
        .unwrap_or_default();
    for rule in &manifest_rules {
        append_commit_check_rule_row(&rules_box, &rule_rows, Some(rule));
    }

    let rule_status_text = snapshot
        .manifest_path
        .as_ref()
        .map(|path| {
            format!(
                "Loaded {} commit check rules from {}. Use Save Rules to persist edits.",
                manifest_rules.len(),
                path.display()
            )
        })
        .unwrap_or_else(|| {
            format!(
                "Loaded {} commit check rules. No workspace manifest is loaded.",
                manifest_rules.len()
            )
        });
    let rule_status = Label::new(Some(&rule_status_text));
    rule_status.set_xalign(0.0);
    rule_status.set_wrap(true);
    rule_status.add_css_class("muted");

    let rule_actions = GtkBox::new(Orientation::Horizontal, 8);
    let add_rule = Button::with_label("Add Rule");
    add_rule.connect_clicked({
        let rules_box = rules_box.clone();
        let rule_rows = rule_rows.clone();
        let rule_status = rule_status.clone();
        move |_| {
            open_add_commit_check_rule_dialog(
                active_window(),
                rules_box.clone(),
                rule_rows.clone(),
                rule_status.clone(),
            );
        }
    });
    let save_rules = Button::with_label("Save Rules");
    save_rules.connect_clicked({
        let rule_rows = rule_rows.clone();
        let rule_status = rule_status.clone();
        move |_| match build_commit_check_rules_from_rows(&rule_rows.borrow()) {
            Ok(rules) => {
                rule_status.set_text("Saving commit check rules...");
                match save_commit_check_rules(rules) {
                    Ok(message) => {
                        rule_status.set_text(&message);
                        append_log(message);
                        refresh_views();
                    }
                    Err(message) => {
                        rule_status.set_text(&message);
                        append_log(message);
                        refresh_views();
                    }
                }
            }
            Err(message) => {
                rule_status.set_text(&message);
            }
        }
    });
    let discard_changes = Button::with_label("Discard Changes");
    discard_changes.connect_clicked(|_| {
        refresh_views();
    });
    for button in [add_rule, save_rules, discard_changes] {
        rule_actions.append(&button);
    }

    root.append(&hero);
    root.append(&actions);
    root.append(&rules_header);
    root.append(&rules_help);
    root.append(&rule_actions);
    root.append(&rule_status);
    root.append(&rules_box);
}

fn append_commit_check_rule_row(
    rules_box: &GtkBox,
    rule_rows: &Rc<RefCell<Vec<CommitCheckRuleRowHandle>>>,
    rule: Option<&CommitCheckRule>,
) {
    let row = GtkBox::new(Orientation::Vertical, 8);
    row.add_css_class("boxed-list");

    let enabled = CheckButton::with_label("Enabled");
    enabled.set_active(rule.map(|rule| rule.enabled).unwrap_or(true));

    let allow = CheckButton::with_label("Allow");
    allow.set_active(
        rule.map(|rule| matches!(rule.effect, CommitCheckRuleEffect::Allow))
            .unwrap_or(false),
    );

    let hash_matcher = CheckButton::with_label("Hash");
    hash_matcher.set_active(
        rule.map(|rule| matches!(rule.matcher, CommitCheckRuleMatcher::CommitHash { .. }))
            .unwrap_or(false),
    );

    let name = Entry::new();
    name.set_placeholder_text(Some("Rule name"));
    name.set_hexpand(true);
    name.set_text(rule.map(|rule| rule.name.as_str()).unwrap_or(""));

    let priority = Entry::new();
    priority.set_placeholder_text(Some("Priority"));
    priority.set_width_chars(8);
    priority.set_text(
        &rule
            .map(|rule| rule.priority.to_string())
            .unwrap_or_else(|| "100".to_string()),
    );

    let value = Entry::new();
    value.set_placeholder_text(Some("Regex pattern or commit hash"));
    value.set_hexpand(true);
    value.set_text(
        &rule
            .map(commit_check_rule_value)
            .unwrap_or_else(String::new),
    );

    let repository_ids = Entry::new();
    repository_ids.set_placeholder_text(Some("Repo IDs, comma-separated; empty means all"));
    repository_ids.set_hexpand(true);
    repository_ids.set_text(
        &rule
            .map(commit_check_rule_scope_value)
            .unwrap_or_else(String::new),
    );

    let remove = Button::with_label("Remove");
    let rule_id = rule
        .map(|rule| rule.id.clone())
        .unwrap_or_else(new_commit_check_rule_id);
    remove.connect_clicked({
        let rules_box = rules_box.clone();
        let rule_rows = rule_rows.clone();
        let row = row.clone();
        let rule_id = rule_id.clone();
        move |_| {
            rules_box.remove(&row);
            rule_rows
                .borrow_mut()
                .retain(|handle| handle.rule_id != rule_id);
        }
    });

    let top = GtkBox::new(Orientation::Horizontal, 8);
    top.append(&enabled);
    top.append(&allow);
    top.append(&hash_matcher);
    top.append(&name);
    top.append(&priority);
    top.append(&remove);

    let bottom = GtkBox::new(Orientation::Horizontal, 8);
    bottom.append(&value);
    bottom.append(&repository_ids);

    row.append(&top);
    row.append(&bottom);
    rules_box.append(&row);

    rule_rows.borrow_mut().push(CommitCheckRuleRowHandle {
        enabled,
        allow,
        hash_matcher,
        name,
        priority,
        value,
        repository_ids,
        rule_id,
    });
}

fn open_add_commit_check_rule_dialog(
    parent: Option<Window>,
    rules_box: GtkBox,
    rule_rows: Rc<RefCell<Vec<CommitCheckRuleRowHandle>>>,
    status: Label,
) {
    let dialog = Dialog::builder()
        .modal(true)
        .title("Add Commit Check Rule")
        .build();
    if let Some(parent) = parent.as_ref() {
        dialog.set_transient_for(Some(parent));
    }
    let cancel = dialog.add_button("Cancel", ResponseType::Cancel);
    cancel.add_css_class(&button_css_class("secondary"));
    let add = dialog.add_button("Add Rule", ResponseType::Accept);
    add.add_css_class(&button_css_class("primary"));
    dialog.set_default_response(ResponseType::Accept);
    dialog.add_css_class(&surface_css_class("ronomepo-workbench"));
    dialog.add_css_class(&text_css_class("body"));

    let content = dialog.content_area();
    content.add_css_class(&surface_css_class("ronomepo-workbench"));
    content.add_css_class(&text_css_class("body"));
    content.set_margin_top(16);
    content.set_margin_bottom(16);
    content.set_margin_start(16);
    content.set_margin_end(16);
    content.set_spacing(12);

    let body = GtkBox::new(Orientation::Vertical, 8);
    body.add_css_class(&surface_css_class("ronomepo-workbench"));
    body.add_css_class(&text_css_class("body"));

    let enabled = CheckButton::with_label("Enabled");
    enabled.set_active(true);

    let allow = CheckButton::with_label("Allow");

    let hash_matcher = CheckButton::with_label("Hash");

    let name = Entry::new();
    name.add_css_class(&input_css_class("search"));
    name.set_hexpand(true);
    name.set_placeholder_text(Some("Rule name"));

    let priority = Entry::new();
    priority.add_css_class(&input_css_class("search"));
    priority.set_placeholder_text(Some("100"));
    priority.set_text("100");

    let value = Entry::new();
    value.add_css_class(&input_css_class("search"));
    value.set_hexpand(true);
    value.set_placeholder_text(Some("Regex pattern or commit hash"));

    let repository_ids = Entry::new();
    repository_ids.add_css_class(&input_css_class("search"));
    repository_ids.set_hexpand(true);
    repository_ids.set_placeholder_text(Some("Repo IDs, comma-separated; empty means all"));

    let toggles = GtkBox::new(Orientation::Horizontal, 8);
    toggles.append(&enabled);
    toggles.append(&allow);
    toggles.append(&hash_matcher);

    let error = Label::new(None);
    error.set_xalign(0.0);
    error.set_wrap(true);
    error.add_css_class("error");

    body.append(&toggles);
    body.append(&labeled_field("Name", &name));
    body.append(&labeled_field("Priority", &priority));
    body.append(&labeled_field("Pattern Or Hash", &value));
    body.append(&labeled_field("Repository Scope", &repository_ids));
    body.append(&error);
    content.append(&body);

    dialog.connect_response({
        let dialog = dialog.clone();
        let enabled = enabled.clone();
        let allow = allow.clone();
        let hash_matcher = hash_matcher.clone();
        let name = name.clone();
        let priority = priority.clone();
        let value = value.clone();
        let repository_ids = repository_ids.clone();
        let error = error.clone();
        move |_, response| {
            if response != ResponseType::Accept {
                dialog.close();
                return;
            }

            let rule_name = name.text().trim().to_string();
            if rule_name.is_empty() {
                error.set_text("Rule needs a name.");
                return;
            }

            let rule_value = value.text().trim().to_string();
            if rule_value.is_empty() {
                error.set_text("Rule needs a regex pattern or commit hash.");
                return;
            }

            let priority_value = match priority.text().trim().parse::<i32>() {
                Ok(priority) => priority,
                Err(_) => {
                    error.set_text("Priority must be an integer.");
                    return;
                }
            };

            let repository_ids = repository_ids
                .text()
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();

            let rule = CommitCheckRule {
                id: new_commit_check_rule_id(),
                name: rule_name,
                enabled: enabled.is_active(),
                priority: priority_value,
                effect: if allow.is_active() {
                    CommitCheckRuleEffect::Allow
                } else {
                    CommitCheckRuleEffect::Block
                },
                scope: if repository_ids.is_empty() {
                    CommitCheckRuleScope::All
                } else {
                    CommitCheckRuleScope::Repositories { repository_ids }
                },
                matcher: if hash_matcher.is_active() {
                    CommitCheckRuleMatcher::CommitHash { hash: rule_value }
                } else {
                    CommitCheckRuleMatcher::Regex {
                        pattern: rule_value,
                    }
                },
            };

            let mut rules = match build_commit_check_rules_from_rows(&rule_rows.borrow()) {
                Ok(rules) => rules,
                Err(message) => {
                    error.set_text(&message);
                    return;
                }
            };
            rules.push(rule.clone());

            status.set_text("Saving commit check rule...");
            match save_commit_check_rules(rules) {
                Ok(message) => {
                    append_commit_check_rule_row(&rules_box, &rule_rows, Some(&rule));
                    status.set_text(&format!("Saved rule \"{}\". {}", rule.name, message));
                    append_log(message);
                    dialog.close();
                    refresh_commit_check_views_now();
                    refresh_log_surfaces();
                }
                Err(message) => {
                    error.set_text(&message);
                    status.set_text(&message);
                    append_log(message);
                    refresh_log_surfaces();
                }
            }
        }
    });

    dialog.present();
}

fn commit_check_rule_value(rule: &CommitCheckRule) -> String {
    match &rule.matcher {
        CommitCheckRuleMatcher::Regex { pattern } => pattern.clone(),
        CommitCheckRuleMatcher::CommitHash { hash } => hash.clone(),
    }
}

fn commit_check_rule_scope_value(rule: &CommitCheckRule) -> String {
    match &rule.scope {
        CommitCheckRuleScope::All => String::new(),
        CommitCheckRuleScope::Repositories { repository_ids } => repository_ids.join(", "),
    }
}

fn build_commit_check_rules_from_rows(
    rows: &[CommitCheckRuleRowHandle],
) -> Result<Vec<CommitCheckRule>, String> {
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let name = row.name.text().trim().to_string();
            let value = row.value.text().trim().to_string();
            if name.is_empty() && value.is_empty() {
                return Err(format!(
                    "Rule {} needs a name and a pattern or hash.",
                    index + 1
                ));
            }
            if name.is_empty() {
                return Err(format!("Rule {} needs a name.", index + 1));
            }
            if value.is_empty() {
                return Err(format!("Rule {name} needs a regex pattern or commit hash."));
            }
            let priority = row
                .priority
                .text()
                .trim()
                .parse::<i32>()
                .map_err(|_| format!("Rule {name} priority must be an integer."))?;

            let repository_ids = row
                .repository_ids
                .text()
                .split(',')
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();

            Ok(CommitCheckRule {
                id: row.rule_id.clone(),
                name,
                enabled: row.enabled.is_active(),
                priority,
                effect: if row.allow.is_active() {
                    CommitCheckRuleEffect::Allow
                } else {
                    CommitCheckRuleEffect::Block
                },
                scope: if repository_ids.is_empty() {
                    CommitCheckRuleScope::All
                } else {
                    CommitCheckRuleScope::Repositories { repository_ids }
                },
                matcher: if row.hash_matcher.is_active() {
                    CommitCheckRuleMatcher::CommitHash { hash: value }
                } else {
                    CommitCheckRuleMatcher::Regex { pattern: value }
                },
            })
        })
        .collect()
}

fn new_commit_check_rule_id() -> String {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("rule-{millis}")
}

fn save_commit_check_rules(rules: Vec<CommitCheckRule>) -> Result<String, String> {
    let (manifest_path, mut manifest) = {
        let app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.clone() else {
            return Err(format!(
                "Cannot save commit check rules because no {} is loaded.",
                MANIFEST_FILE_NAME
            ));
        };
        let manifest_path = app_state
            .manifest_path
            .clone()
            .unwrap_or_else(|| default_manifest_path(&manifest.root));
        (manifest_path, manifest)
    };
    manifest.commit_check_rules = Some(rules);
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;

    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.manifest_path = Some(manifest_path.clone());
        app_state.manifest = Some(manifest);
    }

    Ok(format!(
        "Saved commit check rules to {}.",
        manifest_path.display()
    ))
}

extern "C" fn create_repo_overview_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .min_content_height(0)
        .child(&root)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);

    let instance_key =
        unsafe { request.as_ref() }.and_then(|request| decode_mzstr(request.instance_key));
    let snapshot = snapshot();
    render_repo_overview_into(&root, &snapshot, instance_key.as_deref(), host);

    let terminal_panel = GtkBox::new(Orientation::Vertical, 0);
    terminal_panel.set_hexpand(true);
    terminal_panel.set_vexpand(true);
    terminal_panel.append(&build_repo_terminal_panel(
        &snapshot,
        instance_key.as_deref(),
    ));
    let split = Paned::new(Orientation::Vertical);
    split.set_wide_handle(true);
    split.set_resize_start_child(true);
    split.set_shrink_start_child(false);
    split.set_resize_end_child(true);
    split.set_shrink_end_child(false);
    split.set_position(520);
    split.set_start_child(Some(&scroller));
    split.set_end_child(Some(&terminal_panel));

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    let terminal_panel_ref = glib::WeakRef::new();
    terminal_panel_ref.set(Some(&terminal_panel));
    REPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            auxiliary_root: terminal_panel_ref,
            instance_key,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(split.upcast())
            as *mut std::ffi::c_void
    }
}

#[derive(Clone)]
struct RepoEditorRowHandle {
    enabled: CheckButton,
    name: Entry,
    dir_name: Entry,
    remote_url: Entry,
}

extern "C" fn create_workspace_settings_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let snapshot = snapshot();
    render_workspace_settings_into(&root, &snapshot, host);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    WORKSPACE_SETTINGS_VIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            auxiliary_root: glib::WeakRef::new(),
            instance_key: None,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn render_workspace_settings_into(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    clear_box(root);

    let manifest = snapshot
        .manifest
        .clone()
        .unwrap_or_else(|| WorkspaceManifest {
            name: workspace_name_from_root(&snapshot.workspace_root),
            root: snapshot.workspace_root.clone(),
            repos: Vec::new(),
            shared_hooks_path: Some(snapshot.workspace_root.join("hooks")),
            commit_check_rules: Some(default_commit_check_rules()),
        });

    let title = Label::new(Some("Workspace Settings"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");

    let subtitle = Label::new(Some(
        "Edit the workspace manifest directly. Changes here become the source of truth for Ronomepo.",
    ));
    subtitle.set_xalign(0.0);
    subtitle.set_wrap(true);
    subtitle.add_css_class("muted");

    let name_entry = Entry::new();
    name_entry.set_text(&manifest.name);
    let root_entry = Entry::new();
    root_entry.set_text(&manifest.root.display().to_string());
    let hooks_entry = Entry::new();
    hooks_entry.set_text(
        &manifest
            .shared_hooks_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
    );

    let form = GtkBox::new(Orientation::Vertical, 8);
    form.append(&labeled_field("Workspace Name", &name_entry));
    form.append(&labeled_field("Workspace Root", &root_entry));
    form.append(&labeled_field("Shared Hooks Path", &hooks_entry));

    let repo_header = Label::new(Some("Repositories"));
    repo_header.set_xalign(0.0);
    repo_header.add_css_class("title-4");

    let repo_help = Label::new(Some(
        "Each repository row controls one manifest entry. Empty rows are ignored on save.",
    ));
    repo_help.set_xalign(0.0);
    repo_help.set_wrap(true);
    repo_help.add_css_class("muted");

    let repo_rows_box = GtkBox::new(Orientation::Vertical, 8);
    let repo_rows = Rc::new(RefCell::new(Vec::<RepoEditorRowHandle>::new()));
    for repo in &manifest.repos {
        append_repo_editor_row(&repo_rows_box, &repo_rows, Some(repo));
    }

    let repo_scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .min_content_height(280)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&repo_rows_box)
        .build();

    let status = Label::new(Some(
        "Use Save Manifest to persist changes, or Import repos.txt to regenerate entries from the legacy file.",
    ));
    status.set_xalign(0.0);
    status.set_wrap(true);
    status.add_css_class("muted");

    let actions = GtkBox::new(Orientation::Horizontal, 8);
    let add_repo = Button::with_label("Add Repo");
    let save = Button::with_label("Save Manifest");
    let reload = Button::with_label("Reload Manifest");
    let import = Button::with_label("Import repos.txt");
    let edit_manifest = Button::with_label("Edit Manifest File");

    add_repo.connect_clicked({
        let root = root.clone();
        let repo_rows = repo_rows.clone();
        let status = status.clone();
        let name_entry = name_entry.clone();
        let root_entry = root_entry.clone();
        let hooks_entry = hooks_entry.clone();
        move |_| {
            open_add_repository_dialog(
                root.root()
                    .and_then(|widget| widget.downcast::<Window>().ok()),
                host_ptr,
                &name_entry,
                &root_entry,
                &hooks_entry,
                repo_rows.clone(),
                &status,
            );
        }
    });

    save.connect_clicked({
        let status = status.clone();
        let repo_rows = repo_rows.clone();
        let name_entry = name_entry.clone();
        let root_entry = root_entry.clone();
        let hooks_entry = hooks_entry.clone();
        move |_| {
            status.set_text("Saving manifest...");
            if let Err(message) = queue_save_workspace_manifest_from_editor(
                host_ptr,
                name_entry.text().as_str(),
                root_entry.text().as_str(),
                hooks_entry.text().as_str(),
                &repo_rows.borrow(),
                None,
                false,
                &status,
            ) {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
        }
    });

    reload.connect_clicked({
        let status = status.clone();
        move |_| {
            status.set_text("Refreshing workspace...");
            if let Err(message) = queue_refresh_workspace(Some(status_text_sender(&status))) {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
        }
    });

    import.connect_clicked({
        let status = status.clone();
        move |_| {
            status.set_text("Importing repos.txt...");
            if let Err(message) =
                queue_import_workspace_from_repos_txt(Some(status_text_sender(&status)))
            {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
        }
    });

    let manifest_edit_path = snapshot
        .manifest_path
        .clone()
        .unwrap_or_else(|| default_manifest_path(&snapshot.workspace_root));
    edit_manifest.connect_clicked(move |_| {
        let path = manifest_edit_path.clone();
        open_text_editor_for_path(host_ptr, &path);
    });

    for button in [add_repo, save, reload, import, edit_manifest] {
        actions.append(&button);
    }

    root.append(&title);
    root.append(&subtitle);
    root.append(&actions);
    root.append(&form);
    root.append(&repo_header);
    root.append(&repo_help);
    root.append(&repo_scroller);
    root.append(&status);
}

fn labeled_field(label: &str, widget: &impl IsA<gtk::Widget>) -> GtkBox {
    let row = GtkBox::new(Orientation::Vertical, 4);
    let title = Label::new(Some(label));
    title.set_xalign(0.0);
    title.add_css_class("title-5");
    row.append(&title);
    row.append(widget);
    row
}

fn append_repo_editor_row(
    repo_rows_box: &GtkBox,
    repo_rows: &Rc<RefCell<Vec<RepoEditorRowHandle>>>,
    repo: Option<&RepositoryEntry>,
) {
    let row = GtkBox::new(Orientation::Horizontal, 8);
    row.add_css_class("boxed-list");

    let enabled = CheckButton::with_label("Enabled");
    enabled.set_active(repo.map(|repo| repo.enabled).unwrap_or(true));

    let name = Entry::new();
    name.set_placeholder_text(Some("Name"));
    name.set_hexpand(true);
    name.set_text(repo.map(|repo| repo.name.as_str()).unwrap_or(""));

    let dir_name = Entry::new();
    dir_name.set_placeholder_text(Some("dir_name"));
    dir_name.set_text(repo.map(|repo| repo.dir_name.as_str()).unwrap_or(""));

    let remote_url = Entry::new();
    remote_url.set_placeholder_text(Some("Remote URL"));
    remote_url.set_hexpand(true);
    remote_url.set_text(repo.map(|repo| repo.remote_url.as_str()).unwrap_or(""));

    let remove = Button::with_label("Remove");
    remove.connect_clicked({
        let repo_rows_box = repo_rows_box.clone();
        let repo_rows = repo_rows.clone();
        let row = row.clone();
        let enabled = enabled.clone();
        let name = name.clone();
        let dir_name = dir_name.clone();
        let remote_url = remote_url.clone();
        move |_| {
            repo_rows_box.remove(&row);
            repo_rows.borrow_mut().retain(|handle| {
                !handle.enabled.eq(&enabled)
                    && !handle.name.eq(&name)
                    && !handle.dir_name.eq(&dir_name)
                    && !handle.remote_url.eq(&remote_url)
            });
        }
    });

    row.append(&enabled);
    row.append(&name);
    row.append(&dir_name);
    row.append(&remote_url);
    row.append(&remove);
    repo_rows_box.append(&row);

    repo_rows.borrow_mut().push(RepoEditorRowHandle {
        enabled,
        name,
        dir_name,
        remote_url,
    });
}

fn open_add_repository_dialog(
    parent: Option<Window>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    workspace_name: &Entry,
    workspace_root: &Entry,
    shared_hooks_path: &Entry,
    repo_rows: Rc<RefCell<Vec<RepoEditorRowHandle>>>,
    status: &Label,
) {
    let dialog = Dialog::builder()
        .modal(true)
        .title("Add Repository")
        .build();
    if let Some(parent) = parent.as_ref() {
        dialog.set_transient_for(Some(parent));
    }
    dialog.add_button("Cancel", ResponseType::Cancel);
    dialog.add_button("Add Repository", ResponseType::Accept);
    dialog.set_default_response(ResponseType::Accept);

    let content = dialog.content_area();
    content.set_margin_top(16);
    content.set_margin_bottom(16);
    content.set_margin_start(16);
    content.set_margin_end(16);
    content.set_spacing(12);

    let body = GtkBox::new(Orientation::Vertical, 8);

    let remote_url = Entry::new();
    remote_url.set_hexpand(true);
    remote_url.set_placeholder_text(Some("git@github.com:org/repo.git"));

    let dir_name = Entry::new();
    dir_name.set_hexpand(true);
    dir_name.set_placeholder_text(Some("repo-dir"));

    let clone_now = CheckButton::with_label("Clone now");

    let error = Label::new(None);
    error.set_xalign(0.0);
    error.set_wrap(true);
    error.add_css_class("error");

    body.append(&labeled_field("Git Remote URL", &remote_url));
    body.append(&labeled_field("Directory Name", &dir_name));
    body.append(&clone_now);
    body.append(&error);
    content.append(&body);

    dialog.connect_response({
        let dialog = dialog.clone();
        let error = error.clone();
        let remote_url = remote_url.clone();
        let dir_name = dir_name.clone();
        let clone_now = clone_now.clone();
        let workspace_name = workspace_name.clone();
        let workspace_root = workspace_root.clone();
        let shared_hooks_path = shared_hooks_path.clone();
        let repo_rows = repo_rows.clone();
        let status = status.clone();
        move |_, response| {
            if response != ResponseType::Accept {
                dialog.close();
                return;
            }

            let mut repo_inputs = build_repo_editor_row_inputs(&repo_rows.borrow());
            let (_, manifest) = match build_workspace_manifest_from_inputs(
                workspace_name.text().as_str(),
                workspace_root.text().as_str(),
                shared_hooks_path.text().as_str(),
                &repo_inputs,
            ) {
                Ok(result) => result,
                Err(message) => {
                    error.set_text(&message);
                    return;
                }
            };

            let new_repo = match validate_new_repository_entry(
                &manifest,
                remote_url.text().as_str(),
                dir_name.text().as_str(),
            ) {
                Ok(repo) => repo,
                Err(message) => {
                    error.set_text(&message);
                    return;
                }
            };

            repo_inputs.push(RepoEditorRowInput {
                enabled: true,
                name: new_repo.name.clone(),
                dir_name: new_repo.dir_name.clone(),
                remote_url: new_repo.remote_url.clone(),
            });

            status.set_text("Saving manifest...");
            if let Err(message) = queue_save_workspace_manifest(
                host_ptr,
                workspace_name.text().as_str(),
                workspace_root.text().as_str(),
                shared_hooks_path.text().as_str(),
                repo_inputs,
                Some(new_repo.id.clone()),
                clone_now.is_active(),
                &status,
            ) {
                error.set_text(&message);
                status.set_text(&message);
                append_log(message);
                refresh_views();
                return;
            }

            dialog.close();
        }
    });

    dialog.present();
}

fn persist_last_workspace_path(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    workspace_root: &Path,
) {
    if host_ptr.is_null() {
        return;
    }
    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let Ok(mut config) = ensure_config(&host) else {
        return;
    };
    config.last_workspace_path = Some(workspace_root.display().to_string());
    if let Ok(payload) = serde_json::to_vec(&config) {
        let _ = host.write_config(&payload);
    }
}

fn persist_monitor_filter_mode(mode: MonitorFilterMode) {
    persist_plugin_config_direct(|config| {
        config.monitor_filter_mode = mode;
    });
}

fn persist_monitor_sort_mode(mode: MonitorSortMode) {
    persist_plugin_config_direct(|config| {
        config.monitor_sort_mode = mode;
    });
}

fn persist_monitor_sort_descending(descending: bool) {
    persist_plugin_config_direct(|config| {
        config.monitor_sort_descending = descending;
    });
}

fn persist_plugin_config_direct(update: impl FnOnce(&mut RonomepoPluginConfig)) {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StoredPluginConfigEntry {
        Legacy(Vec<u8>),
        Versioned {
            #[allow(dead_code)]
            schema_version: Option<u32>,
            payload: Vec<u8>,
        },
    }

    let path = persisted_plugin_configs_path("ronomepo");
    let mut document = fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str::<std::collections::HashMap<String, Value>>(&raw).ok())
        .unwrap_or_default();

    let mut config = document
        .get(PLUGIN_ID)
        .cloned()
        .and_then(|value| serde_json::from_value::<StoredPluginConfigEntry>(value).ok())
        .and_then(|entry| match entry {
            StoredPluginConfigEntry::Legacy(payload) => serde_json::from_slice(&payload).ok(),
            StoredPluginConfigEntry::Versioned { payload, .. } => {
                serde_json::from_slice(&payload).ok()
            }
        })
        .unwrap_or_default();

    update(&mut config);

    let Ok(payload) = serde_json::to_vec(&config) else {
        return;
    };
    document.insert(
        PLUGIN_ID.to_string(),
        serde_json::json!({
            "schema_version": Value::Null,
            "payload": payload,
        }),
    );

    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(raw) = serde_json::to_string_pretty(&document) {
        let _ = fs::write(path, raw);
    }
}

fn persisted_plugin_configs_path(persistence_id: &str) -> PathBuf {
    let mut path = if let Ok(dir) = env::var("XDG_CONFIG_HOME") {
        PathBuf::from(dir)
    } else if let Ok(home) = env::var("HOME") {
        PathBuf::from(home).join(".config")
    } else {
        PathBuf::from(".")
    };
    path.push(persistence_id);
    path.push("plugins.json");
    path
}

fn status_text_sender(status: &Label) -> mpsc::Sender<String> {
    let (sender, receiver) = mpsc::channel::<String>();
    let status = glib::SendWeakRef::from(status.downgrade());
    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let Ok(message) = receiver.recv() else {
            return;
        };
        main_context.invoke(move || {
            if let Some(status) = status.upgrade() {
                status.set_text(&message);
            }
        });
    });
    sender
}

fn build_repo_editor_row_inputs(repo_rows: &[RepoEditorRowHandle]) -> Vec<RepoEditorRowInput> {
    repo_rows
        .iter()
        .map(|handle| RepoEditorRowInput {
            enabled: handle.enabled.is_active(),
            name: handle.name.text().trim().to_string(),
            dir_name: handle.dir_name.text().trim().to_string(),
            remote_url: handle.remote_url.text().trim().to_string(),
        })
        .collect()
}

fn build_workspace_manifest_from_inputs(
    workspace_name: &str,
    workspace_root: &str,
    shared_hooks_path: &str,
    repo_rows: &[RepoEditorRowInput],
) -> Result<(PathBuf, WorkspaceManifest), String> {
    let workspace_root = normalize_workspace_root(workspace_root.trim());
    if workspace_root.as_os_str().is_empty() {
        return Err("Workspace root cannot be empty.".to_string());
    }

    let mut repos = Vec::new();
    for row in repo_rows {
        let remote_url = row.remote_url.trim().to_string();
        let mut dir_name = row.dir_name.trim().to_string();
        let mut name = row.name.trim().to_string();

        if remote_url.is_empty() && dir_name.is_empty() && name.is_empty() {
            continue;
        }
        if remote_url.is_empty() {
            return Err("Each non-empty repository row needs a remote URL.".to_string());
        }
        if dir_name.is_empty() {
            dir_name = derive_dir_name(&remote_url).map_err(|error| error.to_string())?;
        }
        if name.is_empty() {
            name = dir_name.clone();
        }

        repos.push(RepositoryEntry {
            id: dir_name.clone(),
            name,
            dir_name,
            remote_url,
            enabled: row.enabled,
        });
    }

    let manifest = WorkspaceManifest {
        name: if workspace_name.trim().is_empty() {
            workspace_name_from_root(&workspace_root)
        } else {
            workspace_name.trim().to_string()
        },
        root: workspace_root.clone(),
        repos,
        shared_hooks_path: if shared_hooks_path.trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(shared_hooks_path.trim()))
        },
        commit_check_rules: None,
    };

    Ok((workspace_root.clone(), manifest))
}

fn validate_new_repository_entry(
    manifest: &WorkspaceManifest,
    remote_url: &str,
    dir_name: &str,
) -> Result<RepositoryEntry, String> {
    let remote_url = remote_url.trim();
    let dir_name = dir_name.trim();

    if remote_url.is_empty() {
        return Err("Remote URL is required.".to_string());
    }
    if dir_name.is_empty() {
        return Err("Directory name is required.".to_string());
    }
    if manifest
        .repos
        .iter()
        .any(|repo| repo.remote_url == remote_url)
    {
        return Err(format!(
            "A repository with remote URL {remote_url} already exists in the manifest."
        ));
    }
    if manifest.repos.iter().any(|repo| repo.dir_name == dir_name) {
        return Err(format!(
            "A repository with directory name {dir_name} already exists in the manifest."
        ));
    }

    let repo_path = manifest.root.join(dir_name);
    if repo_path.exists() {
        return Err(format!(
            "Cannot add {dir_name} because {} already exists locally.",
            repo_path.display()
        ));
    }

    Ok(RepositoryEntry {
        id: dir_name.to_string(),
        name: dir_name.to_string(),
        dir_name: dir_name.to_string(),
        remote_url: remote_url.to_string(),
        enabled: true,
    })
}

fn queue_save_workspace_manifest(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    workspace_name: &str,
    workspace_root: &str,
    shared_hooks_path: &str,
    repo_rows: Vec<RepoEditorRowInput>,
    selected_repo_id: Option<String>,
    clone_after_save: bool,
    status: &Label,
) -> Result<(), String> {
    submit_job(WorkerJob::SaveManifestFromEditor {
        host_ptr: host_ptr as usize,
        workspace_name: workspace_name.to_string(),
        workspace_root: workspace_root.to_string(),
        shared_hooks_path: shared_hooks_path.to_string(),
        repo_rows,
        selected_repo_id,
        clone_after_save,
        status_sender: status_text_sender(status),
    })
}

fn queue_save_workspace_manifest_from_editor(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    workspace_name: &str,
    workspace_root: &str,
    shared_hooks_path: &str,
    repo_rows: &[RepoEditorRowHandle],
    selected_repo_id: Option<String>,
    clone_after_save: bool,
    status: &Label,
) -> Result<(), String> {
    queue_save_workspace_manifest(
        host_ptr,
        workspace_name,
        workspace_root,
        shared_hooks_path,
        build_repo_editor_row_inputs(repo_rows),
        selected_repo_id,
        clone_after_save,
        status,
    )
}

fn load_workspace_manifest(workspace_root: &Path) -> Result<RefreshWorkspaceResult, String> {
    let manifest_path = default_manifest_path(workspace_root);
    let manifest = load_manifest_if_present(&manifest_path);
    let message = if manifest.is_some() {
        format!(
            "Reloaded {MANIFEST_FILE_NAME} from {}",
            manifest_path.display()
        )
    } else {
        format!(
            "No {MANIFEST_FILE_NAME} found in {}",
            workspace_root.display()
        )
    };
    Ok(RefreshWorkspaceResult {
        workspace_root: workspace_root.to_path_buf(),
        manifest_path,
        manifest,
        message,
    })
}

fn import_workspace_manifest_from_repos_txt(
    workspace_root: &Path,
) -> Result<ImportWorkspaceResult, String> {
    let repos_path = workspace_root.join("repos.txt");
    let manifest_path = default_manifest_path(workspace_root);
    let mut manifest = import_repos_txt(
        &repos_path,
        workspace_root,
        &workspace_name_from_root(workspace_root),
    )
    .map_err(|error| error.to_string())?;
    ensure_commit_check_rules_initialized(&mut manifest);
    let repo_count = manifest.repos.len();
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;
    Ok(ImportWorkspaceResult {
        manifest_path,
        manifest,
        message: format!(
            "Imported {repo_count} repositories from {} into {}",
            repos_path.display(),
            default_manifest_path(workspace_root).display()
        ),
    })
}

fn build_history_report(
    manifest: &WorkspaceManifest,
    selected_repo_ids: &[String],
    num_commits: usize,
) -> Result<HistoryReportResult, String> {
    let report = collect_commit_check_report(manifest, selected_repo_ids, num_commits);
    let mut lines = Vec::new();
    for invalid in &report.invalid_rules {
        lines.push(format!(
            "Invalid rule | {} | {} | {}",
            invalid.rule_id, invalid.rule_name, invalid.message
        ));
    }
    if report.matches.is_empty() {
        lines.push(format!(
            "No commit check rule blocks found in the last {num_commits} commits."
        ));
    } else {
        lines.extend(report.matches.into_iter().map(|entry| {
            let markers = entry.matching_lines.join(" | ");
            format!(
                "{} | HEAD~{} | {} | {} | blocked by {} ({}) | {}",
                entry.repository_name,
                entry.head_offset,
                entry.commit_hash,
                entry.subject,
                entry.rule_name,
                entry.rule_id,
                markers
            )
        }));
    }
    let count = lines.len();
    Ok(HistoryReportResult {
        lines,
        message: format!(
            "History check completed over the last {num_commits} commits ({count} report line(s))."
        ),
    })
}

fn build_line_stats_report(
    manifest: &WorkspaceManifest,
    since_date: Option<&str>,
) -> Result<LineStatsResult, String> {
    let stats = collect_workspace_line_stats(manifest, since_date);
    let mut lines = stats
        .rows
        .into_iter()
        .map(|row| {
            if row.missing {
                format!("{} | missing", row.repository_name)
            } else {
                format!(
                    "{} | +{} | -{} | {:+}",
                    row.repository_name, row.additions, row.deletions, row.net
                )
            }
        })
        .collect::<Vec<_>>();
    lines.push(format!(
        "TOTAL | +{} | -{} | {:+}",
        stats.total_additions, stats.total_deletions, stats.total_net
    ));
    let rows = lines.len();
    Ok(LineStatsResult {
        lines,
        message: match since_date {
            Some(since_date) => format!("Line stats refreshed since {since_date} ({rows} row(s))."),
            None => format!("Line stats refreshed for all time ({rows} row(s))."),
        },
    })
}

fn save_workspace_manifest_from_inputs(
    host_ptr: usize,
    workspace_name: &str,
    workspace_root: &str,
    shared_hooks_path: &str,
    repo_rows: &[RepoEditorRowInput],
    selected_repo_id: Option<String>,
    clone_after_save: bool,
) -> Result<SaveManifestResult, String> {
    let (workspace_root, mut manifest) = build_workspace_manifest_from_inputs(
        workspace_name,
        workspace_root,
        shared_hooks_path,
        repo_rows,
    )?;
    manifest.commit_check_rules = existing_commit_check_rules_for_workspace(&workspace_root)
        .or_else(|| Some(default_commit_check_rules()));
    let manifest_path = default_manifest_path(&workspace_root);
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;
    sync_workspace_gitignore(&workspace_root, &manifest.repos)
        .map_err(|error| error.to_string())?;
    let repo_count = manifest.repos.len();

    Ok(SaveManifestResult {
        host_ptr,
        workspace_root,
        manifest_path: manifest_path.clone(),
        manifest,
        selected_repo_id,
        clone_after_save,
        message: format!(
            "Saved {} with {} repositories.",
            manifest_path.display(),
            repo_count
        ),
    })
}

fn existing_commit_check_rules_for_workspace(
    workspace_root: &Path,
) -> Option<Vec<CommitCheckRule>> {
    let app_state = state().lock().expect("state mutex poisoned");
    let manifest = app_state.manifest.as_ref()?;
    (manifest.root == workspace_root)
        .then(|| manifest.commit_check_rules.clone())
        .flatten()
}

fn sync_workspace_gitignore(
    workspace_root: &Path,
    repos: &[RepositoryEntry],
) -> Result<(), std::io::Error> {
    let gitignore_path = workspace_root.join(".gitignore");
    let mut content = match fs::read_to_string(&gitignore_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error),
    };

    let mut additions = Vec::new();
    for repo in repos {
        if !gitignore_contains_repo_dir(&content, &repo.dir_name) {
            additions.push(format!("/{}/", repo.dir_name.trim_matches('/')));
        }
    }

    if additions.is_empty() {
        return Ok(());
    }

    if !content.is_empty() && !content.ends_with('\n') {
        content.push('\n');
    }

    for entry in additions {
        content.push_str(&entry);
        content.push('\n');
    }

    fs::write(gitignore_path, content)
}

fn gitignore_contains_repo_dir(content: &str, dir_name: &str) -> bool {
    let normalized = dir_name.trim().trim_matches('/');
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .any(|line| {
            let line = line.trim_end_matches('/');
            let line = line.strip_prefix('/').unwrap_or(line);
            line == normalized
        })
}

fn apply_loaded_manifest(
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    manifest: Option<WorkspaceManifest>,
) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.workspace_root = workspace_root;
    app_state.manifest = manifest.clone();
    app_state.manifest_path = manifest.as_ref().map(|_| manifest_path);
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    sync_repo_runtime_state(&mut app_state);
    drop(app_state);
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

fn apply_imported_manifest(manifest_path: PathBuf, manifest: WorkspaceManifest) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.manifest = Some(manifest);
    app_state.manifest_path = Some(manifest_path);
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    sync_repo_runtime_state(&mut app_state);
    drop(app_state);
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

fn apply_saved_manifest(
    host_ptr: usize,
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    manifest: WorkspaceManifest,
) {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root = workspace_root.clone();
        app_state.manifest_path = Some(manifest_path);
        app_state.manifest = Some(manifest.clone());
        app_state.repo_details_cache.clear();
        app_state.repo_details_loading.clear();
        app_state
            .selected_repo_ids
            .retain(|id| manifest.repos.iter().any(|repo| &repo.id == id));
        if app_state
            .active_repo_id
            .as_ref()
            .is_some_and(|id| !manifest.repos.iter().any(|repo| &repo.id == id))
        {
            app_state.active_repo_id = None;
        }
        sync_repo_runtime_state(&mut app_state);
    }

    persist_last_workspace_path(host_ptr as *const _, &workspace_root);
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

fn render_repo_overview_into(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    instance_key: Option<&str>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    clear_box(root);

    let title = Label::new(Some("Repo Overview"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");
    root.append(&title);

    let target_repo_id = instance_key.or(snapshot.active_repo_id.as_deref());
    let Some(active_repo_id) = target_repo_id else {
        let body = Label::new(Some(
            "No repo target was provided. Open repo overviews from the left monitor.",
        ));
        body.set_xalign(0.0);
        body.set_wrap(true);
        body.add_css_class("muted");
        root.append(&body);
        root.append(&overview_actions(true));
        root.append(&overview_file_actions(snapshot, host_ptr));
        return;
    };

    let items = repository_items(snapshot);
    let Some(item) = items.iter().find(|item| item.id == active_repo_id) else {
        let body = Label::new(Some(
            "The active repo overview target is no longer present in the current manifest.",
        ));
        body.set_xalign(0.0);
        body.set_wrap(true);
        body.add_css_class("muted");
        root.append(&body);
        root.append(&overview_actions(true));
        root.append(&overview_file_actions(snapshot, host_ptr));
        return;
    };

    let subtitle = Label::new(Some(&format!(
        "{} | {} | {}",
        item.name,
        branch_label(item),
        format_sync_label(&item.status.sync)
    )));
    subtitle.set_xalign(0.0);
    subtitle.add_css_class("muted");
    subtitle.set_wrap(true);
    root.append(&subtitle);
    root.append(&repo_overview_actions(item, host_ptr));
    root.append(&overview_actions(true));
    root.append(&overview_file_actions(snapshot, host_ptr));

    let status_cards = GtkBox::new(Orientation::Horizontal, 12);
    for (label, value) in [
        ("Branch", branch_label(item).to_string()),
        ("State", status_label(&item.status.state).to_string()),
        ("Sync", format_sync_label(&item.status.sync)),
        (
            "Selected",
            if snapshot.selected_repo_ids.iter().any(|id| id == &item.id) {
                "Yes".to_string()
            } else {
                "No".to_string()
            },
        ),
    ] {
        status_cards.append(&stat_card(label, &value));
    }

    let sections = GtkBox::new(Orientation::Vertical, 12);
    if !snapshot.repo_details_cache.contains_key(&item.id)
        && !snapshot.repo_details_loading.contains(&item.id)
    {
        schedule_repo_details_load(&item.id, &item.status.repo_path);
    }
    let details = snapshot.repo_details_cache.get(&item.id);
    append_overview_section(
        &sections,
        "Path",
        &item.status.repo_path.display().to_string(),
    );
    append_overview_section(&sections, "Remote", &item.remote_url);
    append_overview_section(&sections, "Directory", &item.dir_name);
    append_overview_section(&sections, "State", status_label(&item.status.state));
    append_overview_section(&sections, "Sync", &format_sync_label(&item.status.sync));
    append_overview_section(
        &sections,
        "Current Selection Scope",
        &repo_selection_scope_label(snapshot, item),
    );
    append_overview_section(
        &sections,
        "Action Eligibility",
        &repo_action_eligibility(item),
    );
    append_overview_section(
        &sections,
        "Last Commit",
        &details
            .and_then(|details| details.last_commit.as_ref())
            .map(|commit| format!("{} {}", commit.short_sha, commit.subject))
            .unwrap_or_else(|| {
                if snapshot.repo_details_loading.contains(&item.id) {
                    "Loading commit information...".to_string()
                } else {
                    "No commit information available.".to_string()
                }
            }),
    );
    append_lines_section(
        &sections,
        "Remotes",
        &details
            .map(|details| details.remotes.clone())
            .unwrap_or_else(|| {
                if snapshot.repo_details_loading.contains(&item.id) {
                    vec!["Loading remotes...".to_string()]
                } else {
                    Vec::new()
                }
            }),
        "No remotes reported for this repository.",
    );
    append_lines_section(
        &sections,
        "Changed Files",
        &details
            .map(|details| details.changed_files.clone())
            .unwrap_or_else(|| {
                if snapshot.repo_details_loading.contains(&item.id) {
                    vec!["Loading working tree details...".to_string()]
                } else {
                    Vec::new()
                }
            }),
        "Working tree is clean.",
    );
    append_log_section(
        &sections,
        "Recent Repo Activity",
        &repo_recent_logs(snapshot, item),
        6,
    );

    root.append(&status_cards);
    root.append(&sections);
}

fn build_repo_terminal_panel(snapshot: &StateSnapshot, instance_key: Option<&str>) -> GtkBox {
    let panel = GtkBox::new(Orientation::Vertical, 10);
    panel.set_hexpand(true);
    panel.set_vexpand(true);

    let title_row = GtkBox::new(Orientation::Horizontal, 8);
    let title = Label::new(Some("Embedded Terminal"));
    title.set_xalign(0.0);
    title.add_css_class("title-4");
    title.set_hexpand(true);
    title_row.append(&title);
    panel.append(&title_row);

    let target_repo_id = instance_key.or(snapshot.active_repo_id.as_deref());
    let Some(item) = target_repo_id.and_then(|repo_id| repo_item_by_id(snapshot, repo_id)) else {
        panel.add_css_class("repo-terminal-empty");
        let body = Label::new(Some(
            "No repository target is attached to this tab, so no terminal session can be started.",
        ));
        body.set_xalign(0.0);
        body.set_wrap(true);
        body.add_css_class("muted");
        panel.append(&body);
        return panel;
    };

    let external = Button::with_label("Open External Terminal");
    let repo_path = item.status.repo_path.clone();
    let repo_name = item.name.clone();
    external.connect_clicked(move |_| {
        open_path_in_terminal(&repo_path, &repo_name);
    });
    title_row.append(&external);

    let subtitle = Label::new(Some(&format!(
        "Shell working directory: {}",
        item.status.repo_path.display()
    )));
    subtitle.set_xalign(0.0);
    subtitle.set_wrap(true);
    subtitle.add_css_class("muted");
    panel.append(&subtitle);

    #[cfg(feature = "embedded-terminal")]
    {
        panel.append(&build_vte_terminal_widget(
            &item.status.repo_path,
            &item.name,
        ));
    }

    #[cfg(not(feature = "embedded-terminal"))]
    {
        let disabled = Label::new(Some(
            "This build was compiled without the `embedded-terminal` feature. Enable that feature and install the VTE development package to render an interactive shell here.",
        ));
        disabled.set_xalign(0.0);
        disabled.set_wrap(true);
        disabled.add_css_class("muted");
        panel.append(&disabled);
    }

    panel
}

fn sync_repo_terminal_panel(panel: &GtkBox, snapshot: &StateSnapshot, instance_key: Option<&str>) {
    let target_repo_id = instance_key.or(snapshot.active_repo_id.as_deref());
    let has_target = target_repo_id
        .and_then(|repo_id| repo_item_by_id(snapshot, repo_id))
        .is_some();
    let current = panel.first_child();

    if current.is_none() {
        panel.append(&build_repo_terminal_panel(snapshot, instance_key));
        return;
    }

    let Some(current) = current else {
        return;
    };
    let is_empty = current.has_css_class("repo-terminal-empty");
    if is_empty && has_target {
        clear_box(panel);
        panel.append(&build_repo_terminal_panel(snapshot, instance_key));
    }
}

fn repo_item_by_id<'a>(
    snapshot: &'a StateSnapshot,
    repo_id: &str,
) -> Option<&'a RepositoryListItem> {
    snapshot
        .repository_items
        .iter()
        .find(|item| item.id == repo_id)
}

#[cfg(feature = "embedded-terminal")]
fn build_vte_terminal_widget(path: &Path, label: &str) -> GtkBox {
    let panel = GtkBox::new(Orientation::Vertical, 8);
    panel.set_hexpand(true);
    panel.set_vexpand(true);

    let terminal = vte4::Terminal::new();
    terminal.set_hexpand(true);
    terminal.set_vexpand(true);
    terminal.set_scrollback_lines(10_000);
    terminal.set_scroll_on_output(false);
    terminal.set_scroll_on_keystroke(true);
    terminal.set_mouse_autohide(true);
    terminal.add_css_class("mono");
    style_embedded_terminal_palette(&terminal);

    let shell = preferred_embedded_shell();
    spawn_shell_in_terminal(&terminal, path, label, &shell);

    let action_row = GtkBox::new(Orientation::Horizontal, 8);
    let restart = Button::with_label("Restart Shell");
    {
        let terminal = terminal.clone();
        let shell = shell.clone();
        let path = path.to_path_buf();
        let label = label.to_string();
        restart.connect_clicked(move |_| {
            spawn_shell_in_terminal(&terminal, &path, &label, &shell);
        });
    }
    action_row.append(&restart);
    panel.append(&action_row);
    panel.append(&terminal);

    panel
}

#[cfg(feature = "embedded-terminal")]
fn preferred_embedded_shell() -> String {
    env::var("SHELL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/bin/bash".to_string())
}

#[cfg(feature = "embedded-terminal")]
fn style_embedded_terminal_palette(terminal: &vte4::Terminal) {
    let palette = [
        rgba("#171717"),
        rgba("#d16969"),
        rgba("#7fb069"),
        rgba("#d7ba7d"),
        rgba("#6cb6ff"),
        rgba("#c586c0"),
        rgba("#4ec9b0"),
        rgba("#d4d4d4"),
        rgba("#6a6a6a"),
        rgba("#f48771"),
        rgba("#8ec07c"),
        rgba("#e5c07b"),
        rgba("#9cdcfe"),
        rgba("#d7a6ff"),
        rgba("#7fe4d2"),
        rgba("#f0f0f0"),
    ];
    let palette_refs = palette.iter().collect::<Vec<_>>();
    terminal.set_colors(None, None, &palette_refs);
}

#[cfg(feature = "embedded-terminal")]
fn rgba(hex: &str) -> RGBA {
    RGBA::parse(hex).expect("invalid hard-coded terminal color")
}

#[cfg(feature = "embedded-terminal")]
fn spawn_shell_in_terminal(terminal: &vte4::Terminal, path: &Path, label: &str, shell: &str) {
    if !path.exists() {
        terminal.feed(b"Working directory does not exist.\r\n");
        return;
    }

    let argv = [shell];
    let cwd = path.to_string_lossy().to_string();
    let repo_label = label.to_string();
    terminal.spawn_async(
        vte4::PtyFlags::DEFAULT,
        Some(cwd.as_str()),
        &argv,
        &[],
        glib::SpawnFlags::SEARCH_PATH,
        || {},
        -1,
        None::<&gio::Cancellable>,
        move |result| {
            if let Err(error) = result {
                append_log(format!(
                    "Failed to start embedded terminal for {repo_label}: {error}"
                ));
            }
        },
    );
}

fn overview_actions(include_open_overview: bool) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);
    let mut entries = vec![
        (
            "Refresh",
            command_refresh_workspace as extern "C" fn(_) -> _,
        ),
        ("Pull", command_pull as extern "C" fn(_) -> _),
        ("Push", command_push as extern "C" fn(_) -> _),
    ];
    if include_open_overview {
        entries.push((
            "Monorepo Overview",
            command_open_overview as extern "C" fn(_) -> _,
        ));
    }

    for (label, handler) in entries {
        let button = Button::with_label(label);
        button.connect_clicked(move |_| {
            let _ = handler(maruzzella_sdk::ffi::MzBytes::empty());
        });
        actions.append(&button);
    }
    actions
}

fn overview_command_group(label: &str, actions: GtkBox) -> GtkBox {
    let group = GtkBox::new(Orientation::Horizontal, 12);
    group.set_hexpand(true);

    let label_widget = Label::new(Some(label));
    label_widget.set_xalign(0.0);
    label_widget.set_width_chars(10);
    label_widget.add_css_class("dim-label");
    group.append(&label_widget);

    actions.set_hexpand(true);
    group.append(&actions);
    group
}

fn action_button(label: &str, count: usize) -> Button {
    let button = Button::with_label(&format!("{label} ({count})"));
    button.set_sensitive(count > 0);
    button
}

fn select_repo_bucket(label: &str, ids: Vec<String>) -> Button {
    let count = ids.len();
    let button = action_button(label, count);
    let log_label = label.to_string();
    button.connect_clicked(move |_| {
        if ids.is_empty() {
            append_log(format!(
                "{log_label} skipped because no repos match that bucket."
            ));
        } else {
            update_selected_repo_ids(ids.clone());
            append_log(format!("{log_label} matched {} repos.", ids.len()));
            refresh_views();
        }
    });
    button
}

fn repo_overview_open_button(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    label: &str,
    repo_ids: Vec<String>,
) -> Button {
    let count = repo_ids.len();
    let button = action_button(label, count);
    button.connect_clicked(move |_| {
        open_repo_overviews(host_ptr, &repo_ids);
    });
    button
}

fn clear_selection_button(selected_count: usize) -> Button {
    let button = action_button("Clear Selection", selected_count);
    button.connect_clicked(move |_| {
        update_selected_repo_ids(Vec::new());
        append_log("Cleared repository selection.".to_string());
        refresh_views();
    });
    button
}

fn report_button(
    label: &str,
    loading: bool,
    handler: extern "C" fn(maruzzella_sdk::ffi::MzBytes) -> maruzzella_sdk::ffi::MzStatus,
) -> Button {
    let button = Button::with_label(if loading { "Running..." } else { label });
    button.set_sensitive(!loading);
    button.connect_clicked(move |_| {
        let _ = handler(maruzzella_sdk::ffi::MzBytes::empty());
    });
    button
}

fn repo_target_button(item: &RepositoryListItem) -> Button {
    let button = Button::with_label("Target");
    let repo_id = item.id.clone();
    let repo_name = item.name.clone();
    button.connect_clicked(move |_| {
        update_selected_repo_ids(vec![repo_id.clone()]);
        append_log(format!("Targeted {repo_name} as the active selection."));
        refresh_views();
    });
    button
}

fn repo_open_button(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> Button {
    let button = Button::with_label("Open");
    let repo_id = item.id.clone();
    button.connect_clicked(move |_| {
        open_repo_overviews(host_ptr, std::slice::from_ref(&repo_id));
    });
    button
}

fn overview_repo_row(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let row = GtkBox::new(Orientation::Horizontal, 10);
    row.set_margin_top(2);
    row.set_margin_bottom(2);
    row.set_margin_start(8);
    row.set_margin_end(8);
    row.set_tooltip_text(Some(&format!(
        "{}\n{}",
        item.status.repo_path.display(),
        item.remote_url
    )));

    let name = monitor_text_cell(
        &item.name,
        MONITOR_NAME_COL_CHARS,
        MONITOR_NAME_COL_WIDTH,
        false,
    );
    let branch = monitor_text_cell(
        branch_label(item),
        MONITOR_BRANCH_COL_CHARS,
        MONITOR_BRANCH_COL_WIDTH,
        false,
    );
    let state = monitor_state_cell(&item.status.state);
    let sync = monitor_sync_cell(&item.status.sync);

    row.append(&name);
    row.append(&branch);
    row.append(&state);
    row.append(&sync);
    row.append(&repo_target_button(item));
    row.append(&repo_open_button(item, host_ptr));
    row
}

fn overview_repo_header() -> GtkBox {
    let header = GtkBox::new(Orientation::Horizontal, 10);
    header.add_css_class("mono");
    header.set_margin_start(8);
    header.set_margin_end(8);
    header.set_margin_bottom(2);

    let name = monitor_text_cell(
        "Name",
        MONITOR_NAME_COL_CHARS,
        MONITOR_NAME_COL_WIDTH,
        false,
    );
    let branch = monitor_text_cell(
        "Branch",
        MONITOR_BRANCH_COL_CHARS,
        MONITOR_BRANCH_COL_WIDTH,
        false,
    );
    let state = monitor_text_cell(
        "State",
        MONITOR_STATE_COL_CHARS,
        MONITOR_STATE_COL_WIDTH,
        false,
    );
    let sync = Label::new(Some("Sync"));
    sync.set_xalign(0.0);
    sync.set_hexpand(true);
    sync.set_ellipsize(EllipsizeMode::End);

    for label in [&name, &branch, &state, &sync] {
        label.add_css_class("dim-label");
        header.append(label);
    }

    let action_space = Label::new(Some(""));
    action_space.set_width_chars(14);
    header.append(&action_space);
    header
}

fn append_facts_section(container: &GtkBox, heading: &str, facts: &[(&str, String)]) {
    let block = GtkBox::new(Orientation::Vertical, 8);

    let heading_label = Label::new(Some(heading));
    heading_label.set_xalign(0.0);
    heading_label.add_css_class("title-4");
    block.append(&heading_label);

    for (label, value) in facts {
        let row = GtkBox::new(Orientation::Horizontal, 10);
        row.set_hexpand(true);

        let key = Label::new(Some(label));
        key.set_xalign(0.0);
        key.set_width_chars(12);
        key.add_css_class("dim-label");

        let value_label = Label::new(Some(value));
        value_label.set_xalign(0.0);
        value_label.set_hexpand(true);
        value_label.set_wrap(true);

        row.append(&key);
        row.append(&value_label);
        block.append(&row);
    }

    container.append(&block);
    container.append(&Separator::new(Orientation::Horizontal));
}

fn sync_bucket_ids(items: &[RepositoryListItem], sync_label: &str) -> Vec<String> {
    collect_repo_ids(items, |item| match sync_label {
        "ahead" => matches!(item.status.sync, ronomepo_core::RepositorySync::Ahead(_)),
        "behind" => matches!(item.status.sync, ronomepo_core::RepositorySync::Behind(_)),
        "diverged" => matches!(
            item.status.sync,
            ronomepo_core::RepositorySync::Diverged { .. }
        ),
        "no_upstream" => {
            matches!(item.status.sync, ronomepo_core::RepositorySync::NoUpstream)
        }
        _ => false,
    })
}

fn repo_overview_actions(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);
    for (label, kind) in [
        ("Target This Repo", None),
        ("Open Folder", None),
        ("Open Terminal", None),
        ("Open In Editor", None),
        ("Edit README", None),
        ("Edit .git/config", None),
        ("Clone Repo", Some(OperationKind::CloneMissing)),
        ("Pull Repo", Some(OperationKind::Pull)),
        ("Push Repo", Some(OperationKind::Push)),
        ("Push Repo Force", Some(OperationKind::PushForce)),
        ("Apply Hooks", Some(OperationKind::ApplyHooks)),
    ] {
        let button = Button::with_label(label);
        let repo_id = item.id.clone();
        let repo_name = item.name.clone();
        let repo_path = item.status.repo_path.clone();
        button.connect_clicked(move |_| match label {
            "Target This Repo" => {
                set_selected_repo_ids(vec![repo_id.clone()]);
                append_log(format!("Targeted {repo_id} as the active selection."));
            }
            "Open Folder" => {
                open_path_in_file_manager(&repo_path, &repo_name);
            }
            "Open Terminal" => {
                open_path_in_terminal(&repo_path, &repo_name);
            }
            "Open In Editor" => {
                open_path_in_editor(&repo_path, &repo_name);
            }
            "Edit README" => {
                open_text_editor_for_path(host_ptr, &repo_path.join("README.md"));
            }
            "Edit .git/config" => {
                open_text_editor_for_path(host_ptr, &repo_path.join(".git/config"));
            }
            _ => {
                set_selected_repo_ids(vec![repo_id.clone()]);
                if let Some(kind) = kind {
                    launch_operation(kind);
                }
            }
        });
        actions.append(&button);
    }
    actions
}

fn overview_file_actions(
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);

    for (label, path) in [
        (
            "Edit Manifest",
            snapshot
                .manifest_path
                .clone()
                .unwrap_or_else(|| default_manifest_path(&snapshot.workspace_root)),
        ),
        ("Edit repos.txt", snapshot.workspace_root.join("repos.txt")),
    ] {
        let button = Button::with_label(label);
        button.connect_clicked(move |_| {
            open_text_editor_for_path(host_ptr, &path);
        });
        actions.append(&button);
    }

    actions
}

fn monorepo_selection_actions(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    items: &[RepositoryListItem],
) -> GtkBox {
    let actions = GtkBox::new(Orientation::Vertical, 8);
    let bucket_row = GtkBox::new(Orientation::Horizontal, 8);
    let scope_row = GtkBox::new(Orientation::Horizontal, 8);

    let current_snapshot = snapshot();
    let selected_ids = current_snapshot.selected_repo_ids;
    let selected_count = selected_ids.len();

    for button in [
        select_repo_bucket(
            "Select Attention",
            collect_repo_ids(items, |item| repo_attention_rank(item) < 7),
        ),
        select_repo_bucket(
            "Select Dirty",
            collect_repo_ids(items, |item| {
                matches!(
                    item.status.state,
                    ronomepo_core::RepositoryState::Dirty
                        | ronomepo_core::RepositoryState::Untracked
                )
            }),
        ),
        select_repo_bucket(
            "Select Missing",
            collect_repo_ids(items, |item| {
                matches!(item.status.state, ronomepo_core::RepositoryState::Missing)
            }),
        ),
        select_repo_bucket("Select Ahead", sync_bucket_ids(items, "ahead")),
        select_repo_bucket("Select Behind", sync_bucket_ids(items, "behind")),
        select_repo_bucket("Select Diverged", sync_bucket_ids(items, "diverged")),
        select_repo_bucket("Select No Upstream", sync_bucket_ids(items, "no_upstream")),
    ] {
        bucket_row.append(&button);
    }

    scope_row.append(&repo_overview_open_button(
        host_ptr,
        "Open Selected",
        selected_ids,
    ));
    scope_row.append(&clear_selection_button(selected_count));

    let settings = Button::with_label("Workspace Settings");
    settings.connect_clicked(move |_| {
        if let Err(message) = open_workspace_settings_tab() {
            append_log(message);
            refresh_views();
        }
    });
    scope_row.append(&settings);

    actions.append(&bucket_row);
    actions.append(&scope_row);
    actions
}

fn monorepo_report_actions(snapshot: &StateSnapshot) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);

    actions.append(&report_button(
        "Check History",
        snapshot.history_report_loading,
        command_check_history,
    ));

    let open_commit_check = Button::with_label("Open Commit Check");
    open_commit_check.connect_clicked(|_| {
        let _ = command_open_commit_check(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&open_commit_check);

    let since_entry = Entry::new();
    since_entry.set_placeholder_text(Some("Since date YYYY-MM-DD"));
    since_entry.set_text(&snapshot.line_stats_since);
    since_entry.set_hexpand(true);
    since_entry.connect_changed(|entry| {
        update_line_stats_since(entry.text().to_string());
    });
    since_entry.connect_activate(|_| {
        let _ = command_line_stats(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&since_entry);

    actions.append(&report_button(
        "Line Stats",
        snapshot.line_stats_loading,
        command_line_stats,
    ));

    let all_time = Button::with_label("All Time");
    all_time.set_sensitive(!snapshot.line_stats_loading);
    all_time.connect_clicked(|_| {
        update_line_stats_since(String::new());
        let _ = command_line_stats(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&all_time);

    actions
}

fn collect_repo_ids<F>(items: &[RepositoryListItem], predicate: F) -> Vec<String>
where
    F: Fn(&RepositoryListItem) -> bool,
{
    items
        .iter()
        .filter(|item| predicate(item))
        .map(|item| item.id.clone())
        .collect()
}

fn stat_card(label: &str, value: &str) -> GtkBox {
    let card = GtkBox::new(Orientation::Vertical, 4);
    card.set_margin_bottom(8);

    let value_label = Label::new(Some(value));
    value_label.set_xalign(0.0);
    value_label.add_css_class("title-3");
    let label_widget = Label::new(Some(label));
    label_widget.set_xalign(0.0);
    label_widget.add_css_class("muted");

    card.append(&value_label);
    card.append(&label_widget);
    card
}

fn append_overview_section(container: &GtkBox, heading: &str, body: &str) {
    let block = GtkBox::new(Orientation::Vertical, 4);
    let heading_label = Label::new(Some(heading));
    heading_label.set_xalign(0.0);
    heading_label.add_css_class("title-4");
    let body_label = Label::new(Some(body));
    body_label.set_xalign(0.0);
    body_label.set_wrap(true);
    body_label.add_css_class("muted");
    block.append(&heading_label);
    block.append(&body_label);
    container.append(&block);
    container.append(&Separator::new(Orientation::Horizontal));
}

fn append_lines_section(container: &GtkBox, heading: &str, lines: &[String], empty_message: &str) {
    let block = GtkBox::new(Orientation::Vertical, 6);
    let heading_label = Label::new(Some(heading));
    heading_label.set_xalign(0.0);
    heading_label.add_css_class("title-4");
    block.append(&heading_label);

    if lines.is_empty() {
        let empty = Label::new(Some(empty_message));
        empty.set_xalign(0.0);
        empty.add_css_class("muted");
        block.append(&empty);
    } else {
        for line in lines {
            let row = Label::new(Some(line));
            row.set_xalign(0.0);
            row.set_wrap(true);
            row.add_css_class("mono");
            block.append(&row);
        }
    }

    container.append(&block);
    container.append(&Separator::new(Orientation::Horizontal));
}

fn append_repo_group_section(
    container: &GtkBox,
    heading: &str,
    body: &str,
    items: &[RepositoryListItem],
    limit: Option<usize>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    let block = GtkBox::new(Orientation::Vertical, 6);

    let heading_label = Label::new(Some(heading));
    heading_label.set_xalign(0.0);
    heading_label.add_css_class("title-4");

    let body_label = Label::new(Some(body));
    body_label.set_xalign(0.0);
    body_label.set_wrap(true);
    body_label.add_css_class("muted");

    block.append(&heading_label);
    block.append(&body_label);

    if items.is_empty() {
        let empty_label = Label::new(Some("Nothing to show."));
        empty_label.set_xalign(0.0);
        empty_label.add_css_class("muted");
        block.append(&empty_label);
    } else {
        let take = limit.unwrap_or(items.len());
        block.append(&overview_repo_header());
        for item in items.iter().take(take) {
            block.append(&overview_repo_row(item, host_ptr));
        }
        if items.len() > take {
            let more_label = Label::new(Some(&format!(
                "{} more repos hidden in this section.",
                items.len() - take
            )));
            more_label.set_xalign(0.0);
            more_label.add_css_class("muted");
            block.append(&more_label);
        }
    }

    container.append(&block);
    container.append(&Separator::new(Orientation::Horizontal));
}

fn append_log_section(container: &GtkBox, heading: &str, logs: &[String], limit: usize) {
    let block = GtkBox::new(Orientation::Vertical, 6);

    let heading_label = Label::new(Some(heading));
    heading_label.set_xalign(0.0);
    heading_label.add_css_class("title-4");
    block.append(&heading_label);

    if logs.is_empty() {
        let empty_label = Label::new(Some("No operations recorded yet."));
        empty_label.set_xalign(0.0);
        empty_label.add_css_class("muted");
        block.append(&empty_label);
    } else {
        for entry in logs.iter().rev().take(limit).rev() {
            let row = Label::new(Some(entry));
            row.set_xalign(0.0);
            row.set_wrap(true);
            row.add_css_class("mono");
            block.append(&row);
        }
    }

    container.append(&block);
    container.append(&Separator::new(Orientation::Horizontal));
}

fn attention_items(items: &[RepositoryListItem]) -> Vec<RepositoryListItem> {
    let mut attention = items
        .iter()
        .filter(|item| repo_attention_rank(item) < 7)
        .cloned()
        .collect::<Vec<_>>();
    attention.sort_by_key(repo_monitor_sort_key);
    attention
}

fn selected_repository_items(
    snapshot: &StateSnapshot,
    items: &[RepositoryListItem],
) -> Vec<RepositoryListItem> {
    let mut selected = items
        .iter()
        .filter(|item| snapshot.selected_repo_ids.iter().any(|id| id == &item.id))
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by_key(repo_monitor_sort_key);
    selected
}

fn clear_box(root: &GtkBox) {
    while let Some(child) = root.first_child() {
        root.remove(&child);
    }
}

fn repo_selection_scope_label(snapshot: &StateSnapshot, item: &RepositoryListItem) -> String {
    if snapshot.selected_repo_ids.is_empty() {
        "No explicit selection in the left monitor. Workspace actions apply to all eligible repos."
            .to_string()
    } else if snapshot.selected_repo_ids.iter().any(|id| id == &item.id) {
        format!(
            "This repo is part of the current selection ({} repos total). Toolbar and overview actions will include it.",
            snapshot.selected_repo_ids.len()
        )
    } else {
        format!(
            "This repo is not in the current selection ({} repos selected elsewhere). Use 'Target This Repo' to scope actions to it.",
            snapshot.selected_repo_ids.len()
        )
    }
}

fn repo_action_eligibility(item: &RepositoryListItem) -> String {
    use ronomepo_core::{RepositoryState, RepositorySync};

    let clone = if matches!(item.status.state, RepositoryState::Missing) {
        "Clone is available."
    } else {
        "Clone will be skipped because the repo already exists locally."
    };
    let pull = match item.status.state {
        RepositoryState::Missing => "Pull will be skipped until the repo is cloned.",
        RepositoryState::Dirty => "Pull will be skipped because the working tree is dirty.",
        RepositoryState::Untracked => "Pull is allowed, but untracked files are present.",
        _ => "Pull is allowed if the repo has a valid remote.",
    };
    let push = match &item.status.sync {
        RepositorySync::Ahead(count) => {
            format!("Push is available with {count} local commit(s) ahead.")
        }
        RepositorySync::Diverged { ahead, behind } => {
            format!("Push is risky: the branch diverged (+{ahead}/-{behind}).")
        }
        RepositorySync::NoUpstream => {
            "Push will be skipped because no upstream is configured.".to_string()
        }
        RepositorySync::Behind(count) => {
            format!("Push is not useful yet because the branch is behind by {count} commit(s).")
        }
        RepositorySync::UpToDate => {
            "Push will be skipped because the repo is already up to date.".to_string()
        }
        RepositorySync::Unknown => {
            "Push eligibility is unknown because Git sync state could not be determined."
                .to_string()
        }
    };

    format!("{clone} {pull} {push}")
}

fn repo_recent_logs(snapshot: &StateSnapshot, item: &RepositoryListItem) -> Vec<String> {
    let item_name = item.name.to_ascii_lowercase();
    let item_id = item.id.to_ascii_lowercase();
    let item_dir = item.dir_name.to_ascii_lowercase();

    let mut logs = snapshot
        .logs
        .iter()
        .filter(|entry| {
            let lower = entry.to_ascii_lowercase();
            lower.contains(&item_name) || lower.contains(&item_id) || lower.contains(&item_dir)
        })
        .cloned()
        .collect::<Vec<_>>();

    if logs.is_empty() {
        logs.push("No repo-specific operations have been logged yet.".to_string());
    }

    logs
}

fn set_selected_repo_ids(ids: Vec<String>) {
    update_selected_repo_ids(ids);
    refresh_views();
}

fn selected_repository_items_from_state() -> Vec<RepositoryListItem> {
    let snapshot = snapshot();
    let items = repository_items(&snapshot);
    selected_repository_items(&snapshot, &items)
}

fn open_selected_repo_folders() {
    let selected = selected_repository_items_from_state();
    if selected.is_empty() {
        append_log("Open Folder skipped because no repos are selected.".to_string());
        return;
    }

    for item in selected {
        open_path_in_file_manager(&item.status.repo_path, &item.name);
    }
}

fn open_selected_repo_terminal() {
    let selected = selected_repository_items_from_state();
    let Some(item) = selected.first() else {
        append_log("Open Terminal skipped because no repos are selected.".to_string());
        return;
    };
    if selected.len() > 1 {
        append_log(format!(
            "Open Terminal will use the first selected repo only: {}.",
            item.name
        ));
    }
    open_path_in_terminal(&item.status.repo_path, &item.name);
}

fn open_selected_repo_in_editor() {
    let selected = selected_repository_items_from_state();
    let Some(item) = selected.first() else {
        append_log("Open In Editor skipped because no repos are selected.".to_string());
        return;
    };
    if selected.len() > 1 {
        append_log(format!(
            "Open In Editor will use the first selected repo only: {}.",
            item.name
        ));
    }
    open_path_in_editor(&item.status.repo_path, &item.name);
}

fn open_text_editor_for_path(host_ptr: *const maruzzella_sdk::ffi::MzHostApi, path: &Path) {
    let resolved = resolve_editor_path(&path.display().to_string());
    let instance_key = resolved.to_string_lossy().to_string();
    let requested_title = editor_title_for_path(&resolved);

    if host_ptr.is_null() {
        append_log(format!(
            "Cannot open {} in the Ronomepo editor because the Maruzzella host handle is unavailable.",
            resolved.display()
        ));
        return;
    }

    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let mut request = OpenViewRequest::new(PLUGIN_ID, VIEW_TEXT_EDITOR, MzViewPlacement::Workbench);
    request.instance_key = Some(&instance_key);
    request.requested_title = Some(&requested_title);
    request.payload = instance_key.as_bytes();

    match host.open_view(&request) {
        Ok(MzViewOpenDisposition::Opened) => {
            append_log(format!(
                "Opened {} in a Ronomepo editor tab.",
                resolved.display()
            ));
        }
        Ok(MzViewOpenDisposition::FocusedExisting) => {
            append_log(format!(
                "Focused existing Ronomepo editor tab for {}.",
                resolved.display()
            ));
        }
        Err(status) => append_log(format!(
            "Failed to open {} in a Ronomepo editor tab: {status:?}",
            resolved.display()
        )),
    }
}

fn open_path_in_file_manager(path: &Path, label: &str) {
    if !path.exists() {
        append_log(format!(
            "Open Folder skipped for {label} because {} does not exist.",
            path.display()
        ));
        return;
    }

    match Command::new("xdg-open").arg(path).spawn() {
        Ok(_) => append_log(format!("Opened folder for {label}: {}", path.display())),
        Err(error) => append_log(format!(
            "Failed to open folder for {label} at {}: {error}",
            path.display()
        )),
    }
}

fn open_path_in_terminal(path: &Path, label: &str) {
    if !path.exists() {
        append_log(format!(
            "Open Terminal skipped for {label} because {} does not exist.",
            path.display()
        ));
        return;
    }

    let path_text = path.display().to_string();
    for (program, flag) in [
        ("x-terminal-emulator", "--working-directory"),
        ("kgx", "--working-directory"),
        ("gnome-terminal", "--working-directory"),
        ("konsole", "--workdir"),
        ("alacritty", "--working-directory"),
        ("kitty", "--directory"),
    ] {
        if Command::new(program)
            .args([flag, path_text.as_str()])
            .spawn()
            .is_ok()
        {
            append_log(format!("Opened terminal for {label}: {}", path.display()));
            return;
        }
    }

    append_log(format!(
        "Failed to open terminal for {label}: no supported terminal launcher was found."
    ));
}

fn open_path_in_editor(path: &Path, label: &str) {
    if !path.exists() {
        append_log(format!(
            "Open In Editor skipped for {label} because {} does not exist.",
            path.display()
        ));
        return;
    }

    for variable in ["RONOMEPO_EDITOR", "VISUAL", "EDITOR"] {
        if let Some(command) = env::var_os(variable) {
            if Command::new(&command).arg(path).spawn().is_ok() {
                append_log(format!(
                    "Opened {label} in editor from {variable}: {}",
                    path.display()
                ));
                return;
            }
        }
    }

    match Command::new("xdg-open").arg(path).spawn() {
        Ok(_) => append_log(format!(
            "Opened {label} in the desktop editor fallback: {}",
            path.display()
        )),
        Err(error) => append_log(format!(
            "Failed to open {label} in an editor at {}: {error}",
            path.display()
        )),
    }
}

extern "C" fn create_text_editor_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 12);
    root.set_margin_top(18);
    root.set_margin_bottom(18);
    root.set_margin_start(18);
    root.set_margin_end(18);

    let initial_path = unsafe { request.as_ref() }
        .and_then(|request| decode_mzstr(request.instance_key))
        .or_else(|| {
            unsafe { request.as_ref() }.and_then(|request| {
                if request.payload.ptr.is_null() || request.payload.len == 0 {
                    None
                } else {
                    let bytes = unsafe {
                        std::slice::from_raw_parts(request.payload.ptr, request.payload.len)
                    };
                    let text = String::from_utf8_lossy(bytes).trim().to_string();
                    (!text.is_empty()).then_some(text)
                }
            })
        });

    let title = Label::new(Some("Text Editor"));
    title.set_xalign(0.0);
    title.add_css_class("title-3");

    let toolbar = GtkBox::new(Orientation::Horizontal, 8);
    let path_entry = Entry::new();
    path_entry.set_hexpand(true);
    path_entry.set_placeholder_text(Some("Relative or absolute file path"));

    let open_button = Button::with_label("Open");
    let save_button = Button::with_label("Save");
    let format_button = Button::with_label("Format");

    toolbar.append(&path_entry);
    toolbar.append(&open_button);
    toolbar.append(&save_button);
    toolbar.append(&format_button);

    let status = Label::new(Some("Open a text file from the workspace."));
    status.set_xalign(0.0);
    status.add_css_class("muted");
    status.set_wrap(true);

    let buffer = TextBuffer::new(None);
    let text = TextView::with_buffer(&buffer);
    text.set_monospace(true);
    text.set_wrap_mode(WrapMode::WordChar);
    text.set_vexpand(true);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Automatic)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&text)
        .build();

    if let Some(path) = initial_path.as_deref() {
        path_entry.set_text(path);
        let resolved = resolve_editor_path(path);
        title.set_text(&editor_title_for_path(&resolved));
        queue_editor_load(&buffer, &status, &title, &path_entry, &resolved);
    }

    open_button.connect_clicked({
        let path_entry = path_entry.clone();
        let buffer = buffer.clone();
        let status = status.clone();
        let title = title.clone();
        let initial_path = initial_path.clone();
        move |_| {
            let path = resolve_editor_path(path_entry.text().as_str());
            let current_instance = initial_path
                .as_deref()
                .map(resolve_editor_path)
                .unwrap_or_default();
            if !current_instance.as_os_str().is_empty() && current_instance != path {
                open_text_editor_for_path(host, &path);
            } else {
                queue_editor_load(&buffer, &status, &title, &path_entry, &path);
                title.set_text(&editor_title_for_path(&path));
            }
        }
    });

    save_button.connect_clicked({
        let path_entry = path_entry.clone();
        let buffer = buffer.clone();
        let status = status.clone();
        let title = title.clone();
        move |_| {
            let path = resolve_editor_path(path_entry.text().as_str());
            let content = buffer.text(&buffer.start_iter(), &buffer.end_iter(), true);
            queue_editor_save(&status, &title, &path, content.to_string(), host as usize);
        }
    });

    format_button.connect_clicked({
        let status = status.clone();
        move |_| {
            status.set_text(
                "Formatting is intentionally deferred. This editor is the Ronomepo-local stopgap until Maruzzella owns it.",
            );
        }
    });

    root.append(&title);
    root.append(&toolbar);
    root.append(&status);
    root.append(&scroller);

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn resolve_editor_path(input: &str) -> PathBuf {
    let trimmed = input.trim();
    let path = PathBuf::from(trimmed);
    if path.is_absolute() {
        path
    } else {
        snapshot().workspace_root.join(path)
    }
}

fn queue_editor_load(
    buffer: &TextBuffer,
    status: &Label,
    title: &Label,
    path_entry: &Entry,
    path: &Path,
) {
    status.set_text(&format!("Loading {}...", path.display()));
    let (sender, receiver) = mpsc::channel::<EditorLoadMessage>();
    let buffer = glib::SendWeakRef::from(buffer.downgrade());
    let status = glib::SendWeakRef::from(status.downgrade());
    let title = glib::SendWeakRef::from(title.downgrade());
    let path_entry = glib::SendWeakRef::from(path_entry.downgrade());
    let queue_error_buffer = buffer.clone();
    let queue_error_status = status.clone();
    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let Ok(message) = receiver.recv() else {
            return;
        };
        main_context.invoke(move || {
            let Some(path_entry) = path_entry.upgrade() else {
                return;
            };
            if resolve_editor_path(path_entry.text().as_str()) == message.path {
                let Some(buffer) = buffer.upgrade() else {
                    return;
                };
                let Some(status) = status.upgrade() else {
                    return;
                };
                let Some(title) = title.upgrade() else {
                    return;
                };
                match message.result {
                    Ok(content) => {
                        buffer.set_text(&content);
                        status.set_text(&format!("Loaded {}", message.path.display()));
                        title.set_text(&editor_title_for_path(&message.path));
                    }
                    Err(error) => {
                        buffer.set_text("");
                        status.set_text(&format!(
                            "Failed to open {}: {error}",
                            message.path.display()
                        ));
                    }
                }
            }
        });
    });

    if let Err(message) = submit_job(WorkerJob::EditorLoad {
        path: path.to_path_buf(),
        reply: sender,
    }) {
        if let Some(buffer) = queue_error_buffer.upgrade() {
            buffer.set_text("");
        }
        if let Some(status) = queue_error_status.upgrade() {
            status.set_text(&format!(
                "Failed to queue load for {}: {message}",
                path.display()
            ));
        }
    }
}

fn queue_editor_save(status: &Label, title: &Label, path: &Path, content: String, host_ptr: usize) {
    status.set_text(&format!("Saving {}...", path.display()));
    let (sender, receiver) = mpsc::channel::<EditorSaveMessage>();
    let status = glib::SendWeakRef::from(status.downgrade());
    let title = glib::SendWeakRef::from(title.downgrade());
    let path = path.to_path_buf();
    let queue_error_status = status.clone();
    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let Ok(message) = receiver.recv() else {
            return;
        };
        main_context.invoke(move || {
            let Some(status) = status.upgrade() else {
                return;
            };
            let Some(title) = title.upgrade() else {
                return;
            };
            match message.result {
                Ok(()) => {
                    status.set_text(&format!("Saved {}", message.path.display()));
                    let title_text = editor_title_for_path(&message.path);
                    title.set_text(&title_text);
                    if host_ptr != 0 {
                        let host = unsafe {
                            HostApi::from_raw(&*(host_ptr as *const maruzzella_sdk::ffi::MzHostApi))
                        };
                        let mut query = maruzzella_sdk::ViewQuery::new(PLUGIN_ID, VIEW_TEXT_EDITOR);
                        let path_key = message.path.to_string_lossy().to_string();
                        query.instance_key = Some(&path_key);
                        let _ = host.update_view_title(&query, &title_text);
                    }
                }
                Err(error) => {
                    status.set_text(&format!(
                        "Failed to save {}: {error}",
                        message.path.display()
                    ));
                }
            }
        });
    });

    if let Err(message) = submit_job(WorkerJob::EditorSave {
        path: path.clone(),
        content,
        reply: sender,
    }) {
        if let Some(status) = queue_error_status.upgrade() {
            status.set_text(&format!(
                "Failed to queue save for {}: {message}",
                path.display()
            ));
        }
    }
}

fn editor_title_for_path(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("Text Editor")
        .to_string()
}

fn host_toolbar_button(
    host: &HostApi<'_>,
    command_id: &str,
    icon_name: &str,
    label: &str,
) -> Option<Button> {
    let spec = ToolbarWidgetSpec {
        icon_name: Some(icon_name),
        label: Some(label),
        command_id,
        payload: &[],
        display_mode: MzToolbarDisplayMode::IconOnly,
        appearance_id: Some("ronomepo-toolbar-ghost"),
    };
    let widget_ptr = host.create_toolbar_widget(&spec).ok()?;
    let widget = unsafe { gtk::Widget::from_glib_full(widget_ptr as *mut gtk::ffi::GtkWidget) };
    widget.downcast::<Button>().ok()
}

fn decode_mzstr(value: maruzzella_sdk::ffi::MzStr) -> Option<String> {
    if value.ptr.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(value.ptr, value.len) };
    let text = String::from_utf8_lossy(bytes).trim().to_string();
    (!text.is_empty()).then_some(text)
}

extern "C" fn create_operations_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 12);
    root.set_margin_top(18);
    root.set_margin_bottom(18);
    root.set_margin_start(18);
    root.set_margin_end(18);

    let summary = Label::new(Some(&operation_summary_text(&snapshot().logs)));
    summary.set_xalign(0.0);
    summary.add_css_class("muted");
    summary.set_wrap(true);
    summary.set_hexpand(true);

    let summary_row = GtkBox::new(Orientation::Horizontal, 8);
    summary_row.set_halign(Align::Fill);
    summary_row.set_hexpand(true);
    summary_row.set_valign(Align::Center);

    let view_host = unsafe { host.as_ref() }.map(HostApi::from_raw);
    let follow = operation_follow_button();
    follow.set_active(true);
    let refresh = view_host
        .as_ref()
        .and_then(|host| {
            host_toolbar_button(host, CMD_REFRESH_LOGS, "view-refresh-symbolic", "Refresh")
        })
        .unwrap_or_else(|| Button::with_label("Refresh"));
    let clear = view_host
        .as_ref()
        .and_then(|host| host_toolbar_button(host, CMD_CLEAR_LOGS, "user-trash-symbolic", "Clear"))
        .unwrap_or_else(|| Button::with_label("Clear"));

    let buffer = TextBuffer::new(None);
    buffer.set_text(&snapshot().logs.join("\n"));
    let buffer_ref = glib::WeakRef::new();
    buffer_ref.set(Some(&buffer));
    OPERATION_BUFFERS.with(|buffers| {
        buffers.borrow_mut().push(buffer_ref);
    });
    let summary_ref = glib::WeakRef::new();
    summary_ref.set(Some(&summary));
    OPERATION_SUMMARIES.with(|labels| {
        labels.borrow_mut().push(summary_ref);
    });

    if view_host.is_none() {
        refresh.connect_clicked({
            let buffer = buffer.clone();
            let summary = summary.clone();
            move |_| {
                let snapshot = snapshot();
                buffer.set_text(&snapshot.logs.join("\n"));
                summary.set_text(&operation_summary_text(&snapshot.logs));
            }
        });
        clear.connect_clicked({
            let buffer = buffer.clone();
            let summary = summary.clone();
            move |_| {
                {
                    let mut app_state = state().lock().expect("state mutex poisoned");
                    app_state.logs.clear();
                    app_state.logs.push("Operations log cleared.".to_string());
                }
                let snapshot = snapshot();
                buffer.set_text(&snapshot.logs.join("\n"));
                summary.set_text(&operation_summary_text(&snapshot.logs));
                refresh_views();
            }
        });
    }

    let text = TextView::with_buffer(&buffer);
    text.set_editable(false);
    text.set_cursor_visible(false);
    text.set_monospace(true);
    text.set_wrap_mode(WrapMode::WordChar);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Automatic)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&text)
        .build();

    let follow_enabled = Rc::new(Cell::new(true));
    let suppress_scroll_events = Rc::new(Cell::new(false));
    follow.connect_toggled({
        let follow_enabled = follow_enabled.clone();
        let scroller = scroller.clone();
        let suppress_scroll_events = suppress_scroll_events.clone();
        move |button| {
            let enabled = button.is_active();
            follow_enabled.set(enabled);
            if enabled {
                suppress_scroll_events.set(true);
                schedule_scroll_to_bottom(&scroller, suppress_scroll_events.clone());
            }
        }
    });
    scroller.vadjustment().connect_value_changed({
        let follow = follow.clone();
        let follow_enabled = follow_enabled.clone();
        let suppress_scroll_events = suppress_scroll_events.clone();
        move |adjustment| {
            if suppress_scroll_events.get() || !follow_enabled.get() {
                return;
            }
            if adjustment_is_at_bottom(adjustment) {
                return;
            }
            follow.set_active(false);
        }
    });

    let scroller_ref = glib::WeakRef::new();
    scroller_ref.set(Some(&scroller));
    let toggle_ref = glib::WeakRef::new();
    toggle_ref.set(Some(&follow));
    OPERATION_FOLLOWERS.with(|followers| {
        followers.borrow_mut().push(OperationFollowHandle {
            scroller: scroller_ref,
            toggle: toggle_ref,
            follow_enabled: follow_enabled.clone(),
            suppress_scroll_events: suppress_scroll_events.clone(),
        });
    });

    summary_row.append(&summary);
    summary_row.append(&follow);
    summary_row.append(&refresh);
    summary_row.append(&clear);
    root.append(&summary_row);
    root.append(&scroller);

    suppress_scroll_events.set(true);
    schedule_scroll_to_bottom(&scroller, suppress_scroll_events);

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn operation_summary_text(logs: &[String]) -> String {
    let total = logs.len();
    let starts = logs.iter().filter(|line| line.contains("[START]")).count();
    let ok = logs.iter().filter(|line| line.contains("[OK]")).count();
    let skipped = logs.iter().filter(|line| line.contains("[SKIP]")).count();
    let failed = logs.iter().filter(|line| line.contains("[FAIL]")).count();
    let latest_failure = logs
        .iter()
        .rev()
        .find(|line| line.contains("[FAIL]"))
        .map(String::as_str);
    let latest = logs
        .last()
        .map(String::as_str)
        .unwrap_or("No operations recorded yet.");

    let mut summary = match latest_failure {
        Some(failure) => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest failure: {failure} | Latest: {latest}"
        ),
        None => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest: {latest}"
        ),
    };
    if let Some(runtime_line) = runtime_profile_summary("operations") {
        summary.push_str(" | ");
        summary.push_str(&runtime_line);
    }
    summary
}

fn load_manifest_if_present(path: &Path) -> Option<WorkspaceManifest> {
    if !path.exists() {
        return None;
    }
    let mut manifest = load_manifest(path).ok()?;
    if ensure_commit_check_rules_initialized(&mut manifest) {
        match save_manifest(path, &manifest) {
            Ok(()) => append_log(format!(
                "Initialized default commit check rules in {}.",
                path.display()
            )),
            Err(error) => append_log(format!(
                "Failed to save default commit check rules to {}: {error}",
                path.display()
            )),
        }
    }
    Some(manifest)
}

fn normalized_watch_path(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    };

    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn workspace_name_from_root(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("Workspace")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ronomepo_core::{RepositoryState, RepositorySync};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::UNIX_EPOCH;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn temp_test_dir(label: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("ronomepo-plugin-{label}-{id}"));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn descriptor_uses_expected_plugin_id() {
        let descriptor = RonomepoPlugin::descriptor();
        assert_eq!(descriptor.id, PLUGIN_ID);
        assert_eq!(descriptor.dependencies.len(), 1);
        assert_eq!(descriptor.dependencies[0].plugin_id, "maruzzella.base");
    }

    #[test]
    fn fetch_jitter_is_stable_and_bounded() {
        let first = repo_fetch_jitter_secs("repo-a");
        let second = repo_fetch_jitter_secs("repo-a");
        let other = repo_fetch_jitter_secs("repo-b");
        assert_eq!(first, second);
        assert!(first <= REMOTE_FETCH_JITTER_SECS);
        assert!(other <= REMOTE_FETCH_JITTER_SECS);
    }

    #[test]
    fn retry_due_at_is_earlier_than_full_interval() {
        let now = UNIX_EPOCH + Duration::from_secs(1_000);
        let retry = retry_remote_fetch_due_at(now, "repo-a");
        let full = next_remote_fetch_due_at(now, "repo-a");
        assert!(retry < full);
    }

    #[test]
    fn watch_filter_ignores_git_objects_and_editor_noise() {
        assert!(!watch_path_is_relevant(Path::new(".git/objects/ab/cd")));
        assert!(!watch_path_is_relevant(Path::new("src/lib.rs.swp")));
        assert!(watch_path_is_relevant(Path::new(".git/HEAD")));
        assert!(watch_path_is_relevant(Path::new("src/lib.rs")));
    }

    #[test]
    fn workspace_watch_matches_root_git_events() {
        let workspace_root = temp_test_dir("workspace-watch-root-git");
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root.clone(),
            repos: Vec::new(),
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        assert!(workspace_watch_path_matches(
            &manifest,
            &workspace_root.join(".git/refs/heads/main")
        ));
        assert!(workspace_watch_path_matches(
            &manifest,
            &workspace_root.join(".git/HEAD")
        ));
        assert!(!workspace_watch_path_matches(
            &manifest,
            &workspace_root.join(".git/objects/ab/cd")
        ));
    }

    #[test]
    fn workspace_watch_matches_top_level_workspace_files_only() {
        let workspace_root = temp_test_dir("workspace-watch-root-files");
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root.clone(),
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:org/alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        assert!(workspace_watch_path_matches(
            &manifest,
            &workspace_root.join("Cargo.toml")
        ));
        assert!(!workspace_watch_path_matches(
            &manifest,
            &workspace_root.join("alpha/src/lib.rs")
        ));
    }

    #[test]
    fn relative_manifest_root_still_matches_absolute_watch_paths() {
        let cwd = std::env::current_dir().unwrap();
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: PathBuf::from("../"),
            repos: vec![RepositoryEntry {
                id: "maruzzella".to_string(),
                name: "maruzzella".to_string(),
                dir_name: "maruzzella".to_string(),
                remote_url: "git@example.com:org/maruzzella.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let repo_event_path = cwd.join("../maruzzella/.git/refs/heads/main");
        assert_eq!(
            repo_id_for_watch_path(&manifest, &repo_event_path).as_deref(),
            Some("maruzzella")
        );
    }

    #[test]
    fn repo_refresh_completion_requeues_if_new_changes_arrived_mid_scan() {
        let repo_path = temp_test_dir("refresh-race");
        let repo_id = "alpha";
        let mut app_state = AppState::default();
        app_state.repository_items.push(RepositoryListItem {
            id: repo_id.to_string(),
            name: "alpha".to_string(),
            dir_name: "alpha".to_string(),
            remote_url: "git@example.com:org/alpha.git".to_string(),
            status: RepositoryStatus {
                state: RepositoryState::Clean,
                branch: Some("main".to_string()),
                sync: RepositorySync::UpToDate,
                repo_path: repo_path.clone(),
            },
            repo_manifest: Some(RepoManifestScan {
                path: repo_path.join("ronomepo.repo.json"),
                state: RepoManifestScanState::Missing,
            }),
        });
        app_state.repo_runtime.insert(
            repo_id.to_string(),
            RepoRuntimeState {
                invalidation_seq: 2,
                scheduled_scan_seq: 1,
                last_scanned_seq: 0,
                local_refresh_in_flight: true,
                remote_fetch_in_flight: false,
                last_local_scan_at: None,
                last_fetch_at: None,
                next_fetch_due_at: UNIX_EPOCH,
            },
        );

        let follow_up = finalize_repo_status_refresh(&mut app_state, repo_id);
        let runtime = app_state.repo_runtime.get(repo_id).unwrap();

        assert_eq!(follow_up, Some(repo_path));
        assert!(runtime.local_refresh_in_flight);
        assert_eq!(runtime.last_scanned_seq, 1);
        assert_eq!(runtime.scheduled_scan_seq, 2);
        assert!(runtime.needs_rescan());
    }

    #[test]
    fn sync_repo_runtime_state_prunes_removed_repo_entries() {
        let workspace_root = temp_test_dir("runtime-prune");
        let mut app_state = AppState::default();
        app_state.manifest = Some(WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root,
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:org/alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        });
        app_state.repo_runtime.insert(
            "alpha".to_string(),
            RepoRuntimeState::new(UNIX_EPOCH, "alpha"),
        );
        app_state.repo_runtime.insert(
            "beta".to_string(),
            RepoRuntimeState::new(UNIX_EPOCH, "beta"),
        );
        app_state.repo_details_cache.insert(
            "beta".to_string(),
            RepositoryDetails {
                remotes: vec!["origin".to_string()],
                last_commit: None,
                changed_files: Vec::new(),
            },
        );
        app_state.repo_details_loading.insert("beta".to_string());

        sync_repo_runtime_state(&mut app_state);

        assert!(app_state.repo_runtime.contains_key("alpha"));
        assert!(!app_state.repo_runtime.contains_key("beta"));
        assert!(!app_state.repo_details_cache.contains_key("beta"));
        assert!(!app_state.repo_details_loading.contains("beta"));
    }

    #[test]
    fn mark_repo_stale_only_reports_first_transition_to_stale() {
        let mut app_state = AppState::default();
        app_state.repo_runtime.insert(
            "alpha".to_string(),
            RepoRuntimeState::new(UNIX_EPOCH, "alpha"),
        );

        assert!(mark_repo_stale(&mut app_state, "alpha"));
        assert!(!mark_repo_stale(&mut app_state, "alpha"));
        assert!(app_state.repo_runtime["alpha"].needs_rescan());
    }

    #[test]
    fn validate_new_repository_entry_rejects_manifest_duplicates() {
        let workspace_root = temp_test_dir("manifest-duplicates");
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root,
            repos: vec![RepositoryEntry {
                id: "alpha".to_string(),
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:org/alpha.git".to_string(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let remote_error =
            validate_new_repository_entry(&manifest, "git@example.com:org/alpha.git", "beta")
                .unwrap_err();
        assert!(remote_error.contains("already exists in the manifest"));

        let dir_error =
            validate_new_repository_entry(&manifest, "git@example.com:org/beta.git", "alpha")
                .unwrap_err();
        assert!(dir_error.contains("already exists in the manifest"));
    }

    #[test]
    fn validate_new_repository_entry_rejects_existing_local_directory() {
        let workspace_root = temp_test_dir("existing-dir");
        fs::create_dir_all(workspace_root.join("alpha")).unwrap();
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root,
            repos: Vec::new(),
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        let error =
            validate_new_repository_entry(&manifest, "git@example.com:org/alpha.git", "alpha")
                .unwrap_err();
        assert!(error.contains("already exists locally"));
    }

    #[test]
    fn save_workspace_manifest_preserves_add_dialog_selection_and_clone_flags() {
        let workspace_root = temp_test_dir("save-manifest");
        let result = save_workspace_manifest_from_inputs(
            7,
            "Workspace",
            workspace_root.to_str().unwrap(),
            "",
            &[RepoEditorRowInput {
                enabled: true,
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:org/alpha.git".to_string(),
            }],
            Some("alpha".to_string()),
            true,
        )
        .unwrap();

        assert_eq!(result.selected_repo_id.as_deref(), Some("alpha"));
        assert!(result.clone_after_save);
        assert_eq!(result.manifest.repos.len(), 1);
        assert_eq!(result.manifest.repos[0].id, "alpha");
        assert_eq!(result.manifest.repos[0].name, "alpha");
    }

    #[test]
    fn save_workspace_manifest_adds_repo_directories_to_gitignore() {
        let workspace_root = temp_test_dir("save-manifest-gitignore");
        fs::write(workspace_root.join(".gitignore"), "hooks/\n/alpha/\n").unwrap();

        save_workspace_manifest_from_inputs(
            7,
            "Workspace",
            workspace_root.to_str().unwrap(),
            "",
            &[
                RepoEditorRowInput {
                    enabled: true,
                    name: "alpha".to_string(),
                    dir_name: "alpha".to_string(),
                    remote_url: "git@example.com:org/alpha.git".to_string(),
                },
                RepoEditorRowInput {
                    enabled: true,
                    name: "beta".to_string(),
                    dir_name: "beta".to_string(),
                    remote_url: "git@example.com:org/beta.git".to_string(),
                },
            ],
            None,
            false,
        )
        .unwrap();

        let gitignore = fs::read_to_string(workspace_root.join(".gitignore")).unwrap();
        assert!(gitignore.contains("hooks/\n"));
        assert_eq!(gitignore.matches("/alpha/\n").count(), 1);
        assert_eq!(gitignore.matches("/beta/\n").count(), 1);
    }

    #[test]
    fn save_workspace_manifest_creates_gitignore_when_missing() {
        let workspace_root = temp_test_dir("save-manifest-gitignore-create");

        save_workspace_manifest_from_inputs(
            7,
            "Workspace",
            workspace_root.to_str().unwrap(),
            "",
            &[RepoEditorRowInput {
                enabled: true,
                name: "alpha".to_string(),
                dir_name: "alpha".to_string(),
                remote_url: "git@example.com:org/alpha.git".to_string(),
            }],
            None,
            false,
        )
        .unwrap();

        let gitignore = fs::read_to_string(workspace_root.join(".gitignore")).unwrap();
        assert_eq!(gitignore, "/alpha/\n");
    }
}

export_plugin!(RonomepoPlugin);
