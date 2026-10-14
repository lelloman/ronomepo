mod diag;
#[cfg(feature = "embedded-terminal")]
mod terminal_theme;

use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write as _;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use gtk::gdk::Rectangle;
use gtk::gio;
use gtk::glib::{
    self,
    translate::{FromGlibPtrFull, IntoGlibPtr},
    BoxedAnyObject,
};
use gtk::pango::EllipsizeMode;
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, CheckButton, CustomFilter, CustomSorter, DropDown, Entry,
    EventControllerMotion, FilterChange, GestureClick, Image, Label, ListBox, ListBoxRow,
    Orientation, Paned, PolicyType, Popover, PositionType, ScrolledWindow, SelectionMode,
    SortListModel, SorterChange, Stack, TextBuffer, TextView, ToggleButton, Window, WrapMode,
};
use maruzzella_sdk::ui;
use maruzzella_sdk::{
    attach_text_tooltip, export_plugin, input_css_class, CommandSpec, HostApi, MzLogLevel,
    MzStatusCode, MzToolbarDisplayMode, MzViewOpenDisposition, MzViewPlacement,
    MzViewTeardownDecision, MzViewTeardownRequest, MzViewTeardownResult, OpenViewRequest, Plugin,
    PluginDependency, PluginDescriptor, SurfaceContributionSpec, ToolbarWidgetSpec, Version,
    ViewFactorySpec,
};
use notify::event::{AccessKind, AccessMode, CreateKind, ModifyKind};
use notify::{
    Config as NotifyConfig, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher,
};
use ronomepo_core::{
    assess_repo_capability_policy, attention_rank, build_repository_list, capability_due_reason,
    capability_next_due_at_epoch_secs, collect_capability_git_snapshot,
    collect_commit_check_report, collect_repository_details, collect_workspace_line_stats,
    current_epoch_secs, default_capability_registry_path, default_manifest_path,
    default_repo_manifest_path, derive_attention_signals, ensure_commit_check_rules_initialized,
    format_sync_label, list_repo_artifacts, load_capability_registry, load_fucina_policy,
    load_manifest, load_repo_manifest, normalize_workspace_root, parse_capability_result_json,
    plan_capability_action, plan_repo_action, run_fucina_policy, run_workspace_operation,
    save_capability_registry, save_manifest, save_repo_manifest, scan_repo_manifest,
    text_capability_result, upsert_capability_state, verify_repo_dependencies_freshness,
    workspace_summary, AttentionImpact, AttentionKind, AttentionLevel, AttentionSignal,
    AttentionSource, AttentionUrgency, CapabilityFinding, CapabilityFindingSeverity,
    CapabilityRegistry, CapabilityResult, CapabilityResultStatus, CapabilityState, CommitCheckRule,
    CommitCheckRuleEffect, CommitCheckRuleMatcher, CommitCheckRuleScope, FucinaPolicyAction,
    FucinaPolicyCommandResult, FucinaRepositoryPolicy, OperationEvent, OperationEventKind,
    OperationKind, PlannedCommand, RepoActionExecutor, RepoCapabilityDeclaration, RepoManifest,
    RepoManifestScan, RepoManifestScanState, RepositoryDetails, RepositoryListItem,
    RepositoryStatus, StandardActionName, WorkspaceManifest, MANIFEST_FILE_NAME,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "embedded-terminal")]
use vte4::prelude::*;

const PLUGIN_ID: &str = "com.lelloman.ronomepo";
const VIEW_REPO_MONITOR: &str = "com.lelloman.ronomepo.repo_monitor";
const VIEW_MONOREPO_OVERVIEW: &str = "com.lelloman.ronomepo.monorepo_overview";
const VIEW_REPO_OVERVIEW: &str = "com.lelloman.ronomepo.repo_overview";
const VIEW_REPO_MANIFEST_EDITOR: &str = "com.lelloman.ronomepo.repo_manifest_editor";
const VIEW_COMMIT_CHECK: &str = "com.lelloman.ronomepo.commit_check";
const VIEW_FUCINA_POLICY: &str = "com.lelloman.ronomepo.fucina_policy";
const VIEW_TEXT_EDITOR: &str = "com.lelloman.ronomepo.text_editor";
const VIEW_OPERATIONS: &str = "com.lelloman.ronomepo.operations";

fn mark_clickable<W: IsA<gtk::Widget>>(widget: &W) {
    update_clickable_cursor(widget);
    widget.connect_sensitive_notify(|widget| {
        update_clickable_cursor(widget);
    });
}

fn update_clickable_cursor<W: IsA<gtk::Widget>>(widget: &W) {
    if widget.is_sensitive() {
        widget.set_cursor_from_name(Some("pointer"));
    } else {
        widget.set_cursor_from_name(None);
    }
}

fn mark_clickable_list_rows(list: &ListBox) {
    let motion = EventControllerMotion::new();
    motion.connect_motion({
        let list = list.clone();
        move |_, _x, y| {
            if list.is_sensitive() && list.row_at_y(y as i32).is_some() {
                list.set_cursor_from_name(Some("pointer"));
            } else {
                list.set_cursor_from_name(None);
            }
        }
    });
    motion.connect_leave({
        let list = list.clone();
        move |_| {
            list.set_cursor_from_name(None);
        }
    });
    list.add_controller(motion);

    list.connect_sensitive_notify(|list| {
        if !list.is_sensitive() {
            list.set_cursor_from_name(None);
        }
    });
}

fn clickable_button() -> Button {
    let button = Button::new();
    mark_clickable(&button);
    button
}

fn clickable_check_button_with_label(label: impl AsRef<str>) -> CheckButton {
    let button = CheckButton::with_label(label.as_ref());
    mark_clickable(&button);
    button
}

fn clickable_dropdown_from_strings(strings: &[&str]) -> DropDown {
    let dropdown = DropDown::from_strings(strings);
    mark_clickable(&dropdown);
    dropdown
}
const CMD_REFRESH: &str = "ronomepo.workspace.refresh";
const CMD_PULL: &str = "ronomepo.workspace.pull";
const CMD_PUSH: &str = "ronomepo.workspace.push";
const CMD_OPEN_OVERVIEW: &str = "ronomepo.workspace.open_overview";
const CMD_OPEN_COMMIT_CHECK: &str = "ronomepo.workspace.open_commit_check";
const CMD_FILTER: &str = "ronomepo.workspace.filter";
const CMD_OPEN_FUCINA_POLICY: &str = "ronomepo.workspace.open_fucina_policy";
const CMD_EXIT: &str = "ronomepo.workspace.exit";
const CMD_REFRESH_LOGS: &str = "ronomepo.logs.refresh";
const CMD_CLEAR_LOGS: &str = "ronomepo.logs.clear";
// Fixed monitor column widths in pixels; the name (and the remote, in the
// Manifest view) column takes the remaining width.
const MONITOR_BRANCH_COL_WIDTH: i32 = 112;
const MONITOR_MANIFEST_COL_WIDTH: i32 = 96;
const MONITOR_STATE_COL_WIDTH: i32 = 80;
const MONITOR_SYNC_COL_WIDTH: i32 = 76;
const MONITOR_CAPABILITY_COL_WIDTH: i32 = 72;
const MONITOR_ATTENTION_COL_WIDTH: i32 = 76;
const MONITOR_ACTIONS_COL_WIDTH: i32 = 60;
const MONITOR_ENABLED_COL_WIDTH: i32 = 72;
const WORKER_POOL_SIZE: usize = 4;
const LOCAL_RESCAN_INTERVAL_SECS: u32 = 5 * 60;
const REMOTE_FETCH_TICK_SECS: u32 = 30;
const REMOTE_FETCH_INTERVAL_SECS: u64 = 60 * 60;
const REMOTE_FETCH_JITTER_SECS: u64 = 30 * 60;
const REMOTE_FETCH_CONCURRENCY: usize = 3;
const WATCH_POLL_FALLBACK_SECS: u64 = 15;
const UI_REFRESH_DEBOUNCE_MILLIS: u64 = 75;
const CONFIG_PERSIST_DEBOUNCE_MILLIS: u64 = 300;
const MAX_LOG_ENTRIES: usize = 500;
const RUNTIME_PROFILE_ENV: &str = "RONOMEPO_PROFILE";

pub struct RonomepoPlugin;

// Older configs also carry `last_workspace_path` and `import_banner_dismissed`;
// neither was ever used, and serde ignores the unknown keys.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(from = "StoredPluginConfig")]
struct RonomepoPluginConfig {
    monitor_view_mode: MonitorViewMode,
    /// Per-view filter and sort settings.
    monitor_views: MonitorSettings,
}

/// The config as stored, including the flat monitor fields written before
/// the per-view settings were grouped. Loading migrates them, so saving a
/// loaded config never drops them. Unknown keys are ignored.
#[derive(Deserialize)]
struct StoredPluginConfig {
    #[serde(default)]
    monitor_view_mode: MonitorViewMode,
    #[serde(default)]
    monitor_views: Option<MonitorSettings>,
    #[serde(flatten)]
    legacy_monitor: LegacyMonitorConfig,
}

impl From<StoredPluginConfig> for RonomepoPluginConfig {
    fn from(stored: StoredPluginConfig) -> Self {
        Self {
            monitor_view_mode: stored.monitor_view_mode,
            monitor_views: stored
                .monitor_views
                .unwrap_or_else(|| stored.legacy_monitor.to_settings()),
        }
    }
}

/// Monitor settings as older versions stored them, read only for migration.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct LegacyMonitorConfig {
    monitor_filter_mode: MonitorFilterMode,
    monitor_sort_mode: MonitorSortMode,
    monitor_sort_descending: bool,
    overview_filter_mode: MonitorFilterMode,
    git_filter_mode: MonitorFilterMode,
    checks_filter_mode: MonitorFilterMode,
    manifest_filter_mode: MonitorFilterMode,
    overview_sort_mode: MonitorSortMode,
    git_sort_mode: MonitorSortMode,
    checks_sort_mode: MonitorSortMode,
    manifest_sort_mode: MonitorSortMode,
    overview_sort_descending: bool,
    git_sort_descending: bool,
    checks_sort_descending: bool,
    manifest_sort_descending: bool,
}

impl RonomepoPluginConfig {
    fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        if bytes.is_empty() {
            Ok(Self::default())
        } else {
            serde_json::from_slice::<Self>(bytes)
        }
    }
}

impl LegacyMonitorConfig {
    fn to_settings(&self) -> MonitorSettings {
        // The oldest configs had a single monitor_* setting, which applies to
        // the overview when no overview-specific value was stored.
        let overview_filter = if self.overview_filter_mode == MonitorFilterMode::All {
            match self.monitor_filter_mode {
                MonitorFilterMode::Dirty
                | MonitorFilterMode::ToSync
                | MonitorFilterMode::Issues => MonitorFilterMode::Issues,
                _ => MonitorFilterMode::All,
            }
        } else {
            self.overview_filter_mode
        };
        let overview_sort = if self.overview_sort_mode == MonitorSortMode::Name {
            self.monitor_sort_mode
        } else {
            self.overview_sort_mode
        };
        MonitorSettings {
            overview: MonitorViewSettings {
                filter: overview_filter,
                sort: overview_sort,
                descending: self.monitor_sort_descending || self.overview_sort_descending,
            },
            git: MonitorViewSettings {
                filter: self.git_filter_mode,
                sort: self.git_sort_mode,
                descending: self.git_sort_descending,
            },
            checks: MonitorViewSettings {
                filter: self.checks_filter_mode,
                sort: self.checks_sort_mode,
                descending: self.checks_sort_descending,
            },
            manifest: MonitorViewSettings {
                filter: self.manifest_filter_mode,
                sort: self.manifest_sort_mode,
                descending: self.manifest_sort_descending,
            },
        }
    }
}

/// Filter and sort settings of one monitor view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct MonitorViewSettings {
    filter: MonitorFilterMode,
    sort: MonitorSortMode,
    descending: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
struct MonitorSettings {
    overview: MonitorViewSettings,
    git: MonitorViewSettings,
    checks: MonitorViewSettings,
    manifest: MonitorViewSettings,
}

impl MonitorSettings {
    fn view(&self, view: MonitorViewMode) -> &MonitorViewSettings {
        match view {
            MonitorViewMode::Overview => &self.overview,
            MonitorViewMode::Git => &self.git,
            MonitorViewMode::Checks => &self.checks,
            MonitorViewMode::Manifest => &self.manifest,
        }
    }

    fn view_mut(&mut self, view: MonitorViewMode) -> &mut MonitorViewSettings {
        match view {
            MonitorViewMode::Overview => &mut self.overview,
            MonitorViewMode::Git => &mut self.git,
            MonitorViewMode::Checks => &mut self.checks,
            MonitorViewMode::Manifest => &mut self.manifest,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MonitorFilterMode {
    #[default]
    All,
    Dirty,
    ToSync,
    Issues,
    NeedsAttention,
    Healthy,
    Diverged,
    Clean,
    Due,
    Failing,
    MissingManifest,
    NoChecks,
    Enabled,
    Disabled,
    HasActions,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MonitorSortMode {
    #[default]
    Name,
    SyncState,
    RecentActivity,
    Branch,
    Attention,
    CheckState,
    Manifest,
    Actions,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MonitorViewMode {
    #[default]
    Overview,
    Git,
    Checks,
    Manifest,
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
    monitor_view_mode: MonitorViewMode,
    monitor_settings: MonitorSettings,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    last_log_message: Option<String>,
    last_log_repeats: usize,
    last_watch_failure: Option<String>,
    last_unreadable_watch_paths: Vec<PathBuf>,
    next_operation_batch: usize,
    history_report: Vec<String>,
    history_report_loading: bool,
    line_stats_report: Vec<String>,
    line_stats_loading: bool,
    /// Line stats were requested again while a run was in flight; that run's
    /// result is superseded and the report is recomputed when it finishes.
    line_stats_rerun: bool,
    line_stats_since: String,
    repo_runtime: HashMap<String, RepoRuntimeState>,
    watch_manager_sync_in_flight: bool,
    watch_manager_sync_pending: bool,
    /// Whether any sync collapsed into `watch_manager_sync_pending` was forced.
    watch_manager_sync_pending_force: bool,
    /// Source of the installed watch manager, so checking whether a sync is a
    /// no-op does not need the watch manager lock.
    watch_source: Option<WatchSource>,
    /// New directories seen while a watch rebuild runs, registered on the new
    /// manager once it is installed.
    deferred_watch_dirs: Vec<PathBuf>,
    /// The Pull/Push/... batch currently running; only one runs at a time.
    operation_in_flight: Option<OperationBatchState>,
    /// Each repo's invalidation_seq when the running full workspace scan was
    /// submitted; repos invalidated since then must not take its results.
    workspace_scan_seqs: HashMap<String, u64>,
    /// Sequence number one past the newest log line; see `LogCursor`.
    log_end_seq: u64,
    log_rewrites: u64,
    /// Parsed `.ronomepo/capability-state.json`, re-read only when capability
    /// checks write it or a workspace scan completes.
    capability_registry: Arc<CapabilityRegistry>,
}

#[derive(Clone, Debug)]
struct OperationBatchState {
    batch_id: usize,
    operation: &'static str,
    /// Per-repo failures, reported together once the batch completes.
    failures: Vec<OperationFailure>,
}

#[derive(Clone, Debug)]
struct OperationFailure {
    repository_name: Option<String>,
    message: String,
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
            monitor_view_mode: MonitorViewMode::default(),
            monitor_settings: MonitorSettings::default(),
            selected_repo_ids: Vec::new(),
            active_repo_id: None,
            logs: Vec::new(),
            last_log_message: None,
            last_log_repeats: 0,
            last_watch_failure: None,
            last_unreadable_watch_paths: Vec::new(),
            next_operation_batch: 0,
            history_report: Vec::new(),
            history_report_loading: false,
            line_stats_report: Vec::new(),
            line_stats_loading: false,
            line_stats_rerun: false,
            line_stats_since: String::new(),
            repo_runtime: HashMap::new(),
            watch_manager_sync_in_flight: false,
            watch_manager_sync_pending: false,
            watch_manager_sync_pending_force: false,
            watch_source: None,
            deferred_watch_dirs: Vec::new(),
            operation_in_flight: None,
            workspace_scan_seqs: HashMap::new(),
            log_end_seq: 0,
            log_rewrites: 0,
            capability_registry: Arc::new(CapabilityRegistry::default()),
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
    backend: WatchBackend,
    backend_kind: WatchBackendKind,
    /// Number of directories registered (each one non-recursively).
    watched_paths: usize,
    unreadable_paths: Vec<PathBuf>,
    source: WatchSource,
    /// Git-ignored directories found so far; never watched, nor anything below them.
    ignored_dirs: HashSet<PathBuf>,
}

/// What a watch manager was built from. A sync whose source matches the
/// current manager's is a no-op, so scans and operations do not re-register
/// every directory in the workspace.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WatchSource {
    workspace_root: PathBuf,
    workspace_git_dir: Option<PathBuf>,
    repo_roots: Vec<PathBuf>,
}

#[derive(Default)]
struct PendingWatchEvents {
    // Paths are routed to their repo on arrival, so the pending batch is bounded
    // by the repo count no matter how many files a build touches.
    workspace_touched: bool,
    repo_ids: HashSet<String>,
    flush_scheduled: bool,
    backend_rescan: bool,
    /// Directories created (or moved in) under a watched directory. Watches are
    /// per directory, so these need registering before their contents report.
    new_dirs: HashSet<PathBuf>,
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
    LineStats,
    CapabilityChecks,
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
    WatchNewDirs {
        dirs: Vec<PathBuf>,
    },
    RefreshWorkspace {
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
    CapabilityChecks {
        workspace_root: PathBuf,
        targets: Vec<CapabilityRunTarget>,
        run_reason: String,
    },
    RepoManifestActions {
        action: StandardActionName,
        targets: Vec<(RepositoryListItem, Arc<RepoManifest>)>,
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
    HistoryReportCompleted {
        result: Result<HistoryReportResult, String>,
    },
    LineStatsCompleted {
        result: Result<LineStatsResult, String>,
    },
    CapabilityChecksCompleted {
        result: Result<CapabilityRunReport, String>,
        repo_ids: Vec<String>,
    },
    OperationBatchCompleted {
        batch_id: usize,
    },
    JobPanicked {
        recovery: JobRecovery,
        message: String,
    },
}

/// State to reset on the main thread for a job that panicked and has no
/// failure result of its own.
enum JobRecovery {
    OperationBatch {
        batch_id: usize,
        operation: &'static str,
    },
    WorkspaceScan,
    RepoStatus(String),
    RepoDetails(String),
}

struct RefreshWorkspaceResult {
    workspace_root: PathBuf,
    manifest_path: PathBuf,
    manifest: Option<WorkspaceManifest>,
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

#[derive(Clone)]
struct CapabilityRunTarget {
    repo_id: String,
    repo_name: String,
    repo_path: PathBuf,
    manifest: Arc<RepoManifest>,
    capability_ids: Option<Vec<String>>,
}

struct CapabilityRunReport {
    lines: Vec<String>,
    message: String,
}

#[derive(Clone)]
struct RepoManifestItemRowHandle {
    row: GtkBox,
    id: Entry,
    item_type: Entry,
    path: Entry,
    config: Option<Value>,
    artifacts: Vec<ronomepo_core::RepoArtifactDefinition>,
    actions: Vec<ronomepo_core::RepoActionCommand>,
}

#[derive(Clone)]
struct RepoManifestActionRowHandle {
    row: GtkBox,
    id: Entry,
    command: Entry,
    workdir: Entry,
    timeout_seconds: Entry,
    output: DropDown,
    env: BTreeMap<String, String>,
}

#[derive(Clone)]
struct RepoManifestCapabilityRowHandle {
    row: GtkBox,
    id: Entry,
    capability: Entry,
    status: DropDown,
    item_id: Entry,
    root: Entry,
    action_kind: DropDown,
    action_value: Entry,
    schedule_kind: DropDown,
    interval_seconds: Entry,
    reason: Entry,
    preserved_action_ref: Option<ronomepo_core::RepoActionRef>,
    preserved_standard_action: Option<String>,
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

struct RepositoryViewHandle {
    list: glib::WeakRef<ListBox>,
    controls: MonitorControls,
    store: gio::ListStore,
    filter_model: gtk::FilterListModel,
    filter: CustomFilter,
    sorter: CustomSorter,
    /// Filter and sort settings read by `filter` and `sorter`.
    query: Rc<RefCell<MonitorQuery>>,
    /// Mirror of the rows in `store`, used to update only rows that changed.
    rows: RefCell<Vec<MonitorRow>>,
    /// Whether the empty placeholder currently shown was built for a loaded
    /// manifest; `None` while the list has rows.
    placeholder_for_manifest: Cell<Option<bool>>,
}

/// A Fucina Policy view. The policy part is rebuilt only when repos.toml
/// changes; the action status label lives outside it so running actions keep
/// reporting into a live label.
struct FucinaPolicyViewHandle {
    root: glib::WeakRef<GtkBox>,
    policy_area: GtkBox,
    repository_area: GtkBox,
    action_status: Label,
    rendered_source: RefCell<Option<FucinaPolicySource>>,
}

/// What the rendered policy was read from.
#[derive(Clone, Debug, PartialEq, Eq)]
struct FucinaPolicySource {
    policy_root: PathBuf,
    modified: Option<SystemTime>,
}

/// A Monorepo Overview view. Its chrome and the report controls (including
/// the line-stats date entry) are built once; refreshes re-render only the
/// slots, and only while the view is on screen.
struct MonorepoOverview {
    root: glib::WeakRef<GtkBox>,
    top: GtkBox,
    selection_slot: GtkBox,
    files_slot: GtkBox,
    sections: GtkBox,
    reports: MonorepoReportControls,
    host_ptr: usize,
    /// Set when a refresh was skipped because the view was not mapped.
    stale: Cell<bool>,
}

struct MonorepoReportControls {
    row: GtkBox,
    check_history: Button,
    since_entry: Entry,
    line_stats: Button,
    all_time: Button,
}

struct CommitCheckViewHandle {
    root: glib::WeakRef<GtkBox>,
    editor: Rc<CommitCheckEditor>,
}

/// The rule editor of a Commit Check view. It is built once; refreshes only
/// touch the rows when the manifest's rules change, and never discard edits.
struct CommitCheckEditor {
    rules_box: GtkBox,
    rule_rows: Rc<RefCell<Vec<CommitCheckRuleRowHandle>>>,
    status: Label,
    /// Manifest rules the rows were last synced with.
    baseline: RefCell<Vec<CommitCheckRule>>,
}

/// A Repo Overview view. Only the visible tab of its stack is rendered on a
/// refresh; the other tabs are marked stale and rendered when shown.
struct RepoOverviewView {
    stack: glib::WeakRef<Stack>,
    pages: Vec<(RepoOverviewPage, GtkBox)>,
    stale: RefCell<HashSet<RepoOverviewPage>>,
    terminal_root: GtkBox,
    instance_key: Option<String>,
    host_ptr: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RepoOverviewPage {
    Summary,
    Git,
    Manifest,
    Activity,
}

impl RepoOverviewPage {
    const ALL: [RepoOverviewPage; 4] = [
        RepoOverviewPage::Summary,
        RepoOverviewPage::Git,
        RepoOverviewPage::Manifest,
        RepoOverviewPage::Activity,
    ];

    fn name(self) -> &'static str {
        match self {
            RepoOverviewPage::Summary => "summary",
            RepoOverviewPage::Git => "git",
            RepoOverviewPage::Manifest => "manifest",
            RepoOverviewPage::Activity => "activity",
        }
    }

    fn title(self) -> &'static str {
        match self {
            RepoOverviewPage::Summary => "Summary",
            RepoOverviewPage::Git => "Git",
            RepoOverviewPage::Manifest => "Manifest",
            RepoOverviewPage::Activity => "Activity",
        }
    }
}

/// Identifies the log contents: `logs[i]` has sequence number
/// `end_seq - logs.len() + i`, and `rewrites` counts in-place updates of the
/// last line (repeat collapsing).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LogCursor {
    first_seq: u64,
    end_seq: u64,
    rewrites: u64,
}

/// What a log view showing `shown` must do to show the log at a newer cursor.
#[derive(Debug, PartialEq, Eq)]
enum LogViewEdit {
    Unchanged,
    Reset,
    /// Drop `drop_front` entries from the start, keep the next `keep`
    /// entries, and replace the rest with `logs[keep..]`.
    Update {
        drop_front: usize,
        keep: usize,
    },
}

struct OperationBufferHandle {
    buffer: glib::WeakRef<TextBuffer>,
    shown: Cell<LogCursor>,
    /// Number of text lines of each shown entry (entries may span lines).
    line_counts: RefCell<std::collections::VecDeque<usize>>,
}

struct TextEditorState {
    /// The host instance key of the view ("" for a keyless view), which is
    /// what a teardown request names.
    instance_key: String,
    dirty: bool,
}

/// The live counters in the Operations header.
struct OperationSummaryHandle {
    root: glib::WeakRef<GtkBox>,
    started: Label,
    ok: Label,
    skipped: Label,
    failed: Label,
    latest: Label,
}

struct OperationFollowHandle {
    scroller: glib::WeakRef<ScrolledWindow>,
    toggle: glib::WeakRef<ToggleButton>,
    follow_enabled: Rc<Cell<bool>>,
    suppress_scroll_events: Rc<Cell<bool>>,
}

thread_local! {
    static REPOSITORY_VIEWS: RefCell<Vec<RepositoryViewHandle>> = const { RefCell::new(Vec::new()) };
    static MONOREPO_OVERVIEWS: RefCell<Vec<Rc<MonorepoOverview>>> = const { RefCell::new(Vec::new()) };
    static REPO_OVERVIEWS: RefCell<Vec<Rc<RepoOverviewView>>> = const { RefCell::new(Vec::new()) };
    static COMMIT_CHECK_VIEWS: RefCell<Vec<CommitCheckViewHandle>> = const { RefCell::new(Vec::new()) };
    static FUCINA_POLICY_VIEWS: RefCell<Vec<FucinaPolicyViewHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_BUFFERS: RefCell<Vec<OperationBufferHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_SUMMARY_CURSOR: Cell<Option<LogCursor>> = const { Cell::new(None) };
    static OPERATION_SUMMARIES: RefCell<Vec<OperationSummaryHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_FOLLOWERS: RefCell<Vec<OperationFollowHandle>> = const { RefCell::new(Vec::new()) };
    /// Pull/Push buttons, disabled while an operation batch runs. Views keep
    /// them across refreshes, so their state is synced on every refresh.
    static OPERATION_BUTTONS: RefCell<Vec<glib::WeakRef<Button>>> = const { RefCell::new(Vec::new()) };
    /// Open text editor views by view id.
    static TEXT_EDITORS: RefCell<HashMap<usize, TextEditorState>> = RefCell::new(HashMap::new());
    static HOST_APIS: RefCell<Vec<&'static maruzzella_sdk::ffi::MzHostApi>> = const { RefCell::new(Vec::new()) };
    static PENDING_CONFIG_UPDATES: RefCell<Vec<PluginConfigUpdate>> = const { RefCell::new(Vec::new()) };
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
static PENDING_WATCH_REPOS: AtomicUsize = AtomicUsize::new(0);
static PENDING_WATCH_REPOS_HIGH_WATER: AtomicUsize = AtomicUsize::new(0);
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
    let workspace_settings_views = FUCINA_POLICY_VIEWS.with(|views| views.borrow().len());
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
    let pending_watch_repos = PENDING_WATCH_REPOS.load(Ordering::Relaxed);
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
        "[runtime:{reason}] watch_backend={} watched_paths={} repo_runtime={} repo_details_cache={} repo_details_loading={} pending_watch_repos={} pending_watch_repos_high_water={} watch_sync_requests={} watch_sync_completions={} watch_sync_failures={} watch_sync_pending_collapses={} watch_events={} watch_event_flushes={} local_rescan_ticks={} remote_fetch_ticks={} dispatched_jobs={} in_flight_jobs={} view_refreshes={} log_refreshes={} repository_views={} monorepo_views={} repo_views={} commit_check_views={} workspace_settings_views={} operation_buffers={} operation_summaries={}",
        watch_backend_kind_label(current_watch_backend_kind()),
        WATCHED_PATH_COUNT.load(Ordering::Relaxed),
        app_state.repo_runtime.len(),
        app_state.repo_details_cache.len(),
        app_state.repo_details_loading.len(),
        pending_watch_repos,
        PENDING_WATCH_REPOS_HIGH_WATER.load(Ordering::Relaxed),
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
    diag::log(&line);
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
            let receiver = receiver
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let label = worker_job_label(&queued_job.job);
    let recover = worker_job_panic_recovery(&queued_job.job);
    // A panicking job must still release its key and reset the state flags that
    // were set when it was queued; otherwise that work never runs again.
    let result = match panic::catch_unwind(AssertUnwindSafe(|| execute_worker_job(queued_job.job)))
    {
        Ok(result) => result,
        Err(payload) => {
            let message = panic_message(payload.as_ref());
            report_panic(label, &message);
            recover(format!("{label} panicked: {message}"))
        }
    };

    complete_worker_job(&executor().in_flight, key, || {
        if let Some(result) = result {
            dispatch_worker_result(result);
        }
    });
}

fn worker_job_label(job: &WorkerJob) -> &'static str {
    match job {
        WorkerJob::OperationBatch { .. } => "worker-job:OperationBatch",
        WorkerJob::WorkspaceScan { .. } => "worker-job:WorkspaceScan",
        WorkerJob::WorkspaceRootStatusRefresh { .. } => "worker-job:WorkspaceRootStatusRefresh",
        WorkerJob::RepositoryStatusRefresh { .. } => "worker-job:RepositoryStatusRefresh",
        WorkerJob::RepositoryRemoteFetch { .. } => "worker-job:RepositoryRemoteFetch",
        WorkerJob::RepoDetailsLoad { .. } => "worker-job:RepoDetailsLoad",
        WorkerJob::WatchManagerSync { .. } => "worker-job:WatchManagerSync",
        WorkerJob::WatchNewDirs { .. } => "worker-job:WatchNewDirs",
        WorkerJob::RefreshWorkspace { .. } => "worker-job:RefreshWorkspace",
        WorkerJob::HistoryReport { .. } => "worker-job:HistoryReport",
        WorkerJob::LineStats { .. } => "worker-job:LineStats",
        WorkerJob::CapabilityChecks { .. } => "worker-job:CapabilityChecks",
        WorkerJob::RepoManifestActions { .. } => "worker-job:RepoManifestActions",
    }
}

/// Builds the result reported in place of a job's own when it panics, so the
/// main thread clears whatever "in flight" state the job was holding.
fn worker_job_panic_recovery(job: &WorkerJob) -> Box<dyn FnOnce(String) -> Option<WorkerResult>> {
    match job {
        WorkerJob::OperationBatch { kind, batch_id, .. } => {
            let recovery = JobRecovery::OperationBatch {
                batch_id: *batch_id,
                operation: operation_kind_title(*kind),
            };
            Box::new(move |message| Some(WorkerResult::JobPanicked { recovery, message }))
        }
        WorkerJob::WorkspaceScan { .. } => Box::new(|message| {
            Some(WorkerResult::JobPanicked {
                recovery: JobRecovery::WorkspaceScan,
                message,
            })
        }),
        WorkerJob::RepositoryStatusRefresh { item } => {
            let recovery = JobRecovery::RepoStatus(item.id.clone());
            Box::new(move |message| Some(WorkerResult::JobPanicked { recovery, message }))
        }
        WorkerJob::RepoDetailsLoad { repo_id, .. } => {
            let recovery = JobRecovery::RepoDetails(repo_id.clone());
            Box::new(move |message| Some(WorkerResult::JobPanicked { recovery, message }))
        }
        WorkerJob::RepositoryRemoteFetch {
            repo_id, repo_name, ..
        } => {
            let repo_id = repo_id.clone();
            let repo_name = repo_name.clone();
            Box::new(move |message| {
                Some(WorkerResult::RepositoryRemoteFetchCompleted {
                    repo_id,
                    repo_name,
                    result: Err(message),
                })
            })
        }
        WorkerJob::WatchManagerSync { sync_seq, .. } => {
            let sync_seq = *sync_seq;
            Box::new(move |message| {
                Some(WorkerResult::WatchManagerSyncCompleted {
                    sync_seq,
                    result: Err(message),
                })
            })
        }
        WorkerJob::RefreshWorkspace { status_sender, .. } => {
            let status_sender = status_sender.clone();
            Box::new(move |message| {
                Some(WorkerResult::RefreshWorkspaceCompleted {
                    result: Err(message),
                    status_sender,
                })
            })
        }
        WorkerJob::HistoryReport { .. } => Box::new(|message| {
            Some(WorkerResult::HistoryReportCompleted {
                result: Err(message),
            })
        }),
        WorkerJob::LineStats { .. } => Box::new(|message| {
            Some(WorkerResult::LineStatsCompleted {
                result: Err(message),
            })
        }),
        WorkerJob::CapabilityChecks { targets, .. } => {
            let repo_ids = capability_target_repo_ids(targets);
            Box::new(move |message| {
                Some(WorkerResult::CapabilityChecksCompleted {
                    result: Err(message),
                    repo_ids,
                })
            })
        }
        // Nothing is held in flight for these; the panic itself is logged.
        WorkerJob::WorkspaceRootStatusRefresh { .. }
        | WorkerJob::WatchNewDirs { .. }
        | WorkerJob::RepoManifestActions { .. } => Box::new(|_| None),
    }
}

fn execute_worker_job(job: WorkerJob) -> Option<WorkerResult> {
    match job {
        WorkerJob::OperationBatch {
            manifest,
            selected_repo_ids,
            kind,
            batch_id,
        } => {
            let main_context = glib::MainContext::default();
            // Shared by every event of the batch instead of cloned per event.
            let manifest = Arc::new(manifest);
            run_workspace_operation(&manifest, &selected_repo_ids, kind, |event| {
                let event = event.clone();
                let manifest = Arc::clone(&manifest);
                main_context.invoke(move || {
                    guard_panics("handle_operation_event", (), || {
                        handle_operation_event(batch_id, manifest, event)
                    })
                });
            });
            Some(WorkerResult::OperationBatchCompleted { batch_id })
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
            Some(WorkerResult::WorkspaceScanCompleted {
                workspace_status,
                repository_items,
            })
        }
        WorkerJob::WorkspaceRootStatusRefresh { workspace_root } => {
            let workspace_status = ronomepo_core::collect_repository_status(&workspace_root);
            Some(WorkerResult::WorkspaceRootStatusRefreshed { workspace_status })
        }
        WorkerJob::RepositoryStatusRefresh { mut item } => {
            item.status = ronomepo_core::collect_repository_status(&item.status.repo_path);
            item.repo_manifest = Some(scan_repo_manifest(&item.status.repo_path));
            Some(WorkerResult::RepositoryStatusRefreshed { item })
        }
        WorkerJob::RepositoryRemoteFetch {
            repo_id,
            repo_name,
            repo_path,
        } => {
            let result = fetch_repository_remote(&repo_path);
            Some(WorkerResult::RepositoryRemoteFetchCompleted {
                repo_id,
                repo_name,
                result,
            })
        }
        WorkerJob::RepoDetailsLoad { repo_id, repo_path } => {
            let details = collect_repository_details(&repo_path);
            Some(WorkerResult::RepoDetailsLoaded { repo_id, details })
        }
        WorkerJob::WatchManagerSync { manifest, sync_seq } => {
            let result = manifest
                .map(|manifest| build_watch_manager(&manifest))
                .transpose();
            Some(WorkerResult::WatchManagerSyncCompleted { sync_seq, result })
        }
        WorkerJob::WatchNewDirs { dirs } => {
            watch_new_dirs(dirs);
            None
        }
        WorkerJob::RefreshWorkspace {
            workspace_root,
            status_sender,
        } => {
            let result = load_workspace_manifest(&workspace_root);
            Some(WorkerResult::RefreshWorkspaceCompleted {
                result,
                status_sender,
            })
        }
        WorkerJob::HistoryReport {
            manifest,
            selected_repo_ids,
            num_commits,
        } => {
            let result = build_history_report(&manifest, &selected_repo_ids, num_commits);
            Some(WorkerResult::HistoryReportCompleted { result })
        }
        WorkerJob::LineStats {
            manifest,
            since_date,
        } => {
            let result = build_line_stats_report(&manifest, since_date.as_deref());
            Some(WorkerResult::LineStatsCompleted { result })
        }
        WorkerJob::CapabilityChecks {
            workspace_root,
            targets,
            run_reason,
        } => {
            let repo_ids = capability_target_repo_ids(&targets);
            let result = run_capability_checks(&workspace_root, targets, &run_reason);
            Some(WorkerResult::CapabilityChecksCompleted { result, repo_ids })
        }
        WorkerJob::RepoManifestActions { action, targets } => {
            run_repo_manifest_actions(action, targets);
            None
        }
    }
}

fn capability_target_repo_ids(targets: &[CapabilityRunTarget]) -> Vec<String> {
    let mut repo_ids = targets
        .iter()
        .map(|target| target.repo_id.clone())
        .collect::<Vec<_>>();
    repo_ids.sort();
    repo_ids.dedup();
    repo_ids
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Logs a caught panic. The panicking code may have held one of the global
/// locks, so clear their poison first: the caller recovers and carries on, and
/// every later `lock().expect(..)` would otherwise panic too.
fn report_panic(context: &str, message: &str) {
    state().clear_poison();
    executor().in_flight.clear_poison();
    watch_manager().clear_poison();
    pending_watch_events().clear_poison();
    diag::log(format!("PANIC in {context}: {message}"));
    append_log(format!("Internal error in {context}: {message}"));
}

/// Runs `body` and turns a panic into `fallback`. Panics must not unwind into
/// the host through `extern "C"` entry points (that aborts the process), nor
/// out of main-loop callbacks.
fn guard_panics<T>(context: &'static str, fallback: T, body: impl FnOnce() -> T) -> T {
    match panic::catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => value,
        Err(payload) => {
            report_panic(context, &panic_message(payload.as_ref()));
            fallback
        }
    }
}

fn guard_command(
    context: &'static str,
    body: impl FnOnce() -> maruzzella_sdk::ffi::MzStatus,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_panics(
        context,
        maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError),
        body,
    )
}

macro_rules! guarded_view_factory {
    ($wrapper:ident, $factory:ident) => {
        extern "C" fn $wrapper(
            host: *const maruzzella_sdk::ffi::MzHostApi,
            request: *const maruzzella_sdk::ffi::MzViewRequest,
        ) -> *mut std::ffi::c_void {
            guard_panics(stringify!($factory), std::ptr::null_mut(), || {
                $factory(host, request)
            })
        }
    };
}

guarded_view_factory!(create_repo_monitor_view_ffi, create_repo_monitor_view);
guarded_view_factory!(
    create_monorepo_overview_view_ffi,
    create_monorepo_overview_view
);
guarded_view_factory!(create_repo_overview_view_ffi, create_repo_overview_view);
guarded_view_factory!(
    create_repo_manifest_editor_view_ffi,
    create_repo_manifest_editor_view
);
guarded_view_factory!(create_commit_check_view_ffi, create_commit_check_view);
guarded_view_factory!(
    create_workspace_settings_view_ffi,
    create_workspace_settings_view
);
guarded_view_factory!(create_text_editor_view_ffi, create_text_editor_view);
guarded_view_factory!(create_operations_view_ffi, create_operations_view);

extern "C" fn prepare_text_editor_teardown_ffi(
    request: *const MzViewTeardownRequest,
) -> MzViewTeardownResult {
    guard_panics(
        "prepare_text_editor_teardown",
        MzViewTeardownResult {
            decision: MzViewTeardownDecision::Confirm,
            message: maruzzella_sdk::ffi::MzStr::from_static(
                "Could not check this text editor for unsaved changes.",
            ),
        },
        || prepare_text_editor_teardown(request),
    )
}

fn complete_worker_job(
    in_flight: &Mutex<HashSet<JobKey>>,
    key: Option<JobKey>,
    publish_result: impl FnOnce(),
) {
    // Completion may immediately schedule another job with the same key. Release
    // the key (and mutex) before making the result visible to the main thread.
    if let Some(key) = key {
        in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key);
    }
    publish_result();
}

fn dispatch_worker_result(result: WorkerResult) {
    let main_context = glib::MainContext::default();
    main_context
        .invoke(move || guard_panics("handle_worker_result", (), || handle_worker_result(result)));
}

fn empty_repository_status(repo_path: PathBuf) -> ronomepo_core::RepositoryStatus {
    ronomepo_core::RepositoryStatus {
        state: ronomepo_core::RepositoryState::Unknown,
        branch: None,
        sync: ronomepo_core::RepositorySync::Unknown,
        head_commit: None,
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

        let config = load_config(host)?;
        initialize_state(&config);
        #[cfg(feature = "embedded-terminal")]
        terminal_theme::register(host);

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
            CommandSpec::new(PLUGIN_ID, CMD_OPEN_FUCINA_POLICY, "Fucina Policy")
                .with_handler(command_open_fucina_policy),
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
            "Repository monitor, Git operations, commit checks and Fucina policy.",
        ))?;

        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_REPO_MONITOR,
            "Repository Monitor",
            MzViewPlacement::SidePanel,
            create_repo_monitor_view_ffi,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_MONOREPO_OVERVIEW,
            "Monorepo Overview",
            MzViewPlacement::Workbench,
            create_monorepo_overview_view_ffi,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_REPO_OVERVIEW,
            "Repo Overview",
            MzViewPlacement::Workbench,
            create_repo_overview_view_ffi,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_REPO_MANIFEST_EDITOR,
            "Repo Manifest Editor",
            MzViewPlacement::Workbench,
            create_repo_manifest_editor_view_ffi,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_COMMIT_CHECK,
            "Commit Check",
            MzViewPlacement::Workbench,
            create_commit_check_view_ffi,
        ))?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_FUCINA_POLICY,
            "Fucina Policy",
            MzViewPlacement::Workbench,
            create_workspace_settings_view_ffi,
        ))?;
        host.register_view_factory(
            ViewFactorySpec::new(
                PLUGIN_ID,
                VIEW_TEXT_EDITOR,
                "Text Editor",
                MzViewPlacement::Workbench,
                create_text_editor_view_ffi,
            )
            .with_prepare_teardown(prepare_text_editor_teardown_ffi),
        )?;
        host.register_view_factory(ViewFactorySpec::new(
            PLUGIN_ID,
            VIEW_OPERATIONS,
            "Operations",
            MzViewPlacement::BottomPanel,
            create_operations_view_ffi,
        ))?;

        Ok(())
    }
}

/// Reads the stored config. An undecodable one is reported and replaced by
/// defaults for this session, but left on disk untouched.
fn load_config(host: &HostApi<'_>) -> Result<RonomepoPluginConfig, MzStatusCode> {
    let bytes = host.read_config()?;
    Ok(
        RonomepoPluginConfig::decode(&bytes).unwrap_or_else(|error| {
            host.log(
                MzLogLevel::Warn,
                "Ronomepo config could not be decoded; using defaults",
            );
            diag::log(format!("plugin config could not be decoded: {error}"));
            RonomepoPluginConfig::default()
        }),
    )
}

fn initialize_state(config: &RonomepoPluginConfig) {
    // Both install main-loop timeouts, so run them on the main context.
    let main_context = glib::MainContext::default();
    main_context.invoke(diag::start_main_thread_watchdog);
    main_context.invoke(ensure_background_loops_started);

    let workspace_root = env::var_os("RONOMEPO_WORKSPACE_ROOT")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .map(normalize_workspace_root)
        .unwrap_or_else(|| PathBuf::from("."));
    let manifest_path = default_manifest_path(&workspace_root);
    let manifest = load_manifest_if_present(&manifest_path);
    let capability_registry = read_capability_registry(&workspace_root);

    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.workspace_root = workspace_root.clone();
    app_state.capability_registry = capability_registry;
    app_state.manifest_path = manifest.as_ref().map(|_| manifest_path.clone());
    app_state.manifest = manifest;
    app_state.workspace_status = empty_repository_status(workspace_root.clone());
    app_state.repository_items.clear();
    app_state.repository_items_loading = false;
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    app_state.monitor_view_mode = config.monitor_view_mode;
    app_state.monitor_settings = config.monitor_views;
    sync_repo_runtime_state(&mut app_state);
    if app_state.logs.is_empty() {
        let message = format!(
            "Ronomepo initialized for workspace {}",
            workspace_root.display()
        );
        diag::log(format!("app-log: {message}"));
        push_log_line(&mut app_state, message);
    }
    drop(app_state);
    emit_runtime_profile("startup");
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

extern "C" fn command_refresh_workspace(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command(
        "command_refresh_workspace",
        || match queue_refresh_workspace(None) {
            Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
            Err(message) => {
                append_log(format!("Refresh failed: {message}"));
                refresh_views();
                maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
            }
        },
    )
}

extern "C" fn command_clone_missing(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_clone_missing", || {
        launch_operation(OperationKind::CloneMissing);
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_pull(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_pull", || {
        launch_operation(OperationKind::Pull);
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_push(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_push", || {
        launch_operation(OperationKind::Push);
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_push_force(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_push_force", || {
        launch_operation(OperationKind::PushSkipChecks);
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_apply_hooks(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_apply_hooks", || {
        launch_operation(OperationKind::ApplyHooks);
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_open_overview(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_open_overview", || {
        open_workbench_view_command(VIEW_MONOREPO_OVERVIEW, "Monorepo Overview")
    })
}

extern "C" fn command_open_commit_check(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_open_commit_check", || {
        open_workbench_view_command(VIEW_COMMIT_CHECK, "Commit Check")
    })
}

extern "C" fn command_open_fucina_policy(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_open_fucina_policy", || {
        open_workbench_view_command(VIEW_FUCINA_POLICY, "Fucina Policy")
    })
}

fn open_workbench_view_command(view_id: &str, title: &str) -> maruzzella_sdk::ffi::MzStatus {
    let status = match open_plugin_view(view_id, MzViewPlacement::Workbench, title) {
        Ok(_) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(message);
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    };
    refresh_views();
    status
}

extern "C" fn command_exit(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_exit", || {
        if let Some(application) = gio::Application::default() {
            application.quit();
            maruzzella_sdk::ffi::MzStatus::OK
        } else {
            append_log("Cannot exit because no GTK application is active.".to_string());
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    })
}

extern "C" fn command_check_history(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_check_history", || match queue_history_report(25) {
        Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    })
}

extern "C" fn command_line_stats(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command(
        "command_line_stats",
        || match queue_line_stats_report_from_state() {
            Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
            Err(message) => {
                append_log(message);
                refresh_views();
                maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
            }
        },
    )
}

extern "C" fn command_refresh_logs(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_refresh_logs", || {
        refresh_log_surfaces();
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

extern "C" fn command_clear_logs(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    guard_command("command_clear_logs", || {
        diag::log("app-log: Operations log cleared.");
        {
            let mut app_state = state().lock().expect("state mutex poisoned");
            app_state.logs.clear();
            app_state.last_log_message = None;
            push_log_line(&mut app_state, "Operations log cleared.".to_string());
        }
        refresh_views();
        maruzzella_sdk::ffi::MzStatus::OK
    })
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
        JobKey::LineStats,
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
            state()
                .lock()
                .expect("state mutex poisoned")
                .line_stats_rerun = true;
            append_log(
                "Line stats refresh is already running; it will run again when it finishes."
                    .to_string(),
            );
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

/// Applies a finished line stats run, unless a newer request superseded it.
/// Returns the message to log and whether to run the report again.
fn complete_line_stats(
    app_state: &mut AppState,
    result: Result<LineStatsResult, String>,
) -> (String, bool) {
    let rerun = std::mem::take(&mut app_state.line_stats_rerun);
    app_state.line_stats_loading = false;
    let message = match result {
        Ok(result) => {
            if !rerun {
                app_state.line_stats_report = result.lines;
            }
            result.message
        }
        Err(message) => message,
    };
    (message, rerun)
}

fn append_log(message: String) {
    diag::log(format!("app-log: {message}"));
    let mut app_state = state().lock().expect("state mutex poisoned");
    push_log_line(&mut app_state, message);
    drop(app_state);
    schedule_log_surface_refresh();
}

/// Timestamps `message` and collapses consecutive repeats into the last line.
/// Callers mirror it to the diagnostics log before taking the state lock.
fn push_log_line(app_state: &mut AppState, message: String) {
    let stamp = log_timestamp();
    let repeated = app_state.last_log_message.as_deref() == Some(message.as_str())
        && !app_state.logs.is_empty();
    if repeated {
        app_state.last_log_repeats += 1;
        let line = format!("{stamp} {message} (x{})", app_state.last_log_repeats);
        if let Some(last) = app_state.logs.last_mut() {
            *last = line;
        }
        app_state.log_rewrites += 1;
        return;
    }
    app_state.logs.push(format!("{stamp} {message}"));
    app_state.log_end_seq += 1;
    app_state.last_log_message = Some(message);
    app_state.last_log_repeats = 1;
    if app_state.logs.len() > MAX_LOG_ENTRIES {
        let excess = app_state.logs.len() - MAX_LOG_ENTRIES;
        app_state.logs.drain(0..excess);
    }
}

fn log_timestamp() -> String {
    glib::DateTime::now_local()
        .and_then(|now| now.format("%H:%M:%S"))
        .map(|stamp| stamp.to_string())
        .unwrap_or_else(|_| "--:--:--".to_string())
}

fn schedule_log_surface_refresh() {
    if !on_main_context(schedule_log_surface_refresh) {
        return;
    }
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

/// Whether a Pull/Push/... batch is running. Only one runs at a time, since
/// concurrent batches would run git against the same repositories.
fn operation_in_flight() -> bool {
    state()
        .lock()
        .expect("state mutex poisoned")
        .operation_in_flight
        .is_some()
}

fn launch_operation(kind: OperationKind) {
    let operation = operation_kind_title(kind);
    let claimed = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        claim_operation_batch(&mut app_state, operation)
    };
    let (manifest, selected_repo_ids, batch_id) = match claimed {
        Ok(claimed) => claimed,
        Err(message) => {
            append_log(message);
            refresh_views();
            return;
        }
    };

    if let Err(message) = submit_job(WorkerJob::OperationBatch {
        manifest,
        selected_repo_ids,
        kind,
        batch_id,
    }) {
        append_log(format!(
            "[run {batch_id}] {operation} failed to start: {message}"
        ));
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.operation_in_flight = None;
    }
    refresh_views();
}

/// Reserves the single operation slot for a new batch, or explains why the
/// batch cannot start.
fn claim_operation_batch(
    app_state: &mut AppState,
    operation: &'static str,
) -> Result<(WorkspaceManifest, Vec<String>, usize), String> {
    let batch_id = app_state.next_operation_batch.max(1);
    if let Some(running) = &app_state.operation_in_flight {
        return Err(format!(
            "{operation} not started: {} (run {}) is still running.",
            running.operation, running.batch_id
        ));
    }
    let Some(manifest) = app_state.manifest.clone() else {
        app_state.next_operation_batch = batch_id + 1;
        return Err(format!(
            "[run {batch_id}] {operation} skipped because no {MANIFEST_FILE_NAME} is loaded."
        ));
    };
    app_state.next_operation_batch = batch_id + 1;
    app_state.operation_in_flight = Some(OperationBatchState {
        batch_id,
        operation,
        failures: Vec::new(),
    });
    Ok((manifest, app_state.selected_repo_ids.clone(), batch_id))
}

/// Releases the operation slot and reports the batch's failures in one dialog.
fn finish_operation_batch(batch_id: usize) {
    let finished = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        take_finished_operation_batch(&mut app_state, batch_id)
    };
    let Some(finished) = finished else {
        return;
    };
    if !finished.failures.is_empty()
        && matches!(finished.operation, "Pull" | "Push" | "Push (skip checks)")
    {
        present_operation_failure_dialog(finished.operation, &finished.failures);
    }
}

fn take_finished_operation_batch(
    app_state: &mut AppState,
    batch_id: usize,
) -> Option<OperationBatchState> {
    if app_state
        .operation_in_flight
        .as_ref()
        .is_some_and(|running| running.batch_id == batch_id)
    {
        app_state.operation_in_flight.take()
    } else {
        None
    }
}

fn record_operation_failure(app_state: &mut AppState, batch_id: usize, event: &OperationEvent) {
    if let Some(running) = app_state
        .operation_in_flight
        .as_mut()
        .filter(|running| running.batch_id == batch_id)
    {
        running.failures.push(OperationFailure {
            repository_name: event.repository_name.clone(),
            message: event.message.clone(),
        });
    }
}

fn operation_kind_title(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::CloneMissing => "Clone Missing",
        OperationKind::Pull => "Pull",
        OperationKind::Push => "Push",
        OperationKind::PushSkipChecks => "Push (skip checks)",
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

fn present_operation_failure_dialog(operation: &'static str, failures: &[OperationFailure]) {
    let is_generated_commit_failure = operation == "Push"
        && failures.iter().any(|failure| {
            failure
                .message
                .contains("commit check rules blocked commits")
        });
    let window = Window::builder()
        .modal(true)
        .title(format!("{operation} failed"))
        .default_width(560)
        .build();
    window.add_css_class("app-dialog");
    if let Some(parent) = active_window().as_ref() {
        window.set_transient_for(Some(parent));
    }

    let content = GtkBox::new(Orientation::Vertical, 16);
    content.set_margin_top(20);
    content.set_margin_bottom(16);
    content.set_margin_start(20);
    content.set_margin_end(20);

    let header = ui::hbox(10);
    let icon = ui::severity_icon(ui::Severity::Error);
    icon.set_valign(Align::Center);

    let title_text = match failures {
        [failure] => failure.repository_name.as_deref().map_or_else(
            || format!("{operation} failed."),
            |repo_name| format!("{operation} failed for {repo_name}."),
        ),
        _ => format!("{operation} failed for {} repositories.", failures.len()),
    };
    let title = Label::new(Some(&title_text));
    title.set_xalign(0.0);
    title.add_css_class(ui::class::HEADING);
    title.set_wrap(true);
    header.append(&icon);
    header.append(&title);
    content.append(&header);

    let messages = ui::list_card();
    for failure in failures {
        let text = match (failures.len(), failure.repository_name.as_deref()) {
            (2.., Some(repo_name)) => format!("{repo_name}: {}", failure.message),
            _ => failure.message.clone(),
        };
        let message = Label::new(Some(&text));
        message.set_xalign(0.0);
        message.set_wrap(true);
        message.set_wrap_mode(gtk::pango::WrapMode::WordChar);
        message.set_selectable(true);
        message.add_css_class("mono");
        message.add_css_class(ui::class::META);
        ui::list_card_item(&messages, &message);
    }
    let scroller = ScrolledWindow::new();
    scroller.set_policy(PolicyType::Never, PolicyType::Automatic);
    scroller.set_propagate_natural_height(true);
    scroller.set_max_content_height(360);
    scroller.set_vexpand(true);
    scroller.set_child(Some(&messages));
    content.append(&scroller);

    let buttons = ui::hbox(8);
    buttons.set_halign(Align::End);
    if is_generated_commit_failure {
        let open_commit_check = ui::button("Open Commit Check", ui::ButtonKind::Secondary);
        let window_ref = window.downgrade();
        open_commit_check.connect_clicked(move |_| {
            let _ = command_open_commit_check(maruzzella_sdk::ffi::MzBytes::empty());
            let _ = queue_history_report(25);
            if let Some(window) = window_ref.upgrade() {
                window.close();
            }
        });
        buttons.append(&open_commit_check);
    }
    let close = ui::button("Close", ui::ButtonKind::Primary);
    let window_ref = window.downgrade();
    close.connect_clicked(move |_| {
        if let Some(window) = window_ref.upgrade() {
            window.close();
        }
    });
    buttons.append(&close);
    content.append(&buttons);

    let escape = gtk::EventControllerKey::new();
    let window_ref = window.downgrade();
    escape.connect_key_pressed(move |_, key, _, _| {
        if key == gtk::gdk::Key::Escape {
            if let Some(window) = window_ref.upgrade() {
                window.close();
            }
            return glib::Propagation::Stop;
        }
        glib::Propagation::Proceed
    });
    window.add_controller(escape);

    window.set_child(Some(&content));
    window.set_default_widget(Some(&close));
    window.present();
}

fn handle_operation_event(
    batch_id: usize,
    manifest: Arc<WorkspaceManifest>,
    event: OperationEvent,
) {
    let _timed = diag::timed("operation-event");
    if matches!(event.kind, OperationEventKind::Failed) {
        let mut app_state = state().lock().expect("state mutex poisoned");
        record_operation_failure(&mut app_state, batch_id, &event);
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

fn worker_result_label(result: &WorkerResult) -> &'static str {
    match result {
        WorkerResult::WorkspaceScanCompleted { .. } => "worker-result:WorkspaceScanCompleted",
        WorkerResult::WorkspaceRootStatusRefreshed { .. } => {
            "worker-result:WorkspaceRootStatusRefreshed"
        }
        WorkerResult::RepositoryStatusRefreshed { .. } => "worker-result:RepositoryStatusRefreshed",
        WorkerResult::RepositoryRemoteFetchCompleted { .. } => {
            "worker-result:RepositoryRemoteFetchCompleted"
        }
        WorkerResult::RepoDetailsLoaded { .. } => "worker-result:RepoDetailsLoaded",
        WorkerResult::WatchManagerSyncCompleted { .. } => "worker-result:WatchManagerSyncCompleted",
        WorkerResult::RefreshWorkspaceCompleted { .. } => "worker-result:RefreshWorkspaceCompleted",
        WorkerResult::HistoryReportCompleted { .. } => "worker-result:HistoryReportCompleted",
        WorkerResult::LineStatsCompleted { .. } => "worker-result:LineStatsCompleted",
        WorkerResult::CapabilityChecksCompleted { .. } => "worker-result:CapabilityChecksCompleted",
        WorkerResult::OperationBatchCompleted { .. } => "worker-result:OperationBatchCompleted",
        WorkerResult::JobPanicked { .. } => "worker-result:JobPanicked",
    }
}

fn handle_worker_result(result: WorkerResult) {
    let _timed = diag::timed(worker_result_label(&result));
    match result {
        WorkerResult::WorkspaceScanCompleted {
            workspace_status,
            repository_items,
        } => {
            let (rerun, prefetch_sort_details, changed_mid_scan) = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                let changed_mid_scan = apply_full_workspace_scan(&mut app_state, repository_items);
                app_state.workspace_status = workspace_status;
                app_state.repository_items_loading = false;
                let rerun = app_state.repository_items_refresh_pending;
                app_state.repository_items_refresh_pending = false;
                let prefetch_sort_details = app_state
                    .monitor_settings
                    .view(app_state.monitor_view_mode)
                    .sort
                    == MonitorSortMode::RecentActivity;
                (rerun, prefetch_sort_details, changed_mid_scan)
            };
            reload_capability_registry();
            if changed_mid_scan {
                schedule_pending_local_rescans();
            }
            sync_watch_manager_from_state();
            if prefetch_sort_details {
                prefetch_monitor_sort_details();
            }
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
            let refresh_item = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                mark_remote_fetch_completed(&mut app_state, &repo_id, result.is_ok());
                result.is_ok().then(|| {
                    app_state
                        .repository_items
                        .iter()
                        .find(|item| item.id == repo_id)
                        .cloned()
                })
            };
            let message = match result {
                Ok(()) => format!("Remote sync refreshed for {repo_name}."),
                Err(error) => format!("Remote sync refresh failed for {repo_name}: {error}"),
            };
            if let Some(item) = refresh_item.flatten() {
                request_repository_status_refresh(&item);
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
            // The debounced refresh re-sorts the monitor, which covers the
            // recent-activity sort that depends on these details.
            if changed {
                refresh_views();
            }
        }
        WorkerResult::WatchManagerSyncCompleted { sync_seq, result } => {
            let (rerun, rerun_force) = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.watch_manager_sync_in_flight = false;
                app_state.watch_source = result
                    .as_ref()
                    .ok()
                    .and_then(Option::as_ref)
                    .map(|manager| manager.source.clone());
                let rerun = app_state.watch_manager_sync_pending;
                let rerun_force = app_state.watch_manager_sync_pending_force;
                app_state.watch_manager_sync_pending = false;
                app_state.watch_manager_sync_pending_force = false;
                (rerun, rerun_force)
            };

            match result {
                Ok(manager) => {
                    let unreadable = manager
                        .as_ref()
                        .map(|manager| manager.unreadable_paths.clone())
                        .unwrap_or_default();
                    log_watch_sync_outcome(None, unreadable);
                    LAST_WATCH_SYNC_SEQ_COMPLETED.store(sync_seq, Ordering::Relaxed);
                    WATCH_SYNC_COMPLETIONS.fetch_add(1, Ordering::Relaxed);
                    let previous = {
                        let mut slot = watch_manager()
                            .lock()
                            .expect("watch manager mutex poisoned");
                        let previous = std::mem::replace(&mut *slot, manager);
                        record_watch_manager_metrics(slot.as_ref(), "watch-backend-changed");
                        previous
                    };
                    let previous_paths = previous.as_ref().map_or(0, |m| m.watched_paths);
                    let _drop_timed = diag::timed_with(
                        "drop-previous-watch-manager",
                        format!("had_previous={} sync_seq={sync_seq}", previous.is_some()),
                    );
                    drop(previous);
                    diag::log(format!(
                        "watch manager replaced sync_seq={sync_seq} previous_roots={previous_paths}"
                    ));
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
                    log_watch_sync_outcome(Some(message), Vec::new());
                    emit_runtime_profile("watch-sync-failed");
                }
            }

            if rerun {
                WATCH_SYNC_PENDING_COLLAPSES.fetch_add(1, Ordering::Relaxed);
                sync_watch_manager(rerun_force);
            }
            let deferred_dirs = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                take_deferred_watch_dirs(&mut app_state)
            };
            if !deferred_dirs.is_empty() {
                queue_watch_new_dirs(deferred_dirs);
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
            let (message, rerun) = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                complete_line_stats(&mut app_state, result)
            };
            append_log(message);
            if rerun {
                if let Err(message) = queue_line_stats_report_from_state() {
                    append_log(message);
                }
            }
            refresh_views();
        }
        WorkerResult::CapabilityChecksCompleted { result, repo_ids } => {
            let message = match result {
                Ok(result) => {
                    for line in result.lines {
                        append_log(line);
                    }
                    result.message
                }
                Err(message) => format!("Capability checks failed: {message}"),
            };
            append_log(message);
            reload_capability_registry();
            // Only the checked repos' working trees may have changed (e.g.
            // build outputs).
            for repo_id in repo_ids {
                invalidate_repo_details(&repo_id);
                if let Some(item) = repository_item_from_state(&repo_id) {
                    request_repository_status_refresh(&item);
                }
            }
            refresh_views();
        }
        WorkerResult::OperationBatchCompleted { batch_id } => {
            finish_operation_batch(batch_id);
            refresh_views();
        }
        WorkerResult::JobPanicked { recovery, message } => {
            recover_panicked_job(recovery, message);
            refresh_views();
        }
    }
}

fn recover_panicked_job(recovery: JobRecovery, message: String) {
    match recovery {
        JobRecovery::OperationBatch {
            batch_id,
            operation,
        } => {
            append_log(format!("[run {batch_id}] {operation} aborted: {message}"));
            finish_operation_batch(batch_id);
            sync_watch_manager_from_state();
        }
        JobRecovery::WorkspaceScan => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            app_state.repository_items_loading = false;
            // Do not rerun right away: the same scan would likely panic again.
            app_state.repository_items_refresh_pending = false;
        }
        JobRecovery::RepoStatus(repo_id) => {
            // The repo stays stale, so the next periodic rescan retries it.
            let mut app_state = state().lock().expect("state mutex poisoned");
            if let Some(runtime) = app_state.repo_runtime.get_mut(&repo_id) {
                runtime.local_refresh_in_flight = false;
            }
        }
        JobRecovery::RepoDetails(repo_id) => {
            let mut app_state = state().lock().expect("state mutex poisoned");
            app_state.repo_details_loading.remove(&repo_id);
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
        // The status refresh job rescans the repo manifest off the main thread.
        repo_manifest: None,
    };
    request_repository_status_refresh(&item);
}

/// Returns whether the caller already runs on the GTK main context. Otherwise
/// `retry` is queued there and the caller must return: worker threads log and
/// request refreshes too, but timeouts and widgets are main-thread only.
fn on_main_context(retry: fn()) -> bool {
    let main_context = glib::MainContext::default();
    if main_context.is_owner() {
        return true;
    }
    main_context.invoke(retry);
    false
}

fn refresh_views() {
    if !on_main_context(refresh_views) {
        return;
    }
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
    let _timed = diag::timed("refresh-views");
    let snapshot = snapshot();
    refresh_repository_views(&snapshot);
    refresh_overview_views(&snapshot);
    refresh_workspace_settings_views(&snapshot);
    refresh_operation_views(&snapshot);
    refresh_operation_buttons();
}

fn refresh_repository_views(snapshot: &StateSnapshot) {
    REPOSITORY_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| {
            let Some(list) = handle.list.upgrade() else {
                return false;
            };
            refresh_repository_view_handle(handle, &list, snapshot);
            true
        });
    });
}

fn refresh_overview_views(snapshot: &StateSnapshot) {
    MONOREPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|view| {
            let Some(root) = view.root.upgrade() else {
                return false;
            };
            if root.is_mapped() {
                render_monorepo_overview(view, snapshot);
            } else {
                view.stale.set(true);
            }
            true
        });
    });

    sync_commit_check_views(snapshot);

    REPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|view| {
            if view.stack.upgrade().is_none() {
                return false;
            }
            view.stale
                .borrow_mut()
                .extend(RepoOverviewPage::ALL.iter().copied());
            render_visible_repo_overview_page(view, snapshot);
            sync_repo_terminal_panel(&view.terminal_root, snapshot, view.instance_key.as_deref());
            true
        });
    });
}

fn refresh_commit_check_views_now() {
    sync_commit_check_views(&snapshot());
}

fn sync_commit_check_views(snapshot: &StateSnapshot) {
    COMMIT_CHECK_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| {
            if handle.root.upgrade().is_none() {
                return false;
            }
            sync_commit_check_editor(&handle.editor, snapshot, false);
            true
        });
    });
}

fn refresh_workspace_settings_views(snapshot: &StateSnapshot) {
    FUCINA_POLICY_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| {
            if handle.root.upgrade().is_none() {
                return false;
            }
            sync_fucina_policy_view(handle, snapshot);
            true
        });
    });
}

fn refresh_operation_views(snapshot: &StateSnapshot) {
    refresh_operation_log_views(snapshot.log_cursor, &snapshot.logs);
}

/// Brings every Operations view up to `cursor`, editing only the changed
/// lines of each buffer.
fn refresh_operation_log_views(cursor: LogCursor, logs: &[String]) {
    let mut changed = false;
    OPERATION_BUFFERS.with(|buffers| {
        let mut buffers = buffers.borrow_mut();
        buffers.retain(|handle| match handle.buffer.upgrade() {
            Some(buffer) => {
                changed |= sync_operation_buffer(handle, &buffer, cursor, logs);
                true
            }
            None => false,
        });
    });

    let summary_stale = OPERATION_SUMMARY_CURSOR.with(|shown| shown.replace(Some(cursor)))
        != Some(cursor)
        || runtime_profile_enabled();
    if summary_stale {
        let summary = operation_summary(logs);
        OPERATION_SUMMARIES.with(|handles| {
            handles
                .borrow_mut()
                .retain(|handle| match handle.root.upgrade() {
                    Some(_) => {
                        sync_operation_summary(handle, &summary);
                        true
                    }
                    None => false,
                });
        });
    }

    if !changed {
        return;
    }
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

fn log_cursor(app_state: &AppState) -> LogCursor {
    LogCursor {
        first_seq: app_state
            .log_end_seq
            .saturating_sub(app_state.logs.len() as u64),
        end_seq: app_state.log_end_seq,
        rewrites: app_state.log_rewrites,
    }
}

fn log_view_edit(shown: LogCursor, current: LogCursor) -> LogViewEdit {
    if shown == current {
        return LogViewEdit::Unchanged;
    }
    if current.first_seq < shown.first_seq || current.end_seq < shown.end_seq {
        return LogViewEdit::Reset;
    }
    // Only the newest line is ever rewritten, so a rewrite since `shown`
    // touched at most the last shown entry.
    let replace_from = if shown.rewrites == current.rewrites {
        shown.end_seq
    } else {
        shown.end_seq.saturating_sub(1)
    };
    if replace_from < current.first_seq || replace_from < shown.first_seq {
        return LogViewEdit::Reset;
    }
    LogViewEdit::Update {
        drop_front: (current.first_seq - shown.first_seq) as usize,
        keep: (replace_from - current.first_seq) as usize,
    }
}

fn log_entry_line_count(entry: &str) -> usize {
    entry.matches('\n').count() + 1
}

/// Returns whether the buffer text changed.
fn sync_operation_buffer(
    handle: &OperationBufferHandle,
    buffer: &TextBuffer,
    cursor: LogCursor,
    logs: &[String],
) -> bool {
    let edit = log_view_edit(handle.shown.get(), cursor);
    let mut line_counts = handle.line_counts.borrow_mut();
    let (drop_front, keep) = match edit {
        LogViewEdit::Unchanged => return false,
        LogViewEdit::Update { drop_front, keep } if drop_front <= line_counts.len() => {
            (drop_front, keep)
        }
        _ => {
            buffer.set_text(&logs.join("\n"));
            *line_counts = logs
                .iter()
                .map(|entry| log_entry_line_count(entry))
                .collect();
            handle.shown.set(cursor);
            return true;
        }
    };

    let dropped_lines = line_counts.drain(..drop_front).sum::<usize>();
    if dropped_lines > 0 {
        let mut start = buffer.start_iter();
        let mut end = buffer
            .iter_at_line(dropped_lines as i32)
            .unwrap_or_else(|| buffer.end_iter());
        buffer.delete(&mut start, &mut end);
    }

    line_counts.truncate(keep);
    let kept_lines = line_counts.iter().sum::<usize>();
    let mut cut = if kept_lines == 0 {
        buffer.start_iter()
    } else {
        let mut line_end = buffer
            .iter_at_line(kept_lines as i32 - 1)
            .unwrap_or_else(|| buffer.end_iter());
        if !line_end.ends_line() {
            line_end.forward_to_line_end();
        }
        line_end
    };
    let mut end = buffer.end_iter();
    buffer.delete(&mut cut, &mut end);

    let appended = &logs[keep.min(logs.len())..];
    if !appended.is_empty() {
        let mut text = appended.join("\n");
        if kept_lines > 0 {
            text.insert(0, '\n');
        }
        buffer.insert(&mut buffer.end_iter(), &text);
        line_counts.extend(appended.iter().map(|entry| log_entry_line_count(entry)));
    }
    handle.shown.set(cursor);
    true
}

fn refresh_log_surfaces() {
    let _timed = diag::timed("refresh-log-surfaces");
    let (cursor, logs) = {
        let app_state = state().lock().expect("state mutex poisoned");
        (log_cursor(&app_state), app_state.logs.clone())
    };
    refresh_operation_log_views(cursor, &logs);
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
    ui::icon_toggle("go-bottom-symbolic", "Follow scroll to bottom")
}

/// The host hands view factories a pointer to a temporary copy of its API that
/// is dropped as soon as the factory returns, so the pointer must never be kept.
/// Keep one leaked copy per shell host instead (bounded by the number of
/// surfaces) and hand out pointers to that. The copy drops the per-view config
/// context, which only lives as long as the view it was created for.
fn remember_host_ptr(
    host: *const maruzzella_sdk::ffi::MzHostApi,
) -> *const maruzzella_sdk::ffi::MzHostApi {
    let Some(host) = (unsafe { host.as_ref() }) else {
        return std::ptr::null();
    };
    let mut api = *host;
    api.config_context = std::ptr::null_mut();
    api.read_config_in_context = None;
    api.write_config_in_context = None;
    let stable: &'static maruzzella_sdk::ffi::MzHostApi = HOST_APIS.with(|apis| {
        let mut apis = apis.borrow_mut();
        if let Some(existing) = apis
            .iter()
            .find(|existing| existing.host_context == api.host_context)
        {
            return *existing;
        }
        let leaked: &'static maruzzella_sdk::ffi::MzHostApi = Box::leak(Box::new(api));
        apis.push(leaked);
        leaked
    });
    let stable = stable as *const maruzzella_sdk::ffi::MzHostApi;
    LAST_HOST_PTR.store(stable as usize, Ordering::Relaxed);
    stable
}

fn current_host_ptr() -> *const maruzzella_sdk::ffi::MzHostApi {
    LAST_HOST_PTR.load(Ordering::Relaxed) as *const _
}

/// Opens one of the plugin's views, or focuses it when already open.
fn open_plugin_view(
    view_id: &str,
    placement: MzViewPlacement,
    title: &str,
) -> Result<MzViewOpenDisposition, String> {
    let Some(host) = (unsafe { current_host_ptr().as_ref() }).map(HostApi::from_raw) else {
        return Err(format!(
            "Cannot open {title} because the Maruzzella host handle is unavailable."
        ));
    };
    host.open_view(&OpenViewRequest::new(PLUGIN_ID, view_id, placement))
        .map_err(|status| format!("Failed to open {title}: {status:?}"))
}

fn open_workspace_settings_tab() -> Result<(), String> {
    open_plugin_view(
        VIEW_FUCINA_POLICY,
        MzViewPlacement::Workbench,
        "Fucina Policy",
    )
    .map(|_| ())
}

fn open_operations_panel() -> Result<(), String> {
    open_plugin_view(VIEW_OPERATIONS, MzViewPlacement::BottomPanel, "Operations").map(|_| ())
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
    monitor_view_mode: MonitorViewMode,
    monitor_settings: MonitorSettings,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    log_cursor: LogCursor,
    history_report: Vec<String>,
    history_report_loading: bool,
    line_stats_report: Vec<String>,
    line_stats_loading: bool,
    line_stats_since: String,
    capability_registry: Arc<CapabilityRegistry>,
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
        monitor_view_mode: app_state.monitor_view_mode,
        monitor_settings: app_state.monitor_settings,
        selected_repo_ids: app_state.selected_repo_ids.clone(),
        active_repo_id: app_state.active_repo_id.clone(),
        logs: app_state.logs.clone(),
        log_cursor: log_cursor(&app_state),
        history_report: app_state.history_report.clone(),
        history_report_loading: app_state.history_report_loading,
        line_stats_report: app_state.line_stats_report.clone(),
        line_stats_loading: app_state.line_stats_loading,
        line_stats_since: app_state.line_stats_since.clone(),
        capability_registry: Arc::clone(&app_state.capability_registry),
    }
}

fn refresh_repository_view_handle(
    handle: &RepositoryViewHandle,
    list: &ListBox,
    snapshot: &StateSnapshot,
) {
    let query = monitor_query(snapshot);
    let (filter_changed, sort_changed) = {
        let mut current = handle.query.borrow_mut();
        let filter_changed =
            current.filter_mode != query.filter_mode || current.filter_text != query.filter_text;
        let sort_changed =
            current.sort_mode != query.sort_mode || current.descending != query.descending;
        *current = query;
        (filter_changed, sort_changed)
    };

    let next_rows = monitor_rows(snapshot);
    let update = monitor_store_update(&handle.rows.borrow(), &next_rows);
    match update {
        MonitorStoreUpdate::Unchanged => {}
        MonitorStoreUpdate::Positions(positions) => {
            for position in positions {
                handle.store.splice(
                    position as u32,
                    1,
                    &[BoxedAnyObject::new(next_rows[position].clone())],
                );
            }
        }
        MonitorStoreUpdate::Replace => {
            let objects = next_rows
                .iter()
                .cloned()
                .map(BoxedAnyObject::new)
                .collect::<Vec<_>>();
            handle.store.splice(0, handle.store.n_items(), &objects);
        }
    }
    *handle.rows.borrow_mut() = next_rows;

    // Changed rows are re-filtered and re-sorted by the models themselves; a
    // full pass is only needed when the criteria change.
    if filter_changed {
        handle.filter.changed(FilterChange::Different);
    }
    if sort_changed {
        handle.sorter.changed(SorterChange::Different);
    }
    sync_monitor_controls(&handle.controls, snapshot);
    sync_repository_monitor_selection(list, &snapshot.selected_repo_ids);
    update_repository_monitor_empty_state(
        handle,
        list,
        snapshot,
        handle.filter_model.n_items() == 0,
    );
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
        Some(RepoManifestScanState::Valid(summary))
            if summary.missing_required_capability_count > 0 =>
        {
            "Manifest!"
        }
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
            "{}\n{} items | types: {} | actions: {} | capabilities: {} | policy issues: {}",
            path.display(),
            summary.item_count,
            summary.item_types.join(", "),
            summary
                .supported_actions
                .iter()
                .map(|action| standard_action_label(*action).to_string())
                .collect::<Vec<_>>()
                .join(", "),
            summary.capability_count,
            summary.missing_required_capability_count
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

/// The view, filter and sort controls of a repository monitor.
struct MonitorControls {
    syncing: Rc<Cell<bool>>,
    view_buttons: Vec<(MonitorViewMode, ToggleButton)>,
    filter_buttons: Vec<(MonitorViewMode, MonitorFilterMode, ToggleButton)>,
    sort_dropdowns: Vec<(MonitorViewMode, DropDown)>,
    sort_direction: Rc<Cell<bool>>,
    sort_direction_icon: Image,
    /// Filter, sort and column-header stacks, each with one page per view.
    stacks: [Stack; 3],
}

/// Mirrors the monitor settings into the controls, which only follow local
/// clicks otherwise (another monitor view may have changed them).
fn sync_monitor_controls(controls: &MonitorControls, snapshot: &StateSnapshot) {
    controls.syncing.set(true);
    let active_view = snapshot.monitor_view_mode;
    for (view, button) in &controls.view_buttons {
        if *view == active_view && !button.is_active() {
            button.set_active(true);
        }
    }
    let view_key = monitor_view_mode_key(active_view);
    for stack in &controls.stacks {
        if stack.visible_child_name().as_deref() != Some(view_key) {
            stack.set_visible_child_name(view_key);
        }
    }
    for (view, filter_mode, button) in &controls.filter_buttons {
        if snapshot.monitor_settings.view(*view).filter == *filter_mode && !button.is_active() {
            button.set_active(true);
        }
    }
    for (view, dropdown) in &controls.sort_dropdowns {
        let index = monitor_sort_mode_index(*view, snapshot.monitor_settings.view(*view).sort);
        if dropdown.selected() != index {
            dropdown.set_selected(index);
        }
    }
    let descending = active_monitor_settings(snapshot).descending;
    if controls.sort_direction.get() != descending {
        controls.sort_direction.set(descending);
        controls
            .sort_direction_icon
            .set_icon_name(Some(monitor_sort_direction_icon(descending)));
    }
    controls.syncing.set(false);
}

fn update_repository_monitor_empty_state(
    handle: &RepositoryViewHandle,
    list: &ListBox,
    snapshot: &StateSnapshot,
    is_empty: bool,
) {
    let next = is_empty.then_some(snapshot.manifest.is_some());
    if handle.placeholder_for_manifest.replace(next) == next {
        return;
    }
    if is_empty {
        list.add_css_class("repo-monitor-empty");
        list.set_placeholder(Some(&repository_monitor_empty_placeholder(snapshot)));
    } else {
        list.remove_css_class("repo-monitor-empty");
        list.set_placeholder(Option::<&gtk::Widget>::None);
    }
}

fn repository_monitor_empty_placeholder(snapshot: &StateSnapshot) -> GtkBox {
    if snapshot.manifest.is_some() {
        ui::empty_state(
            "edit-find-symbolic",
            "No repositories match the current filter",
            "Try a broader search term or clear the filter to see the full workspace.",
            None,
        )
    } else {
        ui::empty_state(
            "folder-symbolic",
            "No workspace manifest loaded",
            "Ronomepo is running, but no ronomepo.json was found. Import repos.txt from the current workspace root to bootstrap the manifest.",
            None,
        )
    }
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

fn with_monitor_row<T>(object: &glib::Object, read: impl FnOnce(&MonitorRow) -> T) -> Option<T> {
    object
        .downcast_ref::<BoxedAnyObject>()
        .map(|boxed| read(&boxed.borrow::<MonitorRow>()))
}

/// One repository monitor row: the item plus every derived value the row
/// widgets, filter and sorter read, computed once per refresh. Rows whose
/// inputs did not change compare equal and keep their widgets.
#[derive(Clone, Debug, PartialEq)]
struct MonitorRow {
    item: RepositoryListItem,
    view_mode: MonitorViewMode,
    name_key: String,
    attention_signals: Vec<AttentionSignal>,
    attention_rank: u8,
    capability: CapabilityMonitorSummary,
    enabled: Option<bool>,
    last_commit_epoch_secs: i64,
    /// Lowercased searchable fields, separated so a filter cannot match
    /// across two of them.
    search_text: String,
}

/// Filter and sort settings applied by the monitor list models.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct MonitorQuery {
    filter_mode: MonitorFilterMode,
    /// Trimmed and lowercased; empty matches everything.
    filter_text: String,
    sort_mode: MonitorSortMode,
    descending: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum MonitorStoreUpdate {
    Unchanged,
    /// Same rows in the same order; only these positions changed.
    Positions(Vec<usize>),
    Replace,
}

fn monitor_query(snapshot: &StateSnapshot) -> MonitorQuery {
    let settings = active_monitor_settings(snapshot);
    MonitorQuery {
        filter_mode: settings.filter,
        filter_text: snapshot.monitor_filter.trim().to_ascii_lowercase(),
        sort_mode: settings.sort,
        descending: settings.descending,
    }
}

fn monitor_rows(snapshot: &StateSnapshot) -> Vec<MonitorRow> {
    std::iter::once(monorepo_monitor_item(snapshot))
        .chain(snapshot.repository_items.iter().cloned())
        .map(|item| {
            let enabled = workspace_repo_enabled(snapshot, &item);
            let last_commit_epoch_secs = snapshot
                .repo_details_cache
                .get(&item.id)
                .and_then(|details| details.last_commit.as_ref())
                .map(|commit| commit.committed_at_epoch_secs)
                .unwrap_or(i64::MIN);
            build_monitor_row(
                item,
                snapshot.monitor_view_mode,
                &snapshot.capability_registry,
                enabled,
                last_commit_epoch_secs,
            )
        })
        .collect()
}

fn build_monitor_row(
    item: RepositoryListItem,
    view_mode: MonitorViewMode,
    registry: &CapabilityRegistry,
    enabled: Option<bool>,
    last_commit_epoch_secs: i64,
) -> MonitorRow {
    let attention_signals = derive_attention_signals(&item, None, registry);
    let attention_rank = attention_signals_rank(&attention_signals);
    let capability = capability_monitor_summary(&item, registry);
    let search_text = [
        item.name.as_str(),
        item.dir_name.as_str(),
        item.remote_url.as_str(),
        branch_label(&item),
        &format_sync_label(&item.status.sync),
        status_label(&item.status.state),
        manifest_presence_search_text(item.repo_manifest.as_ref()),
        &capability_monitor_search_text(&capability),
        &attention_monitor_search_text(&attention_signals),
    ]
    .join("\u{1f}")
    .to_ascii_lowercase();
    MonitorRow {
        name_key: item.name.to_ascii_lowercase(),
        item,
        view_mode,
        attention_signals,
        attention_rank,
        capability,
        enabled,
        last_commit_epoch_secs,
        search_text,
    }
}

fn monitor_store_update(current: &[MonitorRow], next: &[MonitorRow]) -> MonitorStoreUpdate {
    if current.len() != next.len()
        || current
            .iter()
            .zip(next)
            .any(|(current, next)| current.item.id != next.item.id)
    {
        return MonitorStoreUpdate::Replace;
    }
    let changed = current
        .iter()
        .zip(next)
        .enumerate()
        .filter(|(_, (current, next))| current != next)
        .map(|(position, _)| position)
        .collect::<Vec<_>>();
    if changed.is_empty() {
        MonitorStoreUpdate::Unchanged
    } else if changed.len() * 2 > next.len() {
        // One splice rebuilds everything at once instead of re-sorting per row.
        MonitorStoreUpdate::Replace
    } else {
        MonitorStoreUpdate::Positions(changed)
    }
}

fn monitor_row_matches(row: &MonitorRow, query: &MonitorQuery) -> bool {
    monitor_row_matches_mode(row, query.filter_mode)
        && (query.filter_text.is_empty() || row.search_text.contains(&query.filter_text))
}

fn build_repo_monitor_row(
    row: &MonitorRow,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let item = &row.item;
    let content = ui::hbox(12);
    content.add_css_class(ui::class::LIST_ROW);
    mark_clickable(&content);
    content.set_widget_name(&item.id);
    content.set_tooltip_text(Some(&format!(
        "{}\n{}\n{}",
        item.status.repo_path.display(),
        item.remote_url,
        item.dir_name
    )));

    append_monitor_name_cell(&content, item, row.view_mode == MonitorViewMode::Overview);
    match row.view_mode {
        MonitorViewMode::Overview => {
            content.append(&monitor_state_cell(&item.status.state));
            content.append(&monitor_sync_cell(&item.status.sync));
            content.append(&monitor_attention_cell(&row.attention_signals));
        }
        MonitorViewMode::Git => {
            let branch = monitor_cell(branch_label(item), Some(MONITOR_BRANCH_COL_WIDTH));
            ui::set_fg_tone(&branch, ui::Tone::Muted);
            branch.add_css_class(ui::class::META);
            content.append(&branch);
            content.append(&monitor_state_cell(&item.status.state));
            content.append(&monitor_sync_cell(&item.status.sync));
        }
        MonitorViewMode::Checks => {
            content.append(&monitor_manifest_cell(item.repo_manifest.as_ref()));
            content.append(&monitor_capability_cell(&row.capability));
            content.append(&monitor_attention_cell(&row.attention_signals));
        }
        MonitorViewMode::Manifest => {
            content.append(&monitor_manifest_cell(item.repo_manifest.as_ref()));
            content.append(&monitor_manifest_actions_cell(item));
            content.append(&monitor_enabled_cell(row.enabled));
            let remote = monitor_cell(&item.remote_url, None);
            remote.add_css_class("mono");
            remote.add_css_class(ui::class::META);
            ui::set_fg_tone(&remote, ui::Tone::Muted);
            content.append(&remote);
        }
    }
    attach_repo_monitor_context_menu(&content, host_ptr);
    content
}

fn build_repo_monitor_list_row(
    row: &MonitorRow,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> ListBoxRow {
    let list_row = ListBoxRow::new();
    list_row.set_child(Some(&build_repo_monitor_row(row, host_ptr)));
    // Selection only ever targets repositories, so the monorepo row would be
    // deselected again on the next refresh.
    list_row.set_selectable(row.item.id != MONOREPO_ROW_ID);
    list_row
}

/// The name cell, colored by working tree state like IDEA's VCS colors. With
/// `show_branch`, the branch follows in small muted text, as in IDEA's
/// Project tool window.
fn append_monitor_name_cell(row: &GtkBox, item: &RepositoryListItem, show_branch: bool) {
    let name = monitor_cell(&item.name, None);
    if item.id == MONOREPO_ROW_ID {
        ui::set_fg_tone(&name, ui::Tone::Strong);
        name.add_css_class(ui::class::STRONG);
    } else {
        ui::set_fg_tone(&name, repo_name_tone(&item.status));
    }
    if !show_branch || item.id == MONOREPO_ROW_ID {
        row.append(&name);
        return;
    }
    name.set_hexpand(false);
    name.set_size_request(-1, -1);
    // The name keeps its full width while it fits; the branch shrinks first.
    name.set_max_width_chars(-1);
    let cell = ui::hbox(6);
    cell.set_hexpand(true);
    cell.set_size_request(96, -1);
    let branch = monitor_cell(branch_label(item), None);
    ui::set_fg_tone(&branch, ui::Tone::Muted);
    branch.add_css_class(ui::class::META);
    branch.set_size_request(24, -1);
    cell.append(&name);
    cell.append(&branch);
    row.append(&cell);
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

fn active_monitor_settings(snapshot: &StateSnapshot) -> MonitorViewSettings {
    *snapshot.monitor_settings.view(snapshot.monitor_view_mode)
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
        MonitorSortMode::Branch => "Branch",
        MonitorSortMode::Attention => "Attention",
        MonitorSortMode::CheckState => "Check state",
        MonitorSortMode::Manifest => "Manifest",
        MonitorSortMode::Actions => "Actions",
    }
}

fn monitor_view_mode_label(mode: MonitorViewMode) -> &'static str {
    match mode {
        MonitorViewMode::Overview => "Overview",
        MonitorViewMode::Git => "Git",
        MonitorViewMode::Checks => "Checks",
        MonitorViewMode::Manifest => "Manifest",
    }
}

fn monitor_view_mode_key(mode: MonitorViewMode) -> &'static str {
    match mode {
        MonitorViewMode::Overview => "overview",
        MonitorViewMode::Git => "git",
        MonitorViewMode::Checks => "checks",
        MonitorViewMode::Manifest => "manifest",
    }
}

fn monitor_filter_mode_label(mode: MonitorFilterMode) -> &'static str {
    match mode {
        MonitorFilterMode::All => "All",
        MonitorFilterMode::Dirty => "Dirty",
        MonitorFilterMode::ToSync => "To sync",
        MonitorFilterMode::Issues => "Issues",
        MonitorFilterMode::NeedsAttention => "Needs attention",
        MonitorFilterMode::Healthy => "Healthy",
        MonitorFilterMode::Diverged => "Diverged",
        MonitorFilterMode::Clean => "Clean",
        MonitorFilterMode::Due => "Due",
        MonitorFilterMode::Failing => "Failing",
        MonitorFilterMode::MissingManifest => "Missing manifest",
        MonitorFilterMode::NoChecks => "No checks",
        MonitorFilterMode::Enabled => "Enabled",
        MonitorFilterMode::Disabled => "Disabled",
        MonitorFilterMode::HasActions => "Has actions",
    }
}

fn monitor_view_modes() -> &'static [MonitorViewMode] {
    &[
        MonitorViewMode::Overview,
        MonitorViewMode::Git,
        MonitorViewMode::Checks,
        MonitorViewMode::Manifest,
    ]
}

fn monitor_filter_modes_for_view(view: MonitorViewMode) -> &'static [MonitorFilterMode] {
    match view {
        MonitorViewMode::Overview => &[
            MonitorFilterMode::All,
            MonitorFilterMode::NeedsAttention,
            MonitorFilterMode::Issues,
            MonitorFilterMode::Healthy,
        ],
        MonitorViewMode::Git => &[
            MonitorFilterMode::All,
            MonitorFilterMode::Dirty,
            MonitorFilterMode::ToSync,
            MonitorFilterMode::Diverged,
            MonitorFilterMode::Clean,
        ],
        MonitorViewMode::Checks => &[
            MonitorFilterMode::All,
            MonitorFilterMode::Due,
            MonitorFilterMode::Failing,
            MonitorFilterMode::MissingManifest,
            MonitorFilterMode::NoChecks,
        ],
        MonitorViewMode::Manifest => &[
            MonitorFilterMode::All,
            MonitorFilterMode::Enabled,
            MonitorFilterMode::Disabled,
            MonitorFilterMode::MissingManifest,
            MonitorFilterMode::HasActions,
        ],
    }
}

fn monitor_sort_modes_for_view(view: MonitorViewMode) -> &'static [MonitorSortMode] {
    match view {
        MonitorViewMode::Overview => &[
            MonitorSortMode::Name,
            MonitorSortMode::Attention,
            MonitorSortMode::SyncState,
            MonitorSortMode::RecentActivity,
        ],
        MonitorViewMode::Git => &[
            MonitorSortMode::Name,
            MonitorSortMode::Branch,
            MonitorSortMode::SyncState,
            MonitorSortMode::RecentActivity,
        ],
        MonitorViewMode::Checks => &[
            MonitorSortMode::Name,
            MonitorSortMode::CheckState,
            MonitorSortMode::Manifest,
            MonitorSortMode::Attention,
        ],
        MonitorViewMode::Manifest => &[
            MonitorSortMode::Name,
            MonitorSortMode::Manifest,
            MonitorSortMode::Actions,
            MonitorSortMode::RecentActivity,
        ],
    }
}

fn monitor_sort_mode_from_index(view: MonitorViewMode, index: u32) -> MonitorSortMode {
    monitor_sort_modes_for_view(view)
        .get(index as usize)
        .copied()
        .unwrap_or(MonitorSortMode::Name)
}

fn monitor_sort_mode_index(view: MonitorViewMode, mode: MonitorSortMode) -> u32 {
    monitor_sort_modes_for_view(view)
        .iter()
        .position(|candidate| *candidate == mode)
        .unwrap_or(0) as u32
}

fn monitor_sort_direction_icon(descending: bool) -> &'static str {
    if descending {
        "view-sort-descending-symbolic"
    } else {
        "view-sort-ascending-symbolic"
    }
}

fn monitor_row_cmp(
    query: &MonitorQuery,
    left: &MonitorRow,
    right: &MonitorRow,
) -> std::cmp::Ordering {
    let monorepo_rank =
        u8::from(left.item.id == MONOREPO_ROW_ID).cmp(&u8::from(right.item.id == MONOREPO_ROW_ID));
    if monorepo_rank != std::cmp::Ordering::Equal {
        return monorepo_rank;
    }

    let by_name = || left.name_key.cmp(&right.name_key);
    let ordering = match query.sort_mode {
        MonitorSortMode::Name => by_name(),
        MonitorSortMode::Branch => branch_label(&left.item)
            .to_ascii_lowercase()
            .cmp(&branch_label(&right.item).to_ascii_lowercase())
            .then_with(by_name),
        MonitorSortMode::SyncState => {
            repo_sync_state_sort_key(&left.item).cmp(&repo_sync_state_sort_key(&right.item))
        }
        MonitorSortMode::RecentActivity => (left.last_commit_epoch_secs, left.attention_rank)
            .cmp(&(right.last_commit_epoch_secs, right.attention_rank))
            .then_with(by_name),
        MonitorSortMode::Attention => left
            .attention_rank
            .cmp(&right.attention_rank)
            .then_with(by_name),
        MonitorSortMode::CheckState => capability_sort_key(&left.capability)
            .cmp(&capability_sort_key(&right.capability))
            .then_with(by_name),
        MonitorSortMode::Manifest => manifest_sort_key(&left.item)
            .cmp(&manifest_sort_key(&right.item))
            .then_with(by_name),
        MonitorSortMode::Actions => manifest_action_count_sort_key(&left.item)
            .cmp(&manifest_action_count_sort_key(&right.item))
            .then_with(by_name),
    };

    if query.descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn monitor_row_matches_mode(row: &MonitorRow, mode: MonitorFilterMode) -> bool {
    use ronomepo_core::{RepositoryState, RepositorySync};

    let item = &row.item;
    let summary = &row.capability;
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
        MonitorFilterMode::Diverged => matches!(item.status.sync, RepositorySync::Diverged { .. }),
        MonitorFilterMode::Clean => {
            matches!(item.status.state, RepositoryState::Clean)
                && matches!(
                    item.status.sync,
                    RepositorySync::UpToDate | RepositorySync::NoUpstream
                )
        }
        MonitorFilterMode::Issues => !row.attention_signals.is_empty(),
        MonitorFilterMode::NeedsAttention => row.attention_rank <= 2,
        MonitorFilterMode::Healthy => row.attention_signals.is_empty(),
        MonitorFilterMode::Due => capability_summary_needs_attention(summary),
        MonitorFilterMode::Failing => {
            summary.policy_issues > 0
                || summary.unsupported > 0
                || summary.error > 0
                || summary.failed > 0
        }
        MonitorFilterMode::MissingManifest => {
            item.id != MONOREPO_ROW_ID
                && matches!(
                    item.repo_manifest.as_ref().map(|scan| &scan.state),
                    Some(RepoManifestScanState::Missing | RepoManifestScanState::Invalid { .. })
                        | None
                )
        }
        MonitorFilterMode::NoChecks => {
            item.id != MONOREPO_ROW_ID && summary.declared == 0 && summary.stored == 0
        }
        MonitorFilterMode::Enabled => row.enabled == Some(true),
        MonitorFilterMode::Disabled => row.enabled == Some(false),
        MonitorFilterMode::HasActions => manifest_action_count(item) > 0,
    }
}

fn repo_attention_rank(snapshot: &StateSnapshot, item: &RepositoryListItem) -> u8 {
    attention_signals_rank(&repo_attention_signals(snapshot, item))
}

fn attention_signals_rank(signals: &[AttentionSignal]) -> u8 {
    if signals.is_empty() {
        return 7;
    }

    match attention_rank(signals) {
        0 => 0,
        1 => 1,
        2 => 3,
        3 => 4,
        4 => 6,
        _ => 7,
    }
}

fn repo_sync_state_sort_key(item: &RepositoryListItem) -> (u8, usize, u8, String) {
    (
        repo_sync_sort_rank(&item.status.sync),
        repo_sync_distance_sort_rank(&item.status.sync),
        repo_local_state_sort_rank(&item.status.state),
        item.name.to_ascii_lowercase(),
    )
}

fn capability_sort_key(summary: &CapabilityMonitorSummary) -> (u8, usize) {
    let rank = if summary.policy_issues > 0 || summary.unsupported > 0 {
        0
    } else if summary.error > 0 {
        1
    } else if summary.failed > 0 {
        2
    } else if summary.stale > 0 {
        3
    } else if summary.warning > 0 {
        4
    } else if summary.unknown > 0 {
        5
    } else if summary.running > 0 {
        6
    } else if summary.declared > 0 || summary.stored > 0 {
        7
    } else {
        8
    };
    (
        rank,
        usize::MAX
            - summary
                .declared
                .saturating_add(summary.stored)
                .saturating_add(summary.policy_issues)
                .saturating_add(summary.unsupported),
    )
}

fn manifest_sort_key(item: &RepositoryListItem) -> (u8, String) {
    let rank = match item.repo_manifest.as_ref().map(|scan| &scan.state) {
        Some(RepoManifestScanState::Invalid { .. }) => 0,
        Some(RepoManifestScanState::Missing) | None => 1,
        Some(RepoManifestScanState::Valid(summary))
            if summary.missing_required_capability_count > 0 =>
        {
            2
        }
        Some(RepoManifestScanState::Valid(_)) => 3,
    };
    (rank, item.name.to_ascii_lowercase())
}

fn manifest_action_count_sort_key(item: &RepositoryListItem) -> (usize, String) {
    (
        usize::MAX - manifest_action_count(item),
        item.name.to_ascii_lowercase(),
    )
}

fn manifest_action_count(item: &RepositoryListItem) -> usize {
    match item.repo_manifest.as_ref().map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(summary)) => summary.action_count,
        _ => 0,
    }
}

fn workspace_repo_enabled(snapshot: &StateSnapshot, item: &RepositoryListItem) -> Option<bool> {
    if item.id == MONOREPO_ROW_ID {
        return None;
    }
    snapshot
        .manifest
        .as_ref()
        .and_then(|manifest| manifest.repos.iter().find(|repo| repo.id == item.id))
        .map(|repo| repo.enabled)
}

fn repo_local_state_sort_rank(state: &ronomepo_core::RepositoryState) -> u8 {
    match state {
        ronomepo_core::RepositoryState::Missing => 0,
        ronomepo_core::RepositoryState::Dirty => 1,
        ronomepo_core::RepositoryState::Untracked => 2,
        ronomepo_core::RepositoryState::Unknown => 3,
        ronomepo_core::RepositoryState::Clean => 4,
    }
}

fn repo_sync_sort_rank(sync: &ronomepo_core::RepositorySync) -> u8 {
    match sync {
        ronomepo_core::RepositorySync::Diverged { .. } => 0,
        ronomepo_core::RepositorySync::Behind(_) => 1,
        ronomepo_core::RepositorySync::Ahead(_) => 2,
        ronomepo_core::RepositorySync::Unknown => 3,
        ronomepo_core::RepositorySync::NoUpstream => 4,
        ronomepo_core::RepositorySync::UpToDate => 5,
    }
}

fn repo_sync_distance_sort_rank(sync: &ronomepo_core::RepositorySync) -> usize {
    let distance = match sync {
        ronomepo_core::RepositorySync::Ahead(count)
        | ronomepo_core::RepositorySync::Behind(count) => *count,
        ronomepo_core::RepositorySync::Diverged { ahead, behind } => ahead.saturating_add(*behind),
        _ => 0,
    };
    usize::MAX - distance
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
    guard_command("command_filter", || {
        let text = if payload.ptr.is_null() || payload.len == 0 {
            String::new()
        } else {
            let bytes = unsafe { std::slice::from_raw_parts(payload.ptr, payload.len) };
            String::from_utf8_lossy(bytes).into_owned()
        };
        update_monitor_filter(text);
        refresh_views();
        maruzzella_sdk::ffi::MzStatus::OK
    })
}

/// Applies `update` to the active view's settings and persists them.
fn update_active_monitor_settings(update: impl FnOnce(&mut MonitorViewSettings)) {
    let settings = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let view = app_state.monitor_view_mode;
        update(app_state.monitor_settings.view_mut(view));
        app_state.monitor_settings
    };
    persist_plugin_config_direct(move |config| {
        config.monitor_views = settings;
    });
}

fn update_monitor_filter_mode(mode: MonitorFilterMode) {
    update_active_monitor_settings(|settings| settings.filter = mode);
}

fn update_monitor_sort_mode(mode: MonitorSortMode) {
    update_active_monitor_settings(|settings| settings.sort = mode);
    if mode == MonitorSortMode::RecentActivity {
        prefetch_monitor_sort_details();
    }
}

fn update_monitor_sort_descending(descending: bool) {
    update_active_monitor_settings(|settings| settings.descending = descending);
}

fn update_monitor_view_mode(mode: MonitorViewMode) {
    let sort_mode = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.monitor_view_mode = mode;
        app_state.monitor_settings.view(mode).sort
    };
    if sort_mode == MonitorSortMode::RecentActivity {
        prefetch_monitor_sort_details();
    }
    persist_plugin_config_direct(move |config| {
        config.monitor_view_mode = mode;
    });
}

fn prefetch_monitor_sort_details() {
    let repos = state()
        .lock()
        .expect("state mutex poisoned")
        .repository_items
        .iter()
        .map(|item| (item.id.clone(), item.status.repo_path.clone()))
        .collect::<Vec<_>>();
    for (repo_id, repo_path) in repos {
        schedule_repo_details_load(&repo_id, &repo_path);
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

/// Merges a full workspace scan into the state. Repos invalidated after the
/// scan was submitted keep their current item (the scan may have read them
/// before the change, while a per-repo refresh may already have newer data)
/// and stay stale. Per-repo refresh flags belong to the RepoStatus jobs and
/// are left alone. Returns whether any scanned repo still needs a rescan.
fn apply_full_workspace_scan(
    app_state: &mut AppState,
    repository_items: Vec<RepositoryListItem>,
) -> bool {
    let now = SystemTime::now();
    let scan_seqs = std::mem::take(&mut app_state.workspace_scan_seqs);
    let mut previous_items = std::mem::take(&mut app_state.repository_items)
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<HashMap<_, _>>();
    let mut needs_rescan = false;
    let mut merged = Vec::with_capacity(repository_items.len());
    for item in repository_items {
        let runtime = app_state
            .repo_runtime
            .entry(item.id.clone())
            .or_insert_with(|| RepoRuntimeState::new(now, &item.id));
        let scanned_seq = scan_seqs.get(&item.id).copied().unwrap_or(0);
        if runtime.invalidation_seq == scanned_seq {
            runtime.last_scanned_seq = runtime.last_scanned_seq.max(scanned_seq);
            runtime.scheduled_scan_seq = runtime.scheduled_scan_seq.max(scanned_seq);
            runtime.last_local_scan_at = Some(now);
            merged.push(item);
            continue;
        }
        needs_rescan |= runtime.needs_rescan();
        merged.push(previous_items.remove(&item.id).unwrap_or(item));
    }
    app_state.repository_items = merged;
    needs_rescan
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

/// Invalidates the repo and claims a scan for the new invalidation, unless one is
/// already in flight (its completion then issues a follow-up scan).
fn begin_repo_status_refresh(app_state: &mut AppState, repo_id: &str) -> bool {
    mark_repo_stale(app_state, repo_id);
    let runtime = app_state
        .repo_runtime
        .get_mut(repo_id)
        .expect("mark_repo_stale creates runtime state");
    if runtime.local_refresh_in_flight {
        return false;
    }
    runtime.local_refresh_in_flight = true;
    runtime.scheduled_scan_seq = runtime.invalidation_seq;
    true
}

/// Status refreshes must go through the invalidation sequence: submitting a
/// job directly would let a later watcher scan coalesce into it and record
/// state read before the change as up to date.
fn request_repository_status_refresh(item: &RepositoryListItem) {
    let should_submit = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        begin_repo_status_refresh(&mut app_state, &item.id)
    };
    if should_submit {
        schedule_repository_status_refresh(item);
    }
}

fn mark_repos_stale(app_state: &mut AppState, repo_ids: &HashSet<String>) {
    for repo_id in repo_ids {
        mark_repo_stale(app_state, repo_id);
    }
}

fn schedule_pending_local_rescans() {
    let scheduled = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let app_state = &mut *app_state;
        let mut scheduled = Vec::new();
        for item in &app_state.repository_items {
            let Some(runtime) = app_state.repo_runtime.get_mut(&item.id) else {
                continue;
            };
            if runtime.local_refresh_in_flight || !runtime.needs_rescan() {
                continue;
            }
            runtime.local_refresh_in_flight = true;
            runtime.scheduled_scan_seq = runtime.invalidation_seq;
            scheduled.push(item.clone());
        }
        scheduled
    };

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
        claim_due_remote_fetches(&mut app_state, SystemTime::now())
    };

    for (repo_id, repo_name, repo_path) in scheduled {
        schedule_repository_remote_fetch(&repo_id, &repo_name, repo_path);
    }
}

/// Selects the repos whose remote fetch is due, earliest first, up to the free
/// fetch capacity, and marks only those as in flight.
fn claim_due_remote_fetches(
    app_state: &mut AppState,
    now: SystemTime,
) -> Vec<(String, String, PathBuf)> {
    // Background fetches would race the running batch's git commands.
    if app_state.operation_in_flight.is_some() {
        return Vec::new();
    }
    let inflight = app_state
        .repo_runtime
        .values()
        .filter(|runtime| runtime.remote_fetch_in_flight)
        .count();
    let capacity = REMOTE_FETCH_CONCURRENCY.saturating_sub(inflight);
    if capacity == 0 {
        return Vec::new();
    }

    let mut due = app_state
        .repository_items
        .iter()
        .filter(|item| {
            !matches!(item.status.state, ronomepo_core::RepositoryState::Missing)
                && !matches!(
                    item.status.sync,
                    ronomepo_core::RepositorySync::NoUpstream
                        | ronomepo_core::RepositorySync::Unknown
                )
        })
        .filter_map(|item| {
            let runtime = app_state.repo_runtime.get(&item.id)?;
            if runtime.remote_fetch_in_flight || runtime.next_fetch_due_at > now {
                return None;
            }
            Some((runtime.next_fetch_due_at, item))
        })
        .collect::<Vec<_>>();
    due.sort_by_key(|(due_at, _)| *due_at);
    due.truncate(capacity);
    let claimed = due
        .into_iter()
        .map(|(_, item)| {
            (
                item.id.clone(),
                item.name.clone(),
                item.status.repo_path.clone(),
            )
        })
        .collect::<Vec<_>>();

    for (repo_id, _, _) in &claimed {
        if let Some(runtime) = app_state.repo_runtime.get_mut(repo_id) {
            runtime.remote_fetch_in_flight = true;
        }
    }
    claimed
}

fn schedule_workspace_scan() {
    diag::log("workspace scan requested");
    let (workspace_root, manifest) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.repository_items_loading {
            app_state.repository_items_refresh_pending = true;
            return;
        }
        app_state.repository_items_loading = true;
        app_state.repository_items_refresh_pending = false;
        app_state.workspace_scan_seqs = app_state
            .repo_runtime
            .iter()
            .map(|(repo_id, runtime)| (repo_id.clone(), runtime.invalidation_seq))
            .collect();
        (app_state.workspace_root.clone(), app_state.manifest.clone())
    };

    if let Err(message) = submit_coalesced_job(
        JobKey::WorkspaceScan,
        WorkerJob::WorkspaceScan {
            workspace_root,
            manifest,
        },
    ) {
        state()
            .lock()
            .expect("state mutex poisoned")
            .repository_items_loading = false;
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
        Ok(false) => {
            // An older scan of this repo is still running and will be credited
            // with scheduled_scan_seq on completion. It may have read the repo
            // before the latest invalidation, so roll the claim back: the repo
            // stays stale and that completion schedules the follow-up.
            let mut app_state = state().lock().expect("state mutex poisoned");
            if let Some(runtime) = app_state.repo_runtime.get_mut(&repo_id) {
                runtime.scheduled_scan_seq = runtime.last_scanned_seq;
                runtime.local_refresh_in_flight = true;
            }
        }
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

/// Watch syncs rerun on every manifest change and rescan, so only log when the
/// outcome differs from the previous sync instead of repeating it each time.
fn log_watch_sync_outcome(failure: Option<String>, unreadable: Vec<PathBuf>) {
    let mut messages = Vec::new();
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.last_watch_failure != failure {
            match &failure {
                Some(message) => {
                    messages.push(format!("Repository watcher setup failed: {message}"))
                }
                None => messages.push("Repository watcher recovered.".to_string()),
            }
            app_state.last_watch_failure = failure;
        }
        if app_state.last_watch_failure.is_none()
            && app_state.last_unreadable_watch_paths != unreadable
        {
            for path in &unreadable {
                if !app_state.last_unreadable_watch_paths.contains(path) {
                    messages.push(format!(
                        "Repository watcher skipping unreadable directory: {}",
                        path.display()
                    ));
                }
            }
            app_state.last_unreadable_watch_paths = unreadable;
        }
    }
    for message in messages {
        append_log(message);
    }
}

fn sync_watch_manager_from_state() {
    sync_watch_manager(false);
}

/// Rebuilds the watch manager in the background. Unless `force`d, this is a
/// no-op when the set of watched roots has not changed.
fn sync_watch_manager(force: bool) {
    let manifest = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.watch_manager_sync_in_flight {
            collapse_watch_manager_sync(&mut app_state, force);
            return;
        }
        app_state.manifest.clone()
    };
    // Checks the filesystem, so keep it outside the state lock. Syncs only
    // start on the main thread, so nothing claims one in between.
    let source = manifest.as_ref().map(watch_source);
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if !claim_watch_manager_sync(&mut app_state, force, source.as_ref()) {
            return;
        }
    }

    WATCH_SYNC_REQUESTS.fetch_add(1, Ordering::Relaxed);
    let sync_seq = WATCH_MANAGER_SYNC_SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    if let Err(error) = submit_coalesced_job(
        JobKey::WatchManagerSync,
        WorkerJob::WatchManagerSync { manifest, sync_seq },
    ) {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.watch_manager_sync_in_flight = false;
        app_state.watch_manager_sync_pending = false;
        app_state.watch_manager_sync_pending_force = false;
        drop(app_state);
        WATCH_SYNC_FAILURES.fetch_add(1, Ordering::Relaxed);
        append_log(format!("Repository watcher setup failed to start: {error}"));
        emit_runtime_profile("watch-sync-submit-failed");
    }
}

/// Records a sync requested while another runs; it reruns on completion, and
/// stays forced if any collapsed request was (e.g. after an inotify overflow).
fn collapse_watch_manager_sync(app_state: &mut AppState, force: bool) {
    app_state.watch_manager_sync_pending = true;
    app_state.watch_manager_sync_pending_force |= force;
    WATCH_SYNC_PENDING_COLLAPSES.fetch_add(1, Ordering::Relaxed);
}

/// Marks a sync as in flight unless one already is, or it would rebuild the
/// installed manager from the same source without being forced.
fn claim_watch_manager_sync(
    app_state: &mut AppState,
    force: bool,
    source: Option<&WatchSource>,
) -> bool {
    if app_state.watch_manager_sync_in_flight {
        collapse_watch_manager_sync(app_state, force);
        return false;
    }
    if !force && source.is_some() && app_state.watch_source.as_ref() == source {
        return false;
    }
    app_state.watch_manager_sync_in_flight = true;
    app_state.watch_manager_sync_pending = false;
    app_state.watch_manager_sync_pending_force = false;
    true
}

fn build_watch_manager(manifest: &WorkspaceManifest) -> Result<WatchManager, String> {
    let started = Instant::now();
    diag::log(format!(
        "watch manager build started repos={}",
        manifest.repos.len()
    ));
    let result = build_watch_manager_inner(manifest);
    diag::log(format!(
        "watch manager build finished in {}ms ok={} roots={} unreadable={} error={:?}",
        started.elapsed().as_millis(),
        result.is_ok(),
        result.as_ref().map_or(0, |m| m.watched_paths),
        result.as_ref().map_or(0, |m| m.unreadable_paths.len()),
        result.as_ref().err(),
    ));
    result
}

fn watch_source(manifest: &WorkspaceManifest) -> WatchSource {
    let workspace_root = normalized_watch_path(&manifest.root);
    let workspace_git_dir = Some(workspace_root.join(".git")).filter(|path| path.exists());
    let mut repo_roots = manifest
        .repos
        .iter()
        .map(|repo| normalized_watch_path(&workspace_root.join(&repo.dir_name)))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    repo_roots.sort();
    repo_roots.dedup();
    WatchSource {
        workspace_root,
        workspace_git_dir,
        repo_roots,
    }
}

fn build_watch_manager_inner(manifest: &WorkspaceManifest) -> Result<WatchManager, String> {
    let source = watch_source(manifest);
    let (mut backend, backend_kind) = create_watch_backend(Arc::new(WatchRouting::new(manifest)))?;
    let mut watched_paths = 0;
    let mut unreadable_paths = Vec::new();
    let mut ignored_dirs = HashSet::new();

    if source.workspace_root.exists() {
        watch_backend_mut(&mut backend)
            .watch(&source.workspace_root, RecursiveMode::NonRecursive)
            .map_err(|error| format!("{}: {error}", source.workspace_root.display()))?;
        watched_paths += 1;
    }
    if let Some(git_dir) = &source.workspace_git_dir {
        watched_paths += watch_dir_tree(
            watch_backend_mut(&mut backend),
            git_dir,
            &ignored_dirs,
            &mut unreadable_paths,
        )
        .map_err(|error| format!("{}: {error}", git_dir.display()))?;
    }

    for repo_root in &source.repo_roots {
        ignored_dirs.extend(git_ignored_dirs(repo_root, repo_root));
        watched_paths += watch_dir_tree(
            watch_backend_mut(&mut backend),
            repo_root,
            &ignored_dirs,
            &mut unreadable_paths,
        )
        .map_err(|error| format!("{}: {error}", repo_root.display()))?;
    }

    if watched_paths == 0 {
        return Err("no local repositories are available to watch".to_string());
    }

    Ok(WatchManager {
        backend,
        backend_kind,
        watched_paths,
        unreadable_paths,
        source,
        ignored_dirs,
    })
}

/// Git-ignored directories at or below `dir` inside `repo_root`, as absolute
/// paths. Build outputs, dependency caches and toolchains live there and can
/// hold 100k+ directories, none of which affect repository status.
fn git_ignored_dirs(repo_root: &Path, dir: &Path) -> Vec<PathBuf> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args([
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--directory",
            "--",
        ])
        .arg(dir)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output();
    let Ok(output) = output else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter_map(|entry| std::str::from_utf8(entry).ok())
        .filter_map(|entry| entry.strip_suffix('/'))
        .map(|relative| repo_root.join(relative))
        .collect()
}

/// Git internals that change constantly but never affect status on their own.
fn is_unwatched_git_internal_dir(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name == "objects" || name == "lfs")
        && path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == ".git")
}

fn is_under_ignored_dir(ignored: &HashSet<PathBuf>, path: &Path) -> bool {
    !ignored.is_empty() && path.ancestors().any(|ancestor| ignored.contains(ancestor))
}

/// Watches `root` and its subdirectories one directory at a time, skipping
/// `ignored` subtrees, `.git/objects`, symlinks (matching
/// `follow_symlinks(false)`) and directories the current user cannot read
/// (e.g. root-owned capture dirs inside a repo). Returns how many directories
/// were registered. Only a failure on `root` itself is an error.
fn watch_dir_tree(
    watcher: &mut dyn Watcher,
    root: &Path,
    ignored: &HashSet<PathBuf>,
    unreadable: &mut Vec<PathBuf>,
) -> notify::Result<usize> {
    let dirs = collect_dir_tree(root, ignored, unreadable).map_err(notify::Error::io)?;
    watch_dirs(watcher, dirs, Some(root), unreadable)
}

/// Lists `root` and its subdirectories (root first) for `watch_dir_tree`,
/// recording unreadable ones. Only failing to read `root` itself is an error.
fn collect_dir_tree(
    root: &Path,
    ignored: &HashSet<PathBuf>,
    unreadable: &mut Vec<PathBuf>,
) -> std::io::Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let is_root = dir == root;
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                unreadable.push(dir);
                continue;
            }
            Err(error) if is_root => return Err(error),
            // Vanished while walking.
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            if !entry.file_type().is_ok_and(|file_type| file_type.is_dir()) {
                continue;
            }
            let path = entry.path();
            if ignored.contains(&path) || is_unwatched_git_internal_dir(&path) {
                continue;
            }
            stack.push(path);
        }
        dirs.push(dir);
    }
    Ok(dirs)
}

/// Registers each directory non-recursively and returns how many were. Only a
/// failure on `root` is an error; unreadable directories are recorded.
fn watch_dirs(
    watcher: &mut dyn Watcher,
    dirs: Vec<PathBuf>,
    root: Option<&Path>,
    unreadable: &mut Vec<PathBuf>,
) -> notify::Result<usize> {
    let mut registered = 0;
    for dir in dirs {
        match watcher.watch(&dir, RecursiveMode::NonRecursive) {
            Ok(()) => registered += 1,
            Err(error) if is_permission_denied(&error) => unreadable.push(dir),
            Err(error) if Some(dir.as_path()) == root => return Err(error),
            // Vanished since it was listed.
            Err(_) => {}
        }
    }
    Ok(registered)
}

/// Registers directories created after the watch manager was built, along with
/// anything already inside them, unless they are git-ignored. Runs on a worker:
/// it spawns git, and the inotify backend cannot be driven from its own
/// event callback. The walk and the git calls run without the watch manager
/// lock, which is only held to register the result.
fn watch_new_dirs(mut dirs: Vec<PathBuf>) {
    let (source, mut ignored) = {
        let slot = watch_manager()
            .lock()
            .expect("watch manager mutex poisoned");
        let Some(manager) = slot.as_ref() else {
            return;
        };
        (manager.source.clone(), manager.ignored_dirs.clone())
    };
    // Parents first, so a collected parent's walk covers its children.
    dirs.sort_by_key(|dir| dir.components().count());
    let mut new_ignored = Vec::new();
    let mut collected = HashSet::new();
    let mut to_watch = Vec::new();
    let mut unreadable = Vec::new();
    for dir in dirs {
        let dir = normalized_watch_path(&dir);
        if collected.contains(&dir)
            || !dir.is_dir()
            || is_under_ignored_dir(&ignored, &dir)
            || dir.ancestors().any(is_unwatched_git_internal_dir)
        {
            continue;
        }
        let repo_root = source
            .repo_roots
            .iter()
            .filter(|root| dir.starts_with(root) && dir != **root)
            .max_by_key(|root| root.components().count());
        let in_workspace_git = source
            .workspace_git_dir
            .as_ref()
            .is_some_and(|git_dir| dir.starts_with(git_dir));
        if let Some(repo_root) = repo_root {
            let found = git_ignored_dirs(repo_root, &dir);
            ignored.extend(found.iter().cloned());
            new_ignored.extend(found);
            if ignored.contains(&dir) {
                continue;
            }
        } else if !in_workspace_git {
            // New top-level workspace entries (e.g. a fresh clone) are picked up
            // by the full resync that follows the manifest/scan change.
            continue;
        }
        let Ok(tree) = collect_dir_tree(&dir, &ignored, &mut unreadable) else {
            continue;
        };
        for path in tree {
            if collected.insert(path.clone()) {
                to_watch.push(path);
            }
        }
    }
    if to_watch.is_empty() && new_ignored.is_empty() && unreadable.is_empty() {
        return;
    }

    let mut slot = watch_manager()
        .lock()
        .expect("watch manager mutex poisoned");
    // A rebuild may have replaced the manager meanwhile; registering on the
    // new one is still right (re-watching a directory is harmless).
    let Some(manager) = slot.as_mut() else {
        return;
    };
    manager.ignored_dirs.extend(new_ignored);
    let added = watch_dirs(
        watch_backend_mut(&mut manager.backend),
        to_watch,
        None,
        &mut unreadable,
    )
    .unwrap_or(0);
    manager.unreadable_paths.extend(unreadable);
    if added > 0 {
        manager.watched_paths += added;
        WATCHED_PATH_COUNT.store(manager.watched_paths, Ordering::Relaxed);
        diag::log(format!(
            "watching {added} new directories (total {})",
            manager.watched_paths
        ));
    }
}

fn is_permission_denied(error: &notify::Error) -> bool {
    matches!(
        &error.kind,
        notify::ErrorKind::Io(io_error) if io_error.kind() == std::io::ErrorKind::PermissionDenied
    )
}

/// Watch roots normalized once per watch backend: the notify thread routes
/// every event path through them.
struct WatchRouting {
    workspace_root: PathBuf,
    workspace_git_dir: PathBuf,
    /// Repo ids and roots, deepest roots first so nested repos win.
    repos: Vec<(String, PathBuf)>,
}

impl WatchRouting {
    fn new(manifest: &WorkspaceManifest) -> Self {
        let workspace_root = normalized_watch_path(&manifest.root);
        let mut repos = manifest
            .repos
            .iter()
            .map(|repo| {
                (
                    repo.id.clone(),
                    normalized_watch_path(&workspace_root.join(&repo.dir_name)),
                )
            })
            .collect::<Vec<_>>();
        // Stable, so equally deep roots keep manifest order.
        repos.sort_by_key(|(_, root)| std::cmp::Reverse(root.components().count()));
        Self {
            workspace_git_dir: workspace_root.join(".git"),
            workspace_root,
            repos,
        }
    }
}

fn create_watch_backend(
    routing: Arc<WatchRouting>,
) -> Result<(WatchBackend, WatchBackendKind), String> {
    let config = repository_watch_config();
    let poll_routing = Arc::clone(&routing);
    match RecommendedWatcher::new(
        move |event| dispatch_watch_event_result(&routing, event),
        config,
    ) {
        Ok(watcher) => Ok((
            WatchBackend::Recommended(watcher),
            WatchBackendKind::Recommended,
        )),
        Err(_) => PollWatcher::new(
            move |event| dispatch_watch_event_result(&poll_routing, event),
            config.with_poll_interval(Duration::from_secs(WATCH_POLL_FALLBACK_SECS)),
        )
        .map(|watcher| (WatchBackend::Poll(watcher), WatchBackendKind::Poll))
        .map_err(|error| error.to_string()),
    }
}

fn repository_watch_config() -> NotifyConfig {
    // Repository symlinks (for example Wine's z: -> /) must not make
    // recursive registration or polling traverse unrelated filesystem trees.
    NotifyConfig::default().with_follow_symlinks(false)
}

fn watch_backend_mut(backend: &mut WatchBackend) -> &mut dyn Watcher {
    match backend {
        WatchBackend::Recommended(watcher) => watcher,
        WatchBackend::Poll(watcher) => watcher,
    }
}

fn dispatch_watch_event_result(routing: &WatchRouting, event: notify::Result<notify::Event>) {
    let main_context = glib::MainContext::default();
    match event {
        Ok(event) => {
            if !watch_event_may_change_status(&event) {
                return;
            }
            WATCH_EVENTS_RECEIVED.fetch_add(1, Ordering::Relaxed);
            let should_schedule = {
                let mut pending = pending_watch_events()
                    .lock()
                    .expect("pending watch events mutex poisoned");
                record_watch_event(&mut pending, routing, &event);
                let pending_len = pending.repo_ids.len() + usize::from(pending.workspace_touched);
                PENDING_WATCH_REPOS.store(pending_len, Ordering::Relaxed);
                update_max_atomic(&PENDING_WATCH_REPOS_HIGH_WATER, pending_len);
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

/// The inotify backend also reports opens and read-only closes. Our own status
/// scans open files across every repo, so treating those as changes makes each
/// refresh trigger the next one (and floods the kernel queue into overflow).
fn watch_event_may_change_status(event: &notify::Event) -> bool {
    if event.need_rescan() {
        return true;
    }
    if event.paths.is_empty() {
        return false;
    }
    match event.kind {
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
        EventKind::Access(_) => false,
        _ => true,
    }
}

fn watch_event_may_add_dir(event: &notify::Event) -> bool {
    matches!(
        event.kind,
        EventKind::Create(CreateKind::Folder | CreateKind::Any)
            | EventKind::Modify(ModifyKind::Name(_))
    )
}

fn record_watch_event(
    pending: &mut PendingWatchEvents,
    routing: &WatchRouting,
    event: &notify::Event,
) {
    // The backend dropped events (e.g. inotify queue overflow) and reports it
    // as a path-less Rescan event.
    if event.need_rescan() {
        pending.backend_rescan = true;
    }
    if pending.backend_rescan {
        return;
    }
    if watch_event_may_add_dir(event) {
        for path in &event.paths {
            if fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir()) {
                pending.new_dirs.insert(path.clone());
            }
        }
    }
    for path in &event.paths {
        if workspace_watch_path_matches(routing, path) {
            pending.workspace_touched = true;
        }
        if let Some(repo_id) = repo_id_for_watch_path(routing, path) {
            pending.repo_ids.insert(repo_id);
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
    let mut timed = diag::timed("flush-watch-events");
    let pending = {
        let mut pending = pending_watch_events()
            .lock()
            .expect("pending watch events mutex poisoned");
        PENDING_WATCH_REPOS.store(0, Ordering::Relaxed);
        std::mem::take(&mut *pending)
    };

    timed.set_detail(format!(
        "backend_rescan={} workspace_touched={} repos={} total_events={}",
        pending.backend_rescan,
        pending.workspace_touched,
        pending.repo_ids.len(),
        WATCH_EVENTS_RECEIVED.load(Ordering::Relaxed)
    ));
    if pending.backend_rescan {
        diag::log("watcher backend requested rescan (dropped events / overflow)");
        handle_watch_backend_rescan();
        return;
    }
    if !pending.new_dirs.is_empty() {
        queue_watch_new_dirs(pending.new_dirs.into_iter().collect());
    }

    handle_watch_touches(pending.workspace_touched, pending.repo_ids);
}

fn queue_watch_new_dirs(dirs: Vec<PathBuf>) {
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if defer_watch_dirs_during_sync(&mut app_state, &dirs) {
            return;
        }
    }
    if let Err(error) = submit_job(WorkerJob::WatchNewDirs { dirs }) {
        append_log(format!(
            "Repository watcher could not add new directories: {error}"
        ));
    }
}

/// While a rebuild runs, its walk may already have passed where these
/// directories appeared, and anything registered on the outgoing manager is
/// dropped with it, so hold them until the new manager is installed.
fn defer_watch_dirs_during_sync(app_state: &mut AppState, dirs: &[PathBuf]) -> bool {
    if !app_state.watch_manager_sync_in_flight {
        return false;
    }
    app_state.deferred_watch_dirs.extend_from_slice(dirs);
    true
}

fn take_deferred_watch_dirs(app_state: &mut AppState) -> Vec<PathBuf> {
    if app_state.watch_manager_sync_in_flight {
        return Vec::new();
    }
    std::mem::take(&mut app_state.deferred_watch_dirs)
}

fn handle_watch_touches(workspace_touched: bool, mut repo_ids: HashSet<String>) {
    let any_touched = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.as_ref() else {
            return;
        };
        // Events routed by a watcher built for an older manifest may name repos
        // that no longer exist.
        repo_ids.retain(|repo_id| manifest.repos.iter().any(|repo| &repo.id == repo_id));
        mark_repos_stale(&mut app_state, &repo_ids);
        !repo_ids.is_empty()
    };

    if workspace_touched {
        let workspace_root = {
            let app_state = state().lock().expect("state mutex poisoned");
            app_state.workspace_root.clone()
        };
        schedule_workspace_root_status_refresh(workspace_root);
    }
    // Repos already scanning are skipped and get a follow-up on completion;
    // stale repos whose scan failed to start are retried here.
    if any_touched {
        schedule_pending_local_rescans();
    }
}

fn handle_watch_backend_rescan() {
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    mark_all_repos_stale();
    append_log(
        "Repository watcher backend dropped events (e.g. inotify queue overflow); forcing a full local refresh."
            .to_string(),
    );
    schedule_workspace_root_status_refresh(workspace_root);
    schedule_pending_local_rescans();
    // Dropped events may include directory creations that were never registered.
    sync_watch_manager(true);
}

fn workspace_watch_path_matches(routing: &WatchRouting, path: &Path) -> bool {
    let path = normalized_watch_path(path);

    if let Ok(relative) = path.strip_prefix(&routing.workspace_root) {
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

    path.strip_prefix(&routing.workspace_git_dir)
        .ok()
        .is_some_and(watch_path_is_relevant)
}

fn repo_id_for_watch_path(routing: &WatchRouting, path: &Path) -> Option<String> {
    let path = normalized_watch_path(path);
    routing
        .repos
        .iter()
        .find(|(_, repo_root)| {
            path.strip_prefix(repo_root)
                .is_ok_and(|relative| watch_path_is_relevant(relative))
        })
        .map(|(repo_id, _)| repo_id.clone())
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
    // Same as core's pull/push: never block on a credential or ssh prompt,
    // and give up on a remote that stops responding.
    let mut command = ronomepo_core::network_git_command(Some(repo_path));
    command.args(["fetch", "--quiet", "--all", "--prune"]);
    let output = ronomepo_core::run_with_timeout(command, ronomepo_core::NETWORK_GIT_TIMEOUT)
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_string())
    }
}

fn open_repo_manifest_editor(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    repo_id: &str,
    repo_name: &str,
) -> Result<(), String> {
    if host_ptr.is_null() {
        return Err(
            "Cannot open Repo Manifest Editor because the host handle is unavailable.".to_string(),
        );
    }
    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let instance_key = repo_id.to_string();
    let title = format!("{repo_name} Manifest");
    let mut request = OpenViewRequest::new(
        PLUGIN_ID,
        VIEW_REPO_MANIFEST_EDITOR,
        MzViewPlacement::Workbench,
    );
    request.instance_key = Some(&instance_key);
    request.requested_title = Some(&title);
    request.payload = instance_key.as_bytes();

    match host.open_view(&request) {
        Ok(_) => Ok(()),
        Err(status) => Err(format!(
            "Failed to open repo manifest editor for {repo_name}: {status:?}"
        )),
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
        Ok(_) => Ok(()),
        Err(status) => Err(format!(
            "Failed to open repo overview for {}: {status:?}",
            item.name
        )),
    }
}

fn attach_repo_monitor_context_menu(
    anchor: &impl IsA<gtk::Widget>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    // The menu is built when it opens and unparented once it closes: rows are
    // replaced on refresh, and a popover parented to each of them would leak.
    let open_menu: Rc<RefCell<Option<Popover>>> = Rc::default();
    anchor.connect_unrealize({
        let open_menu = Rc::clone(&open_menu);
        move |_| close_repo_context_menu(&open_menu, None)
    });
    let gesture = GestureClick::new();
    gesture.set_button(3);
    gesture.connect_pressed(move |gesture, _, x, y| {
        let Some(anchor) = gesture.widget() else {
            return;
        };
        if let Some(row) = anchor.parent().and_downcast::<ListBoxRow>() {
            if let Some(list) = row.parent().and_downcast::<ListBox>() {
                update_repo_context_selection(&list, &row);
            }
        }
        close_repo_context_menu(&open_menu, None);
        let popover = build_repo_context_menu(&anchor, host_ptr);
        popover.connect_closed({
            let open_menu = Rc::clone(&open_menu);
            move |popover| {
                // Let the click that closed the menu finish dispatching first.
                let open_menu = Rc::clone(&open_menu);
                let popover = popover.clone();
                glib::idle_add_local_once(move || {
                    close_repo_context_menu(&open_menu, Some(&popover));
                });
            }
        });
        *open_menu.borrow_mut() = Some(popover.clone());
        popover.set_pointing_to(Some(&Rectangle::new(x as i32, y as i32, 1, 1)));
        popover.popup();
    });
    anchor.add_controller(gesture);
}

/// Pops down and unparents the open context menu; with `only`, only when that
/// popover is still the open one.
fn close_repo_context_menu(open_menu: &RefCell<Option<Popover>>, only: Option<&Popover>) {
    let popover = {
        let mut open_menu = open_menu.borrow_mut();
        if only.is_some_and(|only| open_menu.as_ref() != Some(only)) {
            return;
        }
        open_menu.take()
    };
    if let Some(popover) = popover {
        popover.popdown();
        if popover.parent().is_some() {
            popover.unparent();
        }
    }
}

fn build_repo_context_menu(
    anchor: &gtk::Widget,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> Popover {
    let popover = Popover::new();
    popover.add_css_class(ui::class::POPUP);
    popover.set_autohide(true);
    popover.set_has_arrow(false);
    popover.set_position(PositionType::Bottom);
    popover.set_parent(anchor);

    populate_repo_context_menu(&popover, host_ptr);
    popover
}

/// Returns a closure that pops `popover` down `delay` after the last call
/// unless one of `hovered` is set by then. Calls restart one shared timeout
/// instead of stacking a new one per pointer movement.
fn hover_close_scheduler(
    popover: &Popover,
    delay: Duration,
    hovered: [Rc<Cell<bool>>; 2],
) -> Rc<dyn Fn()> {
    let popover = popover.downgrade();
    let pending: Rc<RefCell<Option<glib::SourceId>>> = Rc::default();
    Rc::new(move || {
        if let Some(source) = pending.borrow_mut().take() {
            source.remove();
        }
        let popover = popover.clone();
        let hovered = hovered.clone();
        let pending_for_timeout = Rc::clone(&pending);
        let source = glib::timeout_add_local_once(delay, move || {
            pending_for_timeout.borrow_mut().take();
            if hovered.iter().all(|hovered| !hovered.get()) {
                if let Some(popover) = popover.upgrade() {
                    popover.popdown();
                }
            }
        });
        *pending.borrow_mut() = Some(source);
    })
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

fn populate_repo_context_menu(popover: &Popover, host_ptr: *const maruzzella_sdk::ffi::MzHostApi) {
    let menu = GtkBox::new(Orientation::Vertical, 0);
    menu.add_css_class(ui::class::MENU);
    menu.set_size_request(160, -1);

    let selection = selected_repository_items_from_state();
    let root_hovered = Rc::new(Cell::new(false));
    let child_hovered = Rc::new(Cell::new(false));
    let schedule_close = hover_close_scheduler(
        popover,
        Duration::from_millis(140),
        [Rc::clone(&root_hovered), Rc::clone(&child_hovered)],
    );

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
        ui::set_fg_tone(&empty, ui::Tone::Muted);
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
    let can_run_capabilities = selection_has_implemented_capabilities(selection);
    if supported_actions.is_empty() && !can_run_capabilities {
        return false;
    }

    let submenu = GtkBox::new(Orientation::Vertical, 0);
    if can_run_capabilities {
        append_context_button(&submenu, popover, "Run Capability Checks", || {
            run_selected_repo_capability_checks();
        });
    }
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
        append_context_button(&submenu, popover, "Push (skip checks)", || {
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
    let header = clickable_button();
    header.set_halign(Align::Fill);
    header.set_hexpand(true);

    let row = GtkBox::new(Orientation::Horizontal, 8);
    row.set_halign(Align::Fill);
    row.set_hexpand(true);

    let label = Label::new(Some(title));
    label.set_xalign(0.0);
    label.set_hexpand(true);
    label.set_halign(Align::Start);

    let chevron = Image::from_icon_name("pan-end-symbolic");
    chevron.set_halign(Align::End);
    chevron.add_css_class(ui::class::MENU_CHEVRON);

    row.append(&label);
    row.append(&chevron);
    header.set_child(Some(&row));

    submenu.add_css_class(ui::class::MENU);
    submenu.set_size_request(180, -1);
    let submenu_popover = Popover::new();
    submenu_popover.add_css_class(ui::class::POPUP);
    submenu_popover.set_autohide(true);
    submenu_popover.set_has_arrow(false);
    submenu_popover.set_position(PositionType::Right);
    submenu_popover.set_parent(&header);
    submenu_popover.set_child(Some(submenu));

    let header_hovered = Rc::new(Cell::new(false));
    let submenu_hovered = Rc::new(Cell::new(false));

    let schedule_close = hover_close_scheduler(
        &submenu_popover,
        Duration::from_millis(120),
        [Rc::clone(&header_hovered), Rc::clone(&submenu_hovered)],
    );

    // Closures attached inside the popovers hold them weakly so the menus do
    // not keep themselves alive after being unparented.
    let popup = submenu_popover.downgrade();
    let header_hovered_for_enter = Rc::clone(&header_hovered);
    let motion = EventControllerMotion::new();
    motion.connect_enter(move |_, _, _| {
        header_hovered_for_enter.set(true);
        if let Some(popup) = popup.upgrade() {
            popup.popup();
        }
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
    let popup = submenu_popover.downgrade();
    let motion = EventControllerMotion::new();
    motion.connect_enter(move |_, _, _| {
        submenu_hovered_for_enter.set(true);
        root_child_hovered_for_enter.set(true);
        if let Some(popup) = popup.upgrade() {
            popup.popup();
        }
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
        let submenu_popover = submenu_popover.downgrade();
        move |_| {
            let Some(submenu_popover) = submenu_popover.upgrade() else {
                return;
            };
            if submenu_popover.is_visible() {
                submenu_popover.popdown();
            } else {
                submenu_popover.popup();
            }
        }
    });

    header.connect_unrealize({
        let submenu_popover = submenu_popover.downgrade();
        let root_child_hovered = Rc::clone(root_child_hovered);
        move |_| {
            root_child_hovered.set(false);
            let Some(submenu_popover) = submenu_popover.upgrade() else {
                return;
            };
            submenu_popover.popdown();
            if submenu_popover.parent().is_some() {
                submenu_popover.unparent();
            }
        }
    });

    parent_popover.connect_closed({
        let submenu_popover = submenu_popover.downgrade();
        let root_child_hovered = Rc::clone(root_child_hovered);
        move |_| {
            root_child_hovered.set(false);
            if let Some(submenu_popover) = submenu_popover.upgrade() {
                submenu_popover.popdown();
            }
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

/// The repo manifest parsed by the last status scan, when it was valid.
fn scanned_repo_manifest(item: &RepositoryListItem) -> Option<&Arc<RepoManifest>> {
    let scan = item.repo_manifest.as_ref()?;
    matches!(scan.state, RepoManifestScanState::Valid(_))
        .then_some(scan.manifest.as_ref())
        .flatten()
}

fn selection_has_implemented_capabilities(selection: &[RepositoryListItem]) -> bool {
    selection.iter().any(|item| {
        scanned_repo_manifest(item).is_some_and(|manifest| {
            manifest.capabilities.iter().any(|capability| {
                capability.status == ronomepo_core::CapabilityDeclarationStatus::Implemented
            })
        })
    })
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
) -> Vec<(RepositoryListItem, Arc<RepoManifest>)> {
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
            let manifest = Arc::clone(scanned_repo_manifest(&item)?);
            Some((item, manifest))
        })
        .collect()
}

fn repo_capability_target_from_item(
    item: RepositoryListItem,
    capability_ids: Option<Vec<String>>,
) -> Option<CapabilityRunTarget> {
    let manifest = Arc::clone(scanned_repo_manifest(&item)?);
    if !manifest.capabilities.iter().any(|capability| {
        capability.status == ronomepo_core::CapabilityDeclarationStatus::Implemented
            && capability_ids
                .as_ref()
                .is_none_or(|ids| ids.iter().any(|id| id == &capability.id))
    }) {
        return None;
    }
    Some(CapabilityRunTarget {
        repo_id: item.id,
        repo_name: item.name,
        repo_path: item.status.repo_path,
        manifest,
        capability_ids,
    })
}

/// Capability run targets for the selected repos, or the whole workspace when
/// none is selected, together with that scope's name. `include` picks the
/// implemented capabilities to run; `None` runs all of them.
fn capability_check_targets_from_state(
    include: Option<
        &dyn Fn(&StateSnapshot, &RepositoryListItem, &RepoCapabilityDeclaration) -> bool,
    >,
) -> (Vec<CapabilityRunTarget>, &'static str) {
    let snapshot = snapshot();
    let items = repository_items(&snapshot);
    let selected = selected_repository_items(&snapshot, &items);
    let (source_items, scope) = if selected.is_empty() {
        (items, "workspace")
    } else {
        (selected, "selected")
    };
    let targets = source_items
        .into_iter()
        .filter_map(|item| {
            let Some(include) = include else {
                return repo_capability_target_from_item(item, None);
            };
            let manifest = Arc::clone(scanned_repo_manifest(&item)?);
            let capability_ids = implemented_capabilities(&manifest)
                .filter(|capability| include(&snapshot, &item, capability))
                .map(|capability| capability.id.clone())
                .collect::<Vec<_>>();
            repo_capability_target_from_item(item, Some(capability_ids))
        })
        .collect();
    (targets, scope)
}

fn implemented_capabilities(
    manifest: &RepoManifest,
) -> impl Iterator<Item = &RepoCapabilityDeclaration> {
    manifest.capabilities.iter().filter(|capability| {
        capability.status == ronomepo_core::CapabilityDeclarationStatus::Implemented
    })
}

/// Whether the capability has no recorded result or its result is stale.
fn capability_needs_rerun(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    capability: &RepoCapabilityDeclaration,
) -> bool {
    let dirty_now = matches!(
        item.status.state,
        ronomepo_core::RepositoryState::Dirty | ronomepo_core::RepositoryState::Untracked
    );
    capability_registry_state(snapshot, &item.id, &capability.id)
        .is_none_or(|state| capability_state_is_stale(item, state, dirty_now))
}

fn capability_is_due(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    capability: &RepoCapabilityDeclaration,
    now_epoch_secs: u64,
) -> bool {
    let state = capability_registry_state(snapshot, &item.id, &capability.id);
    capability_due_reason(item, capability, state, now_epoch_secs)
        .ok()
        .flatten()
        .is_some()
}

fn repo_due_capability_count(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    now_epoch_secs: u64,
) -> usize {
    let Some(manifest) = scanned_repo_manifest(item) else {
        return 0;
    };
    implemented_capabilities(manifest)
        .filter(|capability| capability_is_due(snapshot, item, capability, now_epoch_secs))
        .count()
}

fn submit_capability_check_targets(targets: Vec<CapabilityRunTarget>, scope: &str, mode: &str) {
    if targets.is_empty() {
        append_log(format!(
            "Capability checks skipped because no {scope} repo has {mode} implemented capabilities."
        ));
        return;
    }
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    let repo_count = targets.len();
    match submit_coalesced_job(
        JobKey::CapabilityChecks,
        WorkerJob::CapabilityChecks {
            workspace_root,
            targets,
            run_reason: mode.to_string(),
        },
    ) {
        Ok(true) => append_log(format!(
            "Running {mode} capability checks for {repo_count} {scope} repo(s)."
        )),
        Ok(false) => append_log("Capability checks are already running.".to_string()),
        Err(message) => append_log(format!("Capability checks failed to start: {message}")),
    }
    refresh_views();
}

fn run_selected_repo_capability_checks() {
    let (targets, scope) = capability_check_targets_from_state(None);
    submit_capability_check_targets(targets, scope, "all");
}

fn run_stale_repo_capability_checks() {
    let (targets, scope) = capability_check_targets_from_state(Some(&capability_needs_rerun));
    submit_capability_check_targets(targets, scope, "stale");
}

fn run_due_repo_capability_checks() {
    let now = current_epoch_secs();
    let (targets, scope) =
        capability_check_targets_from_state(Some(&|snapshot, item, capability| {
            capability_is_due(snapshot, item, capability, now)
        }));
    submit_capability_check_targets(targets, scope, "due");
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

    let repo_count = targets.len();
    match submit_job(WorkerJob::RepoManifestActions { action, targets }) {
        Ok(()) => append_log(format!(
            "Queued Ronomepo {} for {repo_count} repo(s).",
            standard_action_label(action)
        )),
        Err(message) => append_log(format!(
            "Ronomepo {} failed to start: {message}",
            standard_action_label(action)
        )),
    }
}

/// Runs a context-menu manifest action on a worker thread, logging progress
/// and command output to the Operations log as it goes.
fn run_repo_manifest_actions(
    action: StandardActionName,
    targets: Vec<(RepositoryListItem, Arc<RepoManifest>)>,
) {
    let main_context = glib::MainContext::default();
    let log = |message: String| main_context.invoke(move || append_log(message));
    let label = standard_action_label(action);

    for (item, manifest) in targets {
        match action {
            StandardActionName::ListArtifacts => {
                match list_repo_artifacts(&item.status.repo_path, &manifest) {
                    Ok(artifacts) if artifacts.is_empty() => log(format!(
                        "Ronomepo list_artifacts for {} found no artifacts.",
                        item.name
                    )),
                    Ok(artifacts) => {
                        log(format!(
                            "Ronomepo list_artifacts for {}: {} artifact(s).",
                            item.name,
                            artifacts.len()
                        ));
                        for artifact in artifacts {
                            log(format!(
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
                    Err(error) => log(format!(
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
                        log(format!(
                            "Ronomepo verify_dependencies_freshness for {}: {} report(s), {} finding(s).",
                            item.name,
                            reports.len(),
                            findings
                        ));
                        for report in reports {
                            if report.findings.is_empty() {
                                log(format!("  [{}] no issues found.", report.item_id));
                            } else {
                                for finding in report.findings {
                                    log(format!("  [{}] {}", report.item_id, finding.message));
                                }
                            }
                        }
                    }
                    Err(error) => log(format!(
                        "Ronomepo verify_dependencies_freshness failed for {}: {error}",
                        item.name
                    )),
                }
            }
            _ => match plan_repo_action(&item.status.repo_path, &manifest, action) {
                Ok(plan) => {
                    log(format!(
                        "Running Ronomepo {label} for {} ({} step(s)).",
                        item.name,
                        plan.steps.len()
                    ));
                    for step in plan.steps {
                        match step.executor {
                            RepoActionExecutor::Command(command) => {
                                let succeeded = run_planned_repo_command(&item.name, action, &command, &log);
                                if !succeeded {
                                    log(format!(
                                        "Ronomepo {label} for {} stopped after a failed step.",
                                        item.name
                                    ));
                                    break;
                                }
                            }
                            RepoActionExecutor::BuiltInInspector => log(format!(
                                "Ronomepo {label} for {} uses a built-in inspector and has no command to run.",
                                item.name
                            )),
                        }
                    }
                }
                Err(error) => log(format!(
                    "Ronomepo {label} failed for {}: {error}",
                    item.name
                )),
            },
        }
    }
}

/// Number of trailing output lines copied into the Operations log per command.
const REPO_ACTION_OUTPUT_TAIL_LINES: usize = 20;

/// Runs one planned command to completion and logs its result and the tail of
/// its output. Returns whether it succeeded.
fn run_planned_repo_command(
    repo_name: &str,
    action: StandardActionName,
    command: &PlannedCommand,
    log: &impl Fn(String),
) -> bool {
    let label = standard_action_label(action);
    let command_text = planned_command_text(command);
    log(format!(
        "Started Ronomepo {label} for {repo_name}: {command_text} (cwd: {}).",
        command.workdir.display()
    ));
    let output = match run_planned_command_to_output(command) {
        Ok(output) => output,
        Err(error) => {
            log(format!("Ronomepo {label} for {repo_name} failed: {error}"));
            return false;
        }
    };
    let succeeded = output.status.success();
    log(format!(
        "Ronomepo {label} for {repo_name} {} ({}).",
        if succeeded { "succeeded" } else { "failed" },
        output.status
    ));
    for line in command_output_tail(&output, REPO_ACTION_OUTPUT_TAIL_LINES) {
        log(format!("  {line}"));
    }
    succeeded
}

/// The last `limit` non-empty lines of stdout followed by stderr.
fn command_output_tail(output: &Output, limit: usize) -> Vec<String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let lines = stdout
        .lines()
        .chain(stderr.lines())
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();
    lines[lines.len().saturating_sub(limit)..]
        .iter()
        .map(|line| line.to_string())
        .collect()
}

fn append_context_button<F>(menu: &GtkBox, popover: &Popover, label: &str, action: F)
where
    F: Fn() + 'static,
{
    let button = clickable_button();
    button.set_halign(Align::Fill);
    button.set_hexpand(true);
    let text = Label::new(Some(label));
    text.set_xalign(0.0);
    text.set_halign(Align::Start);
    text.set_hexpand(true);
    button.set_child(Some(&text));
    let popover = popover.downgrade();
    button.connect_clicked(move |_| {
        if let Some(popover) = popover.upgrade() {
            popover.popdown();
        }
        action();
    });
    menu.append(&button);
}

fn create_repo_monitor_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let content = GtkBox::new(Orientation::Vertical, 0);
    content.set_hexpand(true);
    content.set_valign(Align::Fill);
    content.set_vexpand(true);

    let initial_snapshot = snapshot();
    // Set while a refresh mirrors the state into the controls, so their
    // handlers do not treat it as user input.
    let syncing = Rc::new(Cell::new(false));
    let mut filter_buttons = Vec::new();
    let mut sort_dropdowns = Vec::new();

    // Tool window header: title, view tabs and the sort controls.
    let header = ui::tool_window_header("Repositories", 0);

    let mut first_view_button: Option<ToggleButton> = None;
    let mut view_buttons = Vec::new();
    for view in monitor_view_modes() {
        let button = ui::tab_button(monitor_view_mode_label(*view));
        if let Some(first) = first_view_button.as_ref() {
            button.set_group(Some(first));
        } else {
            first_view_button = Some(button.clone());
        }
        button.set_active(initial_snapshot.monitor_view_mode == *view);
        view_buttons.push((*view, button.clone()));
        header.append(&button);
    }
    header.append(&ui::spacer());

    let filter_stack = Stack::new();
    filter_stack.set_hexpand(true);
    filter_stack.set_hhomogeneous(false);
    for view in monitor_view_modes() {
        let filter_box = ui::hbox(4);
        let active_filter = initial_snapshot.monitor_settings.view(*view).filter;
        let mut first_filter_button: Option<ToggleButton> = None;
        for filter_mode in monitor_filter_modes_for_view(*view) {
            let button = ui::chip_toggle(monitor_filter_mode_label(*filter_mode));
            if let Some(first) = first_filter_button.as_ref() {
                button.set_group(Some(first));
            } else {
                first_filter_button = Some(button.clone());
            }
            button.set_active(active_filter == *filter_mode);
            let filter_mode = *filter_mode;
            button.connect_toggled({
                let syncing = Rc::clone(&syncing);
                move |button| {
                    if button.is_active() && !syncing.get() {
                        update_monitor_filter_mode(filter_mode);
                        refresh_views();
                    }
                }
            });
            filter_buttons.push((*view, filter_mode, button.clone()));
            filter_box.append(&button);
        }
        filter_stack.add_named(&filter_box, Some(monitor_view_mode_key(*view)));
    }
    filter_stack.set_visible_child_name(monitor_view_mode_key(initial_snapshot.monitor_view_mode));

    let sort_stack = Stack::new();
    sort_stack.set_hhomogeneous(false);
    for view in monitor_view_modes() {
        let labels = monitor_sort_modes_for_view(*view)
            .iter()
            .map(|mode| monitor_sort_mode_label(*mode))
            .collect::<Vec<_>>();
        let dropdown = clickable_dropdown_from_strings(&labels);
        ui::style_dropdown(&dropdown);
        let active_sort = initial_snapshot.monitor_settings.view(*view).sort;
        dropdown.set_selected(monitor_sort_mode_index(*view, active_sort));
        dropdown.set_tooltip_text(Some("Sort repositories by the selected criterion"));
        let view = *view;
        dropdown.connect_selected_notify({
            let syncing = Rc::clone(&syncing);
            move |dropdown| {
                if syncing.get() {
                    return;
                }
                let mode = monitor_sort_mode_from_index(view, dropdown.selected());
                update_monitor_sort_mode(mode);
                // Sorting only affects the monitor lists, so skip the full refresh.
                refresh_repository_views(&snapshot());
            }
        });
        sort_stack.add_named(&dropdown, Some(monitor_view_mode_key(view)));
        sort_dropdowns.push((view, dropdown));
    }
    sort_stack.set_visible_child_name(monitor_view_mode_key(initial_snapshot.monitor_view_mode));
    header.append(&sort_stack);

    let sort_direction = Rc::new(Cell::new(
        active_monitor_settings(&initial_snapshot).descending,
    ));
    let sort_indicator = ui::icon_button(
        monitor_sort_direction_icon(sort_direction.get()),
        "Toggle sort direction",
    );
    let sort_indicator_icon = sort_indicator
        .child()
        .and_downcast::<Image>()
        .expect("icon buttons hold an image");
    sort_indicator.connect_clicked({
        let sort_direction = Rc::clone(&sort_direction);
        let sort_indicator_icon = sort_indicator_icon.clone();
        move |_| {
            let next = !sort_direction.get();
            sort_direction.set(next);
            sort_indicator_icon.set_icon_name(Some(monitor_sort_direction_icon(next)));
            update_monitor_sort_descending(next);
            refresh_repository_views(&snapshot());
        }
    });
    header.append(&sort_indicator);

    let header_stack = Stack::new();
    for view in monitor_view_modes() {
        header_stack.add_named(
            &repo_monitor_header_for_view(*view),
            Some(monitor_view_mode_key(*view)),
        );
    }
    header_stack.set_visible_child_name(monitor_view_mode_key(initial_snapshot.monitor_view_mode));

    for (view, button) in &view_buttons {
        let view = *view;
        let filter_stack = filter_stack.clone();
        let sort_stack = sort_stack.clone();
        let header_stack = header_stack.clone();
        let sort_direction = Rc::clone(&sort_direction);
        let sort_indicator_icon = sort_indicator_icon.clone();
        let syncing = Rc::clone(&syncing);
        button.connect_toggled(move |button| {
            if !button.is_active() || syncing.get() {
                return;
            }
            update_monitor_view_mode(view);
            filter_stack.set_visible_child_name(monitor_view_mode_key(view));
            sort_stack.set_visible_child_name(monitor_view_mode_key(view));
            header_stack.set_visible_child_name(monitor_view_mode_key(view));
            let snapshot = snapshot();
            let descending = active_monitor_settings(&snapshot).descending;
            sort_direction.set(descending);
            sort_indicator_icon.set_icon_name(Some(monitor_sort_direction_icon(descending)));
            refresh_views();
        });
    }

    let filter_bar = ui::hbox(0);
    filter_bar.add_css_class(ui::class::FILTER_BAR);
    filter_bar.append(&filter_stack);

    let store = gio::ListStore::new::<BoxedAnyObject>();
    let query = Rc::new(RefCell::new(monitor_query(&initial_snapshot)));
    let filter = CustomFilter::new({
        let query = Rc::clone(&query);
        move |object| {
            with_monitor_row(object, |row| monitor_row_matches(row, &query.borrow()))
                .unwrap_or(false)
        }
    });
    let filter_model = gtk::FilterListModel::new(Some(store.clone()), Some(filter.clone()));
    let sorter = CustomSorter::new({
        let query = Rc::clone(&query);
        move |left, right| {
            let query = query.borrow();
            with_monitor_row(left, |left| {
                with_monitor_row(right, |right| monitor_row_cmp(&query, left, right))
            })
            .flatten()
            .map_or(gtk::Ordering::Equal, Into::into)
        }
    });
    let sort_model = SortListModel::new(Some(filter_model.clone()), Some(sorter.clone()));

    let list = ListBox::new();
    list.add_css_class(ui::class::LIST);
    mark_clickable_list_rows(&list);
    list.set_hexpand(true);
    list.set_valign(Align::Start);
    list.set_selection_mode(SelectionMode::Multiple);
    list.bind_model(Some(&sort_model), move |object| {
        with_monitor_row(object, |row| build_repo_monitor_list_row(row, host))
            .unwrap_or_else(|| ListBoxRow::new())
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

    let scroller = vertical_view_scroller(&list);

    content.append(&header);
    content.append(&filter_bar);
    content.append(&header_stack);
    content.append(&scroller);

    let list_ref = glib::WeakRef::new();
    list_ref.set(Some(&list));
    let handle = RepositoryViewHandle {
        list: list_ref,
        controls: MonitorControls {
            syncing,
            view_buttons,
            filter_buttons,
            sort_dropdowns,
            sort_direction,
            sort_direction_icon: sort_indicator_icon,
            stacks: [filter_stack, sort_stack, header_stack],
        },
        store,
        filter_model,
        filter,
        sorter,
        query,
        rows: RefCell::new(Vec::new()),
        placeholder_for_manifest: Cell::new(None),
    };
    refresh_repository_view_handle(&handle, &list, &initial_snapshot);
    REPOSITORY_VIEWS.with(|views| {
        views.borrow_mut().push(handle);
    });

    into_view_ptr(content)
}

fn repo_monitor_header_for_view(view: MonitorViewMode) -> GtkBox {
    let header = ui::hbox(12);
    header.add_css_class(ui::class::LIST_HEADER);

    let mut labels = vec![monitor_cell("Name", None)];
    match view {
        MonitorViewMode::Overview => {
            labels.push(monitor_cell("Status", Some(MONITOR_STATE_COL_WIDTH)));
            labels.push(monitor_cell("Sync", Some(MONITOR_SYNC_COL_WIDTH)));
            labels.push(monitor_cell("Attention", Some(MONITOR_ATTENTION_COL_WIDTH)));
        }
        MonitorViewMode::Git => {
            labels.push(monitor_cell("Branch", Some(MONITOR_BRANCH_COL_WIDTH)));
            labels.push(monitor_cell("Status", Some(MONITOR_STATE_COL_WIDTH)));
            labels.push(monitor_cell("Sync", Some(MONITOR_SYNC_COL_WIDTH)));
        }
        MonitorViewMode::Checks => {
            labels.push(monitor_cell("Manifest", Some(MONITOR_MANIFEST_COL_WIDTH)));
            labels.push(monitor_cell("Checks", Some(MONITOR_CAPABILITY_COL_WIDTH)));
            labels.push(monitor_cell("Attention", Some(MONITOR_ATTENTION_COL_WIDTH)));
        }
        MonitorViewMode::Manifest => {
            labels.push(monitor_cell("Manifest", Some(MONITOR_MANIFEST_COL_WIDTH)));
            labels.push(monitor_cell("Actions", Some(MONITOR_ACTIONS_COL_WIDTH)));
            labels.push(monitor_cell("Enabled", Some(MONITOR_ENABLED_COL_WIDTH)));
            labels.push(monitor_cell("Remote", None));
        }
    }
    for label in labels {
        header.append(&label);
    }
    header
}

/// One ellipsizing monitor table cell: `width` pixels wide, or sharing the
/// remaining width when `None`.
fn monitor_cell(text: &str, width: Option<i32>) -> Label {
    let label = Label::new(Some(text));
    label.set_xalign(0.0);
    label.set_ellipsize(EllipsizeMode::End);
    // A tiny natural width keeps long values from widening their column, so
    // every row lines up with the header: fixed columns take their size
    // request, flexible ones share the remaining width equally.
    label.set_max_width_chars(1);
    match width {
        Some(width) => label.set_size_request(width, -1),
        None => {
            label.set_hexpand(true);
            label.set_size_request(96, -1);
        }
    }
    label
}

/// A monitor cell whose text is colored by `tone`.
fn monitor_tone_cell(text: &str, tone: ui::Tone, width: i32, tooltip: Option<String>) -> Label {
    let label = monitor_cell(text, Some(width));
    label.add_css_class(ui::class::META);
    ui::set_fg_tone(&label, tone);
    if let Some(tooltip) = tooltip {
        attach_text_tooltip(&label, tooltip);
    }
    label
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CapabilityMonitorSummary {
    declared: usize,
    stored: usize,
    ok: usize,
    warning: usize,
    failed: usize,
    error: usize,
    unknown: usize,
    running: usize,
    stale: usize,
    policy_issues: usize,
    unsupported: usize,
}

fn capability_monitor_summary(
    item: &RepositoryListItem,
    registry: &CapabilityRegistry,
) -> CapabilityMonitorSummary {
    if item.id == MONOREPO_ROW_ID {
        return CapabilityMonitorSummary::default();
    }

    let (declared, policy_issues, unsupported) =
        match item.repo_manifest.as_ref().map(|scan| &scan.state) {
            Some(RepoManifestScanState::Valid(summary)) => (
                summary.capability_count,
                summary.missing_required_capability_count,
                summary.unsupported_capability_count,
            ),
            _ => (0, 0, 0),
        };
    let mut summary = CapabilityMonitorSummary {
        declared,
        policy_issues,
        unsupported,
        ..CapabilityMonitorSummary::default()
    };
    let dirty_now = matches!(
        item.status.state,
        ronomepo_core::RepositoryState::Dirty | ronomepo_core::RepositoryState::Untracked
    );

    for state in registry
        .states
        .iter()
        .filter(|state| state.repo_id == item.id)
    {
        summary.stored += 1;
        let status = if capability_state_is_stale(item, state, dirty_now) {
            CapabilityResultStatus::Stale
        } else {
            state.status
        };
        match status {
            CapabilityResultStatus::Ok => summary.ok += 1,
            CapabilityResultStatus::Warning => summary.warning += 1,
            CapabilityResultStatus::Failed => summary.failed += 1,
            CapabilityResultStatus::Error => summary.error += 1,
            CapabilityResultStatus::Unknown => summary.unknown += 1,
            CapabilityResultStatus::Running => summary.running += 1,
            CapabilityResultStatus::Stale => summary.stale += 1,
        }
    }

    summary.unknown += declared.saturating_sub(summary.stored);
    summary
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CapabilityStaleReason {
    DirtyWorktree,
    CommitChanged,
}

fn capability_stale_reason(
    item: &RepositoryListItem,
    state: &CapabilityState,
    dirty_now: bool,
) -> Option<CapabilityStaleReason> {
    if dirty_now {
        return Some(CapabilityStaleReason::DirtyWorktree);
    }
    state
        .commit
        .as_ref()
        .zip(item.status.head_commit.as_ref())
        .and_then(|(recorded, current)| {
            (recorded != current).then_some(CapabilityStaleReason::CommitChanged)
        })
}

fn capability_state_is_stale(
    item: &RepositoryListItem,
    state: &CapabilityState,
    dirty_now: bool,
) -> bool {
    capability_stale_reason(item, state, dirty_now).is_some()
}

fn capability_stale_reason_label(reason: CapabilityStaleReason) -> &'static str {
    match reason {
        CapabilityStaleReason::DirtyWorktree => "dirty worktree",
        CapabilityStaleReason::CommitChanged => "commit changed",
    }
}

fn capability_monitor_label(summary: &CapabilityMonitorSummary) -> &'static str {
    if summary.policy_issues > 0 || summary.unsupported > 0 {
        "Policy!"
    } else if summary.declared == 0 && summary.stored == 0 {
        "-"
    } else if summary.error > 0 {
        "Error"
    } else if summary.failed > 0 {
        "Fail"
    } else if summary.stale > 0 {
        "Stale"
    } else if summary.warning > 0 {
        "Warn"
    } else if summary.running > 0 {
        "Run"
    } else if summary.unknown > 0 {
        "Unknown"
    } else {
        "OK"
    }
}

fn capability_monitor_tone(summary: &CapabilityMonitorSummary) -> ui::Tone {
    if summary.policy_issues > 0 || summary.unsupported > 0 {
        ui::Tone::Warning
    } else if summary.error > 0 || summary.failed > 0 {
        ui::Tone::Danger
    } else if summary.stale > 0 || summary.warning > 0 || summary.unknown > 0 {
        ui::Tone::Warning
    } else if summary.declared > 0 || summary.stored > 0 {
        ui::Tone::Success
    } else {
        ui::Tone::Muted
    }
}

fn capability_monitor_tooltip(summary: &CapabilityMonitorSummary) -> String {
    format!(
        "Capabilities: {} declared, {} stored result(s)\nOK: {} | Warning: {} | Failed: {} | Error: {} | Stale: {} | Unknown: {}\nPolicy issues: {} | Unsupported: {}",
        summary.declared,
        summary.stored,
        summary.ok,
        summary.warning,
        summary.failed,
        summary.error,
        summary.stale,
        summary.unknown,
        summary.policy_issues,
        summary.unsupported
    )
}

fn capability_monitor_search_text(summary: &CapabilityMonitorSummary) -> String {
    format!(
        "capabilities {} policy {} unsupported {} stale {} warning {} failed {} error {} unknown {}",
        capability_monitor_label(summary),
        summary.policy_issues,
        summary.unsupported,
        summary.stale,
        summary.warning,
        summary.failed,
        summary.error,
        summary.unknown
    )
}

fn repo_attention_signals(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> Vec<AttentionSignal> {
    derive_attention_signals(item, None, &snapshot.capability_registry)
}

fn repo_attention_detail_signals(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> Vec<AttentionSignal> {
    derive_attention_signals(
        item,
        scanned_repo_manifest(item).map(|manifest| &**manifest),
        &snapshot.capability_registry,
    )
}

fn attention_monitor_label(signals: &[AttentionSignal]) -> &'static str {
    match attention_rank(signals) {
        0 => "Blocked",
        1 => "Urgent",
        2 => "Should",
        3 => "Maybe",
        4 => "Info",
        _ => "OK",
    }
}

fn attention_monitor_tooltip(signals: &[AttentionSignal]) -> String {
    if signals.is_empty() {
        return "No open attention signals.".to_string();
    }

    let mut lines = vec![format!("{} open attention signal(s)", signals.len())];
    for signal in signals.iter().take(8) {
        lines.push(format!(
            "{} | {} | {} | {} | impact {} | {}",
            attention_level_label(signal.level),
            attention_kind_label(signal.kind),
            attention_source_label(signal.source),
            attention_urgency_label(signal.urgency),
            attention_impact_label(signal.impact),
            signal.summary
        ));
    }
    if signals.len() > 8 {
        lines.push(format!("+{} more", signals.len() - 8));
    }
    lines.join("\n")
}

fn attention_monitor_search_text(signals: &[AttentionSignal]) -> String {
    let mut text = format!("attention {}", attention_monitor_label(signals));
    for signal in signals {
        let _ = write!(
            text,
            " {} {} {} {} {}",
            attention_level_label(signal.level),
            attention_kind_label(signal.kind),
            attention_source_label(signal.source),
            attention_urgency_label(signal.urgency),
            signal.summary
        );
    }
    text
}

/// Attention is shown loudly only when it is blocking or urgent; routine
/// levels stay muted so they do not drown the list.
fn monitor_attention_cell(signals: &[AttentionSignal]) -> GtkBox {
    let cell = ui::hbox(0);
    cell.set_size_request(MONITOR_ATTENTION_COL_WIDTH, -1);
    let rank = attention_rank(signals);
    let text = if rank > 4 {
        ""
    } else {
        attention_monitor_label(signals)
    };
    let label = if rank <= 1 {
        ui::badge(text, ui::Tone::Danger)
    } else {
        let label = Label::new(Some(text));
        ui::set_fg_tone(&label, ui::Tone::Muted);
        label.add_css_class(ui::class::META);
        label
    };
    attach_text_tooltip(&label, attention_monitor_tooltip(signals));
    cell.append(&label);
    cell
}

/// Every open attention signal of the workspace as a dense Problems list:
/// severity icon, summary, then the repository and signal metadata.
fn append_attention_signals_section(
    container: &GtkBox,
    snapshot: &StateSnapshot,
    items: &[RepositoryListItem],
) {
    let mut signals = items
        .iter()
        .flat_map(|item| repo_attention_detail_signals(snapshot, item))
        .collect::<Vec<_>>();
    signals.sort_by(|left, right| {
        left.repo_name
            .cmp(&right.repo_name)
            .then_with(|| attention_level_label(left.level).cmp(attention_level_label(right.level)))
            .then_with(|| left.summary.cmp(&right.summary))
    });
    let count = ui::badge(&signals.len().to_string(), ui::Tone::Muted);
    if signals.is_empty() {
        container.append(&ui::section(
            "Attention Signals",
            &[count.upcast_ref()],
            &ui::hint("No open attention signals."),
        ));
        return;
    }
    let list = ui::lines_card();
    for signal in &signals {
        let row = ui::hbox(8);
        row.add_css_class(ui::class::LINE);
        let icon = ui::severity_icon(attention_severity(signal.level));
        icon.set_valign(Align::Center);
        icon.set_margin_top(0);
        row.append(&icon);
        let summary = Label::new(Some(&signal.summary));
        summary.set_xalign(0.0);
        summary.set_ellipsize(EllipsizeMode::End);
        ui::set_fg_tone(&summary, ui::Tone::Strong);
        row.append(&summary);
        let meta = ui::ellipsized(
            &format!(
                "{} · {} · {} · {} · {}",
                signal.repo_name,
                attention_level_label(signal.level),
                attention_kind_label(signal.kind),
                attention_source_label(signal.source),
                attention_urgency_label(signal.urgency),
            ),
            &[&ui::Tone::Muted.fg_class(), ui::class::META],
        );
        meta.set_hexpand(true);
        row.append(&meta);
        list.append(&row);
    }
    container.append(&ui::section(
        "Attention Signals",
        &[count.upcast_ref()],
        &list,
    ));
}

fn attention_level_label(level: AttentionLevel) -> &'static str {
    match level {
        AttentionLevel::Info => "info",
        AttentionLevel::Opportunity => "opportunity",
        AttentionLevel::ShouldDo => "should do",
        AttentionLevel::Urgent => "urgent",
        AttentionLevel::Blocked => "blocked",
    }
}

fn attention_urgency_label(urgency: AttentionUrgency) -> &'static str {
    match urgency {
        AttentionUrgency::Low => "low",
        AttentionUrgency::Medium => "medium",
        AttentionUrgency::High => "high",
    }
}

fn attention_impact_label(impact: AttentionImpact) -> &'static str {
    match impact {
        AttentionImpact::Low => "low",
        AttentionImpact::Medium => "medium",
        AttentionImpact::High => "high",
    }
}

fn attention_kind_label(kind: AttentionKind) -> &'static str {
    match kind {
        AttentionKind::Git => "git",
        AttentionKind::Integrity => "integrity",
        AttentionKind::Maintenance => "maintenance",
        AttentionKind::Security => "security",
        AttentionKind::Operations => "operations",
        AttentionKind::Planning => "planning",
        AttentionKind::Configuration => "configuration",
    }
}

fn attention_source_label(source: AttentionSource) -> &'static str {
    match source {
        AttentionSource::Git => "git",
        AttentionSource::Manifest => "manifest",
        AttentionSource::Capability => "capability",
    }
}

fn repo_capability_detail_lines(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> Vec<String> {
    let mut lines = Vec::new();
    let Some(scan) = item.repo_manifest.as_ref() else {
        return capability_registry_only_lines(snapshot, item);
    };
    let RepoManifestScanState::Valid(_) = &scan.state else {
        return capability_registry_only_lines(snapshot, item);
    };

    let Some(manifest) = scan.manifest.as_deref() else {
        lines.push("Manifest has not been loaded by a status scan yet.".to_string());
        lines.extend(capability_registry_only_lines(snapshot, item));
        return lines;
    };

    let policy = assess_repo_capability_policy(&manifest);
    for issue in policy.issues {
        lines.push(format!(
            "policy | {} | {} | {}",
            issue.repo_node_id, issue.capability, issue.message
        ));
    }

    let dirty_now = matches!(
        item.status.state,
        ronomepo_core::RepositoryState::Dirty | ronomepo_core::RepositoryState::Untracked
    );
    for capability in &manifest.capabilities {
        let state = capability_registry_state(snapshot, &item.id, &capability.id);
        match capability.status {
            ronomepo_core::CapabilityDeclarationStatus::Implemented => {
                if let Some(state) = state {
                    let stale_reason = capability_stale_reason(item, state, dirty_now);
                    let status = if stale_reason.is_some() {
                        CapabilityResultStatus::Stale
                    } else {
                        state.status
                    };
                    let summary = stale_reason
                        .map(|reason| {
                            format!(
                                "{} (stale: {})",
                                state.summary,
                                capability_stale_reason_label(reason)
                            )
                        })
                        .unwrap_or_else(|| state.summary.clone());
                    lines.push(format!(
                        "{} | {} | {} | {}",
                        capability.id,
                        capability.capability,
                        capability_status_label(status),
                        summary
                    ));
                    for finding in state.findings.iter().take(3) {
                        lines.push(format!(
                            "  finding | {} | {}",
                            capability_finding_severity_label(finding.severity),
                            finding.message
                        ));
                    }
                    if state.findings.len() > 3 {
                        lines.push(format!("  finding | +{} more", state.findings.len() - 3));
                    }
                } else {
                    lines.push(format!(
                        "{} | {} | unknown | no recorded result",
                        capability.id, capability.capability
                    ));
                }
            }
            ronomepo_core::CapabilityDeclarationStatus::NotApplicable => lines.push(format!(
                "{} | {} | not_applicable | {}",
                capability.id,
                capability.capability,
                capability.reason.as_deref().unwrap_or("no reason recorded")
            )),
            ronomepo_core::CapabilityDeclarationStatus::Unsupported => lines.push(format!(
                "{} | {} | unsupported | {}",
                capability.id,
                capability.capability,
                capability.reason.as_deref().unwrap_or("no reason recorded")
            )),
        }
    }

    let declared_ids = manifest
        .capabilities
        .iter()
        .map(|capability| capability.id.as_str())
        .collect::<HashSet<_>>();
    for state in snapshot
        .capability_registry
        .states
        .iter()
        .filter(|state| state.repo_id == item.id)
        .filter(|state| !declared_ids.contains(state.capability_instance_id.as_str()))
    {
        lines.push(format!(
            "{} | {} | {} | registry state has no current manifest declaration",
            state.capability_instance_id,
            state.capability,
            capability_status_label(state.status)
        ));
    }

    lines
}

fn capability_registry_only_lines(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> Vec<String> {
    snapshot
        .capability_registry
        .states
        .iter()
        .filter(|state| state.repo_id == item.id)
        .map(|state| {
            let dirty_now = matches!(
                item.status.state,
                ronomepo_core::RepositoryState::Dirty | ronomepo_core::RepositoryState::Untracked
            );
            let stale_reason = capability_stale_reason(item, state, dirty_now);
            let status = if stale_reason.is_some() {
                CapabilityResultStatus::Stale
            } else {
                state.status
            };
            let summary = stale_reason
                .map(|reason| {
                    format!(
                        "{} (stale: {})",
                        state.summary,
                        capability_stale_reason_label(reason)
                    )
                })
                .unwrap_or_else(|| state.summary.clone());
            format!(
                "{} | {} | {} | {}",
                state.capability_instance_id,
                state.capability,
                capability_status_label(status),
                summary
            )
        })
        .collect()
}

fn capability_registry_state<'a>(
    snapshot: &'a StateSnapshot,
    repo_id: &str,
    capability_instance_id: &str,
) -> Option<&'a CapabilityState> {
    snapshot.capability_registry.states.iter().find(|state| {
        state.repo_id == repo_id && state.capability_instance_id == capability_instance_id
    })
}

fn capability_finding_severity_label(severity: CapabilityFindingSeverity) -> &'static str {
    match severity {
        CapabilityFindingSeverity::Info => "info",
        CapabilityFindingSeverity::Warning => "warning",
        CapabilityFindingSeverity::Error => "error",
    }
}

fn repo_needs_capability_attention(snapshot: &StateSnapshot, item: &RepositoryListItem) -> bool {
    capability_summary_needs_attention(&capability_monitor_summary(
        item,
        &snapshot.capability_registry,
    ))
}

fn capability_summary_needs_attention(summary: &CapabilityMonitorSummary) -> bool {
    summary.policy_issues > 0
        || summary.unsupported > 0
        || summary.error > 0
        || summary.failed > 0
        || summary.warning > 0
        || summary.stale > 0
        || summary.unknown > 0
}

fn monitor_capability_cell(summary: &CapabilityMonitorSummary) -> Label {
    monitor_tone_cell(
        capability_monitor_label(summary),
        capability_monitor_tone(summary),
        MONITOR_CAPABILITY_COL_WIDTH,
        Some(capability_monitor_tooltip(summary)),
    )
}

fn monitor_manifest_cell(scan: Option<&RepoManifestScan>) -> Label {
    monitor_tone_cell(
        manifest_presence_label(scan),
        manifest_presence_tone(scan),
        MONITOR_MANIFEST_COL_WIDTH,
        manifest_presence_tooltip(scan),
    )
}

fn monitor_manifest_actions_cell(item: &RepositoryListItem) -> Label {
    let count = manifest_action_count(item);
    let (text, tone) = if count == 0 {
        ("-".to_string(), ui::Tone::Muted)
    } else {
        (count.to_string(), ui::Tone::Neutral)
    };
    monitor_tone_cell(
        &text,
        tone,
        MONITOR_ACTIONS_COL_WIDTH,
        Some(format!("{count} configured repo action(s)")),
    )
}

fn monitor_enabled_cell(enabled: Option<bool>) -> Label {
    let (text, tone) = match enabled {
        Some(true) => ("Enabled", ui::Tone::Neutral),
        Some(false) => ("Disabled", ui::Tone::Muted),
        None => ("-", ui::Tone::Muted),
    };
    monitor_tone_cell(text, tone, MONITOR_ENABLED_COL_WIDTH, None)
}

fn manifest_presence_tone(scan: Option<&RepoManifestScan>) -> ui::Tone {
    match scan.map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(summary))
            if summary.missing_required_capability_count > 0 =>
        {
            ui::Tone::Warning
        }
        Some(RepoManifestScanState::Valid(_)) => ui::Tone::Neutral,
        Some(RepoManifestScanState::Invalid { .. }) => ui::Tone::Danger,
        Some(RepoManifestScanState::Missing) | None => ui::Tone::Muted,
    }
}

fn monitor_state_cell(state: &ronomepo_core::RepositoryState) -> Label {
    monitor_tone_cell(
        status_label(state),
        state_tone(state),
        MONITOR_STATE_COL_WIDTH,
        None,
    )
}

/// The Status column tone. It matches the name colors: only states that need
/// action stand out, and untracked files alone are not alarming.
fn state_tone(state: &ronomepo_core::RepositoryState) -> ui::Tone {
    match state {
        ronomepo_core::RepositoryState::Clean | ronomepo_core::RepositoryState::Untracked => {
            ui::Tone::Muted
        }
        ronomepo_core::RepositoryState::Dirty => ui::Tone::Accent,
        ronomepo_core::RepositoryState::Missing | ronomepo_core::RepositoryState::Unknown => {
            ui::Tone::Danger
        }
    }
}

/// IDEA-style name colors, kept calm: modified names in the accent color,
/// diverged ones in the warning color, missing or unreadable repositories in
/// the danger color. Clean and untracked-only names keep the default color.
fn repo_name_tone(status: &RepositoryStatus) -> ui::Tone {
    match (&status.state, &status.sync) {
        (ronomepo_core::RepositoryState::Missing | ronomepo_core::RepositoryState::Unknown, _) => {
            ui::Tone::Danger
        }
        (_, ronomepo_core::RepositorySync::Diverged { .. }) => ui::Tone::Warning,
        (ronomepo_core::RepositoryState::Dirty, _) => ui::Tone::Accent,
        _ => ui::Tone::Strong,
    }
}

fn monitor_sync_cell(sync: &ronomepo_core::RepositorySync) -> Label {
    let (text, tone) = sync_compact(sync);
    monitor_tone_cell(
        &text,
        tone,
        MONITOR_SYNC_COL_WIDTH,
        Some(sync_description(sync)),
    )
}

/// Compact ahead/behind arrows, e.g. `↑2 ↓1`.
fn sync_compact(sync: &ronomepo_core::RepositorySync) -> (String, ui::Tone) {
    match sync {
        ronomepo_core::RepositorySync::UpToDate => ("✓".to_string(), ui::Tone::Muted),
        ronomepo_core::RepositorySync::Ahead(ahead) => (format!("↑{ahead}"), ui::Tone::Accent),
        ronomepo_core::RepositorySync::Behind(behind) => (format!("↓{behind}"), ui::Tone::Accent),
        ronomepo_core::RepositorySync::Diverged { ahead, behind } => {
            (format!("↑{ahead} ↓{behind}"), ui::Tone::Warning)
        }
        ronomepo_core::RepositorySync::NoUpstream => ("–".to_string(), ui::Tone::Muted),
        ronomepo_core::RepositorySync::Unknown => ("?".to_string(), ui::Tone::Muted),
    }
}

fn sync_description(sync: &ronomepo_core::RepositorySync) -> String {
    match sync {
        ronomepo_core::RepositorySync::UpToDate => "Up to date with upstream".to_string(),
        ronomepo_core::RepositorySync::Ahead(ahead) => format!("{ahead} commit(s) to push"),
        ronomepo_core::RepositorySync::Behind(behind) => format!("{behind} commit(s) to pull"),
        ronomepo_core::RepositorySync::Diverged { ahead, behind } => {
            format!("Diverged: {ahead} to push, {behind} to pull")
        }
        ronomepo_core::RepositorySync::NoUpstream => "No upstream branch".to_string(),
        ronomepo_core::RepositorySync::Unknown => "Sync state unknown".to_string(),
    }
}

fn create_monorepo_overview_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let root = padded_view_root(20, 24);
    root.set_vexpand(true);

    let scroller = vertical_view_scroller(&root);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    let view = Rc::new(MonorepoOverview {
        root: root_ref,
        top: GtkBox::new(Orientation::Vertical, 16),
        selection_slot: GtkBox::new(Orientation::Vertical, 0),
        files_slot: GtkBox::new(Orientation::Vertical, 0),
        sections: GtkBox::new(Orientation::Vertical, 24),
        reports: monorepo_report_controls(),
        host_ptr: host as usize,
        stale: Cell::new(false),
    });

    let command_area = GtkBox::new(Orientation::Vertical, 10);
    command_area.append(&overview_command_group(
        "Workspace",
        overview_actions(false),
    ));
    command_area.append(&overview_command_group(
        "Selection",
        view.selection_slot.clone(),
    ));
    command_area.append(&overview_command_group("Reports", view.reports.row.clone()));
    command_area.append(&overview_command_group("Files", view.files_slot.clone()));
    root.append(&view.top);
    root.append(&ui::card(&command_area));
    root.append(&view.sections);

    render_monorepo_overview(&view, &snapshot());
    // Refreshes skip the view while it is hidden; catch up when it shows.
    root.connect_map({
        let view = Rc::downgrade(&view);
        move |_| {
            let view = view.clone();
            glib::idle_add_local_once(move || {
                if let Some(view) = view.upgrade() {
                    if view.stale.get() {
                        render_monorepo_overview(&view, &snapshot());
                    }
                }
            });
        }
    });
    MONOREPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(view);
    });

    into_view_ptr(scroller)
}

fn create_commit_check_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = padded_view_root(18, 24);

    let scroller = vertical_view_scroller(&root);

    let editor = build_commit_check_view(&root, &snapshot());

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    COMMIT_CHECK_VIEWS.with(|views| {
        views.borrow_mut().push(CommitCheckViewHandle {
            root: root_ref,
            editor,
        });
    });

    into_view_ptr(scroller)
}

fn render_monorepo_overview(view: &MonorepoOverview, snapshot: &StateSnapshot) {
    view.stale.set(false);
    let host_ptr = view.host_ptr as *const maruzzella_sdk::ffi::MzHostApi;
    for slot in [
        &view.top,
        &view.selection_slot,
        &view.files_slot,
        &view.sections,
    ] {
        clear_box(slot);
    }

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
        .filter(|item| repo_attention_rank(snapshot, item) < 7)
        .count();
    let capability_attention = items
        .iter()
        .filter(|item| repo_needs_capability_attention(snapshot, item))
        .count();
    let now = current_epoch_secs();
    let capability_due = items
        .iter()
        .filter(|item| repo_due_capability_count(snapshot, item, now) > 0)
        .count();
    let selected = selected_repository_items(snapshot, &items);

    let hero = ui::vbox(4);
    hero.append(&ui::page_title("Monorepo Overview"));
    let subtitle = ui::hint(&format!(
        "{} repositories tracked in {}",
        summary.repo_count, summary.workspace_name
    ));
    hero.append(&subtitle);
    let tiles = [
        ("Attention", attention, ui::Tone::Warning),
        (
            "Selected",
            snapshot.selected_repo_ids.len(),
            ui::Tone::Accent,
        ),
        ("Dirty", dirty, ui::Tone::Accent),
        ("Missing", missing, ui::Tone::Danger),
        ("Ahead", ahead, ui::Tone::Accent),
        ("Behind", behind, ui::Tone::Warning),
        ("Diverged", diverged, ui::Tone::Warning),
        ("No Upstream", no_upstream, ui::Tone::Strong),
        ("Cap Issues", capability_attention, ui::Tone::Warning),
        ("Cap Due", capability_due, ui::Tone::Warning),
    ]
    .map(|(label, value, tone)| ui::stat_tile(label, value, tone));
    let stats = ui::wrap_box(&tiles.each_ref().map(|tile| tile.upcast_ref()), 8);

    view.selection_slot
        .append(&monorepo_selection_actions(snapshot, host_ptr, &items));
    update_monorepo_report_controls(&view.reports, snapshot);
    view.files_slot
        .append(&overview_file_actions(snapshot, host_ptr));

    let sections = &view.sections;
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
        "Repos with open git, manifest, or capability attention signals.",
        &attention_items(snapshot, &items),
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
    append_attention_signals_section(&sections, snapshot, &items);
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
    append_log_section(sections, "Recent Operations", &snapshot.logs, 8);

    view.top.append(&hero);
    view.top.append(&stats);
}

fn build_commit_check_view(root: &GtkBox, initial: &StateSnapshot) -> Rc<CommitCheckEditor> {
    let hero = ui::vbox(4);
    hero.append(&ui::page_title("Commit Check"));
    hero.append(&ui::hint(
        "Scans recent commits with workspace rules before push.",
    ));

    let actions = ui::hbox(12);
    let rerun = ui::button("Run Check", ui::ButtonKind::Primary);
    rerun.connect_clicked(|_| {
        let _ = command_check_history(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&rerun);

    let open_overview = ui::link_button("Open Overview");
    open_overview.connect_clicked(|_| {
        let _ = command_open_overview(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&open_overview);

    let rules_help = ui::hint(
        "Rules run from the lowest priority to the highest. Later allow rules can unblock commits matched by earlier block rules.",
    );

    let rule_status = Label::new(None);
    rule_status.set_xalign(0.0);
    rule_status.set_wrap(true);
    ui::set_fg_tone(&rule_status, ui::Tone::Muted);
    rule_status.add_css_class(ui::class::META);

    let editor = Rc::new(CommitCheckEditor {
        rules_box: ui::vbox(8),
        rule_rows: Rc::new(RefCell::new(Vec::new())),
        status: rule_status,
        baseline: RefCell::new(Vec::new()),
    });
    sync_commit_check_editor(&editor, initial, true);

    let rule_actions = ui::hbox(8);
    let add_rule = ui::button("Add Rule", ui::ButtonKind::Secondary);
    add_rule.connect_clicked({
        let editor = Rc::clone(&editor);
        move |_| {
            open_add_commit_check_rule_dialog(active_window(), Rc::clone(&editor));
        }
    });
    let save_rules = ui::button("Save Rules", ui::ButtonKind::Secondary);
    save_rules.connect_clicked({
        let editor = Rc::clone(&editor);
        move |_| {
            let rules = build_commit_check_rules_from_rows(&editor.rule_rows.borrow());
            match rules {
                Ok(rules) => {
                    editor.status.set_text("Saving commit check rules...");
                    match save_commit_check_rules(rules.clone()) {
                        Ok(message) => {
                            // The rows are what was saved, so the next refresh
                            // has nothing to sync.
                            *editor.baseline.borrow_mut() = rules;
                            editor.status.set_text(&message);
                            append_log(message);
                            refresh_views();
                        }
                        Err(message) => {
                            editor.status.set_text(&message);
                            append_log(message);
                            refresh_views();
                        }
                    }
                }
                Err(message) => {
                    editor.status.set_text(&message);
                }
            }
        }
    });
    let discard_changes = ui::button("Discard Changes", ui::ButtonKind::Ghost);
    discard_changes.connect_clicked({
        let editor = Rc::clone(&editor);
        move |_| {
            sync_commit_check_editor(&editor, &snapshot(), true);
        }
    });
    for button in [add_rule, save_rules, discard_changes] {
        rule_actions.append(&button);
    }

    let rules = ui::vbox(8);
    rules.append(&rules_help);
    rules.append(&rule_actions);
    rules.append(&editor.status);
    rules.append(&editor.rules_box);

    root.append(&hero);
    root.append(&actions);
    root.append(&ui::section("Rules", &[], &rules));
    editor
}

/// Brings the editor rows in line with the manifest's rules. Rows are rebuilt
/// only when the rules changed (or `force`), and only while the rows hold no
/// unsaved edits; otherwise just the newly added rules are appended.
fn sync_commit_check_editor(editor: &CommitCheckEditor, snapshot: &StateSnapshot, force: bool) {
    let manifest_rules = snapshot
        .manifest
        .as_ref()
        .and_then(|manifest| manifest.commit_check_rules.clone())
        .unwrap_or_default();
    if !force && *editor.baseline.borrow() == manifest_rules {
        return;
    }

    let row_rules = build_commit_check_rules_from_rows(&editor.rule_rows.borrow());
    if !force && commit_check_rows_dirty(&row_rules, &editor.baseline.borrow()) {
        let row_ids = editor
            .rule_rows
            .borrow()
            .iter()
            .map(|row| row.rule_id.clone())
            .collect::<Vec<_>>();
        let added = commit_check_rules_added(&editor.baseline.borrow(), &row_ids, &manifest_rules);
        for rule in &added {
            append_commit_check_rule_row(&editor.rules_box, &editor.rule_rows, Some(rule));
        }
        if !added.is_empty() {
            editor.status.set_text(&format!(
                "Added {} rule(s) saved elsewhere. Your unsaved edits were kept.",
                added.len()
            ));
        }
    } else {
        clear_box(&editor.rules_box);
        editor.rule_rows.borrow_mut().clear();
        for rule in &manifest_rules {
            append_commit_check_rule_row(&editor.rules_box, &editor.rule_rows, Some(rule));
        }
        editor.status.set_text(&commit_check_rule_status_text(
            snapshot,
            manifest_rules.len(),
        ));
    }
    *editor.baseline.borrow_mut() = manifest_rules;
}

fn commit_check_rule_status_text(snapshot: &StateSnapshot, rule_count: usize) -> String {
    snapshot
        .manifest_path
        .as_ref()
        .map(|path| {
            format!(
                "Loaded {rule_count} commit check rules from {}. Use Save Rules to persist edits.",
                path.display()
            )
        })
        .unwrap_or_else(|| {
            format!("Loaded {rule_count} commit check rules. No workspace manifest is loaded.")
        })
}

/// Whether the editor rows differ from the rules they were loaded from. Rows
/// that do not even form valid rules are being edited.
fn commit_check_rows_dirty(
    row_rules: &Result<Vec<CommitCheckRule>, String>,
    baseline: &[CommitCheckRule],
) -> bool {
    !matches!(row_rules, Ok(rules) if rules == baseline)
}

/// Manifest rules that are neither in `baseline` nor already shown as a row.
fn commit_check_rules_added(
    baseline: &[CommitCheckRule],
    row_ids: &[String],
    manifest_rules: &[CommitCheckRule],
) -> Vec<CommitCheckRule> {
    manifest_rules
        .iter()
        .filter(|rule| !baseline.iter().any(|known| known.id == rule.id))
        .filter(|rule| !row_ids.iter().any(|id| *id == rule.id))
        .cloned()
        .collect()
}

fn append_commit_check_rule_row(
    rules_box: &GtkBox,
    rule_rows: &Rc<RefCell<Vec<CommitCheckRuleRowHandle>>>,
    rule: Option<&CommitCheckRule>,
) {
    let row = ui::vbox(8);
    row.add_css_class(ui::class::CARD);

    let enabled = clickable_check_button_with_label("Enabled");
    enabled.set_active(rule.map(|rule| rule.enabled).unwrap_or(true));

    let allow = clickable_check_button_with_label("Allow");
    allow.set_active(
        rule.map(|rule| matches!(rule.effect, CommitCheckRuleEffect::Allow))
            .unwrap_or(false),
    );

    let hash_matcher = clickable_check_button_with_label("Hash");
    hash_matcher.set_active(
        rule.map(|rule| matches!(rule.matcher, CommitCheckRuleMatcher::CommitHash { .. }))
            .unwrap_or(false),
    );

    let name = Entry::new();
    name.add_css_class(&input_css_class("field"));
    name.set_placeholder_text(Some("Rule name"));
    name.set_hexpand(true);
    name.set_text(rule.map(|rule| rule.name.as_str()).unwrap_or(""));

    let priority = Entry::new();
    priority.add_css_class(&input_css_class("field"));
    priority.set_placeholder_text(Some("Priority"));
    priority.set_tooltip_text(Some("Priority"));
    priority.set_width_chars(6);
    priority.set_text(
        &rule
            .map(|rule| rule.priority.to_string())
            .unwrap_or_else(|| "100".to_string()),
    );

    let value = Entry::new();
    value.add_css_class(&input_css_class("field"));
    value.set_placeholder_text(Some("Regex pattern or commit hash"));
    value.set_hexpand(true);
    value.set_text(
        &rule
            .map(commit_check_rule_value)
            .unwrap_or_else(String::new),
    );

    let repository_ids = Entry::new();
    repository_ids.add_css_class(&input_css_class("field"));
    repository_ids.set_placeholder_text(Some("Repo IDs, comma-separated; empty means all"));
    repository_ids.set_hexpand(true);
    repository_ids.set_text(
        &rule
            .map(commit_check_rule_scope_value)
            .unwrap_or_else(String::new),
    );

    let remove = ui::icon_button("user-trash-symbolic", "Remove rule");
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

/// A modal window showing `body` above a right-aligned row of `buttons`, used
/// in place of the deprecated gtk::Dialog. Escape closes it.
fn build_modal_window(
    parent: Option<&Window>,
    title: &str,
    body: &impl IsA<gtk::Widget>,
    buttons: &[&Button],
) -> Window {
    let window = Window::builder().modal(true).title(title).build();
    window.add_css_class("app-dialog");
    if let Some(parent) = parent {
        window.set_transient_for(Some(parent));
    }

    let content = GtkBox::new(Orientation::Vertical, 16);
    content.set_margin_top(20);
    content.set_margin_bottom(16);
    content.set_margin_start(20);
    content.set_margin_end(20);
    content.append(body);

    let button_row = GtkBox::new(Orientation::Horizontal, 8);
    button_row.set_halign(Align::End);
    for button in buttons {
        mark_clickable(*button);
        button_row.append(*button);
    }
    content.append(&button_row);
    window.set_child(Some(&content));

    let escape = gtk::EventControllerKey::new();
    escape.connect_key_pressed(|controller, key, _, _| {
        if key != gtk::gdk::Key::Escape {
            return glib::Propagation::Proceed;
        }
        if let Some(window) = controller.widget().and_downcast::<Window>() {
            window.close();
        }
        glib::Propagation::Stop
    });
    window.add_controller(escape);
    window
}

fn open_add_commit_check_rule_dialog(parent: Option<Window>, editor: Rc<CommitCheckEditor>) {
    let cancel = ui::button("Cancel", ui::ButtonKind::Secondary);
    let add = ui::button("Add Rule", ui::ButtonKind::Primary);

    let body = ui::vbox(12);
    body.set_size_request(440, -1);

    let enabled = clickable_check_button_with_label("Enabled");
    enabled.set_active(true);

    let allow = clickable_check_button_with_label("Allow");

    let hash_matcher = clickable_check_button_with_label("Hash");

    let name = Entry::new();
    name.add_css_class(&input_css_class("field"));
    name.set_hexpand(true);
    name.set_placeholder_text(Some("Rule name"));

    let priority = Entry::new();
    priority.add_css_class(&input_css_class("field"));
    priority.set_placeholder_text(Some("100"));
    priority.set_text("100");

    let value = Entry::new();
    value.add_css_class(&input_css_class("field"));
    value.set_hexpand(true);
    value.set_placeholder_text(Some("Regex pattern or commit hash"));

    let repository_ids = Entry::new();
    repository_ids.add_css_class(&input_css_class("field"));
    repository_ids.set_hexpand(true);
    repository_ids.set_placeholder_text(Some("Repo IDs, comma-separated; empty means all"));

    let toggles = ui::hbox(16);
    toggles.append(&enabled);
    toggles.append(&allow);
    toggles.append(&hash_matcher);

    let error = Label::new(None);
    error.set_xalign(0.0);
    error.set_wrap(true);
    ui::set_fg_tone(&error, ui::Tone::Danger);

    body.append(&toggles);
    body.append(&labeled_field("Name", &name));
    body.append(&labeled_field("Priority", &priority));
    body.append(&labeled_field("Pattern Or Hash", &value));
    body.append(&labeled_field("Repository Scope", &repository_ids));
    body.append(&error);
    for entry in [&name, &priority, &value, &repository_ids] {
        entry.set_activates_default(true);
    }

    let dialog = build_modal_window(
        parent.as_ref(),
        "Add Commit Check Rule",
        &body,
        &[&cancel, &add],
    );
    dialog.set_default_widget(Some(&add));

    cancel.connect_clicked({
        let dialog = dialog.downgrade();
        move |_| {
            if let Some(dialog) = dialog.upgrade() {
                dialog.close();
            }
        }
    });
    // The handler lives on a button inside the window, so it must not hold the
    // window strongly.
    add.connect_clicked({
        let dialog = dialog.downgrade();
        let enabled = enabled.clone();
        let allow = allow.clone();
        let hash_matcher = hash_matcher.clone();
        let name = name.clone();
        let priority = priority.clone();
        let value = value.clone();
        let repository_ids = repository_ids.clone();
        let error = error.clone();
        move |_| {
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

            // Persist only the new rule on top of the saved rules; unsaved
            // edits in the editor rows stay unsaved.
            let mut rules = current_commit_check_rules();
            rules.push(rule.clone());

            editor.status.set_text("Saving commit check rule...");
            match save_commit_check_rules(rules) {
                Ok(message) => {
                    if let Some(dialog) = dialog.upgrade() {
                        dialog.close();
                    }
                    refresh_commit_check_views_now();
                    editor
                        .status
                        .set_text(&format!("Saved rule \"{}\". {}", rule.name, message));
                    append_log(message);
                    refresh_log_surfaces();
                }
                Err(message) => {
                    error.set_text(&message);
                    editor.status.set_text(&message);
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
    // Rows created in the same millisecond (e.g. several Add Rule clicks)
    // would otherwise share an id and be removed together.
    static RULE_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let sequence = RULE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("rule-{millis}-{sequence}")
}

fn current_commit_check_rules() -> Vec<CommitCheckRule> {
    let app_state = state().lock().expect("state mutex poisoned");
    app_state
        .manifest
        .as_ref()
        .and_then(|manifest| manifest.commit_check_rules.clone())
        .unwrap_or_default()
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

fn create_repo_overview_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 0);
    root.set_hexpand(true);
    root.set_vexpand(true);

    let stack = Stack::new();
    stack.set_hexpand(true);
    stack.set_vexpand(true);

    let mut pages = Vec::new();
    for page in RepoOverviewPage::ALL {
        let page_root = repo_overview_page_root();
        stack.add_named(&vertical_view_scroller(&page_root), Some(page.name()));
        pages.push((page, page_root));
    }
    stack.set_visible_child_name(RepoOverviewPage::Summary.name());
    root.append(&stack_tab_bar(
        &stack,
        &RepoOverviewPage::ALL.map(|page| (page.name(), page.title())),
    ));

    let repo_body = Paned::new(Orientation::Vertical);
    repo_body.set_hexpand(true);
    repo_body.set_vexpand(true);
    repo_body.set_wide_handle(true);
    repo_body.set_resize_start_child(true);
    repo_body.set_resize_end_child(false);
    repo_body.set_shrink_start_child(true);
    repo_body.set_shrink_end_child(true);

    let terminal_panel = GtkBox::new(Orientation::Vertical, 0);
    terminal_panel.set_hexpand(true);
    terminal_panel.set_vexpand(true);

    repo_body.set_start_child(Some(&stack));
    repo_body.set_end_child(Some(&terminal_panel));
    root.append(&repo_body);

    let instance_key =
        unsafe { request.as_ref() }.and_then(|request| decode_mzstr(request.instance_key));
    let snapshot = snapshot();
    terminal_panel.append(&build_repo_terminal_panel(
        &snapshot,
        instance_key.as_deref(),
    ));

    let stack_ref = glib::WeakRef::new();
    stack_ref.set(Some(&stack));
    let view = Rc::new(RepoOverviewView {
        stack: stack_ref,
        pages,
        stale: RefCell::new(RepoOverviewPage::ALL.into_iter().collect()),
        terminal_root: terminal_panel,
        instance_key,
        host_ptr: host as usize,
    });
    for (page, page_root) in &view.pages {
        if *page == RepoOverviewPage::Summary {
            view.stale.borrow_mut().remove(page);
            render_repo_overview_page(&view, *page, page_root, &snapshot);
        }
    }

    // Stale tabs are rendered when they become visible, either by switching
    // tabs or by this view's tab coming to the front.
    stack.connect_visible_child_name_notify({
        let view = Rc::downgrade(&view);
        move |_| {
            if let Some(view) = view.upgrade() {
                render_visible_repo_overview_page_if_stale(&view);
            }
        }
    });
    stack.connect_map({
        let view = Rc::downgrade(&view);
        move |_| {
            let view = view.clone();
            glib::idle_add_local_once(move || {
                if let Some(view) = view.upgrade() {
                    render_visible_repo_overview_page_if_stale(&view);
                }
            });
        }
    });
    REPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(view);
    });

    into_view_ptr(root)
}

fn create_workspace_settings_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    remember_host_ptr(host);

    let root = padded_view_root(16, 24);

    let header = ui::vbox(4);
    header.append(&ui::page_title("Fucina repository policy"));
    header.append(&ui::hint(
        "repos.toml is authoritative. Ronomepo delegates validation, audits, and safe remote configuration to fucina_policy.py.",
    ));
    root.append(&header);

    let policy_area = ui::vbox(12);
    let action_status = Label::new(Some("Ready · detailed results appear in Operations"));
    let status_bar = ui::info_bar(&action_status, "dialog-information-symbolic");
    let repository_area = GtkBox::new(Orientation::Vertical, 0);
    repository_area.set_vexpand(true);
    root.append(&policy_area);
    root.append(&status_bar);
    root.append(&repository_area);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    let handle = FucinaPolicyViewHandle {
        root: root_ref,
        policy_area,
        repository_area,
        action_status,
        rendered_source: RefCell::new(None),
    };
    sync_fucina_policy_view(&handle, &snapshot());
    FUCINA_POLICY_VIEWS.with(|views| {
        views.borrow_mut().push(handle);
    });

    into_view_ptr(root)
}

/// Re-renders the policy part of a Fucina view when repos.toml was replaced,
/// edited or moved since it was last rendered.
fn sync_fucina_policy_view(handle: &FucinaPolicyViewHandle, snapshot: &StateSnapshot) {
    let policy_root =
        fucina_policy_root(&snapshot.workspace_root, snapshot.manifest_path.as_deref());
    let source = FucinaPolicySource {
        modified: fs::metadata(policy_root.join("repos.toml"))
            .and_then(|metadata| metadata.modified())
            .ok(),
        policy_root,
    };
    if handle.rendered_source.borrow().as_ref() == Some(&source) {
        return;
    }
    render_fucina_policy(handle, &source.policy_root);
    *handle.rendered_source.borrow_mut() = Some(source);
}

fn render_fucina_policy(handle: &FucinaPolicyViewHandle, policy_root: &Path) {
    clear_box(&handle.policy_area);
    clear_box(&handle.repository_area);
    let root = &handle.policy_area;
    let action_status = &handle.action_status;

    match load_fucina_policy(policy_root) {
        Ok(policy) => {
            set_fucina_status_visible(action_status, true);
            let badges = ui::hbox(6);
            badges.append(&ui::badge(
                &format!("Schema {}", policy.schema_version),
                ui::Tone::Neutral,
            ));
            badges.append(&ui::badge(
                &format!("{} repositories", policy.repositories.len()),
                ui::Tone::Neutral,
            ));
            root.append(&badges);
            let validate = ui::button("Validate", ui::ButtonKind::Secondary);
            let audit = ui::button("Audit all", ui::ButtonKind::Secondary);
            let preview = ui::button("Preview configuration", ui::ButtonKind::Secondary);
            let apply = ui::button("Apply configuration…", ui::ButtonKind::Primary);
            apply.set_tooltip_text(Some(
                "Previews first, then asks for confirmation before applying the safe changes",
            ));
            root.append(&ui::wrap_box(
                &[
                    validate.upcast_ref(),
                    audit.upcast_ref(),
                    preview.upcast_ref(),
                    apply.upcast_ref(),
                ],
                8,
            ));

            connect_fucina_action(
                &validate,
                policy_root.to_path_buf(),
                FucinaPolicyAction::Validate,
                Vec::new(),
                action_status.clone(),
            );
            connect_fucina_action(
                &audit,
                policy_root.to_path_buf(),
                FucinaPolicyAction::Audit,
                Vec::new(),
                action_status.clone(),
            );
            connect_fucina_action(
                &preview,
                policy_root.to_path_buf(),
                FucinaPolicyAction::ConfigurePreview,
                Vec::new(),
                action_status.clone(),
            );
            connect_fucina_apply(&apply, policy_root.to_path_buf(), action_status.clone());

            let repository_list = ui::list_card();
            for repository in &policy.repositories {
                ui::list_card_item(
                    &repository_list,
                    &fucina_repository_row(repository, policy_root, action_status),
                );
            }
            let repositories = ScrolledWindow::builder()
                .hscrollbar_policy(PolicyType::Never)
                .vscrollbar_policy(PolicyType::Automatic)
                .min_content_height(280)
                .vexpand(true)
                .child(&repository_list)
                .build();
            repositories.set_margin_top(4);
            handle.repository_area.append(&repositories);
        }
        Err(message) => {
            set_fucina_status_visible(action_status, false);
            root.append(&ui::problem_row(
                ui::Severity::Error,
                &message,
                "The Fucina policy could not be loaded.",
            ));
        }
    }
}

/// Shows or hides the status bar holding `action_status`.
fn set_fucina_status_visible(action_status: &Label, visible: bool) {
    match action_status.parent() {
        Some(bar) => bar.set_visible(visible),
        None => action_status.set_visible(visible),
    }
}

/// Tints the status bar holding `action_status` by the last result.
fn set_fucina_status_tone(action_status: &Label, tone: ui::Tone) {
    if let Some(bar) = action_status.parent().and_downcast::<GtkBox>() {
        ui::set_info_bar_tone(&bar, tone);
    }
}

fn fucina_policy_root(workspace_root: &Path, manifest_path: Option<&Path>) -> PathBuf {
    if let Some(manifest_directory) = manifest_path.and_then(Path::parent) {
        if manifest_directory.join("repos.toml").is_file()
            && manifest_directory.join("fucina_policy.py").is_file()
        {
            return manifest_directory.to_path_buf();
        }
    }
    workspace_root.to_path_buf()
}

fn fucina_repository_row(
    repository: &FucinaRepositoryPolicy,
    workspace_root: &Path,
    action_status: &Label,
) -> GtkBox {
    let row = ui::hbox(16);

    let details = ui::vbox(4);
    details.set_hexpand(true);
    let title = ui::hbox(6);
    let name = Label::new(Some(&repository.name));
    name.add_css_class(ui::class::HEADING);
    title.append(&name);
    title.append(&ui::badge(&repository.role, ui::Tone::Neutral));
    title.append(&ui::badge(&repository.publication, ui::Tone::Neutral));
    if repository.archived {
        title.append(&ui::badge("archived", ui::Tone::Warning));
    }
    details.append(&title);

    let remotes = ui::vbox(0);
    remotes.set_tooltip_text(Some(&fucina_repository_remotes(repository)));
    let (backup_text, backup_tone) = fucina_repository_backup_line(repository);
    let lines = fucina_repository_remote_lines(repository)
        .into_iter()
        .map(|(kind, url)| (kind, url, ui::Tone::Muted, true))
        .chain(std::iter::once(("Backup", backup_text, backup_tone, false)));
    for (kind, url, tone, mono) in lines {
        let line = ui::hbox(8);
        let key = Label::new(Some(kind));
        key.set_xalign(0.0);
        key.set_size_request(84, -1);
        key.add_css_class(ui::class::KV_KEY);
        key.add_css_class(ui::class::META);
        let value = Label::new(Some(&url));
        value.set_xalign(0.0);
        value.set_hexpand(true);
        value.set_ellipsize(EllipsizeMode::Middle);
        value.set_selectable(true);
        value.set_tooltip_text(Some(&url));
        if mono {
            value.add_css_class("mono");
        }
        value.add_css_class(ui::class::META);
        ui::set_fg_tone(&value, tone);
        line.append(&key);
        line.append(&value);
        remotes.append(&line);
    }
    details.append(&remotes);
    row.append(&details);

    let actions = ui::hbox(12);
    actions.set_valign(Align::Center);
    let audit = ui::link_button("Audit");
    connect_fucina_action(
        &audit,
        workspace_root.to_path_buf(),
        FucinaPolicyAction::Audit,
        vec![repository.name.clone()],
        action_status.clone(),
    );
    actions.append(&audit);
    let preview = ui::link_button("Preview");
    connect_fucina_action(
        &preview,
        workspace_root.to_path_buf(),
        FucinaPolicyAction::ConfigurePreview,
        vec![repository.name.clone()],
        action_status.clone(),
    );
    actions.append(&preview);
    row.append(&actions);
    row
}

fn fucina_repository_remote_lines(
    repository: &FucinaRepositoryPolicy,
) -> Vec<(&'static str, String)> {
    let mut values = vec![("Fucina", repository.forgejo.clone())];
    if let Some(upstream) = &repository.upstream {
        values.push(("Upstream", upstream.clone()));
    }
    if let Some(target) = &repository.publication_target {
        values.push(("Publication", target.clone()));
    }
    if let Some(schedule) = &repository.mirror_schedule {
        values.push(("Pull schedule", schedule.clone()));
    }
    values
}

fn fucina_repository_remotes(repository: &FucinaRepositoryPolicy) -> String {
    fucina_repository_remote_lines(repository)
        .into_iter()
        .map(|(kind, value)| format!("{kind}: {value}"))
        .collect::<Vec<_>>()
        .join(" · ")
}

fn fucina_repository_backup_line(repository: &FucinaRepositoryPolicy) -> (String, ui::Tone) {
    let tone = if repository.backup_required && repository.backups.is_empty() {
        ui::Tone::Warning
    } else if repository.backups.is_empty() {
        ui::Tone::Muted
    } else {
        ui::Tone::Neutral
    };
    (fucina_repository_backup(repository), tone)
}

fn fucina_repository_backup(repository: &FucinaRepositoryPolicy) -> String {
    if repository.backups.is_empty() {
        return if repository.backup_required {
            "Backup required · destination unresolved".to_string()
        } else {
            "Backup not required".to_string()
        };
    }
    repository
        .backups
        .iter()
        .map(|backup| format!("Backup {}: {}", backup.mode, backup.destination))
        .collect::<Vec<_>>()
        .join(" · ")
}

fn connect_fucina_action(
    button: &Button,
    workspace_root: PathBuf,
    action: FucinaPolicyAction,
    repositories: Vec<String>,
    action_status: Label,
) {
    button.connect_clicked(move |button| {
        button.set_sensitive(false);
        action_status.set_text("Running · see Operations for details");
        set_fucina_status_tone(&action_status, ui::Tone::Neutral);
        let button = button.clone();
        let action_status = action_status.clone();
        run_fucina_action_async(
            workspace_root.clone(),
            action,
            repositories.clone(),
            move |result| {
                button.set_sensitive(true);
                present_fucina_result(action, result, &action_status);
            },
        );
    });
}

fn connect_fucina_apply(button: &Button, workspace_root: PathBuf, action_status: Label) {
    button.connect_clicked(move |button| {
        button.set_sensitive(false);
        action_status.set_text("Previewing · see Operations for details");
        set_fucina_status_tone(&action_status, ui::Tone::Neutral);
        let button = button.clone();
        let action_status = action_status.clone();
        let workspace_root = workspace_root.clone();
        run_fucina_action_async(
            workspace_root.clone(),
            FucinaPolicyAction::ConfigurePreview,
            Vec::new(),
            move |preview| {
                button.set_sensitive(true);
                present_fucina_result(
                    FucinaPolicyAction::ConfigurePreview,
                    preview.clone(),
                    &action_status,
                );
                if fucina_preview_allows_confirmation(&preview) {
                    show_fucina_apply_confirmation(workspace_root.clone(), action_status.clone());
                }
            },
        );
    });
}

fn fucina_preview_allows_confirmation(preview: &Result<FucinaPolicyCommandResult, String>) -> bool {
    matches!(preview, Ok(result) if result.success)
}

fn show_fucina_apply_confirmation(workspace_root: PathBuf, action_status: Label) {
    let cancel = ui::button("Cancel", ui::ButtonKind::Secondary);
    let apply = ui::button("Apply", ui::ButtonKind::Danger);
    let content = Label::new(Some(
        "The preview succeeded. Apply only the safe changes accepted by the policy engine? Unexpected remotes and unresolved backup gates will still be refused.",
    ));
    content.set_wrap(true);
    content.set_max_width_chars(60);
    content.set_margin_top(2);
    content.set_margin_bottom(2);
    let dialog = build_modal_window(
        active_window().as_ref(),
        "Apply Fucina configuration?",
        &content,
        &[&cancel, &apply],
    );
    dialog.set_default_widget(Some(&cancel));
    cancel.connect_clicked({
        let dialog = dialog.downgrade();
        move |_| {
            if let Some(dialog) = dialog.upgrade() {
                dialog.close();
            }
        }
    });
    let dialog_ref = dialog.downgrade();
    apply.connect_clicked(move |_| {
        if let Some(dialog) = dialog_ref.upgrade() {
            dialog.close();
        }
        action_status.set_text("Applying · see Operations for details");
        set_fucina_status_tone(&action_status, ui::Tone::Neutral);
        let action_status = action_status.clone();
        run_fucina_action_async(
            workspace_root.clone(),
            FucinaPolicyAction::ConfigureApply,
            Vec::new(),
            move |result| {
                present_fucina_result(FucinaPolicyAction::ConfigureApply, result, &action_status)
            },
        );
    });
    dialog.present();
}

fn run_fucina_action_async(
    workspace_root: PathBuf,
    action: FucinaPolicyAction,
    repositories: Vec<String>,
    completion: impl FnOnce(Result<FucinaPolicyCommandResult, String>) + 'static,
) {
    glib::spawn_future_local(async move {
        let result =
            gio::spawn_blocking(move || run_fucina_policy(&workspace_root, action, &repositories))
                .await
                .unwrap_or_else(|_| Err("Fucina policy worker stopped unexpectedly".to_string()));
        completion(result);
    });
}

fn present_fucina_result(
    action: FucinaPolicyAction,
    result: Result<FucinaPolicyCommandResult, String>,
    action_status: &Label,
) {
    let successful = matches!(&result, Ok(result) if result.success);
    let label = fucina_action_label(action);
    let status = if successful {
        format!("{label} succeeded · details in Operations")
    } else {
        format!("{label} refused or failed · details in Operations")
    };
    action_status.set_text(&status);
    set_fucina_status_tone(
        action_status,
        if successful {
            ui::Tone::Success
        } else {
            ui::Tone::Danger
        },
    );
    append_log(format!(
        "Fucina {label}\n{}",
        fucina_command_result_text(result)
    ));
    refresh_log_surfaces();
    if let Err(message) = open_operations_panel() {
        append_log(message);
        refresh_log_surfaces();
    }
}

fn fucina_action_label(action: FucinaPolicyAction) -> &'static str {
    match action {
        FucinaPolicyAction::Validate => "validation",
        FucinaPolicyAction::Audit => "audit",
        FucinaPolicyAction::ConfigurePreview => "configuration preview",
        FucinaPolicyAction::ConfigureApply => "configuration apply",
    }
}

fn fucina_command_result_text(result: Result<FucinaPolicyCommandResult, String>) -> String {
    match result {
        Err(message) => format!("ERROR\n{message}"),
        Ok(result) => {
            let status = if result.success {
                "SUCCESS"
            } else {
                "REFUSED / FAILED"
            };
            let mut parts = vec![status.to_string()];
            if !result.stdout.is_empty() {
                parts.push(result.stdout);
            }
            if !result.stderr.is_empty() {
                parts.push(format!("stderr:\n{}", result.stderr));
            }
            parts.join("\n\n")
        }
    }
}

fn labeled_field(label: &str, widget: &impl IsA<gtk::Widget>) -> GtkBox {
    let row = ui::vbox(4);
    let title = Label::new(Some(label));
    title.set_xalign(0.0);
    title.add_css_class(ui::class::KV_KEY);
    row.append(&title);
    row.append(widget);
    row
}

type PluginConfigUpdate = Box<dyn FnOnce(&mut RonomepoPluginConfig)>;

/// Applies `update` to the stored plugin config. Monitor toggles call this on
/// every click, so updates are batched into one write shortly after.
fn persist_plugin_config_direct(update: impl FnOnce(&mut RonomepoPluginConfig) + 'static) {
    if !glib::MainContext::default().is_owner() {
        write_plugin_config(&persisted_plugin_configs_path(), vec![Box::new(update)]);
        return;
    }
    let first = PENDING_CONFIG_UPDATES.with(|pending| {
        let mut pending = pending.borrow_mut();
        pending.push(Box::new(update));
        pending.len() == 1
    });
    if first {
        glib::timeout_add_local_once(
            Duration::from_millis(CONFIG_PERSIST_DEBOUNCE_MILLIS),
            || {
                let updates = PENDING_CONFIG_UPDATES.with(|pending| pending.take());
                write_plugin_config(&persisted_plugin_configs_path(), updates);
            },
        );
    }
}

/// Edits the plugin's entry in the host's plugins.json in place, leaving other
/// plugins' entries alone. The host reads the file from disk on each view
/// config access, so writing it directly is safe; a file or entry that cannot
/// be decoded is left untouched rather than replaced by defaults.
fn write_plugin_config(path: &Path, updates: Vec<PluginConfigUpdate>) {
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

    let mut document = match fs::read_to_string(path) {
        Ok(raw) => match serde_json::from_str::<HashMap<String, Value>>(&raw) {
            Ok(document) => document,
            Err(error) => {
                diag::log(format!(
                    "not saving plugin config: {} is not valid: {error}",
                    path.display()
                ));
                return;
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
        Err(error) => {
            diag::log(format!(
                "not saving plugin config: cannot read {}: {error}",
                path.display()
            ));
            return;
        }
    };

    let mut config = match document.get(PLUGIN_ID).cloned() {
        None => RonomepoPluginConfig::default(),
        Some(value) => {
            let decoded = serde_json::from_value::<StoredPluginConfigEntry>(value)
                .map_err(|error| error.to_string())
                .and_then(|entry| {
                    let payload = match entry {
                        StoredPluginConfigEntry::Legacy(payload) => payload,
                        StoredPluginConfigEntry::Versioned { payload, .. } => payload,
                    };
                    RonomepoPluginConfig::decode(&payload).map_err(|error| error.to_string())
                });
            match decoded {
                Ok(config) => config,
                Err(error) => {
                    diag::log(format!(
                        "not saving plugin config: stored entry is invalid: {error}"
                    ));
                    return;
                }
            }
        }
    };

    for update in updates {
        update(&mut config);
    }

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

    let written = serde_json::to_vec_pretty(&document)
        .map_err(std::io::Error::other)
        .and_then(|raw| write_file_atomically(path, &raw));
    if let Err(error) = written {
        diag::log(format!(
            "failed to save plugin config to {}: {error}",
            path.display()
        ));
    }
}

/// Replaces `path` with `bytes` through a synced temporary file and a rename,
/// so a crash or a concurrent reader never sees a partially written file.
fn write_file_atomically(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("path has no file name"))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| {
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        if let Ok(metadata) = fs::metadata(path) {
            file.set_permissions(metadata.permissions())?;
        }
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Persistence id the app hands to Maruzzella; the host keeps its layouts and
/// the plugin configs under `<config root>/<id>`.
pub const PERSISTENCE_ID: &str = "ronomepo";

/// The directory Maruzzella persists configs and layouts under. It must match
/// the host's own lookup (an empty `XDG_CONFIG_HOME` included) because the
/// plugin edits files the host reads; the app clears an empty
/// `XDG_CONFIG_HOME` at startup so both resolve to a sensible path.
pub fn persisted_config_root() -> PathBuf {
    if let Some(dir) = env::var_os("XDG_CONFIG_HOME") {
        PathBuf::from(dir)
    } else if let Some(home) = env::var_os("HOME") {
        PathBuf::from(home).join(".config")
    } else {
        PathBuf::from(".")
    }
}

fn persisted_plugin_configs_path() -> PathBuf {
    persisted_config_root()
        .join(PERSISTENCE_ID)
        .join("plugins.json")
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

fn run_capability_checks(
    workspace_root: &Path,
    targets: Vec<CapabilityRunTarget>,
    run_reason: &str,
) -> Result<CapabilityRunReport, String> {
    let registry_path = default_capability_registry_path(workspace_root);
    let mut registry =
        load_capability_registry(&registry_path).map_err(|error| error.to_string())?;
    let mut lines = Vec::new();
    let mut run_count = 0usize;

    for target in targets {
        let snapshot = collect_capability_git_snapshot(&target.repo_path);
        for capability in target.manifest.capabilities.iter().filter(|capability| {
            capability.status == ronomepo_core::CapabilityDeclarationStatus::Implemented
                && target
                    .capability_ids
                    .as_ref()
                    .is_none_or(|ids| ids.iter().any(|id| id == &capability.id))
        }) {
            run_count += 1;
            let started = Instant::now();
            let result =
                match plan_capability_action(&target.repo_path, &target.manifest, &capability.id) {
                    Ok(plan) => execute_capability_plan(&target, &plan),
                    Err(error) => CapabilityResult {
                        status: CapabilityResultStatus::Error,
                        summary: error.to_string(),
                        findings: Vec::new(),
                    },
                };
            let duration_ms = started.elapsed().as_millis() as u64;
            lines.push(format!(
                "Capability {} for {}: {} ({})",
                capability.id,
                target.repo_name,
                capability_status_label(result.status),
                result.summary
            ));
            let checked_at_epoch_secs = current_epoch_secs();
            let mut state = CapabilityState {
                repo_id: target.repo_id.clone(),
                capability_instance_id: capability.id.clone(),
                capability: capability.capability.clone(),
                item_id: capability.item_id.clone(),
                root: capability.root.clone(),
                status: result.status,
                summary: result.summary,
                findings: result.findings,
                checked_at_epoch_secs: Some(checked_at_epoch_secs),
                commit: snapshot.commit.clone(),
                dirty: snapshot.dirty,
                duration_ms: Some(duration_ms),
                next_due_at_epoch_secs: None,
                last_scheduled_reason: Some(run_reason.to_string()),
            };
            state.next_due_at_epoch_secs =
                capability_next_due_at_epoch_secs(capability, Some(&state))
                    .map_err(|error| error.to_string())?;
            upsert_capability_state(&mut registry, state);
        }
    }

    save_capability_registry(&registry_path, &registry).map_err(|error| error.to_string())?;
    Ok(CapabilityRunReport {
        lines,
        message: format!(
            "Capability checks completed: {run_count} check(s), registry saved to {}.",
            registry_path.display()
        ),
    })
}

fn execute_capability_plan(
    target: &CapabilityRunTarget,
    plan: &ronomepo_core::CapabilityActionPlan,
) -> CapabilityResult {
    let mut aggregate = CapabilityResult {
        status: CapabilityResultStatus::Ok,
        summary: format!("{} step(s) completed", plan.steps.len()),
        findings: Vec::new(),
    };
    let mut summaries = Vec::new();

    for step in &plan.steps {
        let result = match &step.executor {
            RepoActionExecutor::Command(command) => execute_capability_command(command),
            RepoActionExecutor::BuiltInInspector => {
                execute_builtin_capability_inspector(target, step.item_id.as_deref(), plan)
            }
        };
        aggregate.status = worse_capability_status(aggregate.status, result.status);
        summaries.push(result.summary.clone());
        aggregate.findings.extend(result.findings);
        if matches!(
            plan.failure_policy,
            ronomepo_core::AggregationFailurePolicy::FailFast
        ) && matches!(
            result.status,
            CapabilityResultStatus::Failed | CapabilityResultStatus::Error
        ) {
            break;
        }
    }

    if summaries.len() == 1 {
        aggregate.summary = summaries.remove(0);
    } else if !summaries.is_empty() {
        aggregate.summary = summaries.join("; ");
    }
    aggregate
}

fn execute_capability_command(command: &PlannedCommand) -> CapabilityResult {
    match run_planned_command_to_output(command) {
        Ok(output) => capability_result_from_command_output(command, output),
        Err(error) => CapabilityResult {
            status: CapabilityResultStatus::Error,
            summary: error,
            findings: Vec::new(),
        },
    }
}

fn run_planned_command_to_output(command: &PlannedCommand) -> Result<Output, String> {
    let mut process = Command::new(&command.program);
    process.args(&command.args);
    process.current_dir(&command.workdir);
    process.stdout(Stdio::piped());
    process.stderr(Stdio::piped());
    for (key, value) in &command.env {
        process.env(key, value);
    }

    let mut child = process
        .spawn()
        .map_err(|error| format!("failed to run {}: {error}", planned_command_text(command)))?;

    let Some(timeout_seconds) = command.timeout_seconds else {
        return child.wait_with_output().map_err(|error| {
            format!(
                "failed to collect output from {}: {error}",
                planned_command_text(command)
            )
        });
    };

    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child.wait_with_output().map_err(|error| {
                    format!(
                        "failed to collect output from {}: {error}",
                        planned_command_text(command)
                    )
                });
            }
            Ok(None) if started.elapsed() >= Duration::from_secs(timeout_seconds) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "{} timed out after {} second(s)",
                    planned_command_text(command),
                    timeout_seconds
                ));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "failed to wait for {}: {error}",
                    planned_command_text(command)
                ));
            }
        }
    }
}

fn capability_result_from_command_output(
    command: &PlannedCommand,
    output: Output,
) -> CapabilityResult {
    match command.output {
        ronomepo_core::ActionOutputMode::Json => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            match parse_capability_result_json(stdout.trim()) {
                Ok(result) => result,
                Err(error) => CapabilityResult {
                    status: CapabilityResultStatus::Error,
                    summary: format!("invalid capability JSON result: {error}"),
                    findings: Vec::new(),
                },
            }
        }
        ronomepo_core::ActionOutputMode::Text | ronomepo_core::ActionOutputMode::JsonLines => {
            let summary = if output.status.success() {
                format!("{} succeeded", planned_command_text(command))
            } else {
                command_failure_summary(command, &output.stderr)
            };
            text_capability_result(output.status.success(), summary)
        }
    }
}

fn execute_builtin_capability_inspector(
    target: &CapabilityRunTarget,
    item_id: Option<&str>,
    plan: &ronomepo_core::CapabilityActionPlan,
) -> CapabilityResult {
    if plan.capability != "dependencies.outdated" {
        return CapabilityResult {
            status: CapabilityResultStatus::Error,
            summary: "built-in inspector is not supported for this capability".to_string(),
            findings: Vec::new(),
        };
    }

    match verify_repo_dependencies_freshness(&target.repo_path, &target.manifest) {
        Ok(reports) => {
            let mut findings = Vec::new();
            for report in reports {
                if item_id.is_some_and(|item_id| report.item_id != item_id) {
                    continue;
                }
                for finding in report.findings {
                    findings.push(CapabilityFinding {
                        severity: CapabilityFindingSeverity::Warning,
                        message: finding.message,
                        id: None,
                        file: None,
                        line: None,
                        url: None,
                        suggested_action: None,
                    });
                }
            }
            let status = if findings.is_empty() {
                CapabilityResultStatus::Ok
            } else {
                CapabilityResultStatus::Warning
            };
            let summary = if findings.is_empty() {
                "No dependency freshness findings".to_string()
            } else {
                format!("{} dependency freshness finding(s)", findings.len())
            };
            CapabilityResult {
                status,
                summary,
                findings,
            }
        }
        Err(error) => CapabilityResult {
            status: CapabilityResultStatus::Error,
            summary: error.to_string(),
            findings: Vec::new(),
        },
    }
}

fn worse_capability_status(
    left: CapabilityResultStatus,
    right: CapabilityResultStatus,
) -> CapabilityResultStatus {
    if capability_status_rank(right) > capability_status_rank(left) {
        right
    } else {
        left
    }
}

fn capability_status_rank(status: CapabilityResultStatus) -> u8 {
    match status {
        CapabilityResultStatus::Ok => 0,
        CapabilityResultStatus::Unknown => 1,
        CapabilityResultStatus::Running => 2,
        CapabilityResultStatus::Stale => 3,
        CapabilityResultStatus::Warning => 4,
        CapabilityResultStatus::Failed => 5,
        CapabilityResultStatus::Error => 6,
    }
}

fn capability_status_label(status: CapabilityResultStatus) -> &'static str {
    match status {
        CapabilityResultStatus::Ok => "ok",
        CapabilityResultStatus::Warning => "warning",
        CapabilityResultStatus::Failed => "failed",
        CapabilityResultStatus::Error => "error",
        CapabilityResultStatus::Unknown => "unknown",
        CapabilityResultStatus::Running => "running",
        CapabilityResultStatus::Stale => "stale",
    }
}

fn planned_command_text(command: &PlannedCommand) -> String {
    if command.args.is_empty() {
        command.program.clone()
    } else {
        format!("{} {}", command.program, command.args.join(" "))
    }
}

fn command_failure_summary(command: &PlannedCommand, stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    if stderr.is_empty() {
        format!("{} failed", planned_command_text(command))
    } else {
        format!("{} failed: {stderr}", planned_command_text(command))
    }
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
    reload_capability_registry();
    sync_watch_manager_from_state();
    schedule_workspace_scan();
}

fn read_capability_registry(workspace_root: &Path) -> Arc<CapabilityRegistry> {
    Arc::new(
        load_capability_registry(&default_capability_registry_path(workspace_root))
            .unwrap_or_default(),
    )
}

/// Re-reads the capability registry into the state cache. The file is read
/// outside the state lock.
fn reload_capability_registry() {
    let workspace_root = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.clone()
    };
    let registry = read_capability_registry(&workspace_root);
    let mut app_state = state().lock().expect("state mutex poisoned");
    if app_state.workspace_root == workspace_root {
        app_state.capability_registry = registry;
    }
}

/// A vertical view root with the same padding on every side.
/// An underline tab bar switching `stack` between its `(name, title)` pages.
fn stack_tab_bar(stack: &Stack, pages: &[(&'static str, &'static str)]) -> GtkBox {
    let bar = ui::hbox(0);
    bar.add_css_class(ui::class::TAB_BAR);
    let mut tabs: Vec<(&'static str, ToggleButton)> = Vec::new();
    for (name, title) in pages {
        let tab = ui::tab_button(title);
        if let Some((_, first)) = tabs.first() {
            tab.set_group(Some(first));
        }
        tab.set_active(stack.visible_child_name().as_deref() == Some(*name));
        tab.connect_toggled({
            let stack = stack.downgrade();
            let name = *name;
            move |tab| {
                if let Some(stack) = stack.upgrade().filter(|_| tab.is_active()) {
                    stack.set_visible_child_name(name);
                }
            }
        });
        bar.append(&tab);
        tabs.push((name, tab));
    }
    stack.connect_visible_child_name_notify(move |stack| {
        let visible = stack.visible_child_name();
        for (name, tab) in &tabs {
            if visible.as_deref() == Some(*name) && !tab.is_active() {
                tab.set_active(true);
            }
        }
    });
    bar
}

fn padded_view_root(spacing: i32, padding: i32) -> GtkBox {
    let root = GtkBox::new(Orientation::Vertical, spacing);
    root.set_margin_top(padding);
    root.set_margin_bottom(padding);
    root.set_margin_start(padding);
    root.set_margin_end(padding);
    root
}

/// Wraps `child` in a vertically scrolling window that fills its view.
fn vertical_view_scroller(child: &impl IsA<gtk::Widget>) -> ScrolledWindow {
    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .min_content_height(0)
        .child(child)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);
    scroller
}

/// Hands a view factory's top-level widget to the host, which takes the
/// reference.
fn into_view_ptr(widget: impl IsA<gtk::Widget>) -> *mut std::ffi::c_void {
    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(widget.upcast())
            as *mut std::ffi::c_void
    }
}

fn repo_overview_page_root() -> GtkBox {
    let root = GtkBox::new(Orientation::Vertical, 20);
    root.set_margin_top(16);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);
    root.set_hexpand(true);
    root.set_vexpand(true);
    root
}

fn visible_repo_overview_page(view: &RepoOverviewView) -> Option<(RepoOverviewPage, &GtkBox)> {
    let stack = view.stack.upgrade()?;
    if !stack.is_mapped() {
        return None;
    }
    let visible = stack.visible_child_name()?;
    view.pages
        .iter()
        .find(|(page, _)| page.name() == visible.as_str())
        .map(|(page, root)| (*page, root))
}

fn render_visible_repo_overview_page(view: &RepoOverviewView, snapshot: &StateSnapshot) {
    let Some((page, root)) = visible_repo_overview_page(view) else {
        return;
    };
    if view.stale.borrow_mut().remove(&page) {
        render_repo_overview_page(view, page, root, snapshot);
    }
}

fn render_visible_repo_overview_page_if_stale(view: &RepoOverviewView) {
    let Some((page, _)) = visible_repo_overview_page(view) else {
        return;
    };
    if view.stale.borrow().contains(&page) {
        render_visible_repo_overview_page(view, &snapshot());
    }
}

fn render_repo_overview_page(
    view: &RepoOverviewView,
    page: RepoOverviewPage,
    root: &GtkBox,
    snapshot: &StateSnapshot,
) {
    clear_box(root);
    let host_ptr = view.host_ptr as *const maruzzella_sdk::ffi::MzHostApi;

    let target_repo_id = view
        .instance_key
        .as_deref()
        .or(snapshot.active_repo_id.as_deref());
    let Some(active_repo_id) = target_repo_id else {
        if page == RepoOverviewPage::Summary {
            render_repo_overview_empty_page(
                root,
                snapshot,
                host_ptr,
                "No repo target was provided. Open repo overviews from the left monitor.",
            );
        } else {
            render_repo_overview_muted_page(root, "No repo target is attached to this tab.");
        }
        return;
    };

    let Some(item) = repo_item_by_id(snapshot, active_repo_id) else {
        if page == RepoOverviewPage::Summary {
            render_repo_overview_empty_page(
                root,
                snapshot,
                host_ptr,
                "The active repo overview target is no longer present in the current manifest.",
            );
        } else {
            render_repo_overview_muted_page(root, "This repository is no longer in the manifest.");
        }
        return;
    };

    if !snapshot.repo_details_cache.contains_key(&item.id)
        && !snapshot.repo_details_loading.contains(&item.id)
    {
        schedule_repo_details_load(&item.id, &item.status.repo_path);
    }

    let attention_signals = repo_attention_detail_signals(snapshot, item);
    root.append(&repo_overview_context(item, snapshot, &attention_signals));
    match page {
        RepoOverviewPage::Summary => {
            render_repo_overview_summary_page(root, snapshot, item, &attention_signals, host_ptr)
        }
        RepoOverviewPage::Git => render_repo_overview_git_page(
            root,
            snapshot,
            item,
            snapshot.repo_details_cache.get(&item.id),
            host_ptr,
        ),
        RepoOverviewPage::Manifest => {
            render_repo_overview_manifest_page(root, snapshot, item, host_ptr)
        }
        RepoOverviewPage::Activity => {
            render_repo_overview_activity_page(root, snapshot, item, host_ptr)
        }
    }
}

fn render_repo_overview_empty_page(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    message: &str,
) {
    root.append(&ui::empty_state(
        "folder-symbolic",
        "Repo Overview",
        message,
        None,
    ));
    let actions = ui::vbox(8);
    actions.set_halign(Align::Center);
    actions.append(&overview_actions(true));
    let files = overview_file_actions(snapshot, host_ptr);
    files.set_halign(Align::Center);
    actions.append(&files);
    root.append(&actions);
}

fn render_repo_overview_muted_page(root: &GtkBox, message: &str) {
    root.append(&ui::empty_state(
        "folder-symbolic",
        "Repo Overview",
        message,
        None,
    ));
}

fn render_repo_overview_summary_page(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    attention_signals: &[AttentionSignal],
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    root.append(&repo_overview_action_row(
        item,
        host_ptr,
        &[
            ("Open In Editor", None, ui::ButtonKind::Primary),
            ("Run Cap Checks", None, ui::ButtonKind::Secondary),
            ("Target This Repo", None, ui::ButtonKind::Secondary),
            ("Open Folder", None, ui::ButtonKind::Secondary),
        ],
    ));
    root.append(&repo_overview_summary_facts_section(
        snapshot,
        item,
        attention_signals,
    ));
    root.append(&repo_overview_attention_section(attention_signals));
}

fn render_repo_overview_git_page(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    details: Option<&RepositoryDetails>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    root.append(&repo_overview_action_row(
        item,
        host_ptr,
        &[
            (
                "Pull Repo",
                Some(OperationKind::Pull),
                ui::ButtonKind::Primary,
            ),
            (
                "Push Repo",
                Some(OperationKind::Push),
                ui::ButtonKind::Secondary,
            ),
            (
                "Clone Repo",
                Some(OperationKind::CloneMissing),
                ui::ButtonKind::Secondary,
            ),
            (
                "Apply Hooks",
                Some(OperationKind::ApplyHooks),
                ui::ButtonKind::Secondary,
            ),
            (
                "Push Repo (skip checks)",
                Some(OperationKind::PushSkipChecks),
                ui::ButtonKind::Danger,
            ),
        ],
    ));
    for section in repo_overview_git_sections(snapshot, item, details) {
        root.append(&section);
    }
}

fn render_repo_overview_manifest_page(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    root.append(&repo_overview_action_row(
        item,
        host_ptr,
        &[
            ("Edit Repo Manifest", None, ui::ButtonKind::Primary),
            ("Run Cap Checks", None, ui::ButtonKind::Secondary),
            ("Edit README", None, ui::ButtonKind::Link),
            ("Edit .git/config", None, ui::ButtonKind::Link),
        ],
    ));
    root.append(&repo_overview_manifest_status_section(item));
    root.append(&repo_overview_capabilities_section(snapshot, item));
}

fn render_repo_overview_activity_page(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    root.append(&repo_overview_activity_section(
        &repo_recent_logs(snapshot, item),
        18,
    ));
    root.append(&repo_overview_workspace_section(snapshot, host_ptr));
}

fn repo_overview_summary_facts_section(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    attention_signals: &[AttentionSignal],
) -> GtkBox {
    let summary = capability_monitor_summary(item, &snapshot.capability_registry);
    let facts = [
        ("State", status_label(&item.status.state).to_string()),
        ("Sync", format_sync_label(&item.status.sync)),
        ("Branch", branch_label(item).to_string()),
        (
            "Manifest",
            manifest_presence_label(item.repo_manifest.as_ref()).to_string(),
        ),
        (
            "Attention",
            attention_monitor_label(attention_signals).to_string(),
        ),
        (
            "Capabilities",
            capability_monitor_label(&summary).to_string(),
        ),
        (
            "Due Checks",
            repo_due_capability_count(snapshot, item, current_epoch_secs()).to_string(),
        ),
    ];
    ui::section("Status", &[], &ui::card(&ui::key_value_grid(&facts)))
}

fn repo_overview_manifest_status_section(item: &RepositoryListItem) -> GtkBox {
    let manifest_path = item
        .repo_manifest
        .as_ref()
        .map(|scan| scan.path.clone())
        .unwrap_or_else(|| default_repo_manifest_path(&item.status.repo_path));
    let mut facts = vec![
        (
            "Status",
            manifest_presence_label(item.repo_manifest.as_ref()).to_string(),
        ),
        ("Path", manifest_path.display().to_string()),
    ];

    match item.repo_manifest.as_ref().map(|scan| &scan.state) {
        Some(RepoManifestScanState::Valid(summary)) => {
            facts.push(("Items", summary.item_count.to_string()));
            facts.push(("Types", summary.item_types.join(", ")));
            facts.push(("Actions", summary.action_count.to_string()));
            facts.push(("Capabilities", summary.capability_count.to_string()));
            facts.push((
                "Policy Issues",
                summary.missing_required_capability_count.to_string(),
            ));
            facts.push((
                "Unsupported",
                summary.unsupported_capability_count.to_string(),
            ));
        }
        Some(RepoManifestScanState::Invalid { message }) => {
            facts.push(("Error", message.clone()));
        }
        Some(RepoManifestScanState::Missing) => {
            facts.push(("Next", "Create or edit the repo manifest.".to_string()));
        }
        None => {
            facts.push((
                "Scan",
                "Manifest state has not been collected yet.".to_string(),
            ));
        }
    }

    ui::section("Manifest", &[], &ui::card(&ui::key_value_grid(&facts)))
}

/// The page's action toolbar: buttons for operations, links for editors.
fn repo_overview_action_row(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    actions: &[(&'static str, Option<OperationKind>, ui::ButtonKind)],
) -> gtk::FlowBox {
    let buttons = actions
        .iter()
        .map(|(label, kind, style)| {
            repo_overview_action_button(item, host_ptr, label, *kind, *style)
        })
        .collect::<Vec<_>>();
    ui::wrap_box(
        &buttons
            .iter()
            .map(|button| button.upcast_ref())
            .collect::<Vec<_>>(),
        8,
    )
}

/// The page header: repository name with status badges, then its path,
/// branch and sync state.
fn repo_overview_context(
    item: &RepositoryListItem,
    snapshot: &StateSnapshot,
    attention_signals: &[AttentionSignal],
) -> GtkBox {
    let block = ui::vbox(6);

    let title_row = ui::hbox(8);
    let title = ui::page_title(&item.name);
    title_row.append(&title);
    title_row.append(&ui::badge(
        status_label(&item.status.state),
        state_badge_tone(&item.status.state),
    ));
    let rank = attention_rank(attention_signals);
    if rank <= 4 {
        let badge = ui::badge(
            &format!("Attention: {}", attention_monitor_label(attention_signals)),
            match rank {
                0 | 1 => ui::Tone::Danger,
                2 => ui::Tone::Warning,
                _ => ui::Tone::Muted,
            },
        );
        attach_text_tooltip(&badge, attention_monitor_tooltip(attention_signals));
        title_row.append(&badge);
    }
    let capabilities = capability_monitor_summary(item, &snapshot.capability_registry);
    let capability_label = capability_monitor_label(&capabilities);
    let capability_badge = if capability_label == "-" {
        ui::badge("No checks", ui::Tone::Muted)
    } else {
        ui::badge(
            &format!("Checks: {capability_label}"),
            capability_monitor_tone(&capabilities),
        )
    };
    attach_text_tooltip(&capability_badge, capability_monitor_tooltip(&capabilities));
    title_row.append(&capability_badge);
    let due = repo_due_capability_count(snapshot, item, current_epoch_secs());
    if due > 0 {
        title_row.append(&ui::badge(&format!("{due} due"), ui::Tone::Warning));
    }
    if snapshot.selected_repo_ids.iter().any(|id| id == &item.id) {
        title_row.append(&ui::badge("Selected", ui::Tone::Accent));
    }
    block.append(&title_row);

    let meta_row = ui::hbox(12);
    let path = ui::ellipsized(
        &item.status.repo_path.display().to_string(),
        &["mono", ui::class::META, &ui::Tone::Muted.fg_class()],
    );
    path.set_hexpand(false);
    path.set_ellipsize(EllipsizeMode::Start);
    let branch = ui::badge(branch_label(item), ui::Tone::Neutral);
    branch.set_tooltip_text(Some("Current branch"));
    let (sync_arrows, sync_tone) = sync_compact(&item.status.sync);
    let sync = Label::new(Some(&format!(
        "{sync_arrows}  {}",
        sync_description(&item.status.sync)
    )));
    sync.add_css_class(ui::class::META);
    ui::set_fg_tone(&sync, sync_tone);
    meta_row.append(&branch);
    meta_row.append(&sync);
    meta_row.append(&path);
    block.append(&meta_row);

    block
}

fn state_badge_tone(state: &ronomepo_core::RepositoryState) -> ui::Tone {
    match state {
        ronomepo_core::RepositoryState::Clean => ui::Tone::Success,
        ronomepo_core::RepositoryState::Untracked => ui::Tone::Neutral,
        other => state_tone(other),
    }
}

fn attention_severity(level: AttentionLevel) -> ui::Severity {
    match level {
        AttentionLevel::Blocked | AttentionLevel::Urgent => ui::Severity::Error,
        AttentionLevel::ShouldDo => ui::Severity::Warning,
        AttentionLevel::Opportunity => ui::Severity::WeakWarning,
        AttentionLevel::Info => ui::Severity::Info,
    }
}

fn repo_overview_attention_section(signals: &[AttentionSignal]) -> GtkBox {
    let count = ui::badge(&signals.len().to_string(), ui::Tone::Muted);
    if signals.is_empty() {
        let empty = ui::hbox(8);
        let icon = Image::from_icon_name("object-select-symbolic");
        ui::set_fg_tone(&icon, ui::Tone::Success);
        empty.append(&icon);
        let label = Label::new(Some("No open attention signals."));
        ui::set_fg_tone(&label, ui::Tone::Muted);
        empty.append(&label);
        return ui::section("Needs Attention", &[count.upcast_ref()], &empty);
    }
    let list = ui::list_card();
    for signal in signals {
        ui::list_card_item(&list, &repo_overview_attention_row(signal));
    }
    ui::section("Needs Attention", &[count.upcast_ref()], &list)
}

fn repo_overview_attention_row(signal: &AttentionSignal) -> GtkBox {
    ui::problem_row(
        attention_severity(signal.level),
        &signal.summary,
        &format!(
            "{} · {} · {} · urgency {} · impact {}",
            attention_level_label(signal.level),
            attention_kind_label(signal.kind),
            attention_source_label(signal.source),
            attention_urgency_label(signal.urgency),
            attention_impact_label(signal.impact)
        ),
    )
}

fn repo_overview_capabilities_section(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
) -> GtkBox {
    let lines = repo_capability_detail_lines(snapshot, item);
    let body = repo_overview_lines_body(
        &lines,
        "No capabilities declared or recorded for this repository.",
        true,
    );
    ui::section("Capabilities", &[], &body)
}

fn repo_overview_git_sections(
    snapshot: &StateSnapshot,
    item: &RepositoryListItem,
    details: Option<&RepositoryDetails>,
) -> [GtkBox; 3] {
    let facts = ui::key_value_grid(&[
        ("State", status_label(&item.status.state).to_string()),
        ("Sync", format_sync_label(&item.status.sync)),
        ("Selection", repo_selection_scope_label(snapshot, item)),
        (
            "Commit",
            details
                .and_then(|details| details.last_commit.as_ref())
                .map(|commit| format!("{} {}", commit.short_sha, commit.subject))
                .unwrap_or_else(|| {
                    if snapshot.repo_details_loading.contains(&item.id) {
                        "Loading commit information...".to_string()
                    } else {
                        "No commit information available.".to_string()
                    }
                }),
        ),
        ("Remote", item.remote_url.clone()),
    ]);
    let loading = snapshot.repo_details_loading.contains(&item.id);
    let changed_files = details
        .map(|details| details.changed_files.clone())
        .unwrap_or_else(|| {
            if loading {
                vec!["Loading working tree details...".to_string()]
            } else {
                Vec::new()
            }
        });
    let remotes = details
        .map(|details| details.remotes.clone())
        .unwrap_or_else(|| {
            if loading {
                vec!["Loading remotes...".to_string()]
            } else {
                Vec::new()
            }
        });
    let changed_count = ui::badge(&changed_files.len().to_string(), ui::Tone::Muted);
    [
        ui::section("Git", &[], &ui::card(&facts)),
        ui::section(
            "Changed Files",
            &[changed_count.upcast_ref()],
            &repo_overview_lines_body(&changed_files, "Working tree is clean.", true),
        ),
        ui::section(
            "Remotes",
            &[],
            &repo_overview_lines_body(&remotes, "No remotes reported for this repository.", true),
        ),
    ]
}

fn repo_overview_workspace_section(
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let body = ui::vbox(8);
    body.append(&overview_command_group("Actions", overview_actions(true)));
    body.append(&overview_command_group(
        "Files",
        overview_file_actions(snapshot, host_ptr),
    ));
    ui::section("Workspace", &[], &body)
}

fn repo_overview_activity_section(logs: &[String], limit: usize) -> GtkBox {
    let visible = logs.iter().rev().take(limit).cloned().collect::<Vec<_>>();
    let mut visible = visible.into_iter().rev().collect::<Vec<_>>();
    if visible.is_empty() {
        visible.push("No operations recorded yet.".to_string());
    }
    let body = repo_overview_lines_body(&visible, "No operations recorded yet.", true);
    ui::section("Activity", &[], &body)
}

/// Lines in a bordered list (monospace for paths, logs and git output), or a
/// muted message when there are none.
fn repo_overview_lines_body(lines: &[String], empty_message: &str, mono: bool) -> GtkBox {
    if lines.is_empty() {
        let block = ui::vbox(0);
        let empty = Label::new(Some(empty_message));
        empty.set_xalign(0.0);
        ui::set_fg_tone(&empty, ui::Tone::Muted);
        block.append(&empty);
        return block;
    }
    let list = ui::lines_card();
    for line in lines {
        let row = Label::new(Some(line));
        row.set_xalign(0.0);
        row.set_wrap(true);
        row.set_wrap_mode(gtk::pango::WrapMode::WordChar);
        row.set_selectable(true);
        row.add_css_class(ui::class::LINE);
        if mono {
            row.add_css_class("mono");
            row.add_css_class(ui::class::META);
        }
        list.append(&row);
    }
    list
}

fn build_repo_terminal_panel(snapshot: &StateSnapshot, instance_key: Option<&str>) -> GtkBox {
    let panel = GtkBox::new(Orientation::Vertical, 0);
    panel.set_hexpand(true);
    panel.set_vexpand(true);

    let header = ui::tool_window_header("Terminal", 4);

    let restart = ui::icon_button("view-refresh-symbolic", "Restart terminal");
    restart.set_sensitive(false);
    header.append(&restart);

    let spacer = GtkBox::new(Orientation::Horizontal, 0);
    spacer.set_hexpand(true);
    header.append(&spacer);

    let external = ui::icon_button("utilities-terminal-symbolic", "Open external terminal");
    external.set_sensitive(false);
    header.append(&external);
    panel.append(&header);

    let body = GtkBox::new(Orientation::Vertical, 6);
    body.set_hexpand(true);
    body.set_vexpand(true);
    body.set_size_request(-1, 180);
    panel.append(&body);

    let target_repo_id = instance_key.or(snapshot.active_repo_id.as_deref());
    let Some(item) = target_repo_id.and_then(|repo_id| repo_item_by_id(snapshot, repo_id)) else {
        panel.add_css_class("repo-terminal-empty");
        body.append(&ui::empty_state(
            "utilities-terminal-symbolic",
            "No terminal",
            "No repository target is attached to this tab, so no terminal session can be started.",
            None,
        ));
        return panel;
    };

    external.set_sensitive(true);
    let repo_path = item.status.repo_path.clone();
    let repo_name = item.name.clone();
    external.connect_clicked(move |_| {
        open_path_in_terminal(&repo_path, &repo_name);
    });

    #[cfg(feature = "embedded-terminal")]
    {
        body.append(&build_vte_terminal_widget(
            &item.status.repo_path,
            &item.name,
            &restart,
        ));
    }

    #[cfg(not(feature = "embedded-terminal"))]
    {
        let disabled = Label::new(Some(
            "This build was compiled without the `embedded-terminal` feature. Enable that feature and install the VTE development package to render an interactive shell here.",
        ));
        disabled.set_xalign(0.0);
        disabled.set_wrap(true);
        ui::set_fg_tone(&disabled, ui::Tone::Muted);
        body.append(&disabled);
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
fn build_vte_terminal_widget(path: &Path, label: &str, restart: &Button) -> GtkBox {
    let panel = GtkBox::new(Orientation::Vertical, 0);
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
    terminal_theme::attach(&terminal);

    let shell = preferred_embedded_shell();
    let path = path.to_path_buf();
    let label = label.to_string();
    let active_pid = Rc::new(Cell::new(None::<glib::Pid>));
    let restart_pending = Rc::new(Cell::new(false));

    spawn_shell_in_terminal(&terminal, &path, &label, &shell, active_pid.clone(), false);

    terminal.connect_child_exited({
        let terminal = terminal.clone();
        let path = path.clone();
        let label = label.clone();
        let shell = shell.clone();
        let active_pid = active_pid.clone();
        let restart_pending = restart_pending.clone();
        move |_, _| {
            active_pid.set(None);
            if restart_pending.replace(false) {
                spawn_shell_in_terminal(&terminal, &path, &label, &shell, active_pid.clone(), true);
            }
        }
    });

    restart.set_sensitive(true);
    restart.connect_clicked({
        let terminal = terminal.clone();
        let path = path.clone();
        let label = label.clone();
        let shell = shell.clone();
        let active_pid = active_pid.clone();
        let restart_pending = restart_pending.clone();
        move |_| {
            if restart_pending.get() {
                return;
            }
            if let Some(pid) = active_pid.get() {
                restart_pending.set(true);
                if terminate_embedded_shell_group(pid, libc::SIGHUP) {
                    schedule_embedded_shell_kill_fallback(
                        pid,
                        active_pid.clone(),
                        restart_pending.clone(),
                    );
                } else {
                    restart_pending.set(false);
                    active_pid.set(None);
                    spawn_shell_in_terminal(
                        &terminal,
                        &path,
                        &label,
                        &shell,
                        active_pid.clone(),
                        true,
                    );
                }
            } else {
                spawn_shell_in_terminal(&terminal, &path, &label, &shell, active_pid.clone(), true);
            }
        }
    });
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
fn spawn_shell_in_terminal(
    terminal: &vte4::Terminal,
    path: &Path,
    label: &str,
    shell: &str,
    active_pid: Rc<Cell<Option<glib::Pid>>>,
    clear_terminal: bool,
) {
    if clear_terminal {
        terminal.reset(false, true);
    }

    if !path.exists() {
        terminal.feed(b"Working directory does not exist.\r\n");
        return;
    }

    let argv = [shell];
    let cwd = path.to_string_lossy().to_string();
    let repo_label = label.to_string();
    let terminal = terminal.clone();
    terminal.clone().spawn_async(
        vte4::PtyFlags::DEFAULT,
        Some(cwd.as_str()),
        &argv,
        &[],
        glib::SpawnFlags::SEARCH_PATH,
        || unsafe {
            libc::setsid();
        },
        -1,
        None::<&gio::Cancellable>,
        move |result| match result {
            Ok(pid) => {
                terminal.watch_child(pid);
                active_pid.set(Some(pid));
            }
            Err(error) => {
                active_pid.set(None);
                append_log(format!(
                    "Failed to start embedded terminal for {repo_label}: {error}"
                ));
            }
        },
    );
}

#[cfg(feature = "embedded-terminal")]
fn terminate_embedded_shell_group(pid: glib::Pid, signal: libc::c_int) -> bool {
    unsafe { libc::kill(-pid.0, signal) == 0 || libc::kill(pid.0, signal) == 0 }
}

#[cfg(feature = "embedded-terminal")]
fn schedule_embedded_shell_kill_fallback(
    pid: glib::Pid,
    active_pid: Rc<Cell<Option<glib::Pid>>>,
    restart_pending: Rc<Cell<bool>>,
) {
    glib::timeout_add_local_once(Duration::from_millis(750), move || {
        if restart_pending.get() && active_pid.get() == Some(pid) {
            let _ = terminate_embedded_shell_group(pid, libc::SIGKILL);
        }
    });
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

    let operation_running = operation_in_flight();
    for (label, handler) in entries {
        let button = ui::button(label, ui::ButtonKind::Secondary);
        if matches!(label, "Pull" | "Push") {
            set_operation_button_running(&button, operation_running);
            let button_ref = glib::WeakRef::new();
            button_ref.set(Some(&button));
            OPERATION_BUTTONS.with(|buttons| buttons.borrow_mut().push(button_ref));
        }
        button.connect_clicked(move |_| {
            let _ = handler(maruzzella_sdk::ffi::MzBytes::empty());
        });
        actions.append(&button);
    }
    actions
}

fn set_operation_button_running(button: &Button, operation_running: bool) {
    button.set_sensitive(!operation_running);
    button.set_tooltip_text(operation_running.then_some("Another operation is still running"));
}

fn refresh_operation_buttons() {
    let operation_running = operation_in_flight();
    OPERATION_BUTTONS.with(|buttons| {
        buttons.borrow_mut().retain(|button_ref| {
            let Some(button) = button_ref.upgrade() else {
                return false;
            };
            set_operation_button_running(&button, operation_running);
            true
        });
    });
}

/// A labelled row of actions, the label aligned like a settings key.
fn overview_command_group(label: &str, actions: GtkBox) -> GtkBox {
    let group = ui::hbox(16);
    group.set_hexpand(true);

    let label_widget = Label::new(Some(label));
    label_widget.set_xalign(0.0);
    label_widget.set_valign(Align::Center);
    label_widget.set_size_request(80, -1);
    label_widget.add_css_class(ui::class::KV_KEY);
    group.append(&label_widget);

    actions.set_hexpand(true);
    group.append(&actions);
    group
}

fn action_button(label: &str, count: usize) -> Button {
    let button = ui::button(&format!("{label} ({count})"), ui::ButtonKind::Secondary);
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
        refresh_views();
    });
    button
}

fn report_button(
    label: &str,
    handler: extern "C" fn(maruzzella_sdk::ffi::MzBytes) -> maruzzella_sdk::ffi::MzStatus,
) -> Button {
    let button = ui::button(label, ui::ButtonKind::Secondary);
    button.connect_clicked(move |_| {
        let _ = handler(maruzzella_sdk::ffi::MzBytes::empty());
    });
    button
}

fn set_report_button_loading(button: &Button, label: &str, loading: bool) {
    let text = if loading { "Running..." } else { label };
    if button.label().as_deref() != Some(text) {
        button.set_label(text);
    }
    button.set_sensitive(!loading);
}

fn repo_target_button(item: &RepositoryListItem) -> Button {
    let button = ui::link_button("Target");
    let repo_id = item.id.clone();
    button.connect_clicked(move |_| {
        update_selected_repo_ids(vec![repo_id.clone()]);
        refresh_views();
    });
    button
}

fn repo_open_button(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> Button {
    let button = ui::link_button("Open");
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
    let row = ui::hbox(12);
    row.add_css_class(ui::class::LIST_ROW);
    row.set_tooltip_text(Some(&format!(
        "{}\n{}",
        item.status.repo_path.display(),
        item.remote_url
    )));

    let name = monitor_cell(&item.name, None);
    ui::set_fg_tone(&name, repo_name_tone(&item.status));
    let branch = monitor_cell(branch_label(item), Some(MONITOR_BRANCH_COL_WIDTH));
    ui::set_fg_tone(&branch, ui::Tone::Muted);

    row.append(&name);
    row.append(&branch);
    row.append(&monitor_state_cell(&item.status.state));
    row.append(&monitor_sync_cell(&item.status.sync));
    let actions = ui::hbox(12);
    actions.set_size_request(OVERVIEW_ROW_ACTIONS_WIDTH, -1);
    actions.set_halign(Align::End);
    actions.append(&repo_target_button(item));
    actions.append(&repo_open_button(item, host_ptr));
    row.append(&actions);
    row
}

const OVERVIEW_ROW_ACTIONS_WIDTH: i32 = 96;

fn overview_repo_header() -> GtkBox {
    let header = ui::hbox(12);
    header.add_css_class(ui::class::LIST_HEADER);
    header.append(&monitor_cell("Name", None));
    header.append(&monitor_cell("Branch", Some(MONITOR_BRANCH_COL_WIDTH)));
    header.append(&monitor_cell("Status", Some(MONITOR_STATE_COL_WIDTH)));
    header.append(&monitor_cell("Sync", Some(MONITOR_SYNC_COL_WIDTH)));
    let actions = monitor_cell("", Some(OVERVIEW_ROW_ACTIONS_WIDTH));
    header.append(&actions);
    header
}

fn append_facts_section(container: &GtkBox, heading: &str, facts: &[(&str, String)]) {
    container.append(&ui::section(
        heading,
        &[],
        &ui::card(&ui::key_value_grid(facts)),
    ));
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

fn repo_overview_action_button(
    item: &RepositoryListItem,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    label: &'static str,
    kind: Option<OperationKind>,
    style: ui::ButtonKind,
) -> Button {
    let button = ui::button(label, style);
    let repo_id = item.id.clone();
    let repo_name = item.name.clone();
    let repo_path = item.status.repo_path.clone();
    button.connect_clicked(move |_| match label {
        "Target This Repo" => {
            set_selected_repo_ids(vec![repo_id.clone()]);
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
        "Edit Repo Manifest" => {
            if let Err(message) = open_repo_manifest_editor(host_ptr, &repo_id, &repo_name) {
                append_log(message);
            }
        }
        "Edit README" => {
            open_text_editor_for_path(host_ptr, &repo_path.join("README.md"));
        }
        "Edit .git/config" => {
            open_text_editor_for_path(host_ptr, &repo_path.join(".git/config"));
        }
        "Run Cap Checks" => {
            set_selected_repo_ids(vec![repo_id.clone()]);
            run_selected_repo_capability_checks();
        }
        _ => {
            set_selected_repo_ids(vec![repo_id.clone()]);
            if let Some(kind) = kind {
                launch_operation(kind);
            }
        }
    });
    button
}

fn overview_file_actions(
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) -> GtkBox {
    let actions = ui::hbox(16);

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
        let button = ui::link_button(label);
        button.connect_clicked(move |_| {
            open_text_editor_for_path(host_ptr, &path);
        });
        actions.append(&button);
    }

    actions
}

fn monorepo_selection_actions(
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    items: &[RepositoryListItem],
) -> GtkBox {
    let actions = ui::vbox(8);
    let scope_row = ui::hbox(8);

    let selected_ids = snapshot.selected_repo_ids.clone();
    let selected_count = selected_ids.len();

    let buckets = [
        select_repo_bucket(
            "Select Attention",
            collect_repo_ids(items, |item| repo_attention_rank(snapshot, item) < 7),
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
    ];
    let bucket_row = ui::wrap_box(&buckets.each_ref().map(|button| button.upcast_ref()), 8);

    scope_row.append(&repo_overview_open_button(
        host_ptr,
        "Open Selected",
        selected_ids,
    ));
    scope_row.append(&clear_selection_button(selected_count));

    let settings = ui::link_button("Fucina Policy");
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

fn monorepo_report_controls() -> MonorepoReportControls {
    let history_row = ui::hbox(8);
    let check_history = report_button("Check History", command_check_history);
    history_row.append(&check_history);
    let open_commit_check = ui::link_button("Open Commit Check");
    open_commit_check.connect_clicked(|_| {
        let _ = command_open_commit_check(maruzzella_sdk::ffi::MzBytes::empty());
    });
    history_row.append(&open_commit_check);

    let stats_row = ui::hbox(8);
    let since_entry = Entry::new();
    since_entry.add_css_class(&input_css_class("field"));
    since_entry.set_placeholder_text(Some("Since date YYYY-MM-DD"));
    since_entry.set_width_chars(22);
    since_entry.connect_changed(|entry| {
        update_line_stats_since(entry.text().to_string());
    });
    since_entry.connect_activate(|_| {
        let _ = command_line_stats(maruzzella_sdk::ffi::MzBytes::empty());
    });
    stats_row.append(&since_entry);
    let line_stats = report_button("Line Stats", command_line_stats);
    stats_row.append(&line_stats);
    let all_time = ui::button("All Time", ui::ButtonKind::Ghost);
    all_time.connect_clicked(|_| {
        update_line_stats_since(String::new());
        let _ = command_line_stats(maruzzella_sdk::ffi::MzBytes::empty());
    });
    stats_row.append(&all_time);

    let capability_row = ui::hbox(8);
    let capability_checks = ui::button("Capability Checks", ui::ButtonKind::Secondary);
    capability_checks.connect_clicked(|_| {
        run_selected_repo_capability_checks();
    });
    capability_row.append(&capability_checks);
    let stale_capability_checks = ui::button("Stale Cap Checks", ui::ButtonKind::Secondary);
    stale_capability_checks.connect_clicked(|_| {
        run_stale_repo_capability_checks();
    });
    capability_row.append(&stale_capability_checks);
    let due_capability_checks = ui::button("Due Cap Checks", ui::ButtonKind::Secondary);
    due_capability_checks.connect_clicked(|_| {
        run_due_repo_capability_checks();
    });
    capability_row.append(&due_capability_checks);

    let rows = ui::vbox(8);
    rows.append(&history_row);
    rows.append(&stats_row);
    rows.append(&capability_row);

    MonorepoReportControls {
        row: rows,
        check_history,
        since_entry,
        line_stats,
        all_time,
    }
}

/// Updates the persistent report controls in place, so typing in the date
/// entry survives refreshes.
fn update_monorepo_report_controls(controls: &MonorepoReportControls, snapshot: &StateSnapshot) {
    set_report_button_loading(
        &controls.check_history,
        "Check History",
        snapshot.history_report_loading,
    );
    set_report_button_loading(
        &controls.line_stats,
        "Line Stats",
        snapshot.line_stats_loading,
    );
    controls
        .all_time
        .set_sensitive(!snapshot.line_stats_loading);
    // The entry writes every edit to the state, so they only differ when the
    // date was changed elsewhere (e.g. by All Time).
    if controls.since_entry.text().as_str() != snapshot.line_stats_since {
        controls.since_entry.set_text(&snapshot.line_stats_since);
    }
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

fn append_lines_section(container: &GtkBox, heading: &str, lines: &[String], empty_message: &str) {
    container.append(&ui::section(
        heading,
        &[],
        &repo_overview_lines_body(lines, empty_message, true),
    ));
}

fn append_repo_group_section(
    container: &GtkBox,
    heading: &str,
    body: &str,
    items: &[RepositoryListItem],
    limit: Option<usize>,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    let block = ui::vbox(8);
    block.append(&ui::hint(body));

    if items.is_empty() {
        let empty_label = Label::new(Some("Nothing to show."));
        empty_label.set_xalign(0.0);
        ui::set_fg_tone(&empty_label, ui::Tone::Muted);
        block.append(&empty_label);
    } else {
        let take = limit.unwrap_or(items.len());
        let table = ui::list_card();
        table.append(&overview_repo_header());
        for item in items.iter().take(take) {
            table.append(&overview_repo_row(item, host_ptr));
        }
        block.append(&table);
        if items.len() > take {
            let more_label = Label::new(Some(&format!(
                "{} more repos hidden in this section.",
                items.len() - take
            )));
            more_label.set_xalign(0.0);
            ui::set_fg_tone(&more_label, ui::Tone::Muted);
            block.append(&more_label);
        }
    }

    let count = ui::badge(&items.len().to_string(), ui::Tone::Muted);
    container.append(&ui::section(heading, &[count.upcast_ref()], &block));
}

fn append_log_section(container: &GtkBox, heading: &str, logs: &[String], limit: usize) {
    let lines = logs
        .iter()
        .rev()
        .take(limit)
        .rev()
        .cloned()
        .collect::<Vec<_>>();
    append_lines_section(container, heading, &lines, "No operations recorded yet.");
}

fn attention_items(
    snapshot: &StateSnapshot,
    items: &[RepositoryListItem],
) -> Vec<RepositoryListItem> {
    let mut attention = items
        .iter()
        .filter(|item| repo_attention_rank(snapshot, item) < 7)
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
        Ok(_) => {}
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

fn create_repo_manifest_editor_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let repo_id = unsafe { request.as_ref() }
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

    let root = padded_view_root(20, 24);

    let snapshot = snapshot();
    let Some(repo_id) = repo_id else {
        root.append(&ui::empty_state(
            "document-edit-symbolic",
            "Repo Manifest Editor",
            "No repository id was provided for this editor tab.",
            None,
        ));
        return into_view_ptr(root);
    };

    render_repo_manifest_editor_into(&root, &snapshot, &repo_id, host);

    let scroller = vertical_view_scroller(&root);

    into_view_ptr(scroller)
}

fn render_repo_manifest_editor_into(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    repo_id: &str,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    clear_box(root);

    let Some(item) = repository_items(snapshot)
        .into_iter()
        .find(|item| item.id == repo_id)
    else {
        root.append(&ui::empty_state(
            "document-edit-symbolic",
            "Repo Manifest Editor",
            &format!("Repository {repo_id} is not present in the current workspace manifest."),
            None,
        ));
        return;
    };

    let manifest_path = default_repo_manifest_path(&item.status.repo_path);
    let manifest = load_repo_manifest(&manifest_path).unwrap_or_else(|_| RepoManifest {
        schema_version: ronomepo_core::REPO_MANIFEST_SCHEMA_VERSION,
        repo_id: Some(item.id.clone()),
        items: Vec::new(),
        actions: Vec::new(),
        capabilities: Vec::new(),
        repo_actions: Vec::new(),
        aggregation: Vec::new(),
    });

    let title = ui::page_title(&format!("{} Manifest", item.name));
    let subtitle = ui::ellipsized(
        &format!("Editing {}", manifest_path.display()),
        &["mono", ui::class::META, &ui::Tone::Muted.fg_class()],
    );

    let preserved_repo_actions = Rc::new(manifest.repo_actions.clone());
    let preserved_aggregation = Rc::new(manifest.aggregation.clone());

    let repo_id_entry = Entry::new();
    repo_id_entry.add_css_class(&input_css_class("field"));
    repo_id_entry.set_text(manifest.repo_id.as_deref().unwrap_or(&item.id));

    let item_rows_box = ui::vbox(8);
    let item_rows = Rc::new(RefCell::new(Vec::<RepoManifestItemRowHandle>::new()));
    for manifest_item in &manifest.items {
        append_repo_manifest_item_row(&item_rows_box, &item_rows, Some(manifest_item));
    }

    let action_rows_box = ui::vbox(8);
    let action_rows = Rc::new(RefCell::new(Vec::<RepoManifestActionRowHandle>::new()));
    for action in &manifest.actions {
        append_repo_manifest_action_row(&action_rows_box, &action_rows, Some(action));
    }

    let capability_rows_box = ui::vbox(8);
    let capability_rows = Rc::new(RefCell::new(Vec::<RepoManifestCapabilityRowHandle>::new()));
    for capability in &manifest.capabilities {
        append_repo_manifest_capability_row(
            &capability_rows_box,
            &capability_rows,
            Some(capability),
        );
    }

    let status = Label::new(Some(
        "Edit rows, preview JSON, then save the repo manifest.",
    ));
    status.set_xalign(0.0);
    status.set_wrap(true);
    ui::set_fg_tone(&status, ui::Tone::Muted);
    status.add_css_class(ui::class::META);

    let preview_buffer = TextBuffer::new(None);
    let preview_text = TextView::with_buffer(&preview_buffer);
    preview_text.set_monospace(true);
    preview_text.set_editable(false);
    preview_text.add_css_class(ui::class::EDITOR);
    preview_text.set_top_margin(8);
    preview_text.set_bottom_margin(8);
    preview_text.set_left_margin(12);
    preview_text.set_right_margin(12);
    preview_text.set_wrap_mode(WrapMode::WordChar);
    preview_text.set_vexpand(true);
    let preview_scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .min_content_height(260)
        .hscrollbar_policy(PolicyType::Automatic)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&preview_text)
        .build();

    refresh_repo_manifest_preview(
        &preview_buffer,
        &status,
        &repo_id_entry,
        &item_rows.borrow(),
        &action_rows.borrow(),
        &capability_rows.borrow(),
        &preserved_repo_actions,
        &preserved_aggregation,
    );

    let actions = ui::hbox(8);
    let add_cargo = ui::button("Add Cargo Item", ui::ButtonKind::Secondary);
    add_cargo.connect_clicked({
        let item_rows_box = item_rows_box.clone();
        let item_rows = item_rows.clone();
        move |_| {
            append_repo_manifest_item_row(
                &item_rows_box,
                &item_rows,
                Some(&ronomepo_core::RepoItem {
                    id: "server".to_string(),
                    item_type: "cargo".to_string(),
                    path: PathBuf::from("."),
                    config: None,
                    artifacts: Vec::new(),
                    actions: Vec::new(),
                }),
            )
        }
    });
    let add_action = ui::button("Add Action", ui::ButtonKind::Secondary);
    add_action.connect_clicked({
        let action_rows_box = action_rows_box.clone();
        let action_rows = action_rows.clone();
        move |_| append_repo_manifest_action_row(&action_rows_box, &action_rows, None)
    });
    let add_capability = ui::button("Add Capability", ui::ButtonKind::Secondary);
    add_capability.connect_clicked({
        let capability_rows_box = capability_rows_box.clone();
        let capability_rows = capability_rows.clone();
        move |_| append_repo_manifest_capability_row(&capability_rows_box, &capability_rows, None)
    });
    let preview = ui::button("Preview JSON", ui::ButtonKind::Ghost);
    preview.connect_clicked({
        let preview_buffer = preview_buffer.clone();
        let status = status.clone();
        let repo_id_entry = repo_id_entry.clone();
        let item_rows = item_rows.clone();
        let action_rows = action_rows.clone();
        let capability_rows = capability_rows.clone();
        let preserved_repo_actions = preserved_repo_actions.clone();
        let preserved_aggregation = preserved_aggregation.clone();
        move |_| {
            refresh_repo_manifest_preview(
                &preview_buffer,
                &status,
                &repo_id_entry,
                &item_rows.borrow(),
                &action_rows.borrow(),
                &capability_rows.borrow(),
                &preserved_repo_actions,
                &preserved_aggregation,
            );
        }
    });
    let save = ui::button("Save Manifest", ui::ButtonKind::Primary);
    save.connect_clicked({
        let preview_buffer = preview_buffer.clone();
        let status = status.clone();
        let repo_id_entry = repo_id_entry.clone();
        let item_rows = item_rows.clone();
        let action_rows = action_rows.clone();
        let capability_rows = capability_rows.clone();
        let preserved_repo_actions = preserved_repo_actions.clone();
        let preserved_aggregation = preserved_aggregation.clone();
        let manifest_path = manifest_path.clone();
        move |_| match build_repo_manifest_from_editor_rows(
            &repo_id_entry,
            &item_rows.borrow(),
            &action_rows.borrow(),
            &capability_rows.borrow(),
            &preserved_repo_actions,
            &preserved_aggregation,
        ) {
            Ok(manifest) => match save_repo_manifest(&manifest_path, &manifest) {
                Ok(()) => {
                    let json = serde_json::to_string_pretty(&manifest).unwrap_or_default();
                    preview_buffer.set_text(&json);
                    let message = format!("Saved repo manifest to {}.", manifest_path.display());
                    status.set_text(&message);
                    append_log(message);
                    schedule_workspace_scan();
                    refresh_views();
                }
                Err(error) => status.set_text(&format!("Cannot save manifest: {error}")),
            },
            Err(message) => status.set_text(&message),
        }
    });
    let edit_json = ui::link_button("Edit JSON File");
    edit_json.connect_clicked({
        let manifest_path = manifest_path.clone();
        move |_| open_text_editor_for_path(host_ptr, &manifest_path)
    });

    for button in [
        save,
        preview,
        add_cargo,
        add_action,
        add_capability,
        edit_json,
    ] {
        actions.append(&button);
    }

    let header = ui::vbox(4);
    header.append(&title);
    header.append(&subtitle);
    root.append(&header);
    root.append(&actions);
    root.append(&status);
    root.append(&labeled_field("Repo ID", &repo_id_entry));
    append_repo_manifest_editor_section(
        root,
        "Items",
        "Declare code projects inside this repository.",
        &item_rows_box,
    );
    append_repo_manifest_editor_section(
        root,
        "Repo Actions",
        "Commands that capabilities can reference by id.",
        &action_rows_box,
    );
    append_repo_manifest_editor_section(
        root,
        "Capabilities",
        "Operational contracts exposed by this repo.",
        &capability_rows_box,
    );
    append_repo_manifest_editor_section(
        root,
        "JSON Preview",
        "Preview is generated from the structured fields above.",
        &{
            let frame = ui::list_card();
            frame.append(&preview_scroller);
            frame
        },
    );
}

fn append_repo_manifest_editor_section(
    container: &GtkBox,
    heading: &str,
    body: &str,
    child: &impl IsA<gtk::Widget>,
) {
    let block = ui::vbox(8);
    block.append(&ui::hint(body));
    block.append(child);
    container.append(&ui::section(heading, &[], &block));
}

fn append_repo_manifest_item_row(
    rows_box: &GtkBox,
    rows: &Rc<RefCell<Vec<RepoManifestItemRowHandle>>>,
    item: Option<&ronomepo_core::RepoItem>,
) {
    let row = ui::hbox(8);
    row.add_css_class(ui::class::CARD);
    let id = Entry::new();
    id.set_placeholder_text(Some("id"));
    id.set_width_chars(16);
    let item_type = Entry::new();
    item_type.set_placeholder_text(Some("cargo"));
    item_type.set_width_chars(14);
    let path = Entry::new();
    path.set_placeholder_text(Some("path"));
    path.set_hexpand(true);
    let remove = ui::icon_button("user-trash-symbolic", "Remove");

    if let Some(item) = item {
        id.set_text(&item.id);
        item_type.set_text(&item.item_type);
        path.set_text(&item.path.display().to_string());
    }

    row.append(&id);
    row.append(&item_type);
    row.append(&path);
    row.append(&remove);
    rows_box.append(&row);

    rows.borrow_mut().push(RepoManifestItemRowHandle {
        row: row.clone(),
        id: id.clone(),
        item_type: item_type.clone(),
        path: path.clone(),
        config: item.and_then(|item| item.config.clone()),
        artifacts: item.map(|item| item.artifacts.clone()).unwrap_or_default(),
        actions: item.map(|item| item.actions.clone()).unwrap_or_default(),
    });
    remove.connect_clicked({
        let rows_box = rows_box.clone();
        let rows = rows.clone();
        let row = row.clone();
        move |_| {
            rows_box.remove(&row);
            rows.borrow_mut().retain(|handle| !handle.row.eq(&row));
        }
    });
}

fn append_repo_manifest_action_row(
    rows_box: &GtkBox,
    rows: &Rc<RefCell<Vec<RepoManifestActionRowHandle>>>,
    action: Option<&ronomepo_core::RepoActionDefinition>,
) {
    let row = ui::hbox(8);
    row.add_css_class(ui::class::CARD);
    let id = Entry::new();
    id.set_placeholder_text(Some("id"));
    id.set_width_chars(20);
    let command = Entry::new();
    command.set_placeholder_text(Some("command and args"));
    command.set_hexpand(true);
    let workdir = Entry::new();
    workdir.set_placeholder_text(Some("workdir"));
    workdir.set_width_chars(14);
    let timeout_seconds = Entry::new();
    timeout_seconds.set_placeholder_text(Some("timeout"));
    timeout_seconds.set_width_chars(8);
    let output = clickable_dropdown_from_strings(&["text", "json", "json_lines"]);
    let remove = ui::icon_button("user-trash-symbolic", "Remove");

    if let Some(action) = action {
        id.set_text(&action.id);
        command.set_text(&action.command.join(" "));
        workdir.set_text(
            &action
                .workdir
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        );
        timeout_seconds.set_text(
            &action
                .timeout_seconds
                .map(|timeout| timeout.to_string())
                .unwrap_or_default(),
        );
        output.set_selected(action_output_index(action.output));
    }

    row.append(&id);
    row.append(&command);
    row.append(&workdir);
    row.append(&timeout_seconds);
    row.append(&output);
    row.append(&remove);
    rows_box.append(&row);

    rows.borrow_mut().push(RepoManifestActionRowHandle {
        row: row.clone(),
        id: id.clone(),
        command: command.clone(),
        workdir: workdir.clone(),
        timeout_seconds: timeout_seconds.clone(),
        output: output.clone(),
        env: action.map(|action| action.env.clone()).unwrap_or_default(),
    });
    remove.connect_clicked({
        let rows_box = rows_box.clone();
        let rows = rows.clone();
        let row = row.clone();
        move |_| {
            rows_box.remove(&row);
            rows.borrow_mut().retain(|handle| !handle.row.eq(&row));
        }
    });
}

fn append_repo_manifest_capability_row(
    rows_box: &GtkBox,
    rows: &Rc<RefCell<Vec<RepoManifestCapabilityRowHandle>>>,
    capability: Option<&ronomepo_core::RepoCapabilityDeclaration>,
) {
    let row = ui::vbox(6);
    row.add_css_class(ui::class::CARD);
    let top = ui::hbox(8);
    let bottom = ui::hbox(8);
    let id = Entry::new();
    id.set_placeholder_text(Some("capability id"));
    id.set_width_chars(22);
    let capability_name = Entry::new();
    capability_name.set_placeholder_text(Some("integrity.tests"));
    capability_name.set_width_chars(22);
    let status = clickable_dropdown_from_strings(&["implemented", "not_applicable", "unsupported"]);
    let item_id = Entry::new();
    item_id.set_placeholder_text(Some("item_id"));
    item_id.set_width_chars(14);
    let root = Entry::new();
    root.set_placeholder_text(Some("root"));
    root.set_width_chars(14);
    let action_kind = clickable_dropdown_from_strings(&["standard", "repo_action"]);
    let action_value = Entry::new();
    action_value.set_placeholder_text(Some("cargo.test or action id"));
    action_value.set_hexpand(true);
    let schedule_kind =
        clickable_dropdown_from_strings(&["manual", "interval", "on_commit_change"]);
    let interval_seconds = Entry::new();
    interval_seconds.set_placeholder_text(Some("seconds"));
    interval_seconds.set_width_chars(9);
    let reason = Entry::new();
    reason.set_placeholder_text(Some("reason for N/A or unsupported"));
    reason.set_hexpand(true);
    let remove = ui::icon_button("user-trash-symbolic", "Remove");

    if let Some(capability) = capability {
        id.set_text(&capability.id);
        capability_name.set_text(&capability.capability);
        status.set_selected(capability_status_index(capability.status));
        item_id.set_text(capability.item_id.as_deref().unwrap_or_default());
        root.set_text(
            &capability
                .root
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        );
        if let Some(standard) = capability.standard_action.as_deref() {
            action_kind.set_selected(0);
            action_value.set_text(standard);
        } else if let Some(action_ref) = &capability.action_ref {
            match action_ref {
                ronomepo_core::RepoActionRef::Standard {
                    name,
                    item_id: action_item_id,
                    root: action_root,
                } => {
                    action_kind.set_selected(0);
                    action_value.set_text(name);
                    if capability.item_id.is_none() {
                        item_id.set_text(action_item_id.as_deref().unwrap_or_default());
                    }
                    if capability.root.is_none() {
                        root.set_text(
                            &action_root
                                .as_ref()
                                .map(|path| path.display().to_string())
                                .unwrap_or_default(),
                        );
                    }
                }
                ronomepo_core::RepoActionRef::RepoAction { id } => {
                    action_kind.set_selected(1);
                    action_value.set_text(id);
                }
                ronomepo_core::RepoActionRef::Group { .. } => {
                    action_value.set_placeholder_text(Some("group action_ref preserved"));
                }
            }
        }
        if let Some(schedule) = capability.schedule.as_ref() {
            configure_schedule_controls(schedule, &schedule_kind, &interval_seconds);
        }
        reason.set_text(capability.reason.as_deref().unwrap_or_default());
    }

    top.append(&id);
    top.append(&capability_name);
    top.append(&status);
    top.append(&item_id);
    top.append(&root);
    bottom.append(&action_kind);
    bottom.append(&action_value);
    bottom.append(&schedule_kind);
    bottom.append(&interval_seconds);
    bottom.append(&reason);
    bottom.append(&remove);
    row.append(&top);
    row.append(&bottom);
    rows_box.append(&row);

    rows.borrow_mut().push(RepoManifestCapabilityRowHandle {
        row: row.clone(),
        id: id.clone(),
        capability: capability_name.clone(),
        status: status.clone(),
        item_id: item_id.clone(),
        root: root.clone(),
        action_kind: action_kind.clone(),
        action_value: action_value.clone(),
        schedule_kind: schedule_kind.clone(),
        interval_seconds: interval_seconds.clone(),
        reason: reason.clone(),
        preserved_action_ref: capability.and_then(|capability| capability.action_ref.clone()),
        preserved_standard_action: capability
            .and_then(|capability| capability.standard_action.clone()),
    });
    remove.connect_clicked({
        let rows_box = rows_box.clone();
        let rows = rows.clone();
        let row = row.clone();
        move |_| {
            rows_box.remove(&row);
            rows.borrow_mut().retain(|handle| !handle.row.eq(&row));
        }
    });
}

fn refresh_repo_manifest_preview(
    buffer: &TextBuffer,
    status: &Label,
    repo_id: &Entry,
    item_rows: &[RepoManifestItemRowHandle],
    action_rows: &[RepoManifestActionRowHandle],
    capability_rows: &[RepoManifestCapabilityRowHandle],
    preserved_repo_actions: &[ronomepo_core::RepoActionCommand],
    preserved_aggregation: &[ronomepo_core::RepoActionAggregation],
) {
    match build_repo_manifest_from_editor_rows(
        repo_id,
        item_rows,
        action_rows,
        capability_rows,
        preserved_repo_actions,
        preserved_aggregation,
    ) {
        Ok(manifest) => match serde_json::to_string_pretty(&manifest) {
            Ok(json) => {
                buffer.set_text(&json);
                status.set_text("Preview is valid.");
            }
            Err(error) => status.set_text(&format!("Cannot render preview: {error}")),
        },
        Err(message) => status.set_text(&message),
    }
}

fn build_repo_manifest_from_editor_rows(
    repo_id: &Entry,
    item_rows: &[RepoManifestItemRowHandle],
    action_rows: &[RepoManifestActionRowHandle],
    capability_rows: &[RepoManifestCapabilityRowHandle],
    preserved_repo_actions: &[ronomepo_core::RepoActionCommand],
    preserved_aggregation: &[ronomepo_core::RepoActionAggregation],
) -> Result<RepoManifest, String> {
    let items = build_repo_manifest_items(item_rows)?;
    let actions = build_repo_manifest_actions(action_rows)?;
    let capabilities = build_repo_manifest_capabilities(capability_rows)?;
    let manifest = RepoManifest {
        schema_version: ronomepo_core::REPO_MANIFEST_SCHEMA_VERSION,
        repo_id: nonempty_entry_text(repo_id),
        items,
        actions,
        capabilities,
        repo_actions: preserved_repo_actions.to_vec(),
        aggregation: preserved_aggregation.to_vec(),
    };
    ronomepo_core::validate_repo_manifest(&manifest).map_err(|error| error.to_string())?;
    Ok(manifest)
}

fn build_repo_manifest_items(
    rows: &[RepoManifestItemRowHandle],
) -> Result<Vec<ronomepo_core::RepoItem>, String> {
    let mut items = Vec::new();
    for row in rows {
        let id = row.id.text().trim().to_string();
        let item_type = row.item_type.text().trim().to_string();
        let path = row.path.text().trim().to_string();
        if id.is_empty() && item_type.is_empty() && path.is_empty() {
            continue;
        }
        if id.is_empty() || item_type.is_empty() || path.is_empty() {
            return Err("Each non-empty item row needs id, type, and path.".to_string());
        }
        items.push(ronomepo_core::RepoItem {
            id,
            item_type,
            path: PathBuf::from(path),
            config: row.config.clone(),
            artifacts: row.artifacts.clone(),
            actions: row.actions.clone(),
        });
    }
    Ok(items)
}

fn build_repo_manifest_actions(
    rows: &[RepoManifestActionRowHandle],
) -> Result<Vec<ronomepo_core::RepoActionDefinition>, String> {
    let mut actions = Vec::new();
    for row in rows {
        let id = row.id.text().trim().to_string();
        let command = split_command_entry(&row.command.text());
        let workdir = nonempty_entry_path(&row.workdir);
        let timeout_seconds = parse_optional_u64(&row.timeout_seconds, "action timeout")?;
        if id.is_empty() && command.is_empty() && workdir.is_none() && timeout_seconds.is_none() {
            continue;
        }
        if id.is_empty() || command.is_empty() {
            return Err("Each non-empty action row needs id and command.".to_string());
        }
        actions.push(ronomepo_core::RepoActionDefinition {
            id,
            command,
            workdir,
            env: row.env.clone(),
            timeout_seconds,
            output: action_output_from_index(row.output.selected()),
        });
    }
    Ok(actions)
}

fn build_repo_manifest_capabilities(
    rows: &[RepoManifestCapabilityRowHandle],
) -> Result<Vec<ronomepo_core::RepoCapabilityDeclaration>, String> {
    let mut capabilities = Vec::new();
    for row in rows {
        let id = row.id.text().trim().to_string();
        let capability = row.capability.text().trim().to_string();
        if id.is_empty() && capability.is_empty() && row.action_value.text().trim().is_empty() {
            continue;
        }
        if id.is_empty() || capability.is_empty() {
            return Err("Each non-empty capability row needs id and capability.".to_string());
        }
        let status = capability_status_from_index(row.status.selected());
        let action_value = row.action_value.text().trim().to_string();
        let (action_ref, standard_action) =
            if status == ronomepo_core::CapabilityDeclarationStatus::Implemented {
                if action_value.is_empty() {
                    match (
                        row.preserved_action_ref.clone(),
                        row.preserved_standard_action.clone(),
                    ) {
                        (Some(action_ref), None) => (Some(action_ref), None),
                        (None, Some(standard_action)) => (None, Some(standard_action)),
                        _ => {
                            return Err(format!(
                                "Implemented capability {id} needs an action reference."
                            ));
                        }
                    }
                } else if row.action_kind.selected() == 1 {
                    (
                        Some(ronomepo_core::RepoActionRef::RepoAction { id: action_value }),
                        None,
                    )
                } else {
                    (None, Some(action_value))
                }
            } else {
                (None, None)
            };
        let reason = nonempty_entry_text(&row.reason);
        let schedule = schedule_value_from_controls(&row.schedule_kind, &row.interval_seconds)?;
        capabilities.push(ronomepo_core::RepoCapabilityDeclaration {
            id,
            capability,
            status,
            item_id: nonempty_entry_text(&row.item_id),
            root: nonempty_entry_path(&row.root),
            action_ref,
            standard_action,
            reason,
            schedule,
        });
    }
    Ok(capabilities)
}

fn nonempty_entry_text(entry: &Entry) -> Option<String> {
    let value = entry.text().trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn nonempty_entry_path(entry: &Entry) -> Option<PathBuf> {
    nonempty_entry_text(entry).map(PathBuf::from)
}

fn split_command_entry(raw: &str) -> Vec<String> {
    raw.split_whitespace().map(ToString::to_string).collect()
}

fn parse_optional_u64(entry: &Entry, label: &str) -> Result<Option<u64>, String> {
    let raw = entry.text().trim().to_string();
    if raw.is_empty() {
        return Ok(None);
    }
    raw.parse::<u64>()
        .map(Some)
        .map_err(|_| format!("{label} must be a positive integer."))
}

fn action_output_index(output: ronomepo_core::ActionOutputMode) -> u32 {
    match output {
        ronomepo_core::ActionOutputMode::Text => 0,
        ronomepo_core::ActionOutputMode::Json => 1,
        ronomepo_core::ActionOutputMode::JsonLines => 2,
    }
}

fn action_output_from_index(index: u32) -> ronomepo_core::ActionOutputMode {
    match index {
        1 => ronomepo_core::ActionOutputMode::Json,
        2 => ronomepo_core::ActionOutputMode::JsonLines,
        _ => ronomepo_core::ActionOutputMode::Text,
    }
}

fn capability_status_index(status: ronomepo_core::CapabilityDeclarationStatus) -> u32 {
    match status {
        ronomepo_core::CapabilityDeclarationStatus::Implemented => 0,
        ronomepo_core::CapabilityDeclarationStatus::NotApplicable => 1,
        ronomepo_core::CapabilityDeclarationStatus::Unsupported => 2,
    }
}

fn capability_status_from_index(index: u32) -> ronomepo_core::CapabilityDeclarationStatus {
    match index {
        1 => ronomepo_core::CapabilityDeclarationStatus::NotApplicable,
        2 => ronomepo_core::CapabilityDeclarationStatus::Unsupported,
        _ => ronomepo_core::CapabilityDeclarationStatus::Implemented,
    }
}

fn configure_schedule_controls(value: &Value, schedule_kind: &DropDown, interval_seconds: &Entry) {
    match value {
        Value::Object(object) => {
            if object
                .get("kind")
                .or_else(|| object.get("mode"))
                .and_then(Value::as_str)
                == Some("on_commit_change")
                || object
                    .get("on_commit_change")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            {
                schedule_kind.set_selected(2);
            } else if let Some(interval) = object.get("interval_seconds").and_then(Value::as_u64) {
                schedule_kind.set_selected(1);
                interval_seconds.set_text(&interval.to_string());
            }
        }
        Value::String(kind) if kind == "on_commit_change" => schedule_kind.set_selected(2),
        _ => schedule_kind.set_selected(0),
    }
}

fn schedule_value_from_controls(
    schedule_kind: &DropDown,
    interval_seconds: &Entry,
) -> Result<Option<Value>, String> {
    match schedule_kind.selected() {
        1 => {
            let interval = parse_optional_u64(interval_seconds, "capability interval_seconds")?
                .ok_or_else(|| "Interval schedules need interval_seconds.".to_string())?;
            Ok(Some(serde_json::json!({
                "kind": "interval",
                "interval_seconds": interval
            })))
        }
        2 => Ok(Some(serde_json::json!({ "kind": "on_commit_change" }))),
        _ => Ok(None),
    }
}

fn create_text_editor_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let root = padded_view_root(12, 16);

    let instance_key =
        unsafe { request.as_ref() }.and_then(|request| decode_mzstr(request.instance_key));
    let initial_path = instance_key.clone().or_else(|| {
        unsafe { request.as_ref() }.and_then(|request| {
            if request.payload.ptr.is_null() || request.payload.len == 0 {
                None
            } else {
                let bytes =
                    unsafe { std::slice::from_raw_parts(request.payload.ptr, request.payload.len) };
                let text = String::from_utf8_lossy(bytes).trim().to_string();
                (!text.is_empty()).then_some(text)
            }
        })
    });
    let instance_key = instance_key.unwrap_or_default();
    let editor_id = register_text_editor(instance_key.clone());
    root.connect_destroy(move |_| unregister_text_editor(editor_id));

    let title = ui::page_title("Text Editor");

    let toolbar = ui::hbox(8);
    let path_entry = Entry::new();
    path_entry.add_css_class(&input_css_class("field"));
    path_entry.add_css_class("mono");
    path_entry.set_hexpand(true);
    path_entry.set_placeholder_text(Some("Relative or absolute file path"));

    let open_button = ui::button("Open", ui::ButtonKind::Secondary);
    let save_button = ui::button("Save", ui::ButtonKind::Primary);
    let format_button = ui::button("Format", ui::ButtonKind::Ghost);

    toolbar.append(&path_entry);
    toolbar.append(&open_button);
    toolbar.append(&save_button);
    toolbar.append(&format_button);

    let status = Label::new(Some("Open a text file from the workspace."));
    status.set_xalign(0.0);
    ui::set_fg_tone(&status, ui::Tone::Muted);
    status.add_css_class(ui::class::META);
    status.set_wrap(true);

    let buffer = TextBuffer::new(None);
    // Set while the editor itself replaces the buffer text.
    let suppress_dirty = Rc::new(Cell::new(false));
    buffer.connect_changed({
        let suppress_dirty = Rc::clone(&suppress_dirty);
        move |_| {
            if !suppress_dirty.get() {
                set_text_editor_dirty(editor_id, true);
            }
        }
    });
    let text = TextView::with_buffer(&buffer);
    text.set_monospace(true);
    text.set_wrap_mode(WrapMode::WordChar);
    text.set_vexpand(true);
    text.add_css_class(ui::class::EDITOR);
    text.set_top_margin(8);
    text.set_bottom_margin(8);
    text.set_left_margin(12);
    text.set_right_margin(12);

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Automatic)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&text)
        .build();

    let widgets = TextEditorWidgets {
        id: editor_id,
        buffer: buffer.clone(),
        status: status.clone(),
        title: title.clone(),
        path_entry: path_entry.clone(),
        suppress_dirty,
    };

    if let Some(path) = initial_path.as_deref() {
        path_entry.set_text(path);
        let resolved = resolve_editor_path(path);
        title.set_text(&editor_title_for_path(&resolved));
        queue_editor_load(&widgets, &resolved);
    }

    open_button.connect_clicked({
        let widgets = widgets.clone();
        let initial_path = initial_path.clone();
        move |_| {
            let path = resolve_editor_path(widgets.path_entry.text().as_str());
            let current_instance = initial_path
                .as_deref()
                .map(resolve_editor_path)
                .unwrap_or_default();
            if !current_instance.as_os_str().is_empty() && current_instance != path {
                open_text_editor_for_path(host, &path);
            } else {
                queue_editor_load(&widgets, &path);
                widgets.title.set_text(&editor_title_for_path(&path));
            }
        }
    });

    save_button.connect_clicked({
        let widgets = widgets.clone();
        move |_| {
            let path = resolve_editor_path(widgets.path_entry.text().as_str());
            let content = widgets.buffer.text(
                &widgets.buffer.start_iter(),
                &widgets.buffer.end_iter(),
                true,
            );
            queue_editor_save(
                &widgets,
                &path,
                content.to_string(),
                &instance_key,
                host as usize,
            );
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

    let frame = ui::list_card();
    frame.append(&scroller);

    root.append(&title);
    root.append(&toolbar);
    root.append(&status);
    root.append(&frame);

    into_view_ptr(root)
}

/// Widgets of one text editor view that its load and save completions touch.
/// The completions only run on the GTK thread.
#[derive(Clone)]
struct TextEditorWidgets {
    id: usize,
    buffer: TextBuffer,
    status: Label,
    title: Label,
    path_entry: Entry,
    suppress_dirty: Rc<Cell<bool>>,
}

fn register_text_editor(instance_key: String) -> usize {
    static NEXT_TEXT_EDITOR_ID: AtomicUsize = AtomicUsize::new(1);
    let id = NEXT_TEXT_EDITOR_ID.fetch_add(1, Ordering::Relaxed);
    TEXT_EDITORS.with(|editors| {
        editors.borrow_mut().insert(
            id,
            TextEditorState {
                instance_key,
                dirty: false,
            },
        );
    });
    id
}

fn unregister_text_editor(id: usize) {
    TEXT_EDITORS.with(|editors| {
        editors.borrow_mut().remove(&id);
    });
}

fn set_text_editor_dirty(id: usize, dirty: bool) {
    TEXT_EDITORS.with(|editors| {
        if let Some(editor) = editors.borrow_mut().get_mut(&id) {
            editor.dirty = dirty;
        }
    });
}

fn text_editor_instance_dirty(instance_key: &str) -> bool {
    TEXT_EDITORS.with(|editors| {
        editors
            .borrow()
            .values()
            .any(|editor| editor.dirty && editor.instance_key == instance_key)
    })
}

fn prepare_text_editor_teardown(request: *const MzViewTeardownRequest) -> MzViewTeardownResult {
    let Some(request) = (unsafe { request.as_ref() }) else {
        return MzViewTeardownResult {
            decision: MzViewTeardownDecision::Blocked,
            message: maruzzella_sdk::ffi::MzStr::from_static(
                "The editor teardown request was invalid.",
            ),
        };
    };
    let instance_key = decode_mzstr(request.view.instance_key).unwrap_or_default();
    let is_dirty = text_editor_instance_dirty(&instance_key);
    MzViewTeardownResult {
        decision: if is_dirty {
            MzViewTeardownDecision::Confirm
        } else {
            MzViewTeardownDecision::Ready
        },
        message: if is_dirty {
            maruzzella_sdk::ffi::MzStr::from_static("This text editor has unsaved changes.")
        } else {
            maruzzella_sdk::ffi::MzStr::empty()
        },
    }
}

fn resolve_editor_path(input: &str) -> PathBuf {
    let trimmed = input.trim();
    let path = PathBuf::from(trimmed);
    if path.is_absolute() {
        path
    } else {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root.join(path)
    }
}

fn queue_editor_load(widgets: &TextEditorWidgets, path: &Path) {
    widgets.suppress_dirty.set(true);
    widgets
        .status
        .set_text(&format!("Loading {}...", path.display()));
    let widgets = widgets.clone();
    let path = path.to_path_buf();
    glib::spawn_future_local(async move {
        let read_path = path.clone();
        let result = gio::spawn_blocking(move || {
            fs::read_to_string(&read_path).map_err(|error| error.to_string())
        })
        .await
        .unwrap_or_else(|_| Err("the file reader stopped unexpectedly".to_string()));

        // The user may have pointed the editor elsewhere in the meantime.
        if resolve_editor_path(widgets.path_entry.text().as_str()) == path {
            match result {
                Ok(content) => {
                    widgets.buffer.set_text(&content);
                    widgets
                        .status
                        .set_text(&format!("Loaded {}", path.display()));
                    widgets.title.set_text(&editor_title_for_path(&path));
                }
                Err(error) => {
                    widgets.buffer.set_text("");
                    widgets
                        .status
                        .set_text(&format!("Failed to open {}: {error}", path.display()));
                }
            }
            // Either way the buffer no longer holds unsaved edits.
            set_text_editor_dirty(widgets.id, false);
        }
        widgets.suppress_dirty.set(false);
    });
}

fn queue_editor_save(
    widgets: &TextEditorWidgets,
    path: &Path,
    content: String,
    instance_key: &str,
    host_ptr: usize,
) {
    widgets
        .status
        .set_text(&format!("Saving {}...", path.display()));
    let widgets = widgets.clone();
    let path = path.to_path_buf();
    let instance_key = instance_key.to_string();
    glib::spawn_future_local(async move {
        let write_path = path.clone();
        let saved_content = content.clone();
        let result = gio::spawn_blocking(move || {
            fs::write(&write_path, content).map_err(|error| error.to_string())
        })
        .await
        .unwrap_or_else(|_| Err("the file writer stopped unexpectedly".to_string()));

        match result {
            Ok(()) => {
                let buffer = &widgets.buffer;
                let current = buffer.text(&buffer.start_iter(), &buffer.end_iter(), true);
                if current.as_str() == saved_content {
                    set_text_editor_dirty(widgets.id, false);
                }
                widgets
                    .status
                    .set_text(&format!("Saved {}", path.display()));
                let title_text = editor_title_for_path(&path);
                widgets.title.set_text(&title_text);
                if host_ptr != 0 {
                    let host = unsafe {
                        HostApi::from_raw(&*(host_ptr as *const maruzzella_sdk::ffi::MzHostApi))
                    };
                    let mut query = maruzzella_sdk::ViewQuery::new(PLUGIN_ID, VIEW_TEXT_EDITOR);
                    query.instance_key = Some(&instance_key);
                    let _ = host.update_view_title(&query, &title_text);
                }
            }
            Err(error) => {
                widgets
                    .status
                    .set_text(&format!("Failed to save {}: {error}", path.display()));
            }
        }
    });
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
        appearance_id: Some("ghost"),
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

fn create_operations_view(
    host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let host = remember_host_ptr(host);

    let root = GtkBox::new(Orientation::Vertical, 0);
    root.set_hexpand(true);
    root.set_vexpand(true);

    let (cursor, logs) = {
        let app_state = state().lock().expect("state mutex poisoned");
        (log_cursor(&app_state), app_state.logs.clone())
    };

    // Run tool window style header: title, live counters, then actions.
    let header = ui::tool_window_header("Operations", 12);
    let stats = ui::hbox(12);
    let mut stat_values = Vec::new();
    for caption in ["started", "ok", "skipped", "failed"] {
        let (stat, value) = ui::stat(caption);
        stats.append(&stat);
        stat_values.push(value);
    }
    header.append(&stats);
    let latest = Label::new(None);
    latest.set_xalign(0.0);
    latest.set_hexpand(true);
    latest.set_ellipsize(EllipsizeMode::End);
    latest.add_css_class(ui::class::META);
    header.append(&latest);
    let [started, ok, skipped, failed]: [Label; 4] = stat_values
        .try_into()
        .unwrap_or_else(|_| unreachable!("four counters were built"));
    let summary_handle = OperationSummaryHandle {
        root: glib::WeakRef::new(),
        started,
        ok,
        skipped,
        failed,
        latest,
    };
    summary_handle.root.set(Some(&root));
    sync_operation_summary(&summary_handle, &operation_summary(&logs));
    OPERATION_SUMMARIES.with(|handles| {
        handles.borrow_mut().push(summary_handle);
    });

    let view_host = unsafe { host.as_ref() }.map(HostApi::from_raw);
    let follow = operation_follow_button();
    follow.set_active(true);
    let refresh = view_host
        .as_ref()
        .and_then(|host| {
            host_toolbar_button(host, CMD_REFRESH_LOGS, "view-refresh-symbolic", "Refresh")
        })
        .unwrap_or_else(|| ui::icon_button("view-refresh-symbolic", "Refresh"));
    let clear = view_host
        .as_ref()
        .and_then(|host| host_toolbar_button(host, CMD_CLEAR_LOGS, "user-trash-symbolic", "Clear"))
        .unwrap_or_else(|| ui::icon_button("user-trash-symbolic", "Clear"));
    ui::style_icon_button(&refresh, "view-refresh-symbolic", "Refresh logs");
    ui::style_icon_button(&clear, "user-trash-symbolic", "Clear logs");

    let buffer = TextBuffer::new(None);
    let buffer_handle = OperationBufferHandle {
        buffer: glib::WeakRef::new(),
        shown: Cell::new(LogCursor::default()),
        line_counts: RefCell::new(Default::default()),
    };
    buffer_handle.buffer.set(Some(&buffer));
    sync_operation_buffer(&buffer_handle, &buffer, cursor, &logs);
    OPERATION_BUFFERS.with(|buffers| {
        buffers.borrow_mut().push(buffer_handle);
    });

    if view_host.is_none() {
        refresh.connect_clicked(|_| refresh_log_surfaces());
        clear.connect_clicked(|_| {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.logs.clear();
                app_state.last_log_message = None;
                push_log_line(&mut app_state, "Operations log cleared.".to_string());
            }
            refresh_log_surfaces();
            refresh_views();
        });
    }

    let text = TextView::with_buffer(&buffer);
    text.set_editable(false);
    text.set_cursor_visible(false);
    text.set_monospace(true);
    text.set_wrap_mode(WrapMode::WordChar);
    text.add_css_class(ui::class::CONSOLE);
    text.set_top_margin(8);
    text.set_bottom_margin(8);
    text.set_left_margin(12);
    text.set_right_margin(12);

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

    header.append(&follow);
    header.append(&refresh);
    header.append(&clear);
    root.append(&header);
    root.append(&scroller);

    suppress_scroll_events.set(true);
    schedule_scroll_to_bottom(&scroller, suppress_scroll_events);

    into_view_ptr(root)
}

/// Counts of the operation log, shown in the Operations header.
struct OperationSummary {
    total: usize,
    starts: usize,
    ok: usize,
    skipped: usize,
    failed: usize,
    latest_failure: Option<String>,
    latest: String,
}

fn operation_summary(logs: &[String]) -> OperationSummary {
    OperationSummary {
        total: logs.len(),
        starts: logs.iter().filter(|line| line.contains("[START]")).count(),
        ok: logs.iter().filter(|line| line.contains("[OK]")).count(),
        skipped: logs.iter().filter(|line| line.contains("[SKIP]")).count(),
        failed: logs.iter().filter(|line| line.contains("[FAIL]")).count(),
        latest_failure: logs
            .iter()
            .rev()
            .find(|line| line.contains("[FAIL]"))
            .cloned(),
        latest: logs
            .last()
            .cloned()
            .unwrap_or_else(|| "No operations recorded yet.".to_string()),
    }
}

fn operation_summary_text(summary: &OperationSummary) -> String {
    let OperationSummary {
        total,
        starts,
        ok,
        skipped,
        failed,
        latest,
        ..
    } = summary;
    let mut text = match &summary.latest_failure {
        Some(failure) => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest failure: {failure} | Latest: {latest}"
        ),
        None => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest: {latest}"
        ),
    };
    if let Some(runtime_line) = runtime_profile_summary("operations") {
        text.push_str(" | ");
        text.push_str(&runtime_line);
    }
    text
}

fn sync_operation_summary(handle: &OperationSummaryHandle, summary: &OperationSummary) {
    handle.started.set_text(&summary.starts.to_string());
    handle.ok.set_text(&summary.ok.to_string());
    handle.skipped.set_text(&summary.skipped.to_string());
    handle.failed.set_text(&summary.failed.to_string());
    ui::set_fg_tone(
        &handle.ok,
        if summary.ok > 0 {
            ui::Tone::Success
        } else {
            ui::Tone::Strong
        },
    );
    ui::set_fg_tone(
        &handle.failed,
        if summary.failed > 0 {
            ui::Tone::Danger
        } else {
            ui::Tone::Strong
        },
    );
    // The latest failure matters more than the latest line, which the log
    // below already shows.
    match &summary.latest_failure {
        Some(failure) => {
            handle
                .latest
                .set_text(&format!("Latest failure: {failure}"));
            ui::set_fg_tone(&handle.latest, ui::Tone::Danger);
        }
        None => {
            handle.latest.set_text(&summary.latest);
            ui::set_fg_tone(&handle.latest, ui::Tone::Muted);
        }
    }
    handle
        .latest
        .set_tooltip_text(Some(&operation_summary_text(summary)));
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

#[cfg(test)]
mod tests {
    use super::*;
    use ronomepo_core::{
        default_commit_check_rules, RepositoryEntry, RepositoryState, RepositorySync,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::UNIX_EPOCH;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn repo_names_only_stand_out_for_states_that_need_action() {
        let status = |state, sync| RepositoryStatus {
            state,
            sync,
            ..empty_repository_status(PathBuf::from("/tmp/repo"))
        };
        let cases = [
            (
                RepositoryState::Clean,
                RepositorySync::UpToDate,
                ui::Tone::Strong,
            ),
            (
                RepositoryState::Untracked,
                RepositorySync::UpToDate,
                ui::Tone::Strong,
            ),
            (
                RepositoryState::Clean,
                RepositorySync::Ahead(2),
                ui::Tone::Strong,
            ),
            (
                RepositoryState::Clean,
                RepositorySync::Behind(1),
                ui::Tone::Strong,
            ),
            (
                RepositoryState::Clean,
                RepositorySync::NoUpstream,
                ui::Tone::Strong,
            ),
            (
                RepositoryState::Dirty,
                RepositorySync::UpToDate,
                ui::Tone::Accent,
            ),
            (
                RepositoryState::Dirty,
                RepositorySync::Diverged {
                    ahead: 1,
                    behind: 1,
                },
                ui::Tone::Warning,
            ),
            (
                RepositoryState::Clean,
                RepositorySync::Diverged {
                    ahead: 1,
                    behind: 1,
                },
                ui::Tone::Warning,
            ),
            (
                RepositoryState::Missing,
                RepositorySync::Unknown,
                ui::Tone::Danger,
            ),
            (
                RepositoryState::Unknown,
                RepositorySync::Unknown,
                ui::Tone::Danger,
            ),
        ];
        for (state, sync, tone) in cases {
            let label = format!("{state:?} {sync:?}");
            assert_eq!(repo_name_tone(&status(state, sync)), tone, "{label}");
        }
        assert_eq!(state_tone(&RepositoryState::Untracked), ui::Tone::Muted);
        assert_eq!(state_tone(&RepositoryState::Dirty), ui::Tone::Accent);
        assert_eq!(state_tone(&RepositoryState::Missing), ui::Tone::Danger);
    }

    fn temp_test_dir(label: &str) -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        // Include the pid: concurrent test runs share the temp dir and would
        // otherwise delete each other's fixtures.
        let path = std::env::temp_dir().join(format!(
            "ronomepo-plugin-{label}-{}-{id}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn test_repository_item(
        name: &str,
        state: RepositoryState,
        sync: RepositorySync,
    ) -> RepositoryListItem {
        RepositoryListItem {
            id: name.to_string(),
            name: name.to_string(),
            dir_name: name.to_string(),
            remote_url: format!("git@example.com:org/{name}.git"),
            status: RepositoryStatus {
                state,
                branch: Some("main".to_string()),
                sync,
                head_commit: Some("abc123".to_string()),
                repo_path: PathBuf::from(name),
            },
            repo_manifest: None,
        }
    }

    #[test]
    fn descriptor_uses_expected_plugin_id() {
        let descriptor = RonomepoPlugin::descriptor();
        assert_eq!(descriptor.id, PLUGIN_ID);
        assert_eq!(descriptor.dependencies.len(), 1);
        assert_eq!(descriptor.dependencies[0].plugin_id, "maruzzella.base");
    }

    #[test]
    fn fucina_presentation_exposes_policy_and_unresolved_backup_gate() {
        let repository = FucinaRepositoryPolicy {
            name: "private-repo".to_string(),
            role: "primary".to_string(),
            publication: "local-only".to_string(),
            forgejo: "ssh://git@fucina.homelab:2222/lelloman/private-repo.git".to_string(),
            upstream: None,
            publication_target: None,
            mirror_schedule: None,
            archived: false,
            backup_required: true,
            backups: Vec::new(),
        };

        assert!(fucina_repository_remotes(&repository).contains("Fucina:"));
        assert_eq!(
            fucina_repository_backup(&repository),
            "Backup required · destination unresolved"
        );
    }

    #[test]
    fn fucina_failure_output_is_actionable() {
        let text = fucina_command_result_text(Ok(FucinaPolicyCommandResult {
            success: false,
            stdout: "REFUSE private-repo".to_string(),
            stderr: "required backup destination unresolved".to_string(),
        }));
        assert!(text.contains("REFUSED / FAILED"));
        assert!(text.contains("REFUSE private-repo"));
        assert!(text.contains("required backup destination unresolved"));
    }

    #[test]
    fn fucina_apply_confirmation_requires_successful_preview() {
        let success = Ok(FucinaPolicyCommandResult {
            success: true,
            stdout: "PLAN alpha".to_string(),
            stderr: String::new(),
        });
        let refusal = Ok(FucinaPolicyCommandResult {
            success: false,
            stdout: "REFUSE alpha".to_string(),
            stderr: String::new(),
        });
        assert!(fucina_preview_allows_confirmation(&success));
        assert!(!fucina_preview_allows_confirmation(&refusal));
        assert!(!fucina_preview_allows_confirmation(&Err(
            "worker failed".to_string()
        )));
    }

    #[test]
    fn fucina_policy_is_resolved_from_manifest_directory_not_relative_root() {
        let workspace = temp_test_dir("fucina-relative-root");
        let configured_root = workspace.join("..");
        let manifest = workspace.join("ronomepo.json");
        fs::write(&manifest, "{}").unwrap();
        fs::write(workspace.join("repos.toml"), "schema_version = 2").unwrap();
        fs::write(workspace.join("fucina_policy.py"), "# policy engine").unwrap();

        assert_eq!(
            fucina_policy_root(&configured_root, Some(&manifest)),
            workspace
        );
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
    fn sync_state_sort_key_prioritizes_out_of_sync_repositories() {
        let mut items = vec![
            test_repository_item(
                "z-untracked",
                RepositoryState::Untracked,
                RepositorySync::UpToDate,
            ),
            test_repository_item(
                "m-dirty-ahead-four",
                RepositoryState::Dirty,
                RepositorySync::Ahead(4),
            ),
            test_repository_item(
                "a-dirty-no-upstream",
                RepositoryState::Dirty,
                RepositorySync::NoUpstream,
            ),
            test_repository_item(
                "a-untracked",
                RepositoryState::Untracked,
                RepositorySync::UpToDate,
            ),
            test_repository_item(
                "z-dirty-ahead-one",
                RepositoryState::Dirty,
                RepositorySync::Ahead(1),
            ),
        ];

        items.sort_by_key(repo_sync_state_sort_key);

        let names = items
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "m-dirty-ahead-four",
                "z-dirty-ahead-one",
                "a-dirty-no-upstream",
                "a-untracked",
                "z-untracked",
            ]
        );
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

    #[cfg(unix)]
    #[test]
    fn recursive_watchers_skip_symlink_targets_and_still_report_local_changes() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        use std::sync::mpsc;

        for polling in [false, true] {
            let root = temp_test_dir(if polling {
                "poll-symlinks"
            } else {
                "native-symlinks"
            });
            let repo = root.join("repo");
            let blocked = root.join("blocked");
            fs::create_dir_all(repo.join("src")).unwrap();
            fs::create_dir(&blocked).unwrap();
            fs::set_permissions(&blocked, fs::Permissions::from_mode(0o000)).unwrap();
            symlink(&blocked, repo.join("z:")).unwrap();
            symlink(&repo, repo.join("loop")).unwrap();
            symlink(root.join("missing"), repo.join("broken")).unwrap();

            let (tx, rx) = mpsc::channel();
            let config = repository_watch_config();
            let mut backend = if polling {
                WatchBackend::Poll(PollWatcher::new(tx, config.with_manual_polling()).unwrap())
            } else {
                WatchBackend::Recommended(RecommendedWatcher::new(tx, config).unwrap())
            };
            let registration =
                watch_backend_mut(&mut backend).watch(&repo, RecursiveMode::Recursive);
            // Restore permissions even if registration failed, so cleanup is possible.
            fs::set_permissions(&blocked, fs::Permissions::from_mode(0o700)).unwrap();
            registration.unwrap();
            for event in rx.try_iter() {
                event.expect("symlinks must not cause watcher errors");
            }

            let file = repo.join("src/new.rs");
            fs::write(&file, "fn main() {}\n").unwrap();
            if let WatchBackend::Poll(watcher) = &backend {
                watcher.poll().unwrap();
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let event = rx
                    .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                    .expect("local file creation must still be reported")
                    .expect("symlinks must not cause watcher errors");
                if event.paths.contains(&file) {
                    break;
                }
            }
            drop(backend);
            fs::remove_dir_all(root).unwrap();
        }
    }

    fn run_git(repo: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["-c", "user.name=Test", "-c", "user.email=test@example.com"])
            .args([
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.hooksPath=/dev/null",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Feeds whatever the real watcher reports until it goes quiet.
    fn collect_watch_events(
        rx: &mpsc::Receiver<notify::Result<notify::Event>>,
        manifest: &WorkspaceManifest,
    ) -> PendingWatchEvents {
        let mut pending = PendingWatchEvents::default();
        while let Ok(event) = rx.recv_timeout(Duration::from_millis(500)) {
            let event = event.unwrap();
            if watch_event_may_change_status(&event) {
                record_watch_event(&mut pending, &WatchRouting::new(manifest), &event);
            }
        }
        pending
    }

    #[test]
    fn log_lines_are_timestamped_and_consecutive_repeats_collapse() {
        let mut app_state = AppState::default();
        for message in ["watch failed", "watch failed", "other", "watch failed"] {
            push_log_line(&mut app_state, message.to_string());
        }
        assert_eq!(app_state.logs.len(), 3);
        assert!(app_state.logs[0].ends_with(" watch failed (x2)"));
        assert!(app_state.logs[1].ends_with(" other"));
        assert!(app_state.logs[2].ends_with(" watch failed"));
        let stamp = app_state.logs[1].split(' ').next().unwrap();
        assert_eq!(stamp.len(), 8, "{stamp}");
        assert_eq!(stamp.matches(':').count(), 2, "{stamp}");
    }

    #[test]
    fn unreadable_subdirectory_is_skipped_without_losing_the_repository() {
        use std::os::unix::fs::PermissionsExt;

        let root = temp_test_dir("watch-unreadable");
        let manifest = two_repo_watch_manifest(&root);
        let alpha = root.join("alpha");
        let locked = alpha.join("captures/run-locked");
        fs::create_dir_all(&locked).unwrap();
        fs::create_dir_all(alpha.join("src/nested")).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::read_dir(&locked).is_ok() {
            // Running with CAP_DAC_OVERRIDE (e.g. as root): nothing to test.
            fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
            return;
        }

        let (tx, rx) = mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, repository_watch_config()).unwrap();
        let mut unreadable = Vec::new();
        let result = watch_dir_tree(&mut watcher, &alpha, &HashSet::new(), &mut unreadable);
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
        result.unwrap();
        assert_eq!(unreadable, vec![locked]);

        fs::write(alpha.join("src/nested/lib.rs"), "// v1\n").unwrap();
        let pending = collect_watch_events(&rx, &manifest);
        assert!(pending.repo_ids.contains("alpha"), "{:?}", pending.repo_ids);
    }

    #[test]
    fn git_ignored_directories_and_git_objects_are_not_watched() {
        let root = temp_test_dir("watch-ignored");
        let manifest = two_repo_watch_manifest(&root);
        let alpha = root.join("alpha");
        fs::create_dir_all(alpha.join("src/nested")).unwrap();
        fs::create_dir_all(alpha.join("target/debug/deps")).unwrap();
        fs::write(alpha.join(".gitignore"), "target/\n").unwrap();
        fs::write(alpha.join("src/lib.rs"), "// v1\n").unwrap();
        run_git(&alpha, &["init", "-q", "-b", "main"]);
        run_git(&alpha, &["add", "."]);
        run_git(&alpha, &["commit", "-q", "-m", "initial"]);

        let ignored = git_ignored_dirs(&alpha, &alpha)
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(ignored, HashSet::from([alpha.join("target")]));

        let (tx, rx) = mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, repository_watch_config()).unwrap();
        let mut unreadable = Vec::new();
        let registered = watch_dir_tree(&mut watcher, &alpha, &ignored, &mut unreadable).unwrap();
        // alpha, src, src/nested and .git minus objects; never target/**.
        let git_dirs = walk_dirs(&alpha.join(".git"))
            .into_iter()
            .filter(|dir| !dir.starts_with(alpha.join(".git/objects")))
            .count();
        assert_eq!(registered, 3 + git_dirs);
        collect_watch_events(&rx, &manifest);

        fs::write(alpha.join("target/debug/deps/a.o"), "x").unwrap();
        let pending = collect_watch_events(&rx, &manifest);
        assert!(pending.repo_ids.is_empty(), "{:?}", pending.repo_ids);

        fs::write(alpha.join("src/nested/lib.rs"), "// v1\n").unwrap();
        let pending = collect_watch_events(&rx, &manifest);
        assert_eq!(pending.repo_ids, HashSet::from(["alpha".to_string()]));

        // New directories are queued for registration; their contents only
        // report once registered.
        fs::create_dir(alpha.join("src/fresh")).unwrap();
        let pending = collect_watch_events(&rx, &manifest);
        assert_eq!(pending.new_dirs, HashSet::from([alpha.join("src/fresh")]));

        drop(watcher);
        fs::remove_dir_all(root).unwrap();
    }

    fn walk_dirs(root: &Path) -> Vec<PathBuf> {
        let mut dirs = vec![root.to_path_buf()];
        let mut index = 0;
        while index < dirs.len() {
            for entry in fs::read_dir(&dirs[index]).unwrap().flatten() {
                if entry.file_type().unwrap().is_dir() {
                    dirs.push(entry.path());
                }
            }
            index += 1;
        }
        dirs
    }

    #[test]
    fn real_watcher_reports_commits_but_not_status_scans() {
        let root = temp_test_dir("watch-commit");
        let manifest = two_repo_watch_manifest(&root);
        for id in ["alpha", "beta"] {
            let repo = root.join(id);
            fs::create_dir_all(repo.join("src")).unwrap();
            fs::write(repo.join("src/lib.rs"), "// v1\n").unwrap();
            run_git(&repo, &["init", "-q", "-b", "main"]);
            run_git(&repo, &["add", "."]);
            run_git(&repo, &["commit", "-q", "-m", "initial"]);
            // Backdate files so status does not keep rewriting a racily-clean index.
            let old = SystemTime::now() - Duration::from_secs(3600);
            fs::File::options()
                .write(true)
                .open(repo.join("src/lib.rs"))
                .unwrap()
                .set_modified(old)
                .unwrap();
            run_git(&repo, &["status", "--porcelain"]);
        }
        let alpha = root.join("alpha");
        fs::write(alpha.join("src/lib.rs"), "// v2\n").unwrap();
        run_git(&alpha, &["add", "."]);

        let (tx, rx) = mpsc::channel();
        let mut watcher = RecommendedWatcher::new(tx, repository_watch_config()).unwrap();
        for id in ["alpha", "beta"] {
            watcher
                .watch(&root.join(id), RecursiveMode::Recursive)
                .unwrap();
        }
        collect_watch_events(&rx, &manifest);

        // The scanner's own status collection must not invalidate anything.
        for id in ["alpha", "beta"] {
            ronomepo_core::collect_repository_status(&root.join(id));
        }
        let pending = collect_watch_events(&rx, &manifest);
        assert!(!pending.backend_rescan);
        assert!(pending.repo_ids.is_empty(), "{:?}", pending.repo_ids);

        // A commit with no working tree change must still invalidate the repo.
        run_git(&alpha, &["commit", "-q", "-m", "second"]);
        let pending = collect_watch_events(&rx, &manifest);
        assert!(!pending.backend_rescan);
        assert_eq!(pending.repo_ids, HashSet::from(["alpha".to_string()]));

        drop(watcher);
        fs::remove_dir_all(root).unwrap();
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
            &WatchRouting::new(&manifest),
            &workspace_root.join(".git/refs/heads/main")
        ));
        assert!(workspace_watch_path_matches(
            &WatchRouting::new(&manifest),
            &workspace_root.join(".git/HEAD")
        ));
        assert!(!workspace_watch_path_matches(
            &WatchRouting::new(&manifest),
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
            &WatchRouting::new(&manifest),
            &workspace_root.join("Cargo.toml")
        ));
        assert!(!workspace_watch_path_matches(
            &WatchRouting::new(&manifest),
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
            repo_id_for_watch_path(&WatchRouting::new(&manifest), &repo_event_path).as_deref(),
            Some("maruzzella")
        );
    }

    #[test]
    fn sibling_repository_matches_absolute_watch_paths_even_after_file_deletion() {
        let workspace_root = temp_test_dir("sibling-watch");
        let manifest = WorkspaceManifest {
            name: "Workspace".to_string(),
            root: workspace_root.join("workspace"),
            repos: vec![RepositoryEntry {
                id: "simple-ai".to_string(),
                name: "simple-ai".to_string(),
                dir_name: "../simple-ai".to_string(),
                remote_url: String::new(),
                enabled: true,
            }],
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        };

        for relative in [".git/index", "deleted-untracked-file"] {
            let event_path = workspace_root.join("simple-ai").join(relative);
            assert!(!event_path.exists());
            assert_eq!(
                repo_id_for_watch_path(&WatchRouting::new(&manifest), &event_path).as_deref(),
                Some("simple-ai")
            );
        }
    }

    fn two_repo_watch_manifest(root: &Path) -> WorkspaceManifest {
        WorkspaceManifest {
            name: "Workspace".to_string(),
            root: root.to_path_buf(),
            repos: ["alpha", "beta"]
                .into_iter()
                .map(|id| RepositoryEntry {
                    id: id.to_string(),
                    name: id.to_string(),
                    dir_name: id.to_string(),
                    remote_url: String::new(),
                    enabled: true,
                })
                .collect(),
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        }
    }

    #[test]
    fn watch_event_burst_is_routed_per_repository_without_overflow() {
        let root = temp_test_dir("watch-burst");
        let manifest = two_repo_watch_manifest(&root);
        let mut pending = PendingWatchEvents::default();

        // A build writing far more files than any path-based cap would allow.
        let mut event = notify::Event::new(notify::EventKind::Any);
        for index in 0..10_000 {
            event = event.add_path(root.join(format!("alpha/target/debug/deps/{index}.o")));
        }
        record_watch_event(&mut pending, &WatchRouting::new(&manifest), &event);
        let objects = notify::Event::new(notify::EventKind::Any)
            .add_path(root.join("beta/.git/objects/ab/cdef"));
        record_watch_event(&mut pending, &WatchRouting::new(&manifest), &objects);

        assert!(!pending.backend_rescan);
        assert!(!pending.workspace_touched);
        assert_eq!(pending.repo_ids, HashSet::from(["alpha".to_string()]));
    }

    #[test]
    fn read_only_access_events_do_not_invalidate_status() {
        let path = PathBuf::from("/workspace/alpha/src/lib.rs");
        let event = |kind| notify::Event::new(kind).add_path(path.clone());

        // Opens and read-only closes come from scans themselves (git status).
        for kind in [
            EventKind::Access(AccessKind::Open(AccessMode::Any)),
            EventKind::Access(AccessKind::Close(AccessMode::Read)),
            EventKind::Access(AccessKind::Read),
        ] {
            assert!(!watch_event_may_change_status(&event(kind)));
        }
        for kind in [
            EventKind::Access(AccessKind::Close(AccessMode::Write)),
            EventKind::Modify(notify::event::ModifyKind::Any),
            EventKind::Create(notify::event::CreateKind::Any),
            EventKind::Remove(notify::event::RemoveKind::Any),
        ] {
            assert!(watch_event_may_change_status(&event(kind)));
        }
        let rescan = notify::Event::new(EventKind::Other).set_flag(notify::event::Flag::Rescan);
        assert!(watch_event_may_change_status(&rescan));
    }

    #[test]
    fn watch_backend_rescan_event_forces_full_refresh() {
        let root = temp_test_dir("watch-rescan");
        let manifest = two_repo_watch_manifest(&root);
        let mut pending = PendingWatchEvents::default();

        let rescan =
            notify::Event::new(notify::EventKind::Other).set_flag(notify::event::Flag::Rescan);
        record_watch_event(&mut pending, &WatchRouting::new(&manifest), &rescan);
        let later =
            notify::Event::new(notify::EventKind::Any).add_path(root.join("beta/src/lib.rs"));
        record_watch_event(&mut pending, &WatchRouting::new(&manifest), &later);

        assert!(pending.backend_rescan);
        assert!(pending.repo_ids.is_empty());
    }

    #[test]
    fn watch_batch_invalidates_every_repository() {
        let mut app_state = AppState::default();
        let touched = ["alpha", "beta", "gamma"]
            .into_iter()
            .map(str::to_string)
            .collect::<HashSet<_>>();

        mark_repos_stale(&mut app_state, &touched);
        for repo_id in &touched {
            assert!(app_state.repo_runtime[repo_id].needs_rescan());
            assert_eq!(app_state.repo_runtime[repo_id].invalidation_seq, 1);
        }

        // Further events still invalidate every repo while scans are pending.
        mark_repos_stale(&mut app_state, &touched);
        for repo_id in &touched {
            assert_eq!(app_state.repo_runtime[repo_id].invalidation_seq, 2);
        }
    }

    #[test]
    fn refresh_requested_during_running_scan_is_not_lost() {
        let repo_path = temp_test_dir("refresh-coalesce");
        let repo_id = "alpha";
        let mut app_state = AppState::default();
        app_state.repository_items.push(RepositoryListItem {
            id: repo_id.to_string(),
            name: "alpha".to_string(),
            dir_name: "alpha".to_string(),
            remote_url: "git@example.com:org/alpha.git".to_string(),
            status: empty_repository_status(repo_path.clone()),
            repo_manifest: None,
        });

        // E.g. an operation finishes and requests a scan, which starts running.
        assert!(begin_repo_status_refresh(&mut app_state, repo_id));
        // A commit lands while that scan is running; no second job is submitted.
        assert!(!begin_repo_status_refresh(&mut app_state, repo_id));

        // The running scan must not be credited with the later change.
        let follow_up = finalize_repo_status_refresh(&mut app_state, repo_id);
        assert_eq!(follow_up, Some(repo_path.clone()));
        assert!(finalize_repo_status_refresh(&mut app_state, repo_id).is_none());
        assert!(!app_state.repo_runtime[repo_id].needs_rescan());
    }

    #[test]
    fn worker_completion_allows_immediate_follow_up_with_same_key() {
        let key = JobKey::RepoStatus("alpha".to_string());
        let in_flight = Mutex::new(HashSet::from([key.clone()]));

        complete_worker_job(&in_flight, Some(key.clone()), || {
            // Simulate the UI consuming completion before the worker returns.
            // The follow-up must be accepted, and the mutex must be unlocked.
            let mut jobs = in_flight.try_lock().expect("completion holds job mutex");
            assert!(
                jobs.insert(key.clone()),
                "follow-up scan was coalesced away"
            );
        });
        assert!(in_flight.lock().unwrap().contains(&key));
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
                head_commit: Some("abc123".to_string()),
                repo_path: repo_path.clone(),
            },
            repo_manifest: Some(RepoManifestScan {
                path: repo_path.join("ronomepo.repo.json"),
                state: RepoManifestScanState::Missing,
                manifest: None,
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

    fn test_monitor_row(name: &str, state: RepositoryState, sync: RepositorySync) -> MonitorRow {
        build_monitor_row(
            test_repository_item(name, state, sync),
            MonitorViewMode::Overview,
            &CapabilityRegistry::default(),
            Some(true),
            i64::MIN,
        )
    }

    #[test]
    fn monitor_store_update_touches_only_changed_rows() {
        let rows = ["a", "b", "c", "d"]
            .into_iter()
            .map(|name| test_monitor_row(name, RepositoryState::Clean, RepositorySync::UpToDate))
            .collect::<Vec<_>>();
        assert_eq!(
            monitor_store_update(&rows, &rows),
            MonitorStoreUpdate::Unchanged
        );

        let mut next = rows.clone();
        next[2] = test_monitor_row("c", RepositoryState::Dirty, RepositorySync::UpToDate);
        assert_eq!(
            monitor_store_update(&rows, &next),
            MonitorStoreUpdate::Positions(vec![2])
        );

        let mut reordered = rows.clone();
        reordered.swap(0, 1);
        assert_eq!(
            monitor_store_update(&rows, &reordered),
            MonitorStoreUpdate::Replace
        );
        assert_eq!(
            monitor_store_update(&rows, &rows[..3]),
            MonitorStoreUpdate::Replace
        );

        let switched_view = rows
            .iter()
            .cloned()
            .map(|mut row| {
                row.view_mode = MonitorViewMode::Git;
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(
            monitor_store_update(&rows, &switched_view),
            MonitorStoreUpdate::Replace
        );
    }

    #[test]
    fn monitor_row_filter_combines_mode_and_search_text() {
        let dirty = test_monitor_row("Alpha", RepositoryState::Dirty, RepositorySync::UpToDate);
        let clean = test_monitor_row("Beta", RepositoryState::Clean, RepositorySync::UpToDate);
        let mut query = MonitorQuery::default();
        assert!(monitor_row_matches(&dirty, &query));
        assert!(monitor_row_matches(&clean, &query));

        query.filter_mode = MonitorFilterMode::Dirty;
        assert!(monitor_row_matches(&dirty, &query));
        assert!(!monitor_row_matches(&clean, &query));

        query.filter_mode = MonitorFilterMode::All;
        query.filter_text = "alpha".to_string();
        assert!(monitor_row_matches(&dirty, &query));
        assert!(!monitor_row_matches(&clean, &query));

        // Fields are matched one at a time, never across a field boundary.
        query.filter_text = "alphagit@".to_string();
        assert!(!monitor_row_matches(&dirty, &query));
        query.filter_text = "org/beta.git".to_string();
        assert!(monitor_row_matches(&clean, &query));
    }

    #[test]
    fn monitor_row_sort_keeps_monorepo_last_in_both_directions() {
        let mut monorepo = test_monitor_row(
            "(monorepo)",
            RepositoryState::Clean,
            RepositorySync::UpToDate,
        );
        monorepo.item.id = MONOREPO_ROW_ID.to_string();
        let rows = vec![
            monorepo,
            test_monitor_row("beta", RepositoryState::Clean, RepositorySync::UpToDate),
            test_monitor_row("Alpha", RepositoryState::Clean, RepositorySync::UpToDate),
        ];
        let mut query = MonitorQuery::default();
        let sorted_names = |query: &MonitorQuery| {
            let mut rows = rows.clone();
            rows.sort_by(|left, right| monitor_row_cmp(query, left, right));
            rows.into_iter()
                .map(|row| row.item.name)
                .collect::<Vec<_>>()
        };
        assert_eq!(sorted_names(&query), vec!["Alpha", "beta", "(monorepo)"]);
        query.descending = true;
        assert_eq!(sorted_names(&query), vec!["beta", "Alpha", "(monorepo)"]);
    }

    #[test]
    fn commit_check_rule_ids_are_unique_within_a_millisecond() {
        let ids = (0..64)
            .map(|_| new_commit_check_rule_id())
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 64);
    }

    #[test]
    fn commit_check_editor_dirty_tracking_and_added_rules() {
        let rules = default_commit_check_rules();
        assert!(rules.len() >= 2);
        let baseline = rules[..1].to_vec();

        assert!(!commit_check_rows_dirty(&Ok(baseline.clone()), &baseline));
        assert!(commit_check_rows_dirty(&Ok(Vec::new()), &baseline));
        assert!(commit_check_rows_dirty(
            &Err("Rule 1 needs a name.".to_string()),
            &baseline
        ));
        let mut edited = baseline.clone();
        edited[0].priority += 1;
        assert!(commit_check_rows_dirty(&Ok(edited), &baseline));

        let added = commit_check_rules_added(&baseline, &[], &rules);
        assert_eq!(added, rules[1..].to_vec());
        let shown = vec![rules[1].id.clone()];
        let added = commit_check_rules_added(&baseline, &shown, &rules);
        assert_eq!(added, rules[2..].to_vec());
    }

    #[test]
    fn log_view_edits_only_touch_changed_entries() {
        let cursor = |first_seq, end_seq, rewrites| LogCursor {
            first_seq,
            end_seq,
            rewrites,
        };
        let shown = cursor(0, 3, 0);
        assert_eq!(log_view_edit(shown, shown), LogViewEdit::Unchanged);
        // Appended lines.
        assert_eq!(
            log_view_edit(shown, cursor(0, 5, 0)),
            LogViewEdit::Update {
                drop_front: 0,
                keep: 3
            }
        );
        // The last line was rewritten, possibly before more lines arrived.
        assert_eq!(
            log_view_edit(shown, cursor(0, 3, 1)),
            LogViewEdit::Update {
                drop_front: 0,
                keep: 2
            }
        );
        assert_eq!(
            log_view_edit(shown, cursor(0, 5, 2)),
            LogViewEdit::Update {
                drop_front: 0,
                keep: 2
            }
        );
        // Old lines were trimmed at the front.
        assert_eq!(
            log_view_edit(cursor(0, 500, 0), cursor(2, 502, 0)),
            LogViewEdit::Update {
                drop_front: 2,
                keep: 498
            }
        );
        // Cleared and restarted with one line.
        assert_eq!(
            log_view_edit(shown, cursor(3, 4, 0)),
            LogViewEdit::Update {
                drop_front: 3,
                keep: 0
            }
        );
        assert_eq!(log_view_edit(shown, cursor(0, 2, 0)), LogViewEdit::Reset);
    }

    #[test]
    fn log_cursor_follows_appends_rewrites_and_trimming() {
        let mut app_state = AppState::default();
        push_log_line(&mut app_state, "one".to_string());
        push_log_line(&mut app_state, "two".to_string());
        push_log_line(&mut app_state, "two".to_string());
        assert_eq!(
            log_cursor(&app_state),
            LogCursor {
                first_seq: 0,
                end_seq: 2,
                rewrites: 1
            }
        );
        for index in 0..MAX_LOG_ENTRIES {
            push_log_line(&mut app_state, format!("line {index}"));
        }
        let cursor = log_cursor(&app_state);
        assert_eq!(cursor.end_seq, 2 + MAX_LOG_ENTRIES as u64);
        assert_eq!(
            cursor.end_seq - cursor.first_seq,
            app_state.logs.len() as u64
        );
    }

    #[test]
    fn command_output_tail_keeps_last_non_empty_lines() {
        use std::os::unix::process::ExitStatusExt;
        let output = Output {
            status: std::process::ExitStatus::from_raw(0),
            stdout: b"one\n\ntwo\nthree\n".to_vec(),
            stderr: b"warn\n".to_vec(),
        };
        assert_eq!(
            command_output_tail(&output, 3),
            vec!["two", "three", "warn"]
        );
        assert_eq!(command_output_tail(&output, 10).len(), 4);
    }

    #[test]
    fn plugin_config_migrates_flat_monitor_fields() {
        let legacy = serde_json::json!({
            "last_workspace_path": "/work",
            "import_banner_dismissed": false,
            "monitor_view_mode": "git",
            "monitor_filter_mode": "dirty",
            "monitor_sort_mode": "sync_state",
            "git_filter_mode": "diverged",
            "git_sort_mode": "branch",
            "git_sort_descending": true,
            "manifest_filter_mode": "has_actions",
        });
        let config: RonomepoPluginConfig = serde_json::from_value(legacy).unwrap();
        assert_eq!(config.monitor_view_mode, MonitorViewMode::Git);
        let settings = config.monitor_views;
        assert_eq!(
            settings.overview,
            MonitorViewSettings {
                filter: MonitorFilterMode::Issues,
                sort: MonitorSortMode::SyncState,
                descending: false,
            }
        );
        assert_eq!(
            settings.git,
            MonitorViewSettings {
                filter: MonitorFilterMode::Diverged,
                sort: MonitorSortMode::Branch,
                descending: true,
            }
        );
        assert_eq!(settings.manifest.filter, MonitorFilterMode::HasActions);
        assert_eq!(settings.checks, MonitorViewSettings::default());

        // Saving writes the grouped form, which loads back unchanged and wins
        // over any flat fields still present.
        let mut saved = serde_json::to_value(&config).unwrap();
        assert!(saved.get("git_filter_mode").is_none());
        saved["git_filter_mode"] = serde_json::json!("clean");
        let reloaded: RonomepoPluginConfig = serde_json::from_value(saved).unwrap();
        assert_eq!(reloaded.monitor_views, settings);
        // The dropped fields are tolerated on load and not written back.
        assert!(serde_json::to_value(&reloaded)
            .unwrap()
            .get("last_workspace_path")
            .is_none());
    }

    #[test]
    fn due_remote_fetches_beyond_capacity_stay_schedulable() {
        let mut app_state = AppState::default();
        let now = UNIX_EPOCH + Duration::from_secs(10_000);
        let count = REMOTE_FETCH_CONCURRENCY + 2;
        for index in 0..count {
            let name = format!("repo-{index}");
            app_state.repository_items.push(test_repository_item(
                &name,
                RepositoryState::Clean,
                RepositorySync::UpToDate,
            ));
            let mut runtime = RepoRuntimeState::new(UNIX_EPOCH, &name);
            runtime.next_fetch_due_at = UNIX_EPOCH + Duration::from_secs(index as u64);
            app_state.repo_runtime.insert(name, runtime);
        }

        let first = claim_due_remote_fetches(&mut app_state, now);
        assert_eq!(first.len(), REMOTE_FETCH_CONCURRENCY);
        // Earliest due first.
        assert_eq!(first[0].0, "repo-0");
        let in_flight = app_state
            .repo_runtime
            .values()
            .filter(|runtime| runtime.remote_fetch_in_flight)
            .count();
        assert_eq!(in_flight, REMOTE_FETCH_CONCURRENCY);
        assert!(claim_due_remote_fetches(&mut app_state, now).is_empty());

        for (repo_id, _, _) in &first {
            mark_remote_fetch_completed(&mut app_state, repo_id, true);
        }
        let second = claim_due_remote_fetches(&mut app_state, now);
        let mut second_ids = second.into_iter().map(|(id, _, _)| id).collect::<Vec<_>>();
        second_ids.sort();
        let expected = (REMOTE_FETCH_CONCURRENCY..count)
            .map(|index| format!("repo-{index}"))
            .collect::<Vec<_>>();
        assert_eq!(second_ids, expected);
    }

    #[test]
    fn remote_fetch_skips_repos_without_upstream() {
        let mut app_state = AppState::default();
        app_state.repository_items.push(test_repository_item(
            "alpha",
            RepositoryState::Clean,
            RepositorySync::NoUpstream,
        ));
        let mut runtime = RepoRuntimeState::new(UNIX_EPOCH, "alpha");
        runtime.next_fetch_due_at = UNIX_EPOCH;
        app_state.repo_runtime.insert("alpha".to_string(), runtime);

        assert!(claim_due_remote_fetches(&mut app_state, SystemTime::now()).is_empty());
        assert!(!app_state.repo_runtime["alpha"].remote_fetch_in_flight);
    }

    #[test]
    fn append_log_from_worker_thread_does_not_panic() {
        // Stand in for the GTK main loop owning the default context.
        let main_context = glib::MainContext::default();
        let _owner = main_context
            .acquire()
            .expect("default main context is free");
        thread::spawn(|| append_log("logged from a worker".to_string()))
            .join()
            .expect("append_log panicked off the main thread");
        assert!(state()
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .logs
            .iter()
            .any(|line| line.ends_with("logged from a worker")));
    }

    #[test]
    fn guarded_code_turns_panics_into_fallbacks() {
        let value = guard_panics("test-panic", 7, || -> i32 { panic!("boom") });
        assert_eq!(value, 7);
        let status = guard_command("test-command", || panic!("boom"));
        assert_eq!(status.code, MzStatusCode::InternalError);
    }

    #[test]
    fn panicking_fetch_job_reports_a_failed_fetch() {
        let job = WorkerJob::RepositoryRemoteFetch {
            repo_id: "alpha".to_string(),
            repo_name: "Alpha".to_string(),
            repo_path: PathBuf::from("alpha"),
        };
        let recover = worker_job_panic_recovery(&job);
        match recover("boom".to_string()) {
            Some(WorkerResult::RepositoryRemoteFetchCompleted {
                repo_id, result, ..
            }) => {
                assert_eq!(repo_id, "alpha");
                assert_eq!(result, Err("boom".to_string()));
            }
            _ => panic!("expected a failed fetch result"),
        }
    }

    fn empty_test_manifest(root: PathBuf) -> WorkspaceManifest {
        WorkspaceManifest {
            name: "Workspace".to_string(),
            root,
            repos: Vec::new(),
            shared_hooks_path: None,
            commit_check_rules: Some(default_commit_check_rules()),
        }
    }

    #[test]
    fn only_one_operation_batch_runs_at_a_time() {
        let mut app_state = AppState::default();
        app_state.manifest = Some(empty_test_manifest(PathBuf::from("/workspace")));

        let (_, _, first) = claim_operation_batch(&mut app_state, "Pull").unwrap();
        let refused = claim_operation_batch(&mut app_state, "Push").unwrap_err();
        assert!(
            refused.contains("Pull (run 1) is still running"),
            "{refused}"
        );

        // A stale completion for another batch must not free the slot.
        assert!(take_finished_operation_batch(&mut app_state, first + 7).is_none());
        assert!(app_state.operation_in_flight.is_some());

        let failure = OperationEvent {
            kind: OperationEventKind::Failed,
            repository_id: Some("alpha".to_string()),
            repository_name: Some("alpha".to_string()),
            message: "rejected".to_string(),
        };
        record_operation_failure(&mut app_state, first, &failure);
        record_operation_failure(&mut app_state, first, &failure);
        let finished = take_finished_operation_batch(&mut app_state, first).unwrap();
        assert_eq!(finished.failures.len(), 2);
        assert!(app_state.operation_in_flight.is_none());

        let (_, _, second) = claim_operation_batch(&mut app_state, "Push").unwrap();
        assert_eq!(second, first + 1);
    }

    #[test]
    fn remote_fetches_wait_for_running_operation() {
        let mut app_state = AppState::default();
        app_state.manifest = Some(empty_test_manifest(PathBuf::from("/workspace")));
        app_state.repository_items.push(test_repository_item(
            "alpha",
            RepositoryState::Clean,
            RepositorySync::UpToDate,
        ));
        let mut runtime = RepoRuntimeState::new(UNIX_EPOCH, "alpha");
        runtime.next_fetch_due_at = UNIX_EPOCH;
        app_state.repo_runtime.insert("alpha".to_string(), runtime);

        claim_operation_batch(&mut app_state, "Pull").unwrap();
        assert!(claim_due_remote_fetches(&mut app_state, SystemTime::now()).is_empty());
        app_state.operation_in_flight = None;
        assert_eq!(
            claim_due_remote_fetches(&mut app_state, SystemTime::now()).len(),
            1
        );
    }

    #[test]
    fn full_scan_does_not_overwrite_repos_changed_mid_scan() {
        let mut app_state = AppState::default();
        for name in ["alpha", "beta"] {
            app_state.repository_items.push(test_repository_item(
                name,
                RepositoryState::Clean,
                RepositorySync::UpToDate,
            ));
            app_state
                .repo_runtime
                .insert(name.to_string(), RepoRuntimeState::new(UNIX_EPOCH, name));
        }
        // The full scan is submitted with both repos at seq 0.
        app_state.workspace_scan_seqs =
            HashMap::from([("alpha".to_string(), 0), ("beta".to_string(), 0)]);
        // beta changes while the scan runs and its own refresh is in flight.
        assert!(begin_repo_status_refresh(&mut app_state, "beta"));

        let scanned = vec![
            test_repository_item("alpha", RepositoryState::Dirty, RepositorySync::UpToDate),
            test_repository_item("beta", RepositoryState::Dirty, RepositorySync::UpToDate),
        ];
        let needs_rescan = apply_full_workspace_scan(&mut app_state, scanned);

        assert!(needs_rescan);
        assert_eq!(
            app_state.repository_items[0].status.state,
            RepositoryState::Dirty
        );
        // beta keeps its current item and stays stale with its refresh in flight.
        assert_eq!(
            app_state.repository_items[1].status.state,
            RepositoryState::Clean
        );
        assert!(app_state.repo_runtime["beta"].needs_rescan());
        assert!(app_state.repo_runtime["beta"].local_refresh_in_flight);
        assert!(!app_state.repo_runtime["alpha"].needs_rescan());
    }

    #[test]
    fn forced_watch_sync_requested_mid_sync_stays_forced() {
        let source = WatchSource {
            workspace_root: PathBuf::from("/workspace"),
            workspace_git_dir: None,
            repo_roots: vec![PathBuf::from("/workspace/alpha")],
        };
        let mut app_state = AppState::default();
        app_state.watch_source = Some(source.clone());

        // Same source and not forced: nothing to rebuild.
        assert!(!claim_watch_manager_sync(
            &mut app_state,
            false,
            Some(&source)
        ));
        assert!(claim_watch_manager_sync(
            &mut app_state,
            true,
            Some(&source)
        ));

        // An overflow forces a rebuild while one runs; a plain sync follows.
        assert!(!claim_watch_manager_sync(
            &mut app_state,
            true,
            Some(&source)
        ));
        assert!(!claim_watch_manager_sync(
            &mut app_state,
            false,
            Some(&source)
        ));
        assert!(app_state.watch_manager_sync_pending);
        assert!(app_state.watch_manager_sync_pending_force);

        // Completion reruns with the collapsed force, which must not be a no-op.
        app_state.watch_manager_sync_in_flight = false;
        let force = app_state.watch_manager_sync_pending_force;
        assert!(claim_watch_manager_sync(
            &mut app_state,
            force,
            Some(&source)
        ));
        assert!(!app_state.watch_manager_sync_pending_force);
    }

    #[test]
    fn new_dirs_seen_during_watch_rebuild_wait_for_the_new_manager() {
        let mut app_state = AppState::default();
        let dirs = vec![PathBuf::from("/workspace/alpha/src/fresh")];
        assert!(!defer_watch_dirs_during_sync(&mut app_state, &dirs));

        app_state.watch_manager_sync_in_flight = true;
        assert!(defer_watch_dirs_during_sync(&mut app_state, &dirs));
        // Still rebuilding (e.g. a collapsed rerun started): keep holding them.
        assert!(take_deferred_watch_dirs(&mut app_state).is_empty());

        app_state.watch_manager_sync_in_flight = false;
        assert_eq!(take_deferred_watch_dirs(&mut app_state), dirs);
        assert!(app_state.deferred_watch_dirs.is_empty());
    }

    #[test]
    fn legacy_monitor_settings_migrate_once_and_old_keys_still_decode() {
        let legacy = br#"{
            "last_workspace_path": "/somewhere",
            "import_banner_dismissed": true,
            "monitor_filter_mode": "dirty",
            "monitor_sort_mode": "branch",
            "monitor_sort_descending": true
        }"#;
        let config = RonomepoPluginConfig::decode(legacy).unwrap();
        let overview = config.monitor_views.overview;
        assert_eq!(overview.filter, MonitorFilterMode::Issues);
        assert_eq!(overview.sort, MonitorSortMode::Branch);
        assert!(overview.descending);

        let encoded = serde_json::to_value(&config).unwrap();
        assert!(encoded.get("monitor_sort_mode").is_none());
        assert!(encoded.get("last_workspace_path").is_none());
        assert!(encoded.get("import_banner_dismissed").is_none());
        let reloaded = RonomepoPluginConfig::decode(&serde_json::to_vec(&config).unwrap()).unwrap();
        assert_eq!(reloaded.monitor_views.overview, overview);

        assert!(RonomepoPluginConfig::decode(b"not json").is_err());
        assert!(RonomepoPluginConfig::decode(b"").is_ok());
    }

    #[test]
    fn plugin_config_writes_keep_other_entries_and_never_clobber_bad_files() {
        let dir = temp_test_dir("plugin-config");
        let path = dir.join("ronomepo").join("plugins.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"other.plugin": {"schema_version": 3, "payload": [1, 2]}}"#,
        )
        .unwrap();

        write_plugin_config(
            &path,
            vec![
                Box::new(|config: &mut RonomepoPluginConfig| {
                    config.monitor_views.git.sort = MonitorSortMode::Branch;
                }),
                Box::new(|config: &mut RonomepoPluginConfig| {
                    config.monitor_views.git.descending = true;
                }),
            ],
        );
        let document: HashMap<String, Value> =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(document["other.plugin"]["schema_version"], 3);
        let payload: Vec<u8> =
            serde_json::from_value(document[PLUGIN_ID]["payload"].clone()).unwrap();
        let config = RonomepoPluginConfig::decode(&payload).unwrap();
        assert_eq!(config.monitor_views.git.sort, MonitorSortMode::Branch);
        assert!(config.monitor_views.git.descending);
        let leftovers = fs::read_dir(path.parent().unwrap()).unwrap().count();
        assert_eq!(leftovers, 1, "temporary file left behind");

        fs::write(&path, "{ truncated").unwrap();
        write_plugin_config(&path, vec![Box::new(|_: &mut RonomepoPluginConfig| {})]);
        assert_eq!(fs::read_to_string(&path).unwrap(), "{ truncated");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn superseded_line_stats_result_is_dropped_and_rerun() {
        let mut app_state = AppState::default();
        app_state.line_stats_loading = true;
        app_state.line_stats_rerun = true;
        let (_, rerun) = complete_line_stats(
            &mut app_state,
            Ok(LineStatsResult {
                lines: vec!["old range".to_string()],
                message: "done".to_string(),
            }),
        );
        assert!(rerun);
        assert!(app_state.line_stats_report.is_empty());
        assert!(!app_state.line_stats_rerun);

        let (_, rerun) = complete_line_stats(
            &mut app_state,
            Ok(LineStatsResult {
                lines: vec!["new range".to_string()],
                message: "done".to_string(),
            }),
        );
        assert!(!rerun);
        assert_eq!(app_state.line_stats_report, vec!["new range".to_string()]);
        assert!(!app_state.line_stats_loading);
    }

    #[test]
    fn text_editor_teardown_confirms_only_for_dirty_instances() {
        let instance_key = "/tmp/ronomepo-dirty-editor.txt";
        let mut request = MzViewTeardownRequest::default();
        request.view.instance_key = maruzzella_sdk::ffi::MzStr {
            ptr: instance_key.as_ptr(),
            len: instance_key.len(),
        };

        let editor = register_text_editor(instance_key.to_string());
        let keyless = register_text_editor(String::new());
        let ready = prepare_text_editor_teardown(&request);
        assert_eq!(ready.decision, MzViewTeardownDecision::Ready);

        // A dirty editor without an instance key does not block keyed ones.
        set_text_editor_dirty(keyless, true);
        let ready = prepare_text_editor_teardown(&request);
        assert_eq!(ready.decision, MzViewTeardownDecision::Ready);
        assert!(text_editor_instance_dirty(""));

        set_text_editor_dirty(editor, true);
        let confirm = prepare_text_editor_teardown(&request);
        assert_eq!(confirm.decision, MzViewTeardownDecision::Confirm);

        set_text_editor_dirty(editor, false);
        assert!(!text_editor_instance_dirty(instance_key));
        set_text_editor_dirty(editor, true);
        unregister_text_editor(editor);
        unregister_text_editor(keyless);
        assert!(!text_editor_instance_dirty(instance_key));
        assert!(!text_editor_instance_dirty(""));
    }
}

export_plugin!(RonomepoPlugin);
