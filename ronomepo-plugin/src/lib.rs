use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::env;
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

use gtk::gio;
use gtk::glib::{self, translate::IntoGlibPtr, BoxedAnyObject};
use gtk::pango::EllipsizeMode;
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, CheckButton, CustomFilter, CustomSorter, Dialog, Entry,
    FilterChange, GestureClick, Image, Label, ListBox, ListBoxRow, Orientation, PolicyType,
    Popover, PositionType, ResponseType, ScrolledWindow, SelectionMode, Separator, SortListModel,
    SorterChange, TextBuffer, TextView, ToggleButton, Window, WrapMode,
};
use maruzzella_sdk::{
    export_plugin, CommandSpec, HostApi, MzLogLevel, MzStatusCode, MzViewOpenDisposition,
    MzViewPlacement, OpenViewRequest, Plugin, PluginDependency, PluginDescriptor,
    SurfaceContributionSpec, Version, ViewFactorySpec,
};
use notify::{Config as NotifyConfig, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use ronomepo_core::{
    build_repository_list, collect_generated_history_matches, collect_repository_details,
    collect_workspace_line_stats, default_manifest_path, derive_dir_name, format_sync_label,
    import_repos_txt, load_manifest, normalize_workspace_root, run_workspace_operation,
    save_manifest, workspace_summary, OperationEvent, OperationEventKind, OperationKind,
    RepositoryDetails, RepositoryEntry, RepositoryListItem, RepositoryStatus, WorkspaceManifest,
    MANIFEST_FILE_NAME,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
const MONITOR_NAME_COL_CHARS: i32 = 28;
const MONITOR_BRANCH_COL_CHARS: i32 = 14;
const MONITOR_STATE_COL_CHARS: i32 = 12;
const MONITOR_NAME_COL_WIDTH: i32 = 300;
const MONITOR_BRANCH_COL_WIDTH: i32 = 120;
const MONITOR_STATE_COL_WIDTH: i32 = 120;
const WORKER_POOL_SIZE: usize = 4;
const LOCAL_RESCAN_INTERVAL_SECS: u32 = 5 * 60;
const REMOTE_FETCH_TICK_SECS: u32 = 30;
const REMOTE_FETCH_INTERVAL_SECS: u64 = 60 * 60;
const REMOTE_FETCH_JITTER_SECS: u64 = 30 * 60;
const REMOTE_FETCH_CONCURRENCY: usize = 1;
const WATCH_POLL_FALLBACK_SECS: u64 = 15;
const UI_REFRESH_DEBOUNCE_MILLIS: u64 = 75;

pub struct RonomepoPlugin;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RonomepoPluginConfig {
    last_workspace_path: Option<String>,
    import_banner_dismissed: bool,
    #[serde(default)]
    monitor_filter_mode: MonitorFilterMode,
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

struct WatchManager {
    _backend: WatchBackend,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum JobKey {
    WorkspaceScan,
    WorkspaceRootStatus,
    RepoStatus(String),
    RepoFetch(String),
    RepoDetails(String),
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
        repo_id: String,
        repo_path: PathBuf,
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
        repo_id: String,
        status: RepositoryStatus,
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
    instance_key: Option<String>,
    host_ptr: usize,
}

thread_local! {
    static REPOSITORY_VIEWS: RefCell<Vec<RepositoryViewHandle>> = const { RefCell::new(Vec::new()) };
    static MONOREPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static REPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static COMMIT_CHECK_VIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static WORKSPACE_SETTINGS_VIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_BUFFERS: RefCell<Vec<glib::WeakRef<TextBuffer>>> = const { RefCell::new(Vec::new()) };
    static OPERATION_SUMMARIES: RefCell<Vec<glib::WeakRef<Label>>> = const { RefCell::new(Vec::new()) };
}

static STATE: OnceLock<Mutex<AppState>> = OnceLock::new();
static EXECUTOR: OnceLock<ExecutorState> = OnceLock::new();
static WATCH_MANAGER: OnceLock<Mutex<Option<WatchManager>>> = OnceLock::new();
static LAST_HOST_PTR: AtomicUsize = AtomicUsize::new(0);
static BACKGROUND_LOOPS_STARTED: AtomicUsize = AtomicUsize::new(0);
static LOG_REFRESH_SCHEDULED: AtomicUsize = AtomicUsize::new(0);
static WATCH_MANAGER_SYNC_SEQ: AtomicUsize = AtomicUsize::new(0);

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
                main_context.invoke(move || {
                    handle_operation_event(batch_id, operation, manifest, event)
                });
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
        WorkerJob::RepositoryStatusRefresh { repo_id, repo_path } => {
            let status = ronomepo_core::collect_repository_status(&repo_path);
            dispatch_worker_result(WorkerResult::RepositoryStatusRefreshed { repo_id, status });
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
        mark_all_repos_stale();
        schedule_pending_local_rescans();
        glib::ControlFlow::Continue
    });
    glib::timeout_add_seconds_local(REMOTE_FETCH_TICK_SECS, || {
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
    sync_repo_runtime_state(&mut app_state);
    if app_state.logs.is_empty() {
        app_state.logs.push(format!(
            "Ronomepo initialized for workspace {}",
            workspace_root.display()
        ));
    }
    drop(app_state);
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
    let is_generated_commit_failure = operation == "Push"
        && event
            .message
            .contains("generated-commit markers were found");
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
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.workspace_status = workspace_status;
            }
            refresh_views();
        }
        WorkerResult::RepositoryStatusRefreshed { repo_id, status } => {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                mark_repo_scan_completed(&mut app_state, &repo_id);
                if let Some(item) = app_state
                    .repository_items
                    .iter_mut()
                    .find(|item| item.id == repo_id)
                {
                    item.status = status;
                }
            }
            refresh_views();
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
            if let Some(repo_path) = refresh_path {
                schedule_repository_status_refresh(&repo_id, repo_path);
            }
            append_log(message);
            refresh_views();
        }
        WorkerResult::RepoDetailsLoaded { repo_id, details } => {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.repo_details_loading.remove(&repo_id);
                app_state.repo_details_cache.insert(repo_id, details);
            }
            refresh_views();
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
    schedule_repository_status_refresh(repo_id, manifest.root.join(&repo.dir_name));
}

fn refresh_views() {
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
}

fn refresh_log_surfaces() {
    let snapshot = snapshot();
    refresh_overview_views(&snapshot);
    refresh_operation_views(&snapshot);
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
    item.name.to_ascii_lowercase().contains(&filter)
        || item.dir_name.to_ascii_lowercase().contains(&filter)
        || item.remote_url.to_ascii_lowercase().contains(&filter)
        || branch.contains(&filter)
        || sync.contains(&filter)
        || state.contains(&filter)
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
    let status = monitor_state_cell(&item.status.state);
    status.add_css_class("pill");

    let sync = monitor_sync_cell(&item.status.sync);

    content.append(&name);
    content.append(&branch);
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
    }
}

fn visible_monitor_items(snapshot: &StateSnapshot) -> Vec<RepositoryListItem> {
    filtered_repository_items(snapshot, all_monitor_items(snapshot))
}

fn filtered_repository_items(
    snapshot: &StateSnapshot,
    mut items: Vec<RepositoryListItem>,
) -> Vec<RepositoryListItem> {
    items.sort_by_key(repo_monitor_sort_key);

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
        MonitorFilterMode::Issues => matches!(
            item.status.state,
            RepositoryState::Missing | RepositoryState::Unknown
        ) || matches!(
            item.status.sync,
            RepositorySync::Diverged { .. }
                | RepositorySync::NoUpstream
                | RepositorySync::Unknown
        ),
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

fn mark_repo_stale(app_state: &mut AppState, repo_id: &str) {
    let now = SystemTime::now();
    let runtime = app_state
        .repo_runtime
        .entry(repo_id.to_string())
        .or_insert_with(|| RepoRuntimeState::new(now, repo_id));
    runtime.invalidation_seq = runtime.invalidation_seq.saturating_add(1);
}

fn schedule_pending_local_rescans() {
    let scheduled = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state
            .repository_items
            .iter()
            .filter_map(|item| {
                let runtime = app_state.repo_runtime.get(&item.id)?;
                if runtime.local_refresh_in_flight || !runtime.needs_rescan() {
                    return None;
                }
                Some((item.id.clone(), item.status.repo_path.clone()))
            })
            .collect::<Vec<_>>()
    };

    if !scheduled.is_empty() {
        let mut app_state = state().lock().expect("state mutex poisoned");
        for (repo_id, _) in &scheduled {
            if let Some(runtime) = app_state.repo_runtime.get_mut(repo_id) {
                runtime.local_refresh_in_flight = true;
                runtime.scheduled_scan_seq = runtime.invalidation_seq;
            }
        }
    }

    for (repo_id, repo_path) in scheduled {
        schedule_repository_status_refresh(&repo_id, repo_path);
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

fn schedule_repository_status_refresh(repo_id: &str, repo_path: PathBuf) {
    let repo_id = repo_id.to_string();
    match submit_coalesced_job(
        JobKey::RepoStatus(repo_id.clone()),
        WorkerJob::RepositoryStatusRefresh {
            repo_id: repo_id.clone(),
            repo_path,
        },
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
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.manifest.clone()
    };

    let sync_seq = WATCH_MANAGER_SYNC_SEQ.fetch_add(1, Ordering::SeqCst) + 1;
    let Some(manifest) = manifest else {
        let mut manager = watch_manager()
            .lock()
            .expect("watch manager mutex poisoned");
        *manager = None;
        return;
    };

    let spawn_result = thread::Builder::new()
        .name(format!("ronomepo-watch-sync-{sync_seq}"))
        .spawn(move || {
            let next_manager = build_watch_manager(&manifest);
            if WATCH_MANAGER_SYNC_SEQ.load(Ordering::SeqCst) != sync_seq {
                return;
            }

            match next_manager {
                Ok(manager) => {
                    let mut slot = watch_manager()
                        .lock()
                        .expect("watch manager mutex poisoned");
                    if WATCH_MANAGER_SYNC_SEQ.load(Ordering::SeqCst) == sync_seq {
                        *slot = Some(manager);
                    }
                }
                Err(message) => {
                    {
                        let mut slot = watch_manager()
                            .lock()
                            .expect("watch manager mutex poisoned");
                        if WATCH_MANAGER_SYNC_SEQ.load(Ordering::SeqCst) != sync_seq {
                            return;
                        }
                        *slot = None;
                    }
                    glib::MainContext::default().invoke(move || {
                        append_log(format!("Repository watcher setup failed: {message}"));
                    });
                }
            }
        });

    if let Err(error) = spawn_result {
        append_log(format!("Repository watcher setup failed to start: {error}"));
    }
}

fn build_watch_manager(manifest: &WorkspaceManifest) -> Result<WatchManager, String> {
    let repos = manifest
        .repos
        .iter()
        .map(|repo| (repo.id.clone(), manifest.root.join(&repo.dir_name)))
        .filter(|(_, path)| path.exists())
        .collect::<Vec<_>>();

    if repos.is_empty() {
        return Err("no local repositories are available to watch".to_string());
    }

    let mut backend = create_watch_backend()?;
    for (_, path) in &repos {
        watch_backend_mut(&mut backend)
            .watch(path, RecursiveMode::Recursive)
            .map_err(|error| format!("{}: {error}", path.display()))?;
    }

    Ok(WatchManager { _backend: backend })
}

fn create_watch_backend() -> Result<WatchBackend, String> {
    let config = NotifyConfig::default();
    match RecommendedWatcher::new(dispatch_watch_event_result, config) {
        Ok(watcher) => Ok(WatchBackend::Recommended(watcher)),
        Err(_) => PollWatcher::new(
            dispatch_watch_event_result,
            config.with_poll_interval(Duration::from_secs(WATCH_POLL_FALLBACK_SECS)),
        )
        .map(WatchBackend::Poll)
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
            let paths = event.paths;
            main_context.invoke(move || handle_watch_paths(paths));
        }
        Err(error) => {
            let message = error.to_string();
            main_context.invoke(move || append_log(format!("Repository watcher error: {message}")));
        }
    }
}

fn handle_watch_paths(paths: Vec<PathBuf>) {
    let any_marked = {
        let mut touched = HashSet::new();
        let mut app_state = state().lock().expect("state mutex poisoned");
        let Some(manifest) = app_state.manifest.clone() else {
            return;
        };

        for path in paths {
            if let Some(repo_id) = repo_id_for_watch_path(&manifest, &path) {
                touched.insert(repo_id);
            }
        }

        for repo_id in &touched {
            mark_repo_stale(&mut app_state, repo_id);
        }
        !touched.is_empty()
    };

    if any_marked {
        schedule_pending_local_rescans();
    }
}

fn repo_id_for_watch_path(manifest: &WorkspaceManifest, path: &Path) -> Option<String> {
    let mut matches = manifest
        .repos
        .iter()
        .filter_map(|repo| {
            let repo_root = manifest.root.join(&repo.dir_name);
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
        move |_, _, _, _| {
            if let Some(row) = relative_to.parent().and_downcast::<ListBoxRow>() {
                if let Some(list) = row.parent().and_downcast::<ListBox>() {
                    list.unselect_all();
                    list.select_row(Some(&row));
                    update_selected_repo_ids(selection_ids_from_list(&list));
                }
            }
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
    popover.set_has_arrow(true);
    popover.set_position(PositionType::Bottom);
    popover.set_parent(relative_to);

    let menu = GtkBox::new(Orientation::Vertical, 4);
    menu.set_margin_top(8);
    menu.set_margin_bottom(8);
    menu.set_margin_start(8);
    menu.set_margin_end(8);

    append_context_button(&menu, &popover, "Open Overview", move || {
        let repo_ids = {
            let app_state = state().lock().expect("state mutex poisoned");
            app_state.selected_repo_ids.clone()
        };
        open_repo_overviews(host_ptr, &repo_ids);
    });
    append_context_button(&menu, &popover, "Open Folder", || {
        open_selected_repo_folders();
    });
    append_context_button(&menu, &popover, "Open Terminal", || {
        open_selected_repo_terminal();
    });
    append_context_button(&menu, &popover, "Open In Editor", || {
        open_selected_repo_in_editor();
    });
    append_context_button(&menu, &popover, "Pull", || {
        let _ = command_pull(maruzzella_sdk::ffi::MzBytes::empty());
    });
    append_context_button(&menu, &popover, "Push", || {
        let _ = command_push(maruzzella_sdk::ffi::MzBytes::empty());
    });
    append_context_button(&menu, &popover, "Push Force", || {
        let _ = command_push_force(maruzzella_sdk::ffi::MzBytes::empty());
    });
    append_context_button(&menu, &popover, "Clone Missing", || {
        let _ = command_clone_missing(maruzzella_sdk::ffi::MzBytes::empty());
    });
    append_context_button(&menu, &popover, "Apply Hooks", || {
        let _ = command_apply_hooks(maruzzella_sdk::ffi::MzBytes::empty());
    });

    popover.set_child(Some(&menu));
    popover
}

fn append_context_button<F>(menu: &GtkBox, popover: &Popover, label: &str, action: F)
where
    F: Fn() + 'static,
{
    let button = Button::with_label(label);
    button.set_halign(Align::Fill);
    button.add_css_class("flat");
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

    let store = gio::ListStore::new::<BoxedAnyObject>();
    let filter = CustomFilter::new(|object| {
        let snapshot = snapshot();
        repo_item_from_object(object)
            .is_some_and(|item| repo_monitor_filter_matches(&item, &snapshot))
    });
    let filter_model = gtk::FilterListModel::new(Some(store.clone()), Some(filter.clone()));
    let sorter = CustomSorter::new(|left, right| {
        let left = repo_item_from_object(left);
        let right = repo_item_from_object(right);
        match (left, right) {
            (Some(left), Some(right)) => repo_monitor_sort_key(&left)
                .cmp(&repo_monitor_sort_key(&right))
                .into(),
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
        click.connect_pressed(move |gesture, _, _x, y| {
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
            } else if row.is_selected() {
                list_ref.unselect_row(&row);
            } else {
                list_ref.unselect_all();
                list_ref.select_row(Some(&row));
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

    content.append(&filter_box);
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
        .filter(|item| {
            matches!(
                item.status.sync,
                ronomepo_core::RepositorySync::Ahead(_)
                    | ronomepo_core::RepositorySync::Diverged { .. }
            )
        })
        .count();
    let behind = items
        .iter()
        .filter(|item| {
            matches!(
                item.status.sync,
                ronomepo_core::RepositorySync::Behind(_)
                    | ronomepo_core::RepositorySync::Diverged { .. }
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
        ("No Upstream", no_upstream),
    ] {
        stats.append(&stat_card(label, &value.to_string()));
    }

    let actions = overview_actions();
    let selection_actions = monorepo_selection_actions(host_ptr, &items);
    let report_actions = monorepo_report_actions(snapshot);
    let file_actions = overview_file_actions(snapshot, host_ptr);

    let sections = GtkBox::new(Orientation::Vertical, 12);
    append_overview_section(
        &sections,
        "Workspace",
        &format!("Current root: {}", snapshot.workspace_root.display()),
    );
    append_overview_section(
        &sections,
        "Manifest",
        &snapshot
            .manifest_path
            .as_ref()
            .map(|path| format!("Loaded from {}", path.display()))
            .unwrap_or_else(|| format!("No {MANIFEST_FILE_NAME} loaded yet")),
    );
    append_overview_section(
        &sections,
        "Selection Scope",
        &if selected.is_empty() {
            "No repos selected. Toolbar and overview actions apply to the whole workspace."
                .to_string()
        } else {
            format!(
                "{} repos selected. Toolbar and overview actions target the current selection first.",
                selected.len()
            )
        },
    );
    append_overview_section(
        &sections,
        "Repo Overview Focus",
        &snapshot
            .active_repo_id
            .as_ref()
            .map(|repo_id| format!("Active repo overview target: {repo_id}"))
            .unwrap_or_else(|| "No active repo overview target yet".to_string()),
    );
    append_repo_group_section(
        &sections,
        "Needs Attention",
        "Repos that are missing, dirty, behind, diverged, ahead, or missing an upstream.",
        &attention_items(&items),
        Some(8),
    );
    append_repo_group_section(
        &sections,
        "Current Selection",
        "The repos currently selected in the left monitor.",
        &selected,
        Some(8),
    );
    append_lines_section(
        &sections,
        "History Check",
        &if snapshot.history_report_loading {
            vec!["History check is running...".to_string()]
        } else {
            snapshot.history_report.clone()
        },
        "Run Check History to scan recent commits for generated markers.",
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
    root.append(&actions);
    root.append(&selection_actions);
    root.append(&report_actions);
    root.append(&file_actions);
    root.append(&sections);
}

fn render_commit_check_into(root: &GtkBox, snapshot: &StateSnapshot) {
    clear_box(root);

    let hero = GtkBox::new(Orientation::Vertical, 4);
    let title = Label::new(Some("Commit Check"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");
    let subtitle = Label::new(Some(
        "Scans recent commits for AI-generated or generated-commit markers before push.",
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

    let sections = GtkBox::new(Orientation::Vertical, 12);
    append_overview_section(
        &sections,
        "Selection Scope",
        &if snapshot.selected_repo_ids.is_empty() {
            "No repos selected. Commit Check scans all eligible repos in the workspace."
                .to_string()
        } else {
            format!(
                "{} repos selected. Commit Check reports only against the current selection.",
                snapshot.selected_repo_ids.len()
            )
        },
    );
    append_lines_section(
        &sections,
        "Report",
        &if snapshot.history_report_loading {
            vec!["Commit check is running...".to_string()]
        } else {
            snapshot.history_report.clone()
        },
        "Run Check to surface the same generated-commit matches that block a protected push.",
    );
    append_log_section(&sections, "Recent Operations", &snapshot.logs, 8);

    root.append(&hero);
    root.append(&actions);
    root.append(&sections);
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

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    REPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            instance_key,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(scroller.upcast())
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
    let status = status.clone();
    glib::idle_add_local(move || match receiver.try_recv() {
        Ok(message) => {
            status.set_text(&message);
            glib::ControlFlow::Break
        }
        Err(mpsc::TryRecvError::Empty) => glib::ControlFlow::Continue,
        Err(mpsc::TryRecvError::Disconnected) => glib::ControlFlow::Break,
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
    let manifest = import_repos_txt(
        &repos_path,
        workspace_root,
        &workspace_name_from_root(workspace_root),
    )
    .map_err(|error| error.to_string())?;
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
    let matches = collect_generated_history_matches(manifest, selected_repo_ids, num_commits);
    let lines = if matches.is_empty() {
        vec![format!(
            "No generated-commit markers found in the last {num_commits} commits."
        )]
    } else {
        matches
            .into_iter()
            .map(|entry| {
                let markers = entry.matching_lines.join(" | ");
                format!(
                    "{} | HEAD~{} | {} | {} | {}",
                    entry.repository_name,
                    entry.head_offset,
                    entry.commit_hash,
                    entry.subject,
                    markers
                )
            })
            .collect()
    };
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
    let (workspace_root, manifest) = build_workspace_manifest_from_inputs(
        workspace_name,
        workspace_root,
        shared_hooks_path,
        repo_rows,
    )?;
    let manifest_path = default_manifest_path(&workspace_root);
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;
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
        root.append(&overview_actions());
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
        root.append(&overview_actions());
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
    root.append(&overview_actions());
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

fn overview_actions() -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);
    for (label, handler) in [
        (
            "Refresh",
            command_refresh_workspace as extern "C" fn(_) -> _,
        ),
        ("Pull", command_pull as extern "C" fn(_) -> _),
        ("Push", command_push as extern "C" fn(_) -> _),
        (
            "Monorepo Overview",
            command_open_overview as extern "C" fn(_) -> _,
        ),
    ] {
        let button = Button::with_label(label);
        button.connect_clicked(move |_| {
            let _ = handler(maruzzella_sdk::ffi::MzBytes::empty());
        });
        actions.append(&button);
    }
    actions
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
    let actions = GtkBox::new(Orientation::Horizontal, 8);

    for (label, ids) in [
        (
            "Select Attention",
            collect_repo_ids(items, |item| repo_attention_rank(item) < 7),
        ),
        (
            "Select Dirty",
            collect_repo_ids(items, |item| {
                matches!(
                    item.status.state,
                    ronomepo_core::RepositoryState::Dirty
                        | ronomepo_core::RepositoryState::Untracked
                )
            }),
        ),
        (
            "Select Missing",
            collect_repo_ids(items, |item| {
                matches!(item.status.state, ronomepo_core::RepositoryState::Missing)
            }),
        ),
        (
            "Select Ahead",
            collect_repo_ids(items, |item| {
                matches!(
                    item.status.sync,
                    ronomepo_core::RepositorySync::Ahead(_)
                        | ronomepo_core::RepositorySync::Diverged { .. }
                )
            }),
        ),
    ] {
        let button = Button::with_label(label);
        button.connect_clicked(move |_| {
            if ids.is_empty() {
                append_log(format!(
                    "{label} skipped because no repos match that bucket."
                ));
            } else {
                set_selected_repo_ids(ids.clone());
                append_log(format!("{label} matched {} repos.", ids.len()));
            }
        });
        actions.append(&button);
    }

    let open_selection = Button::with_label("Open Selected Overviews");
    open_selection.connect_clicked(move |_| {
        let repo_ids = {
            let app_state = state().lock().expect("state mutex poisoned");
            app_state.selected_repo_ids.clone()
        };
        open_repo_overviews(host_ptr, &repo_ids);
    });
    actions.append(&open_selection);

    let settings = Button::with_label("Workspace Settings");
    settings.connect_clicked(move |_| {
        if let Err(message) = open_workspace_settings_tab() {
            append_log(message);
            refresh_views();
        }
    });
    actions.append(&settings);

    actions
}

fn monorepo_report_actions(snapshot: &StateSnapshot) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);

    let history = Button::with_label("Check History");
    history.connect_clicked(|_| {
        let _ = command_check_history(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&history);

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
    actions.append(&since_entry);

    let line_stats = Button::with_label("Line Stats");
    line_stats.connect_clicked(|_| {
        let _ = command_line_stats(maruzzella_sdk::ffi::MzBytes::empty());
    });
    actions.append(&line_stats);

    let all_time = Button::with_label("All Time");
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
        for item in items.iter().take(take) {
            let row = Label::new(Some(&format!(
                "{}  |  {}  |  {}  |  {}",
                item.name,
                branch_label(item),
                status_label(&item.status.state),
                format_sync_label(&item.status.sync)
            )));
            row.set_xalign(0.0);
            row.add_css_class("mono");
            block.append(&row);
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
    let buffer = buffer.clone();
    let status = status.clone();
    let title = title.clone();
    let path_entry = path_entry.clone();
    let queue_error_buffer = buffer.clone();
    let queue_error_status = status.clone();
    glib::idle_add_local(move || match receiver.try_recv() {
        Ok(message) => {
            if resolve_editor_path(path_entry.text().as_str()) == message.path {
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
            glib::ControlFlow::Break
        }
        Err(mpsc::TryRecvError::Empty) => glib::ControlFlow::Continue,
        Err(mpsc::TryRecvError::Disconnected) => glib::ControlFlow::Break,
    });

    if let Err(message) = submit_job(WorkerJob::EditorLoad {
        path: path.to_path_buf(),
        reply: sender,
    }) {
        queue_error_buffer.set_text("");
        queue_error_status.set_text(&format!(
            "Failed to queue load for {}: {message}",
            path.display()
        ));
    }
}

fn queue_editor_save(status: &Label, title: &Label, path: &Path, content: String, host_ptr: usize) {
    status.set_text(&format!("Saving {}...", path.display()));
    let (sender, receiver) = mpsc::channel::<EditorSaveMessage>();
    let status = status.clone();
    let title = title.clone();
    let path = path.to_path_buf();
    let queue_error_status = status.clone();
    glib::idle_add_local(move || match receiver.try_recv() {
        Ok(message) => {
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
            glib::ControlFlow::Break
        }
        Err(mpsc::TryRecvError::Empty) => glib::ControlFlow::Continue,
        Err(mpsc::TryRecvError::Disconnected) => glib::ControlFlow::Break,
    });

    if let Err(message) = submit_job(WorkerJob::EditorSave {
        path: path.clone(),
        content,
        reply: sender,
    }) {
        queue_error_status.set_text(&format!(
            "Failed to queue save for {}: {message}",
            path.display()
        ));
    }
}

fn editor_title_for_path(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("Text Editor")
        .to_string()
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

    let header = GtkBox::new(Orientation::Horizontal, 8);
    header.set_halign(Align::Fill);
    header.set_hexpand(true);

    let summary = Label::new(Some(&operation_summary_text(&snapshot().logs)));
    summary.set_xalign(0.0);
    summary.add_css_class("muted");
    summary.set_wrap(true);
    summary.set_hexpand(true);

    let refresh = Button::with_label("Refresh Logs");
    let clear = Button::with_label("Clear");
    refresh.set_halign(Align::End);

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

    header.append(&clear);
    header.append(&refresh);
    root.append(&header);
    root.append(&summary);
    root.append(&scroller);

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

    match latest_failure {
        Some(failure) => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest failure: {failure} | Latest: {latest}"
        ),
        None => format!(
            "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest: {latest}"
        ),
    }
}

fn load_manifest_if_present(path: &Path) -> Option<WorkspaceManifest> {
    if !path.exists() {
        return None;
    }
    load_manifest(path).ok()
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
}

export_plugin!(RonomepoPlugin);
