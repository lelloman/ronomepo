use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::thread;

use gtk::glib::{self, translate::IntoGlibPtr};
use gtk::pango::EllipsizeMode;
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, Entry, GestureClick, Label, ListBox, ListBoxRow, Orientation,
    PolicyType, Popover, PositionType, ScrolledWindow, SelectionMode, Separator, TextBuffer,
    TextView, WrapMode, CheckButton,
};
use maruzzella_sdk::{
    export_plugin, CommandSpec, HostApi, MenuItemSpec, MzLogLevel, MzMenuSurface,
    MzStatusCode, MzViewOpenDisposition, MzViewPlacement, OpenViewRequest, Plugin,
    PluginDependency, PluginDescriptor, SurfaceContributionSpec, Version, ViewFactorySpec,
};
use ronomepo_core::{
    build_repository_list, default_manifest_path, derive_dir_name, format_sync_label,
    import_repos_txt, load_manifest, normalize_workspace_root, run_workspace_operation,
    save_manifest, workspace_summary,
    collect_generated_history_matches, collect_repository_details, collect_workspace_line_stats,
    OperationEvent, OperationEventKind, OperationKind, RepositoryDetails, RepositoryEntry,
    RepositoryListItem, RepositoryStatus, MANIFEST_FILE_NAME, WorkspaceManifest,
};
use serde::{Deserialize, Serialize};

const PLUGIN_ID: &str = "com.lelloman.ronomepo";
const VIEW_REPO_MONITOR: &str = "com.lelloman.ronomepo.repo_monitor";
const VIEW_MONOREPO_OVERVIEW: &str = "com.lelloman.ronomepo.monorepo_overview";
const VIEW_REPO_OVERVIEW: &str = "com.lelloman.ronomepo.repo_overview";
const VIEW_WORKSPACE_SETTINGS: &str = "com.lelloman.ronomepo.workspace_settings";
const VIEW_TEXT_EDITOR: &str = "com.lelloman.ronomepo.text_editor";
const VIEW_OPERATIONS: &str = "com.lelloman.ronomepo.operations";
const CMD_REFRESH: &str = "ronomepo.workspace.refresh";
const CMD_IMPORT: &str = "ronomepo.workspace.import_repos_txt";
const CMD_SETTINGS: &str = "ronomepo.workspace.open_settings";
const CMD_CLONE_MISSING: &str = "ronomepo.workspace.clone_missing";
const CMD_PULL: &str = "ronomepo.workspace.pull";
const CMD_PUSH: &str = "ronomepo.workspace.push";
const CMD_PUSH_FORCE: &str = "ronomepo.workspace.push_force";
const CMD_APPLY_HOOKS: &str = "ronomepo.workspace.apply_hooks";
const CMD_OPEN_OVERVIEW: &str = "ronomepo.workspace.open_overview";
const CMD_CHECK_HISTORY: &str = "ronomepo.workspace.check_history";
const CMD_LINE_STATS: &str = "ronomepo.workspace.line_stats";
const MONITOR_NAME_COL_CHARS: i32 = 28;
const MONITOR_BRANCH_COL_CHARS: i32 = 14;
const MONITOR_STATE_COL_CHARS: i32 = 12;
const MONITOR_NAME_COL_WIDTH: i32 = 300;
const MONITOR_BRANCH_COL_WIDTH: i32 = 120;
const MONITOR_STATE_COL_WIDTH: i32 = 120;

pub struct RonomepoPlugin;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RonomepoPluginConfig {
    last_workspace_path: Option<String>,
    import_banner_dismissed: bool,
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
    monitor_show_all: bool,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    next_operation_batch: usize,
    history_report: Vec<String>,
    line_stats_report: Vec<String>,
    line_stats_since: String,
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
            monitor_show_all: true,
            selected_repo_ids: Vec::new(),
            active_repo_id: None,
            logs: Vec::new(),
            next_operation_batch: 0,
            history_report: Vec::new(),
            line_stats_report: Vec::new(),
            line_stats_since: String::new(),
        }
    }
}

struct RepositoryViewHandle {
    summary: glib::WeakRef<Label>,
    filter_entry: glib::WeakRef<Entry>,
    list: glib::WeakRef<ListBox>,
    scroller: glib::WeakRef<ScrolledWindow>,
    host_ptr: usize,
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
    static WORKSPACE_SETTINGS_VIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_BUFFERS: RefCell<Vec<glib::WeakRef<TextBuffer>>> = const { RefCell::new(Vec::new()) };
    static OPERATION_SUMMARIES: RefCell<Vec<glib::WeakRef<Label>>> = const { RefCell::new(Vec::new()) };
}

static STATE: OnceLock<Mutex<AppState>> = OnceLock::new();
static LAST_HOST_PTR: AtomicUsize = AtomicUsize::new(0);

fn state() -> &'static Mutex<AppState> {
    STATE.get_or_init(|| Mutex::new(AppState::default()))
}

fn empty_repository_status(repo_path: PathBuf) -> ronomepo_core::RepositoryStatus {
    ronomepo_core::RepositoryStatus {
        state: ronomepo_core::RepositoryState::Unknown,
        branch: None,
        sync: ronomepo_core::RepositorySync::Unknown,
        repo_path,
    }
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
            CommandSpec::new(PLUGIN_ID, CMD_IMPORT, "Import repos.txt")
                .with_handler(command_import_repos_txt),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_SETTINGS, "Workspace Settings")
                .with_handler(command_open_settings),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_CLONE_MISSING, "Clone Missing")
                .with_handler(command_clone_missing),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_PULL, "Pull").with_handler(command_pull),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_PUSH, "Push").with_handler(command_push),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_PUSH_FORCE, "Push Force")
                .with_handler(command_push_force),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_APPLY_HOOKS, "Apply Hooks")
                .with_handler(command_apply_hooks),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_OPEN_OVERVIEW, "Monorepo Overview")
                .with_handler(command_open_overview),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_CHECK_HISTORY, "Check History")
                .with_handler(command_check_history),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_LINE_STATS, "Line Stats")
                .with_handler(command_line_stats),
        )?;

        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-refresh",
            MzMenuSurface::FileItems,
            "Refresh Workspace",
            CMD_REFRESH,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-import",
            MzMenuSurface::FileItems,
            "Import repos.txt",
            CMD_IMPORT,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-settings",
            MzMenuSurface::FileItems,
            "Workspace Settings",
            CMD_SETTINGS,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-clone-missing",
            MzMenuSurface::FileItems,
            "Clone Missing",
            CMD_CLONE_MISSING,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-pull",
            MzMenuSurface::FileItems,
            "Pull",
            CMD_PULL,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-push",
            MzMenuSurface::FileItems,
            "Push",
            CMD_PUSH,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-push-force",
            MzMenuSurface::FileItems,
            "Push Force",
            CMD_PUSH_FORCE,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-hooks",
            MzMenuSurface::FileItems,
            "Apply Hooks",
            CMD_APPLY_HOOKS,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-overview",
            MzMenuSurface::ViewItems,
            "Monorepo Overview",
            CMD_OPEN_OVERVIEW,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-check-history",
            MzMenuSurface::ViewItems,
            "Check History",
            CMD_CHECK_HISTORY,
        ))?;
        host.register_menu_item(MenuItemSpec::new(
            PLUGIN_ID,
            "ronomepo-line-stats",
            MzMenuSurface::ViewItems,
            "Line Stats",
            CMD_LINE_STATS,
        ))?;

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
    if app_state.logs.is_empty() {
        app_state.logs.push(format!(
            "Ronomepo initialized for workspace {}",
            workspace_root.display()
        ));
    }
    drop(app_state);
    schedule_workspace_scan();
}

extern "C" fn command_refresh_workspace(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match refresh_workspace() {
        Ok(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Err(message) => {
            append_log(format!("Refresh failed: {message}"));
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_import_repos_txt(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match import_workspace_from_repos_txt() {
        Ok(message) => {
            append_log(message);
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::OK
        }
        Err(message) => {
            append_log(format!("Import failed: {message}"));
            refresh_views();
            maruzzella_sdk::ffi::MzStatus::new(MzStatusCode::InternalError)
        }
    }
}

extern "C" fn command_open_settings(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match open_workspace_settings_tab() {
        Ok(()) => maruzzella_sdk::ffi::MzStatus::OK,
        Err(message) => {
            append_log(message);
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

extern "C" fn command_check_history(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match refresh_history_report(25) {
        Ok(message) => {
            append_log(message);
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

extern "C" fn command_line_stats(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    match refresh_line_stats_report_from_state() {
        Ok(message) => {
            append_log(message);
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

fn refresh_workspace() -> Result<String, String> {
    let mut app_state = state().lock().expect("state mutex poisoned");
    let manifest_path = default_manifest_path(&app_state.workspace_root);
    let manifest = load_manifest_if_present(&manifest_path);
    let workspace_root = app_state.workspace_root.clone();
    app_state.manifest = manifest.clone();
    app_state.manifest_path = app_state.manifest.as_ref().map(|_| manifest_path.clone());
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    drop(app_state);
    schedule_workspace_scan();
    if manifest.is_some() {
        Ok(format!("Reloaded {MANIFEST_FILE_NAME} from {}", manifest_path.display()))
    } else {
        Ok(format!(
            "No {MANIFEST_FILE_NAME} found in {}",
            workspace_root.display()
        ))
    }
}

fn import_workspace_from_repos_txt() -> Result<String, String> {
    let mut app_state = state().lock().expect("state mutex poisoned");
    let repos_path = app_state.workspace_root.join("repos.txt");
    let manifest_path = default_manifest_path(&app_state.workspace_root);

    let manifest = import_repos_txt(
        &repos_path,
        &app_state.workspace_root,
        &workspace_name_from_root(&app_state.workspace_root),
    )
    .map_err(|error| error.to_string())?;
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;

    let repo_count = manifest.repos.len();
    app_state.manifest = Some(manifest);
    app_state.manifest_path = Some(manifest_path.clone());
    app_state.repository_items_refresh_pending = false;
    app_state.repo_details_cache.clear();
    app_state.repo_details_loading.clear();
    drop(app_state);
    schedule_workspace_scan();

    Ok(format!(
        "Imported {repo_count} repositories from {} into {}",
        repos_path.display(),
        manifest_path.display()
    ))
}

fn refresh_history_report(num_commits: usize) -> Result<String, String> {
    let (manifest, selected_repo_ids) = {
        let app_state = state().lock().expect("state mutex poisoned");
        (
            app_state.manifest.clone(),
            app_state.selected_repo_ids.clone(),
        )
    };
    let Some(manifest) = manifest else {
        return Err(format!(
            "Check History skipped because no {} is loaded.",
            MANIFEST_FILE_NAME
        ));
    };

    let matches = collect_generated_history_matches(&manifest, &selected_repo_ids, num_commits);
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
                    entry.repository_name, entry.head_offset, entry.commit_hash, entry.subject, markers
                )
            })
            .collect()
    };

    let count = lines.len();
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.history_report = lines;
    Ok(format!(
        "History check completed over the last {num_commits} commits ({count} report line(s))."
    ))
}

fn refresh_line_stats_report(since_date: Option<&str>) -> Result<String, String> {
    let manifest = {
        let app_state = state().lock().expect("state mutex poisoned");
        app_state.manifest.clone()
    };
    let Some(manifest) = manifest else {
        return Err(format!(
            "Line Stats skipped because no {} is loaded.",
            MANIFEST_FILE_NAME
        ));
    };

    let stats = collect_workspace_line_stats(&manifest, since_date);
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
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.line_stats_report = lines;
    Ok(match since_date {
        Some(since_date) => format!("Line stats refreshed since {since_date} ({rows} row(s))."),
        None => format!("Line stats refreshed for all time ({rows} row(s))."),
    })
}

fn refresh_line_stats_report_from_state() -> Result<String, String> {
    let since = {
        let app_state = state().lock().expect("state mutex poisoned");
        let trimmed = app_state.line_stats_since.trim().to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    };
    refresh_line_stats_report(since.as_deref())
}

fn append_log(message: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.logs.push(message);
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

    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        run_workspace_operation(&manifest, &selected_repo_ids, kind, |event| {
            append_log(format!("[run {batch_id}] {}", format_operation_event(&event)));
            let event = event.clone();
            let manifest = manifest.clone();
            main_context.invoke(move || match event.kind {
                OperationEventKind::Success
                | OperationEventKind::Skipped
                | OperationEventKind::Failed => {
                    schedule_refresh_for_operation_event(&manifest, &event);
                    refresh_views();
                }
                OperationEventKind::Finished => refresh_views(),
                OperationEventKind::Started => refresh_views(),
            });
        });
    });
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
    REPOSITORY_VIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| {
            let Some(summary) = handle.summary.upgrade() else {
                return false;
            };
            let Some(filter_entry) = handle.filter_entry.upgrade() else {
                return false;
            };
            let Some(list) = handle.list.upgrade() else {
                return false;
            };
            let Some(scroller) = handle.scroller.upgrade() else {
                return false;
            };
            if filter_entry.text().as_str() != snapshot.monitor_filter {
                filter_entry.set_text(&snapshot.monitor_filter);
            }
            render_repository_view_into(
                &summary,
                &list,
                &scroller,
                &snapshot,
                handle.host_ptr as *const _,
            );
            true
        });
    });

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
    repository_items_loading: bool,
    repo_details_cache: HashMap<String, RepositoryDetails>,
    repo_details_loading: HashSet<String>,
    monitor_filter: String,
    monitor_show_all: bool,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
    history_report: Vec<String>,
    line_stats_report: Vec<String>,
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
        repository_items_loading: app_state.repository_items_loading,
        repo_details_cache: app_state.repo_details_cache.clone(),
        repo_details_loading: app_state.repo_details_loading.clone(),
        monitor_filter: app_state.monitor_filter.clone(),
        monitor_show_all: app_state.monitor_show_all,
        selected_repo_ids: app_state.selected_repo_ids.clone(),
        active_repo_id: app_state.active_repo_id.clone(),
        logs: app_state.logs.clone(),
        history_report: app_state.history_report.clone(),
        line_stats_report: app_state.line_stats_report.clone(),
        line_stats_since: app_state.line_stats_since.clone(),
    }
}

fn render_repository_view_into(
    summary_label: &Label,
    list: &ListBox,
    scroller: &ScrolledWindow,
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    let filtered_items = visible_monitor_items(snapshot);
    let reset_scroll_to_top = snapshot.selected_repo_ids.is_empty();
    let previous_scroll = scroller.vadjustment().value();
    let summary = workspace_summary(
        snapshot.manifest.as_ref(),
        snapshot.manifest_path.as_deref(),
        &snapshot.workspace_root,
    );
    let manifest_status = match &snapshot.manifest_path {
        Some(path) if snapshot.manifest.is_some() => format!("Manifest: {}", path.display()),
        Some(path) => format!("Manifest missing: {}", path.display()),
        None => format!(
            "Manifest not loaded. Use Import repos.txt to create {}.",
            MANIFEST_FILE_NAME
        ),
    };
    let selection_scope = if snapshot.selected_repo_ids.is_empty() {
        "No selection".to_string()
    } else {
        format!("{} selected", snapshot.selected_repo_ids.len())
    };
    let loading_scope = if snapshot.repository_items_loading {
        "Refreshing Git status".to_string()
    } else {
        "Status ready".to_string()
    };
    let filter_scope = if snapshot.monitor_filter.trim().is_empty() {
        format!(
            "{} shown ({})",
            filtered_items.len(),
            if snapshot.monitor_show_all {
                "all repos"
            } else {
                "attention only"
            }
        )
    } else {
        format!(
            "{} shown for \"{}\" ({})",
            filtered_items.len(),
            snapshot.monitor_filter.trim(),
            if snapshot.monitor_show_all {
                "all repos"
            } else {
                "attention only"
            }
        )
    };
    summary_label.set_text(&format!(
        "{} | {} repos | {} | {} | {} | {} | Workspace: {}",
        summary.workspace_name,
        summary.repo_count,
        filter_scope,
        selection_scope,
        loading_scope,
        manifest_status,
        snapshot.workspace_root.display()
    ));

    while let Some(child) = list.first_child() {
        list.remove(&child);
    }
    if reset_scroll_to_top {
        list.unselect_all();
    }

    if !filtered_items.is_empty() {
        for item in &filtered_items {
            let row = ListBoxRow::new();
            let content = GtkBox::new(Orientation::Horizontal, 10);
            content.set_margin_top(8);
            content.set_margin_bottom(8);
            content.set_margin_start(10);
            content.set_margin_end(10);

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

            let sync = Label::new(Some(&format_sync_label(&item.status.sync)));
            sync.set_xalign(0.0);
            sync.add_css_class("mono");
            sync.set_hexpand(true);
            sync.set_ellipsize(EllipsizeMode::End);

            content.append(&name);
            content.append(&branch);
            content.append(&status);
            content.append(&sync);
            row.set_child(Some(&content));
            row.set_widget_name(&item.id);
            row.set_tooltip_text(Some(&format!(
                "{}\n{}\n{}",
                item.status.repo_path.display(),
                item.remote_url,
                item.dir_name
            )));
            attach_row_context_menu(&row, host_ptr);
            list.append(&row);
        }

        for (index, item) in filtered_items.iter().enumerate() {
            if snapshot.selected_repo_ids.iter().any(|id| id == &item.id) {
                if let Some(row) = list.row_at_index(index as i32) {
                    list.select_row(Some(&row));
                }
            }
        }
    } else {
        let row = ListBoxRow::new();
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
        row.set_child(Some(&empty));
        list.append(&row);
    }

    let scroller = scroller.clone();
    glib::idle_add_local_once(move || {
        let adjustment = scroller.vadjustment();
        if reset_scroll_to_top {
            adjustment.set_value(adjustment.lower());
            return;
        }
        let max_value = (adjustment.upper() - adjustment.page_size()).max(adjustment.lower());
        adjustment.set_value(previous_scroll.clamp(adjustment.lower(), max_value));
    });
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
    let mut items = vec![monorepo_monitor_item(snapshot)];
    items.extend(repository_items(snapshot));
    filtered_repository_items(snapshot, items)
}

fn filtered_repository_items(
    snapshot: &StateSnapshot,
    mut items: Vec<RepositoryListItem>,
) -> Vec<RepositoryListItem> {
    items.sort_by_key(repo_monitor_sort_key);

    if !snapshot.monitor_show_all {
        items.retain(repo_requires_attention);
    }

    let filter = snapshot.monitor_filter.trim().to_ascii_lowercase();
    if filter.is_empty() {
        return items;
    }

    items
        .into_iter()
        .filter(|item| {
            let branch = branch_label(item).to_ascii_lowercase();
            let sync = format_sync_label(&item.status.sync).to_ascii_lowercase();
            let state = status_label(&item.status.state).to_ascii_lowercase();
            item.name.to_ascii_lowercase().contains(&filter)
                || item.dir_name.to_ascii_lowercase().contains(&filter)
                || item.remote_url.to_ascii_lowercase().contains(&filter)
                || branch.contains(&filter)
                || sync.contains(&filter)
                || state.contains(&filter)
        })
        .collect()
}

fn repo_monitor_sort_key(item: &RepositoryListItem) -> (u8, String) {
    (u8::from(item.id == MONOREPO_ROW_ID), item.name.to_ascii_lowercase())
}

fn repo_requires_attention(item: &RepositoryListItem) -> bool {
    use ronomepo_core::{RepositoryState, RepositorySync};

    !matches!(item.status.state, RepositoryState::Clean)
        || !matches!(
            item.status.sync,
            RepositorySync::UpToDate | RepositorySync::NoUpstream
        )
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
        .map(|row| row.widget_name().to_string())
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

fn update_monitor_show_all(show_all: bool) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.monitor_show_all = show_all;
}

fn update_line_stats_since(value: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.line_stats_since = value;
}

fn open_repo_overviews(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    repo_ids: &[String],
) {
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

fn schedule_workspace_scan() {
    let (workspace_root, manifest) = {
        let mut app_state = state().lock().expect("state mutex poisoned");
        if app_state.repository_items_loading {
            app_state.repository_items_refresh_pending = true;
            return;
        }
        app_state.repository_items_loading = true;
        app_state.repository_items_refresh_pending = false;
        (
            app_state.workspace_root.clone(),
            app_state.manifest.clone(),
        )
    };

    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let workspace_status = ronomepo_core::collect_repository_status(&workspace_root);
        let repository_items = manifest
            .as_ref()
            .map(build_repository_list)
            .unwrap_or_default();

        main_context.invoke(move || {
            let rerun = {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.workspace_status = workspace_status;
                app_state.repository_items = repository_items;
                app_state.repository_items_loading = false;
                let rerun = app_state.repository_items_refresh_pending;
                app_state.repository_items_refresh_pending = false;
                rerun
            };
            if rerun {
                schedule_workspace_scan();
            }
            refresh_views();
        });
    });
}

fn schedule_workspace_root_status_refresh(workspace_root: PathBuf) {
    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let workspace_status = ronomepo_core::collect_repository_status(&workspace_root);
        main_context.invoke(move || {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.workspace_status = workspace_status;
            }
            refresh_views();
        });
    });
}

fn schedule_repository_status_refresh(repo_id: &str, repo_path: PathBuf) {
    let repo_id = repo_id.to_string();
    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let status = ronomepo_core::collect_repository_status(&repo_path);
        main_context.invoke(move || {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                if let Some(item) = app_state
                    .repository_items
                    .iter_mut()
                    .find(|item| item.id == repo_id)
                {
                    item.status = status.clone();
                }
            }
            refresh_views();
        });
    });
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

    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        let details = collect_repository_details(&repo_path);
        main_context.invoke(move || {
            {
                let mut app_state = state().lock().expect("state mutex poisoned");
                app_state.repo_details_loading.remove(&repo_id);
                app_state.repo_details_cache.insert(repo_id, details);
            }
            refresh_views();
        });
    });
}

fn open_repo_overview_for_item(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    item: &RepositoryListItem,
) -> Result<(), String> {
    let host = unsafe { HostApi::from_raw(&*host_ptr) };
    let mut request = OpenViewRequest::new(PLUGIN_ID, VIEW_REPO_OVERVIEW, MzViewPlacement::Workbench);
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

fn attach_row_context_menu(row: &ListBoxRow, host_ptr: *const maruzzella_sdk::ffi::MzHostApi) {
    let popover = build_repo_context_menu(row, host_ptr);
    let gesture = GestureClick::new();
    gesture.set_button(3);
    gesture.connect_pressed({
        let row = row.clone();
        let popover = popover.clone();
        move |_, _, _, _| {
            if let Some(list) = row.parent().and_downcast::<ListBox>() {
                if !row.is_selected() {
                    list.unselect_all();
                    list.select_row(Some(&row));
                    update_selected_repo_ids(selection_ids_from_list(&list));
                }
            }
            popover.popup();
        }
    });
    row.add_controller(gesture);
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
    content.set_margin_top(18);
    content.set_margin_bottom(18);
    content.set_margin_start(18);
    content.set_margin_end(18);

    let title = Label::new(Some("Repository Monitor"));
    title.set_xalign(0.0);
    title.add_css_class("title-4");

    let summary = Label::new(None);
    summary.set_xalign(0.0);
    summary.set_wrap(true);
    summary.add_css_class("muted");

    let filter_entry = Entry::new();
    filter_entry.set_placeholder_text(Some("Filter repositories"));
    filter_entry.connect_changed(|entry| {
        update_monitor_filter(entry.text().to_string());
        refresh_views();
    });

    let show_all = CheckButton::with_label("Show all");
    show_all.set_active(snapshot().monitor_show_all);
    show_all.connect_toggled(|button| {
        update_monitor_show_all(button.is_active());
        refresh_views();
    });

    let monitor_actions = repo_monitor_actions(host);

    let list = ListBox::new();
    list.add_css_class("boxed-list");
    list.set_hexpand(true);
    list.set_valign(Align::Start);
    list.set_selection_mode(SelectionMode::Multiple);
    list.connect_selected_rows_changed(|list| {
        update_selected_repo_ids(selection_ids_from_list(list));
    });
    list.connect_row_activated(move |_, row| {
        if let Some(list) = row.parent().and_downcast::<ListBox>() {
            let selected_ids = selection_ids_from_list(&list);
            let row_id = row.widget_name().to_string();
            if selected_ids.len() != 1 || selected_ids.first() != Some(&row_id) {
                list.unselect_all();
                list.select_row(Some(row));
                update_selected_repo_ids(selection_ids_from_list(&list));
            }
        }
        let repo_id = row.widget_name().to_string();
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
        .child(&content)
        .build();
    scroller.set_valign(Align::Fill);
    scroller.set_propagate_natural_height(false);

    content.append(&title);
    content.append(&summary);
    content.append(&filter_entry);
    content.append(&show_all);
    content.append(&monitor_actions);
    content.append(&Separator::new(Orientation::Horizontal));
    content.append(&repo_monitor_header());
    content.append(&list);

    let snapshot = snapshot();
    render_repository_view_into(&summary, &list, &scroller, &snapshot, host);

    let summary_ref = glib::WeakRef::new();
    summary_ref.set(Some(&summary));
    let filter_ref = glib::WeakRef::new();
    filter_ref.set(Some(&filter_entry));
    let list_ref = glib::WeakRef::new();
    list_ref.set(Some(&list));
    let scroller_ref = glib::WeakRef::new();
    scroller_ref.set(Some(&scroller));
    REPOSITORY_VIEWS.with(|views| {
        views.borrow_mut().push(RepositoryViewHandle {
            summary: summary_ref,
            filter_entry: filter_ref,
            list: list_ref,
            scroller: scroller_ref,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(scroller.upcast())
            as *mut std::ffi::c_void
    }
}

fn repo_monitor_header() -> GtkBox {
    let header = GtkBox::new(Orientation::Horizontal, 10);
    header.add_css_class("mono");
    header.set_margin_bottom(4);
    let name = monitor_text_cell("Name", MONITOR_NAME_COL_CHARS, MONITOR_NAME_COL_WIDTH, false);
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

fn repo_monitor_actions(host_ptr: *const maruzzella_sdk::ffi::MzHostApi) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);

    let select_visible = Button::with_label("Select Visible");
    select_visible.connect_clicked(move |_| {
        let snapshot = snapshot();
        let ids = visible_monitor_items(&snapshot)
            .into_iter()
            .filter(|item| item.id != MONOREPO_ROW_ID)
            .map(|item| item.id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            append_log("Select Visible skipped because there are no visible repos.".to_string());
            return;
        }
        set_selected_repo_ids(ids.clone());
        append_log(format!("Selected {} visible repos from the monitor.", ids.len()));
    });
    actions.append(&select_visible);

    let select_dirty = Button::with_label("Select Dirty");
    select_dirty.connect_clicked(move |_| {
        let snapshot = snapshot();
        let ids = visible_monitor_items(&snapshot)
            .into_iter()
            .filter(|item| {
                matches!(
                    item.status.state,
                    ronomepo_core::RepositoryState::Dirty
                        | ronomepo_core::RepositoryState::Untracked
                )
            })
            .map(|item| item.id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            append_log("Select Dirty skipped because no visible repos are dirty.".to_string());
            return;
        }
        set_selected_repo_ids(ids.clone());
        append_log(format!("Selected {} dirty repos from the monitor.", ids.len()));
    });
    actions.append(&select_dirty);

    let select_missing = Button::with_label("Select Missing");
    select_missing.connect_clicked(move |_| {
        let snapshot = snapshot();
        let ids = visible_monitor_items(&snapshot)
            .into_iter()
            .filter(|item| matches!(item.status.state, ronomepo_core::RepositoryState::Missing))
            .map(|item| item.id)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            append_log("Select Missing skipped because no visible repos are missing.".to_string());
            return;
        }
        set_selected_repo_ids(ids.clone());
        append_log(format!("Selected {} missing repos from the monitor.", ids.len()));
    });
    actions.append(&select_missing);

    let clear_selection = Button::with_label("Clear");
    clear_selection.connect_clicked(move |_| {
        set_selected_repo_ids(Vec::new());
        append_log("Cleared repo selection.".to_string());
    });
    actions.append(&clear_selection);

    let open_selected = Button::with_label("Open Selected");
    open_selected.connect_clicked(move |_| {
        let repo_ids = {
            let app_state = state().lock().expect("state mutex poisoned");
            app_state.selected_repo_ids.clone()
        };
        open_repo_overviews(host_ptr, &repo_ids);
    });
    actions.append(&open_selected);

    actions
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
        .filter(|item| {
            matches!(
                item.status.state,
                ronomepo_core::RepositoryState::Missing
            )
        })
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
        &snapshot.history_report,
        "Run Check History to scan recent commits for generated markers.",
    );
    append_lines_section(
        &sections,
        "Line Stats",
        &snapshot.line_stats_report,
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

    let instance_key = unsafe { request.as_ref() }
        .and_then(|request| decode_mzstr(request.instance_key));
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

    let manifest = snapshot.manifest.clone().unwrap_or_else(|| WorkspaceManifest {
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
    if manifest.repos.is_empty() {
        append_repo_editor_row(&repo_rows_box, &repo_rows, None);
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
        let repo_rows_box = repo_rows_box.clone();
        let repo_rows = repo_rows.clone();
        move |_| {
            append_repo_editor_row(&repo_rows_box, &repo_rows, None);
        }
    });

    save.connect_clicked({
        let name_entry = name_entry.clone();
        let root_entry = root_entry.clone();
        let hooks_entry = hooks_entry.clone();
        let status = status.clone();
        let repo_rows = repo_rows.clone();
        move |_| match save_workspace_manifest_from_editor(
            host_ptr,
            name_entry.text().as_str(),
            root_entry.text().as_str(),
            hooks_entry.text().as_str(),
            &repo_rows.borrow(),
        ) {
            Ok(message) => {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
            Err(message) => {
                status.set_text(&message);
                append_log(message);
            }
        }
    });

    reload.connect_clicked({
        let status = status.clone();
        move |_| match refresh_workspace() {
            Ok(message) => {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
            Err(message) => {
                status.set_text(&message);
                append_log(message);
            }
        }
    });

    import.connect_clicked({
        let status = status.clone();
        move |_| match import_workspace_from_repos_txt() {
            Ok(message) => {
                status.set_text(&message);
                append_log(message);
                refresh_views();
            }
            Err(message) => {
                status.set_text(&message);
                append_log(message);
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

fn save_workspace_manifest_from_editor(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    workspace_name: &str,
    workspace_root: &str,
    shared_hooks_path: &str,
    repo_rows: &[RepoEditorRowHandle],
) -> Result<String, String> {
    let workspace_root = normalize_workspace_root(workspace_root.trim());
    if workspace_root.as_os_str().is_empty() {
        return Err("Workspace root cannot be empty.".to_string());
    }

    let mut repos = Vec::new();
    for handle in repo_rows {
        let remote_url = handle.remote_url.text().trim().to_string();
        let mut dir_name = handle.dir_name.text().trim().to_string();
        let mut name = handle.name.text().trim().to_string();

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
            enabled: handle.enabled.is_active(),
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

    let manifest_path = default_manifest_path(&workspace_root);
    save_manifest(&manifest_path, &manifest).map_err(|error| error.to_string())?;

    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.workspace_root = workspace_root.clone();
        app_state.manifest_path = Some(manifest_path.clone());
        app_state.manifest = Some(manifest.clone());
        app_state.repo_details_cache.clear();
        app_state.repo_details_loading.clear();
        app_state.selected_repo_ids
            .retain(|id| manifest.repos.iter().any(|repo| &repo.id == id));
        if app_state
            .active_repo_id
            .as_ref()
            .is_some_and(|id| !manifest.repos.iter().any(|repo| &repo.id == id))
        {
            app_state.active_repo_id = None;
        }
    }

    persist_last_workspace_path(host_ptr, &workspace_root);
    schedule_workspace_scan();

    Ok(format!(
        "Saved {} with {} repositories.",
        manifest_path.display(),
        manifest.repos.len()
    ))
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
        ("Selected", if snapshot.selected_repo_ids.iter().any(|id| id == &item.id) {
            "Yes".to_string()
        } else {
            "No".to_string()
        }),
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
    append_overview_section(
        &sections,
        "Sync",
        &format_sync_label(&item.status.sync),
    );
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
        ("Refresh", command_refresh_workspace as extern "C" fn(_) -> _),
        ("Clone Missing", command_clone_missing as extern "C" fn(_) -> _),
        ("Pull", command_pull as extern "C" fn(_) -> _),
        ("Push", command_push as extern "C" fn(_) -> _),
        ("Push Force", command_push_force as extern "C" fn(_) -> _),
        ("Apply Hooks", command_apply_hooks as extern "C" fn(_) -> _),
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
        button.connect_clicked(move |_| {
            match label {
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
                append_log(format!("{label} skipped because no repos match that bucket."));
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
    items.iter()
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

fn append_lines_section(
    container: &GtkBox,
    heading: &str,
    lines: &[String],
    empty_message: &str,
) {
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
        RepositorySync::Ahead(count) => format!("Push is available with {count} local commit(s) ahead."),
        RepositorySync::Diverged { ahead, behind } => format!(
            "Push is risky: the branch diverged (+{ahead}/-{behind})."
        ),
        RepositorySync::NoUpstream => "Push will be skipped because no upstream is configured.".to_string(),
        RepositorySync::Behind(count) => format!(
            "Push is not useful yet because the branch is behind by {count} commit(s)."
        ),
        RepositorySync::UpToDate => "Push will be skipped because the repo is already up to date.".to_string(),
        RepositorySync::Unknown => "Push eligibility is unknown because Git sync state could not be determined.".to_string(),
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

fn open_text_editor_for_path(
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
    path: &Path,
) {
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
        if Command::new(program).args([flag, path_text.as_str()]).spawn().is_ok() {
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
                    let bytes =
                        unsafe { std::slice::from_raw_parts(request.payload.ptr, request.payload.len) };
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
        load_editor_buffer(&buffer, &status, &resolve_editor_path(path));
        title.set_text(&editor_title_for_path(&resolve_editor_path(path)));
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
                load_editor_buffer(&buffer, &status, &path);
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
            match fs::write(&path, content.as_str()) {
                Ok(()) => {
                    status.set_text(&format!("Saved {}", path.display()));
                    let title_text = editor_title_for_path(&path);
                    title.set_text(&title_text);
                    if !host.is_null() {
                        let host = unsafe { HostApi::from_raw(&*host) };
                        let mut query = maruzzella_sdk::ViewQuery::new(PLUGIN_ID, VIEW_TEXT_EDITOR);
                        let path_key = path.to_string_lossy().to_string();
                        query.instance_key = Some(&path_key);
                        let _ = host.update_view_title(&query, &title_text);
                    }
                }
                Err(error) => {
                    status.set_text(&format!("Failed to save {}: {error}", path.display()));
                }
            }
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

fn load_editor_buffer(buffer: &TextBuffer, status: &Label, path: &Path) {
    match fs::read_to_string(path) {
        Ok(content) => {
            buffer.set_text(&content);
            status.set_text(&format!("Loaded {}", path.display()));
        }
        Err(error) => {
            buffer.set_text("");
            status.set_text(&format!("Failed to open {}: {error}", path.display()));
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
    let title = Label::new(Some("Operations"));
    title.set_xalign(0.0);
    title.add_css_class("title-4");
    title.set_hexpand(true);

    let summary = Label::new(Some(&operation_summary_text(&snapshot().logs)));
    summary.set_xalign(0.0);
    summary.add_css_class("muted");
    summary.set_wrap(true);

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

    header.append(&title);
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
    let starts = logs.iter().filter(|line| line.starts_with("[START]")).count();
    let ok = logs.iter().filter(|line| line.starts_with("[OK]")).count();
    let skipped = logs.iter().filter(|line| line.starts_with("[SKIP]")).count();
    let failed = logs.iter().filter(|line| line.starts_with("[FAIL]")).count();
    let latest = logs
        .last()
        .map(String::as_str)
        .unwrap_or("No operations recorded yet.");

    format!(
        "{total} log lines | {starts} started | {ok} ok | {skipped} skipped | {failed} failed | Latest: {latest}"
    )
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

    #[test]
    fn descriptor_uses_expected_plugin_id() {
        let descriptor = RonomepoPlugin::descriptor();
        assert_eq!(descriptor.id, PLUGIN_ID);
        assert_eq!(descriptor.dependencies.len(), 1);
        assert_eq!(descriptor.dependencies[0].plugin_id, "maruzzella.base");
    }
}

export_plugin!(RonomepoPlugin);
