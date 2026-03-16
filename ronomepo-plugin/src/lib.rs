use std::cell::RefCell;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::thread;

use gtk::glib::{self, translate::IntoGlibPtr};
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, Entry, GestureClick, Label, ListBox, ListBoxRow, Orientation,
    PolicyType, Popover, PositionType, ScrolledWindow, SelectionMode, Separator, TextBuffer,
    TextView, WrapMode,
};
use maruzzella_sdk::{
    export_plugin, CommandSpec, HostApi, MenuItemSpec, MzLogLevel, MzMenuSurface,
    MzStatusCode, MzViewOpenDisposition, MzViewPlacement, OpenViewRequest, Plugin,
    PluginDependency, PluginDescriptor, SurfaceContributionSpec, Version, ViewFactorySpec,
};
use ronomepo_core::{
    build_repository_list, default_manifest_path, format_sync_label, import_repos_txt,
    load_manifest, run_workspace_operation, save_manifest, workspace_summary, OperationEvent,
    OperationEventKind, OperationKind, RepositoryListItem, MANIFEST_FILE_NAME, WorkspaceManifest,
};
use serde::{Deserialize, Serialize};

const PLUGIN_ID: &str = "com.lelloman.ronomepo";
const VIEW_REPO_MONITOR: &str = "com.lelloman.ronomepo.repo_monitor";
const VIEW_MONOREPO_OVERVIEW: &str = "com.lelloman.ronomepo.monorepo_overview";
const VIEW_REPO_OVERVIEW: &str = "com.lelloman.ronomepo.repo_overview";
const VIEW_TEXT_EDITOR: &str = "com.lelloman.ronomepo.text_editor";
const VIEW_OPERATIONS: &str = "com.lelloman.ronomepo.operations";
const CMD_REFRESH: &str = "ronomepo.workspace.refresh";
const CMD_IMPORT: &str = "ronomepo.workspace.import_repos_txt";
const CMD_SETTINGS: &str = "ronomepo.workspace.open_settings";
const CMD_CLONE_MISSING: &str = "ronomepo.workspace.clone_missing";
const CMD_PULL: &str = "ronomepo.workspace.pull";
const CMD_PUSH: &str = "ronomepo.workspace.push";
const CMD_APPLY_HOOKS: &str = "ronomepo.workspace.apply_hooks";
const CMD_OPEN_OVERVIEW: &str = "ronomepo.workspace.open_overview";

struct RonomepoPlugin;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RonomepoPluginConfig {
    last_workspace_path: Option<String>,
    import_banner_dismissed: bool,
}

#[derive(Clone, Debug, Default)]
struct AppState {
    workspace_root: PathBuf,
    manifest_path: Option<PathBuf>,
    manifest: Option<WorkspaceManifest>,
    monitor_filter: String,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
}

#[derive(Default)]
struct RepositoryViewHandle {
    summary: glib::WeakRef<Label>,
    filter_entry: glib::WeakRef<Entry>,
    list: glib::WeakRef<ListBox>,
    host_ptr: usize,
}

#[derive(Default)]
struct ContainerViewHandle {
    root: glib::WeakRef<GtkBox>,
    instance_key: Option<String>,
}

thread_local! {
    static REPOSITORY_VIEWS: RefCell<Vec<RepositoryViewHandle>> = const { RefCell::new(Vec::new()) };
    static MONOREPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static REPO_OVERVIEWS: RefCell<Vec<ContainerViewHandle>> = const { RefCell::new(Vec::new()) };
    static OPERATION_BUFFERS: RefCell<Vec<glib::WeakRef<TextBuffer>>> = const { RefCell::new(Vec::new()) };
}

static STATE: OnceLock<Mutex<AppState>> = OnceLock::new();

fn state() -> &'static Mutex<AppState> {
    STATE.get_or_init(|| Mutex::new(AppState::default()))
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
            CommandSpec::new(PLUGIN_ID, CMD_APPLY_HOOKS, "Apply Hooks")
                .with_handler(command_apply_hooks),
        )?;
        host.register_command(
            CommandSpec::new(PLUGIN_ID, CMD_OPEN_OVERVIEW, "Monorepo Overview")
                .with_handler(command_open_overview),
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
    let workspace_root = config
        .last_workspace_path
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let manifest_path = default_manifest_path(&workspace_root);
    let manifest = load_manifest_if_present(&manifest_path);

    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.workspace_root = workspace_root.clone();
    app_state.manifest_path = manifest.as_ref().map(|_| manifest_path.clone());
    app_state.manifest = manifest;
    if app_state.logs.is_empty() {
        app_state.logs.push(format!(
            "Ronomepo initialized for workspace {}",
            workspace_root.display()
        ));
    }
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
    append_log("Workspace settings are not implemented yet.".to_string());
    refresh_views();
    maruzzella_sdk::ffi::MzStatus::OK
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

extern "C" fn command_apply_hooks(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    launch_operation(OperationKind::ApplyHooks);
    maruzzella_sdk::ffi::MzStatus::OK
}

extern "C" fn command_open_overview(
    _payload: maruzzella_sdk::ffi::MzBytes,
) -> maruzzella_sdk::ffi::MzStatus {
    append_log("Monorepo Overview is the default startup tab.".to_string());
    refresh_views();
    maruzzella_sdk::ffi::MzStatus::OK
}

fn refresh_workspace() -> Result<String, String> {
    let mut app_state = state().lock().expect("state mutex poisoned");
    let manifest_path = default_manifest_path(&app_state.workspace_root);
    app_state.manifest = load_manifest_if_present(&manifest_path);
    app_state.manifest_path = app_state.manifest.as_ref().map(|_| manifest_path.clone());
    if app_state.manifest.is_some() {
        Ok(format!("Reloaded {MANIFEST_FILE_NAME} from {}", manifest_path.display()))
    } else {
        Ok(format!(
            "No {MANIFEST_FILE_NAME} found in {}",
            app_state.workspace_root.display()
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

    Ok(format!(
        "Imported {repo_count} repositories from {} into {}",
        repos_path.display(),
        manifest_path.display()
    ))
}

fn append_log(message: String) {
    let mut app_state = state().lock().expect("state mutex poisoned");
    app_state.logs.push(message);
}

fn launch_operation(kind: OperationKind) {
    let (manifest, selected_repo_ids) = {
        let app_state = state().lock().expect("state mutex poisoned");
        (
            app_state.manifest.clone(),
            app_state.selected_repo_ids.clone(),
        )
    };

    let Some(manifest) = manifest else {
        append_log(format!(
            "{} skipped because no {} is loaded.",
            operation_kind_title(kind),
            MANIFEST_FILE_NAME
        ));
        refresh_views();
        return;
    };

    let main_context = glib::MainContext::default();
    thread::spawn(move || {
        run_workspace_operation(&manifest, &selected_repo_ids, kind, |event| {
            append_log(format_operation_event(&event));
            main_context.invoke(refresh_views);
        });
    });
}

fn operation_kind_title(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::CloneMissing => "Clone Missing",
        OperationKind::Pull => "Pull",
        OperationKind::Push => "Push",
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
            if filter_entry.text().as_str() != snapshot.monitor_filter {
                filter_entry.set_text(&snapshot.monitor_filter);
            }
            render_repository_view_into(&summary, &list, &snapshot, handle.host_ptr as *const _);
            true
        });
    });

    MONOREPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_monorepo_overview_into(&root, &snapshot);
                true
            }
            None => false,
        });
    });

    REPO_OVERVIEWS.with(|views| {
        let mut views = views.borrow_mut();
        views.retain(|handle| match handle.root.upgrade() {
            Some(root) => {
                render_repo_overview_into(&root, &snapshot, handle.instance_key.as_deref());
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
}

#[derive(Clone)]
struct StateSnapshot {
    workspace_root: PathBuf,
    manifest_path: Option<PathBuf>,
    manifest: Option<WorkspaceManifest>,
    monitor_filter: String,
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
}

fn snapshot() -> StateSnapshot {
    let app_state = state().lock().expect("state mutex poisoned");
    StateSnapshot {
        workspace_root: app_state.workspace_root.clone(),
        manifest_path: app_state.manifest_path.clone(),
        manifest: app_state.manifest.clone(),
        monitor_filter: app_state.monitor_filter.clone(),
        selected_repo_ids: app_state.selected_repo_ids.clone(),
        active_repo_id: app_state.active_repo_id.clone(),
        logs: app_state.logs.clone(),
    }
}

fn render_repository_view_into(
    summary_label: &Label,
    list: &ListBox,
    snapshot: &StateSnapshot,
    host_ptr: *const maruzzella_sdk::ffi::MzHostApi,
) {
    let repository_items = snapshot
        .manifest
        .as_ref()
        .map(build_repository_list)
        .unwrap_or_default();
    let filtered_items = filtered_repository_items(snapshot, repository_items);
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
    let filter_scope = if snapshot.monitor_filter.trim().is_empty() {
        format!("{} shown", filtered_items.len())
    } else {
        format!(
            "{} shown for \"{}\"",
            filtered_items.len(),
            snapshot.monitor_filter.trim()
        )
    };
    summary_label.set_text(&format!(
        "{} | {} repos | {} | {} | {} | Workspace: {}",
        summary.workspace_name,
        summary.repo_count,
        filter_scope,
        selection_scope,
        manifest_status,
        snapshot.workspace_root.display()
    ));

    while let Some(child) = list.first_child() {
        list.remove(&child);
    }

    if !filtered_items.is_empty() {
        for item in &filtered_items {
            let row = ListBoxRow::new();
            let content = GtkBox::new(Orientation::Horizontal, 10);
            content.set_margin_top(8);
            content.set_margin_bottom(8);
            content.set_margin_start(10);
            content.set_margin_end(10);

            let name = Label::new(Some(&item.name));
            name.set_xalign(0.0);
            name.add_css_class("mono");
            name.set_hexpand(true);
            name.set_width_chars(16);

            let branch = Label::new(Some(branch_label(item)));
            branch.set_xalign(0.0);
            branch.add_css_class("mono");
            branch.set_width_chars(12);

            let status = Label::new(Some(status_label(&item.status.state)));
            status.set_xalign(0.0);
            status.add_css_class("pill");
            status.set_width_chars(10);

            let sync = Label::new(Some(&format_sync_label(&item.status.sync)));
            sync.set_xalign(1.0);
            sync.add_css_class("mono");
            sync.set_hexpand(true);

            content.append(&name);
            content.append(&branch);
            content.append(&status);
            content.append(&sync);
            row.set_child(Some(&content));
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
    snapshot
        .manifest
        .as_ref()
        .map(build_repository_list)
        .unwrap_or_default()
}

fn filtered_repository_items(
    snapshot: &StateSnapshot,
    mut items: Vec<RepositoryListItem>,
) -> Vec<RepositoryListItem> {
    items.sort_by_key(repo_monitor_sort_key);

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
    (repo_attention_rank(item), item.name.to_ascii_lowercase())
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
    let snapshot = snapshot();
    let items = repository_items(&snapshot);
    let mut selected = list
        .selected_rows()
        .into_iter()
        .filter_map(|row| items.get(row.index() as usize).map(|item| item.id.clone()))
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

    let root = GtkBox::new(Orientation::Vertical, 12);
    root.set_margin_top(18);
    root.set_margin_bottom(18);
    root.set_margin_start(18);
    root.set_margin_end(18);

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

    let list = ListBox::new();
    list.add_css_class("boxed-list");
    list.set_selection_mode(SelectionMode::Multiple);
    list.connect_selected_rows_changed(|list| {
        update_selected_repo_ids(selection_ids_from_list(list));
    });
    list.connect_row_activated(move |_, row| {
        let repo_ids = snapshot()
            .manifest
            .as_ref()
            .map(build_repository_list)
            .and_then(|items| items.get(row.index() as usize).map(|item| vec![item.id.clone()]))
            .unwrap_or_default();
        open_repo_overviews(host, &repo_ids);
    });

    let scroller = ScrolledWindow::builder()
        .hexpand(true)
        .vexpand(true)
        .hscrollbar_policy(PolicyType::Never)
        .vscrollbar_policy(PolicyType::Automatic)
        .child(&list)
        .build();

    root.append(&title);
    root.append(&summary);
    root.append(&filter_entry);
    root.append(&Separator::new(Orientation::Horizontal));
    root.append(&repo_monitor_header());
    root.append(&scroller);

    let snapshot = snapshot();
    render_repository_view_into(&summary, &list, &snapshot, host);

    let summary_ref = glib::WeakRef::new();
    summary_ref.set(Some(&summary));
    let filter_ref = glib::WeakRef::new();
    filter_ref.set(Some(&filter_entry));
    let list_ref = glib::WeakRef::new();
    list_ref.set(Some(&list));
    REPOSITORY_VIEWS.with(|views| {
        views.borrow_mut().push(RepositoryViewHandle {
            summary: summary_ref,
            filter_entry: filter_ref,
            list: list_ref,
            host_ptr: host as usize,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn repo_monitor_header() -> GtkBox {
    let header = GtkBox::new(Orientation::Horizontal, 10);
    header.add_css_class("mono");
    header.set_margin_bottom(4);

    for (title, width, expand) in [
        ("Name", 16, false),
        ("Branch", 12, false),
        ("State", 10, false),
        ("Sync", 0, true),
    ] {
        let label = Label::new(Some(title));
        label.set_xalign(0.0);
        if width > 0 {
            label.set_width_chars(width);
        }
        label.set_hexpand(expand);
        label.add_css_class("dim-label");
        header.append(&label);
    }

    header
}

extern "C" fn create_monorepo_overview_view(
    _host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let snapshot = snapshot();
    render_monorepo_overview_into(&root, &snapshot);

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    MONOREPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            instance_key: None,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn render_monorepo_overview_into(root: &GtkBox, snapshot: &StateSnapshot) {
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
    append_log_section(&sections, "Recent Operations", &snapshot.logs, 8);

    root.append(&hero);
    root.append(&stats);
    root.append(&actions);
    root.append(&sections);
}

extern "C" fn create_repo_overview_view(
    _host: *const maruzzella_sdk::ffi::MzHostApi,
    request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let instance_key = unsafe { request.as_ref() }
        .and_then(|request| decode_mzstr(request.instance_key));
    let snapshot = snapshot();
    render_repo_overview_into(&root, &snapshot, instance_key.as_deref());

    let root_ref = glib::WeakRef::new();
    root_ref.set(Some(&root));
    REPO_OVERVIEWS.with(|views| {
        views.borrow_mut().push(ContainerViewHandle {
            root: root_ref,
            instance_key,
        });
    });

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
}

fn render_repo_overview_into(
    root: &GtkBox,
    snapshot: &StateSnapshot,
    instance_key: Option<&str>,
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
    root.append(&repo_overview_actions(item));
    root.append(&overview_actions());

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

fn repo_overview_actions(item: &RepositoryListItem) -> GtkBox {
    let actions = GtkBox::new(Orientation::Horizontal, 8);
    for (label, kind) in [
        ("Target This Repo", None),
        ("Open Folder", None),
        ("Open Terminal", None),
        ("Open In Editor", None),
        ("Clone Repo", Some(OperationKind::CloneMissing)),
        ("Pull Repo", Some(OperationKind::Pull)),
        ("Push Repo", Some(OperationKind::Push)),
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
    _host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

    let root = GtkBox::new(Orientation::Vertical, 12);
    root.set_margin_top(18);
    root.set_margin_bottom(18);
    root.set_margin_start(18);
    root.set_margin_end(18);

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

    open_button.connect_clicked({
        let path_entry = path_entry.clone();
        let buffer = buffer.clone();
        let status = status.clone();
        move |_| {
            let path = resolve_editor_path(path_entry.text().as_str());
            match fs::read_to_string(&path) {
                Ok(content) => {
                    buffer.set_text(&content);
                    status.set_text(&format!("Loaded {}", path.display()));
                }
                Err(error) => {
                    status.set_text(&format!("Failed to open {}: {error}", path.display()));
                }
            }
        }
    });

    save_button.connect_clicked({
        let path_entry = path_entry.clone();
        let buffer = buffer.clone();
        let status = status.clone();
        move |_| {
            let path = resolve_editor_path(path_entry.text().as_str());
            let content = buffer.text(&buffer.start_iter(), &buffer.end_iter(), true);
            match fs::write(&path, content.as_str()) {
                Ok(()) => {
                    status.set_text(&format!("Saved {}", path.display()));
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

fn decode_mzstr(value: maruzzella_sdk::ffi::MzStr) -> Option<String> {
    if value.ptr.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(value.ptr, value.len) };
    let text = String::from_utf8_lossy(bytes).trim().to_string();
    (!text.is_empty()).then_some(text)
}

extern "C" fn create_operations_view(
    _host: *const maruzzella_sdk::ffi::MzHostApi,
    _request: *const maruzzella_sdk::ffi::MzViewRequest,
) -> *mut std::ffi::c_void {
    if !gtk::is_initialized_main_thread() && gtk::init().is_err() {
        return std::ptr::null_mut();
    }

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

    let refresh = Button::with_label("Refresh Logs");
    refresh.set_halign(Align::End);

    let buffer = TextBuffer::new(None);
    buffer.set_text(&snapshot().logs.join("\n"));
    let buffer_ref = glib::WeakRef::new();
    buffer_ref.set(Some(&buffer));
    OPERATION_BUFFERS.with(|buffers| {
        buffers.borrow_mut().push(buffer_ref);
    });

    refresh.connect_clicked({
        let buffer = buffer.clone();
        move |_| {
            buffer.set_text(&snapshot().logs.join("\n"));
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
    header.append(&refresh);
    root.append(&header);
    root.append(&scroller);

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
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

    #[test]
    fn descriptor_uses_expected_plugin_id() {
        let descriptor = RonomepoPlugin::descriptor();
        assert_eq!(descriptor.id, PLUGIN_ID);
        assert_eq!(descriptor.dependencies.len(), 1);
        assert_eq!(descriptor.dependencies[0].plugin_id, "maruzzella.base");
    }
}

export_plugin!(RonomepoPlugin);
