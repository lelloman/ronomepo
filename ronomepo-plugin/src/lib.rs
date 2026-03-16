use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::thread;

use gtk::glib::{self, translate::IntoGlibPtr};
use gtk::prelude::*;
use gtk::{
    Align, Box as GtkBox, Button, GestureClick, Label, ListBox, ListBoxRow, Orientation,
    PolicyType, Popover, PositionType, ScrolledWindow, SelectionMode, Separator, TextBuffer,
    TextView, WrapMode,
};
use maruzzella_sdk::{
    export_plugin, CommandSpec, HostApi, MenuItemSpec, MzLogLevel, MzMenuSurface, MzStatusCode,
    MzViewPlacement, Plugin, PluginDependency, PluginDescriptor, SurfaceContributionSpec, Version,
    ViewFactorySpec,
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
    selected_repo_ids: Vec<String>,
    active_repo_id: Option<String>,
    logs: Vec<String>,
}

#[derive(Default)]
struct RepositoryViewHandle {
    summary: glib::WeakRef<Label>,
    list: glib::WeakRef<ListBox>,
}

thread_local! {
    static REPOSITORY_VIEWS: RefCell<Vec<RepositoryViewHandle>> = const { RefCell::new(Vec::new()) };
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
            let Some(list) = handle.list.upgrade() else {
                return false;
            };
            render_repository_view_into(&summary, &list, &snapshot);
            true
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
        selected_repo_ids: app_state.selected_repo_ids.clone(),
        active_repo_id: app_state.active_repo_id.clone(),
        logs: app_state.logs.clone(),
    }
}

fn render_repository_view_into(summary_label: &Label, list: &ListBox, snapshot: &StateSnapshot) {
    let repository_items = snapshot
        .manifest
        .as_ref()
        .map(build_repository_list)
        .unwrap_or_default();
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
    summary_label.set_text(&format!(
        "{} | {} repos | {} | Workspace: {}",
        summary.workspace_name,
        summary.repo_count,
        manifest_status,
        snapshot.workspace_root.display()
    ));

    while let Some(child) = list.first_child() {
        list.remove(&child);
    }

    if !repository_items.is_empty() {
        for item in &repository_items {
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
            attach_row_context_menu(&row);
            list.append(&row);
        }

        for (index, item) in repository_items.iter().enumerate() {
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

        let title = Label::new(Some("No workspace manifest loaded"));
        title.set_xalign(0.0);
        title.add_css_class("title-4");

        let body = Label::new(Some(
            "Ronomepo is running, but no ronomepo.json was found. Import repos.txt from the current workspace root to bootstrap the manifest.",
        ));
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

fn open_selected_repo_overview() {
    let snapshot = snapshot();
    let items = repository_items(&snapshot);
    let selected = items
        .iter()
        .filter(|item| snapshot.selected_repo_ids.iter().any(|id| id == &item.id))
        .collect::<Vec<_>>();

    if selected.is_empty() {
        append_log("No repository selected.".to_string());
        refresh_views();
        return;
    }

    let first = selected[0];
    {
        let mut app_state = state().lock().expect("state mutex poisoned");
        app_state.active_repo_id = Some(first.id.clone());
    }

    let message = if selected.len() == 1 {
        format!(
            "Selected {} for repo overview. Dynamic repo tabs land in the next task.",
            first.name
        )
    } else {
        format!(
            "Selected {} repositories; {} is now the active repo overview target. Multi-tab opening lands in the next task.",
            selected.len(),
            first.name
        )
    };
    append_log(message);
    refresh_views();
}

fn attach_row_context_menu(row: &ListBoxRow) {
    let popover = build_repo_context_menu(row);
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

fn build_repo_context_menu(relative_to: &impl IsA<gtk::Widget>) -> Popover {
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

    append_context_button(&menu, &popover, "Open Overview", || {
        open_selected_repo_overview();
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

    let title = Label::new(Some("Repository Monitor"));
    title.set_xalign(0.0);
    title.add_css_class("title-4");

    let summary = Label::new(None);
    summary.set_xalign(0.0);
    summary.set_wrap(true);
    summary.add_css_class("muted");

    let list = ListBox::new();
    list.add_css_class("boxed-list");
    list.set_selection_mode(SelectionMode::Multiple);
    list.connect_selected_rows_changed(|list| {
        update_selected_repo_ids(selection_ids_from_list(list));
    });
    list.connect_row_activated(|_, _| {
        open_selected_repo_overview();
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
    root.append(&Separator::new(Orientation::Horizontal));
    root.append(&repo_monitor_header());
    root.append(&scroller);

    let snapshot = snapshot();
    render_repository_view_into(&summary, &list, &snapshot);

    let summary_ref = glib::WeakRef::new();
    summary_ref.set(Some(&summary));
    let list_ref = glib::WeakRef::new();
    list_ref.set(Some(&list));
    REPOSITORY_VIEWS.with(|views| {
        views.borrow_mut().push(RepositoryViewHandle {
            summary: summary_ref,
            list: list_ref,
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

    let snapshot = snapshot();
    let summary = workspace_summary(
        snapshot.manifest.as_ref(),
        snapshot.manifest_path.as_deref(),
        &snapshot.workspace_root,
    );

    let root = GtkBox::new(Orientation::Vertical, 18);
    root.set_margin_top(24);
    root.set_margin_bottom(24);
    root.set_margin_start(24);
    root.set_margin_end(24);

    let hero = GtkBox::new(Orientation::Vertical, 8);
    let title = Label::new(Some("Monorepo Overview"));
    title.set_xalign(0.0);
    title.add_css_class("title-2");
    let subtitle = Label::new(Some(&format!(
        "{} repositories tracked in {}",
        summary.repo_count,
        summary.workspace_name
    )));
    subtitle.set_xalign(0.0);
    subtitle.add_css_class("muted");
    subtitle.set_wrap(true);
    hero.append(&title);
    hero.append(&subtitle);

    let actions = GtkBox::new(Orientation::Horizontal, 8);
    for (label, handler) in [
        ("Refresh", command_refresh_workspace as extern "C" fn(_) -> _),
        ("Import repos.txt", command_import_repos_txt as extern "C" fn(_) -> _),
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

    let sections = GtkBox::new(Orientation::Vertical, 12);
    for (heading, body) in [
        (
            "Workspace",
            format!("Current root: {}", snapshot.workspace_root.display()),
        ),
        (
            "Manifest",
            snapshot
                .manifest_path
                .as_ref()
                .map(|path| format!("Loaded from {}", path.display()))
                .unwrap_or_else(|| format!("No {MANIFEST_FILE_NAME} loaded yet")),
        ),
        (
            "Selection",
            snapshot
                .active_repo_id
                .as_ref()
                .map(|repo_id| format!("Active repo overview target: {repo_id}"))
                .unwrap_or_else(|| "No active repo overview target yet".to_string()),
        ),
        (
            "Next Milestones",
            "Real repo status, selection-aware actions, and repo overview tabs will replace these placeholders.".to_string(),
        ),
    ] {
        let block = GtkBox::new(Orientation::Vertical, 4);
        let heading_label = Label::new(Some(heading));
        heading_label.set_xalign(0.0);
        heading_label.add_css_class("title-4");
        let body_label = Label::new(Some(&body));
        body_label.set_xalign(0.0);
        body_label.set_wrap(true);
        body_label.add_css_class("muted");
        block.append(&heading_label);
        block.append(&body_label);
        sections.append(&block);
        sections.append(&Separator::new(Orientation::Horizontal));
    }

    root.append(&hero);
    root.append(&actions);
    root.append(&sections);

    unsafe {
        <gtk::Widget as IntoGlibPtr<*mut gtk::ffi::GtkWidget>>::into_glib_ptr(root.upcast())
            as *mut std::ffi::c_void
    }
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
