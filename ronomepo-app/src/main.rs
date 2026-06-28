use std::env;
use std::fs;
use std::path::PathBuf;

use gtk::gio::prelude::ApplicationExtManual;
use maruzzella::{
    build_application, default_product_spec, load_static_plugin, plugin_tab, BottomPanelLayout,
    CommandSpec, MaruzzellaConfig, MenuItemSpec, MenuRootSpec, PanelResizePolicy, TabGroupSpec,
    ToolbarDisplayMode, ToolbarItemSpec, WorkbenchNodeSpec,
};
use ronomepo_core::normalize_workspace_root;

fn main() {
    configure_gtk_input_method();
    reset_stale_persisted_layout();

    let workspace_root = parse_workspace_root_arg()
        .unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let workspace_root = normalize_workspace_root(workspace_root);
    env::set_var("RONOMEPO_WORKSPACE_ROOT", &workspace_root);

    let mut product = default_product_spec();
    product.branding.title = "Ronomepo".to_string();
    product.branding.search_placeholder = "Filter repositories".to_string();
    product.branding.search_command_id = Some("ronomepo.workspace.filter".to_string());
    product.branding.status_text =
        "Desktop workspace for many sibling Git repositories".to_string();
    product.include_base_toolbar_items = false;
    product.menu_roots = vec![
        MenuRootSpec {
            id: "file".to_string(),
            label: "File".to_string(),
        },
        MenuRootSpec {
            id: "view".to_string(),
            label: "View".to_string(),
        },
        MenuRootSpec {
            id: "monorepo".to_string(),
            label: "Monorepo".to_string(),
        },
        MenuRootSpec {
            id: "help".to_string(),
            label: "Help".to_string(),
        },
    ];
    product.menu_items = vec![
        MenuItemSpec {
            id: "new-buffer".to_string(),
            root_id: "file".to_string(),
            label: "New".to_string(),
            command_id: "shell.new_buffer".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "file-open".to_string(),
            root_id: "file".to_string(),
            label: "Open".to_string(),
            command_id: "shell.open_file_editor".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "save-buffer".to_string(),
            root_id: "file".to_string(),
            label: "Save".to_string(),
            command_id: "shell.save_buffer".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "save-buffer-as".to_string(),
            root_id: "file".to_string(),
            label: "Save As".to_string(),
            command_id: "shell.save_buffer_as".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "file-separator-1".to_string(),
            root_id: "file".to_string(),
            label: String::new(),
            command_id: String::new(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "plugins".to_string(),
            root_id: "file".to_string(),
            label: "Plugins".to_string(),
            command_id: "shell.plugins".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "settings".to_string(),
            root_id: "file".to_string(),
            label: "Settings".to_string(),
            command_id: "shell.settings".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "file-separator-2".to_string(),
            root_id: "file".to_string(),
            label: String::new(),
            command_id: String::new(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "file-exit".to_string(),
            root_id: "file".to_string(),
            label: "Exit".to_string(),
            command_id: "ronomepo.workspace.exit".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "view-overview".to_string(),
            root_id: "view".to_string(),
            label: "Overview".to_string(),
            command_id: "ronomepo.workspace.open_overview".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "view-commit-check".to_string(),
            root_id: "view".to_string(),
            label: "Commit Check".to_string(),
            command_id: "ronomepo.workspace.open_commit_check".to_string(),
            payload: Vec::new(),
        },
        // Reserve the base plugin menu id so its runtime "Reload Theme" item
        // is not merged into the visible View menu.
        MenuItemSpec {
            id: "reload-theme".to_string(),
            root_id: "ronomepo-hidden".to_string(),
            label: "Reload Theme".to_string(),
            command_id: String::new(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "monorepo-pull".to_string(),
            root_id: "monorepo".to_string(),
            label: "Pull".to_string(),
            command_id: "ronomepo.workspace.pull".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "monorepo-push".to_string(),
            root_id: "monorepo".to_string(),
            label: "Push".to_string(),
            command_id: "ronomepo.workspace.push".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "monorepo-refresh".to_string(),
            root_id: "monorepo".to_string(),
            label: "Refresh".to_string(),
            command_id: "ronomepo.workspace.refresh".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "monorepo-add-repo".to_string(),
            root_id: "monorepo".to_string(),
            label: "Add repo".to_string(),
            command_id: "ronomepo.workspace.add_repo".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "help-about".to_string(),
            root_id: "help".to_string(),
            label: "About".to_string(),
            command_id: "shell.about".to_string(),
            payload: Vec::new(),
        },
    ];
    product.commands = vec![
        CommandSpec {
            id: "ronomepo.workspace.refresh".to_string(),
            title: "Refresh Workspace".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.pull".to_string(),
            title: "Pull".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.push".to_string(),
            title: "Push".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.open_overview".to_string(),
            title: "Monorepo Overview".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.open_commit_check".to_string(),
            title: "Commit Check".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.filter".to_string(),
            title: "Filter Repositories".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.add_repo".to_string(),
            title: "Add Repo".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.exit".to_string(),
            title: "Exit".to_string(),
        },
    ];
    product.toolbar_items = vec![
        ToolbarItemSpec {
            id: "refresh".to_string(),
            icon_name: Some("view-refresh-symbolic".to_string()),
            label: Some("Refresh".to_string()),
            command_id: "ronomepo.workspace.refresh".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "primary".to_string(),
            options: Vec::new(),
            selected_index: 0,
        },
        ToolbarItemSpec {
            id: "pull".to_string(),
            icon_name: Some("go-down-symbolic".to_string()),
            label: Some("Pull".to_string()),
            command_id: "ronomepo.workspace.pull".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "primary".to_string(),
            options: Vec::new(),
            selected_index: 0,
        },
        ToolbarItemSpec {
            id: "push".to_string(),
            icon_name: Some("go-up-symbolic".to_string()),
            label: Some("Push".to_string()),
            command_id: "ronomepo.workspace.push".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "primary".to_string(),
            options: Vec::new(),
            selected_index: 0,
        },
        ToolbarItemSpec {
            id: "monorepo-overview".to_string(),
            icon_name: Some("view-grid-symbolic".to_string()),
            label: Some("Monorepo Overview".to_string()),
            command_id: "ronomepo.workspace.open_overview".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ghost".to_string(),
            options: Vec::new(),
            selected_index: 0,
        },
        ToolbarItemSpec {
            id: "commit-check".to_string(),
            icon_name: Some("dialog-warning-symbolic".to_string()),
            label: Some("Commit Check".to_string()),
            command_id: "ronomepo.workspace.open_commit_check".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ghost".to_string(),
            options: Vec::new(),
            selected_index: 0,
        },
    ];

    product.layout.bottom_panel_layout = BottomPanelLayout::CenterOnly;
    product.layout.left_panel_resize = PanelResizePolicy::CappedProportional { max_factor: 1.5 };
    product.layout.bottom_panel_resize = PanelResizePolicy::CappedProportional { max_factor: 1.5 };
    product.layout.left_panel = TabGroupSpec::new(
        "panel-left",
        Some("repositories"),
        vec![plugin_tab(
            "repositories",
            "panel-left",
            "Repositories",
            "com.lelloman.ronomepo.repo_monitor",
            "The Ronomepo repository monitor could not be created.",
            false,
        )],
    )
    .with_tab_strip_hidden()
    .with_panel_appearance("primary")
    .with_panel_header_appearance("secondary")
    .with_tab_strip_appearance("utility")
    .with_text_appearance("body");
    product.layout.right_panel = TabGroupSpec::new("panel-right", None, Vec::new())
        .with_panel_appearance("secondary")
        .with_panel_header_appearance("secondary")
        .with_tab_strip_appearance("utility")
        .with_text_appearance("body");
    product.layout.bottom_panel = TabGroupSpec::new(
        "panel-bottom",
        Some("operations"),
        vec![plugin_tab(
            "operations",
            "panel-bottom",
            "Operations",
            "com.lelloman.ronomepo.operations",
            "The Ronomepo operations view could not be created.",
            false,
        )],
    )
    .with_tab_strip_hidden()
    .with_panel_appearance("console")
    .with_panel_header_appearance("secondary")
    .with_tab_strip_appearance("console")
    .with_text_appearance("code");
    product.layout.workbench = WorkbenchNodeSpec::Group(
        TabGroupSpec::new(
            "workbench-main",
            Some("monorepo-overview"),
            vec![
                plugin_tab(
                    "monorepo-overview",
                    "workbench-main",
                    "Monorepo Overview",
                    "com.lelloman.ronomepo.monorepo_overview",
                    "The Ronomepo monorepo overview could not be created.",
                    false,
                ),
                plugin_tab(
                    "commit-check",
                    "workbench-main",
                    "Commit Check",
                    "com.lelloman.ronomepo.commit_check",
                    "The Ronomepo commit check view could not be created.",
                    false,
                ),
            ],
        )
        .with_panel_appearance("workbench")
        .with_panel_header_appearance("secondary")
        .with_tab_strip_appearance("editor")
        .with_text_appearance("body"),
    );

    let config = MaruzzellaConfig::new("com.lelloman.ronomepo")
        .with_persistence_id("ronomepo")
        .with_product(product)
        .with_builtin_plugin(embedded_ronomepo_plugin);

    let application = build_application(config);
    let argv0 = env::args()
        .next()
        .unwrap_or_else(|| "ronomepo-app".to_string());
    application.run_with_args(&[argv0]);
}

fn configure_gtk_input_method() {
    if env::var_os("RONOMEPO_USE_SYSTEM_INPUT_METHOD").is_some() {
        return;
    }

    // Keep Ronomepo from talking to IBus. When IBus is wedged, GTK's repeated
    // input-context creation attempts can also disturb the rest of the i3
    // session, so use GTK's built-in simple context for this process.
    env::set_var("GTK_IM_MODULE", "gtk-im-context-simple");

    if env_var_uses_ibus("XMODIFIERS") {
        env::remove_var("XMODIFIERS");
    }
    if env_var_uses_ibus("QT_IM_MODULE") {
        env::remove_var("QT_IM_MODULE");
    }
}

fn env_var_uses_ibus(name: &str) -> bool {
    env::var(name)
        .map(|value| value.to_ascii_lowercase().contains("ibus"))
        .unwrap_or(false)
}

fn reset_stale_persisted_layout() {
    let path = persisted_layout_path("ronomepo");
    let Ok(raw) = fs::read_to_string(&path) else {
        return;
    };

    // Old Ronomepo builds persisted a shell layout that points to non-existent
    // views, base-shell side tabs we don't use, and placeholder tabs, which can
    // leave the workspace in a stale or confusing layout on startup.
    let has_stale_layout = raw
        .contains("\"plugin_view_id\": \"com.lelloman.ronomepo.repositories\"")
        || raw.contains("\"plugin_view_id\": \"maruzzella.base.selection_inspector\"")
        || raw.contains("\"plugin_view_id\": \"maruzzella.base.delivery\"")
        || raw.contains("\"id\": \"selection-inspector\"")
        || raw.contains("\"id\": \"delivery-checklist\"")
        || raw.contains("\"id\": \"ronomepo-clone-missing\"")
        || raw.contains("\"id\": \"ronomepo-push-force\"")
        || raw.contains("\"id\": \"ronomepo-hooks\"")
        || raw.contains("\"id\": \"ronomepo-check-history\"")
        || raw.contains("\"id\": \"ronomepo-line-stats\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.clone_missing\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.push_force\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.apply_hooks\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.check_history\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.line_stats\"")
        || raw.contains(
            "\"placeholder\": \"Workspace path, filters, and import guidance will live here.\"",
        );

    if has_stale_layout {
        let _ = fs::remove_file(path);
    }
}

fn persisted_layout_path(persistence_id: &str) -> PathBuf {
    let mut path = if let Ok(dir) = env::var("XDG_CONFIG_HOME") {
        PathBuf::from(dir)
    } else if let Ok(home) = env::var("HOME") {
        PathBuf::from(home).join(".config")
    } else {
        PathBuf::from(".")
    };
    path.push(persistence_id);
    path.push("layout.json");
    path
}

fn parse_workspace_root_arg() -> Option<PathBuf> {
    let mut args = env::args_os().skip(1);
    let mut positional = None;

    while let Some(arg) = args.next() {
        if arg == "--workspace" {
            return args.next().map(PathBuf::from);
        }
        if let Some(value) = arg
            .to_str()
            .and_then(|text| text.strip_prefix("--workspace="))
        {
            return Some(PathBuf::from(value));
        }
        if positional.is_none() {
            positional = Some(PathBuf::from(arg));
        }
    }

    positional
}

fn embedded_ronomepo_plugin() -> Result<maruzzella::LoadedPlugin, maruzzella::PluginLoadError> {
    load_static_plugin(
        "builtin:ronomepo-plugin",
        ronomepo_plugin::maruzzella_plugin_entry,
    )
}
