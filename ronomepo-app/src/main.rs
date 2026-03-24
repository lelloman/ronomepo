use std::env;
use std::fs;
use std::path::PathBuf;

use gtk::gdk;
use gtk::gio::prelude::ApplicationExtManual;
use gtk::prelude::ApplicationExt;
use gtk::{
    style_context_add_provider_for_display, CssProvider, STYLE_PROVIDER_PRIORITY_APPLICATION,
};
use maruzzella::{
    build_application, default_product_spec, load_static_plugin, plugin_tab, BottomPanelLayout,
    CommandSpec, MaruzzellaConfig, MenuItemSpec, MenuRootSpec, TabGroupSpec, ThemeSpec,
    ToolbarItemSpec, WorkbenchNodeSpec,
};
use ronomepo_core::normalize_workspace_root;

fn main() {
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
            id: "settings".to_string(),
            label: "Settings".to_string(),
        },
        MenuRootSpec {
            id: "help".to_string(),
            label: "Help".to_string(),
        },
    ];
    product.menu_items = vec![
        MenuItemSpec {
            id: "file-new".to_string(),
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
            id: "file-separator-1".to_string(),
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
            id: "settings-theme".to_string(),
            root_id: "settings".to_string(),
            label: "Theme".to_string(),
            command_id: "shell.settings".to_string(),
            payload: Vec::new(),
        },
        MenuItemSpec {
            id: "settings-plugins".to_string(),
            root_id: "settings".to_string(),
            label: "Plugins".to_string(),
            command_id: "shell.plugins".to_string(),
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
        },
        ToolbarItemSpec {
            id: "pull".to_string(),
            icon_name: Some("go-down-symbolic".to_string()),
            label: Some("Pull".to_string()),
            command_id: "ronomepo.workspace.pull".to_string(),
            payload: Vec::new(),
            secondary: false,
        },
        ToolbarItemSpec {
            id: "push".to_string(),
            icon_name: Some("go-up-symbolic".to_string()),
            label: Some("Push".to_string()),
            command_id: "ronomepo.workspace.push".to_string(),
            payload: Vec::new(),
            secondary: false,
        },
        ToolbarItemSpec {
            id: "monorepo-overview".to_string(),
            icon_name: Some("view-grid-symbolic".to_string()),
            label: Some("Monorepo Overview".to_string()),
            command_id: "ronomepo.workspace.open_overview".to_string(),
            payload: Vec::new(),
            secondary: true,
        },
        ToolbarItemSpec {
            id: "commit-check".to_string(),
            icon_name: Some("dialog-warning-symbolic".to_string()),
            label: Some("Commit Check".to_string()),
            command_id: "ronomepo.workspace.open_commit_check".to_string(),
            payload: Vec::new(),
            secondary: true,
        },
    ];

    product.layout.bottom_panel_layout = BottomPanelLayout::CenterOnly;
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
    .with_tab_strip_hidden();
    product.layout.right_panel = TabGroupSpec::new("panel-right", None, Vec::new());
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
    .with_tab_strip_hidden();
    product.layout.workbench = WorkbenchNodeSpec::Group(TabGroupSpec::new(
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
    ));

    let theme = app_theme();
    let config = MaruzzellaConfig::new("com.lelloman.ronomepo")
        .with_persistence_id("ronomepo")
        .with_theme(theme.clone())
        .with_product(product)
        .with_builtin_plugin(embedded_ronomepo_plugin);

    let application = build_application(config);
    application.connect_startup(move |_| {
        install_app_css(&theme);
    });
    let argv0 = env::args()
        .next()
        .unwrap_or_else(|| "ronomepo-app".to_string());
    application.run_with_args(&[argv0]);
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
        || raw.contains("\"id\": \"palette\"")
        || raw.contains("\"id\": \"theme\"")
        || raw.contains("\"id\": \"views\"")
        || raw.contains("\"id\": \"about\"")
        || raw.contains("\"id\": \"settings\"")
        || raw.contains("\"id\": \"new-buffer\"")
        || raw.contains("\"id\": \"save-buffer\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.clone_missing\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.push_force\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.apply_hooks\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.check_history\"")
        || raw.contains("\"command_id\": \"ronomepo.workspace.line_stats\"")
        || raw.contains("\"command_id\": \"shell.open_command_palette\"")
        || raw.contains("\"command_id\": \"shell.reload_theme\"")
        || raw.contains("\"command_id\": \"shell.browse_views\"")
        || raw.contains("\"command_id\": \"shell.about\"")
        || raw.contains("\"command_id\": \"shell.settings\"")
        || raw.contains("\"command_id\": \"shell.new_buffer\"")
        || raw.contains("\"command_id\": \"shell.save_buffer\"")
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

fn app_theme() -> ThemeSpec {
    let mut theme = ThemeSpec::default();
    theme.typography.font_family = "\"Space Grotesk\", \"Noto Sans\", sans-serif".to_string();
    theme.typography.mono_font_family = "\"JetBrains Mono\", monospace".to_string();
    theme.palette.bg_0 = "#13161c".to_string();
    theme.palette.bg_1 = "#1a1f27".to_string();
    theme.palette.workbench = "#0f1318".to_string();
    theme.palette.panel_left = "#121821".to_string();
    theme.palette.panel_right = "#11161e".to_string();
    theme.palette.panel_bottom = "#0d1117".to_string();
    theme.palette.border = "#293241".to_string();
    theme.palette.border_strong = "#415168".to_string();
    theme.palette.text_0 = "#e7edf7".to_string();
    theme.palette.text_1 = "#b4c0d0".to_string();
    theme.palette.text_2 = "#7f8b9e".to_string();
    theme.palette.accent = "#df6d3d".to_string();
    theme.palette.accent_strong = "#ff8e5f".to_string();
    theme.density.radius_medium = 8;
    theme.density.radius_large = 12;
    theme.density.toolbar_height = 38;
    theme.density.tab_height = 28;

    // Toolbar – action buttons match the neutral style, no special fill
    theme = theme
        .with_override("color_accent_action_bg", "transparent")
        .with_override("color_accent_action_text", "#b4c0d0")
        .with_override("color_accent_action_hover", "alpha(#b4c0d0, 0.10)")
        // Tab strip – flat, no rounding, neutral underline for active
        .with_override("color_workbench_tab_bg", "transparent")
        .with_override("color_workbench_tab_text", "#6b7889")
        .with_override("color_workbench_tab_hover", "alpha(#b4c0d0, 0.06)")
        .with_override("color_workbench_tab_hover_text", "#b4c0d0")
        .with_override("color_workbench_tab_active", "transparent")
        .with_override("workbench_tab_border_width", "2px")
        .with_override("color_notebook_tab_bg", "transparent")
        .with_override("color_notebook_tab_text", "#6b7889")
        .with_override("color_notebook_tab_hover", "alpha(#b4c0d0, 0.06)")
        .with_override("color_notebook_tab_hover_text", "#b4c0d0")
        .with_override("color_notebook_tab_active", "transparent")
        .with_override("color_notebook_tab_active_border", "#546378")
        .with_override("notebook_tab_active_border_width", "2px")
        .with_override("color_tab_strip_scroller_bg", "#0f1318")
        .with_override("tab_strip_scroller_border", "1px solid alpha(#293241, 0.4)")
        // Tighter button dimensions
        .with_override("control_height_button", "30px")
        .with_override("space_button_inline", "12px")
        .with_override("button_radius", "2px")
        .with_override("tab_radius", "0")
        .with_override("search_radius", "3px")
        .with_override("search_border", "0")
        .with_override("color_search_bg", "#1e232b")
        .with_override("color_entry_bg", "#1e232b");

    theme
}

fn install_app_css(theme: &ThemeSpec) {
    let Some(display) = gdk::Display::default() else {
        return;
    };

    let css = format!(
        "
        label.repo-state-clean {{
            color: {success};
        }}

        label.repo-state-warn {{
            color: {warning};
        }}

        label.repo-state-error {{
            color: {error};
        }}

        .topbar-masthead {{
            background: {menu_strip};
        }}

        .topbar-masthead .menu-bar {{
            background: transparent;
        }}

        .studio-toolbar button,
        .studio-toolbar .toolbar-button,
        .studio-toolbar .toolbar-icon-button,
        .toolbar-actions button,
        .toolbar-utility-group button {{
            border: 0;
            background: transparent;
            box-shadow: none;
        }}

        .studio-toolbar button:hover,
        .studio-toolbar .toolbar-button:hover,
        .studio-toolbar .toolbar-icon-button:hover {{
            background: alpha(#b4c0d0, 0.10);
            color: #e7edf7;
        }}

        .studio-toolbar button:active,
        .studio-toolbar .toolbar-button:active,
        .studio-toolbar .toolbar-icon-button:active {{
            background: alpha(#b4c0d0, 0.16);
        }}

        .toolbar-actions .toolbar-button-label,
        .toolbar-actions button .toolbar-button-label {{
            color: #b4c0d0;
        }}

        .toolbar-actions button:hover .toolbar-button-label {{
            color: #e7edf7;
        }}

        .workbench-tab-strip > .tab-header,
        .drag-preview,
        notebook header tab {{
            border-radius: 0;
            margin: 0;
        }}
        ",
        success = "#7fdc8a",
        warning = theme.palette.accent_strong,
        error = "#ff6b6b",
        menu_strip = "#161b22",
    );

    let provider = CssProvider::new();
    provider.load_from_data(&css);
    style_context_add_provider_for_display(
        &display,
        &provider,
        STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}
