use std::env;
use std::fs;
use std::path::PathBuf;

use gtk::gdk;
use gtk::gio::prelude::ApplicationExtManual;
use gtk::prelude::ApplicationExt;
use gtk::{
    style_context_add_provider_for_display, CssProvider, STYLE_PROVIDER_PRIORITY_USER,
};
use maruzzella::{
    build_application, default_product_spec, load_static_plugin, plugin_tab, BottomPanelLayout,
    CommandSpec, MaruzzellaConfig, MenuItemSpec, MenuRootSpec, TabGroupSpec, ThemeSpec,
    ToolbarDisplayMode, ToolbarItemSpec, WorkbenchNodeSpec,
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
            display_mode: ToolbarDisplayMode::IconOnly,
        },
        ToolbarItemSpec {
            id: "pull".to_string(),
            icon_name: Some("go-down-symbolic".to_string()),
            label: Some("Pull".to_string()),
            command_id: "ronomepo.workspace.pull".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
        },
        ToolbarItemSpec {
            id: "push".to_string(),
            icon_name: Some("go-up-symbolic".to_string()),
            label: Some("Push".to_string()),
            command_id: "ronomepo.workspace.push".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
        },
        ToolbarItemSpec {
            id: "monorepo-overview".to_string(),
            icon_name: Some("view-grid-symbolic".to_string()),
            label: Some("Monorepo Overview".to_string()),
            command_id: "ronomepo.workspace.open_overview".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
        },
        ToolbarItemSpec {
            id: "commit-check".to_string(),
            icon_name: Some("dialog-warning-symbolic".to_string()),
            label: Some("Commit Check".to_string()),
            command_id: "ronomepo.workspace.open_commit_check".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
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

    // Typography – match IntelliJ/AS system font feel
    theme.typography.font_family = "\"Inter\", \"Noto Sans\", sans-serif".to_string();
    theme.typography.mono_font_family = "\"JetBrains Mono\", monospace".to_string();
    theme.typography.font_size_base = 13;
    theme.typography.font_size_ui = 12;
    theme.typography.font_size_small = 11;
    theme.typography.font_size_tiny = 10;

    // Palette – Darcula-inspired warm dark grays
    theme.palette.bg_0 = "#3c3f41".to_string();
    theme.palette.bg_1 = "#45494a".to_string();
    theme.palette.workbench = "#2b2b2b".to_string();
    theme.palette.panel_left = "#3c3f41".to_string();
    theme.palette.panel_right = "#3c3f41".to_string();
    theme.palette.panel_bottom = "#3c3f41".to_string();
    theme.palette.border = "#323232".to_string();
    theme.palette.border_strong = "#515151".to_string();
    theme.palette.text_0 = "#bbbbbb".to_string();
    theme.palette.text_1 = "#a9b7c6".to_string();
    theme.palette.text_2 = "#787878".to_string();
    theme.palette.accent = "#4b6eaf".to_string();
    theme.palette.accent_strong = "#589df6".to_string();

    // Density – compact like Android Studio
    theme.density.radius_none = 0;
    theme.density.radius_small = 2;
    theme.density.radius_medium = 3;
    theme.density.radius_large = 4;
    theme.density.radius_pill = 3;
    theme.density.toolbar_height = 30;
    theme.density.tab_height = 26;
    theme.density.space_xs = 2;
    theme.density.space_sm = 4;
    theme.density.space_md = 4;
    theme.density.space_lg = 6;
    theme.density.space_xl = 8;
    theme.density.panel_header_height = 26;
    theme.density.icon_size = 16;

    // Overrides for specific tokens
    theme = theme
        // Toolbar
        .with_override("color_toolbar_bg", "#3c3f41")
        .with_override("toolbar_bottom_border", "1px solid #323232")
        .with_override("color_accent_action_bg", "transparent")
        .with_override("color_accent_action_text", "#a9b7c6")
        .with_override("color_accent_action_hover", "alpha(#bbbbbb, 0.10)")
        .with_override("color_button_text", "#a9b7c6")
        .with_override("color_button_hover", "alpha(#bbbbbb, 0.08)")
        .with_override("color_button_active", "alpha(#bbbbbb, 0.14)")
        .with_override("control_height_button", "26px")
        .with_override("space_button_inline", "6px")
        .with_override("button_radius", "2px")
        .with_override("icon_button_width", "24px")
        .with_override("icon_button_height", "24px")
        .with_override("icon_button_padding", "2px")
        .with_override("icon_button_border", "0")
        .with_override("color_icon_button_hover", "alpha(#bbbbbb, 0.10)")
        .with_override("color_icon_button_hover_border", "transparent")
        // Menu bar
        .with_override("color_menu_bg", "#3c3f41")
        .with_override("color_menu_text", "#bbbbbb")
        .with_override("color_menu_hover", "alpha(#bbbbbb, 0.10)")
        .with_override("menu_bar_height", "24px")
        .with_override("control_height_small", "22px")
        .with_override("space_menu_button_inline", "8px")
        // Top bar
        .with_override("color_topbar", "#3c3f41")
        .with_override("topbar_border", "1px solid #323232")
        .with_override("window_strip_height", "26px")
        .with_override("window_strip_border", "1px solid #323232")
        // Search
        .with_override("search_radius", "2px")
        .with_override("search_border", "1px solid #515151")
        .with_override("color_search_bg", "#45494a")
        .with_override("search_focus_border", "1px solid #589df6")
        .with_override("color_search_focus_bg", "#45494a")
        .with_override("color_entry_bg", "#45494a")
        // Tabs – flat, no rounding
        .with_override("tab_radius", "0")
        .with_override("color_workbench_tab_bg", "transparent")
        .with_override("color_workbench_tab_text", "#787878")
        .with_override("color_workbench_tab_hover", "alpha(#bbbbbb, 0.06)")
        .with_override("color_workbench_tab_hover_text", "#a9b7c6")
        .with_override("color_workbench_tab_active", "transparent")
        .with_override("workbench_tab_border_width", "2px")
        .with_override("color_notebook_tab_bg", "transparent")
        .with_override("color_notebook_tab_text", "#787878")
        .with_override("color_notebook_tab_hover", "alpha(#bbbbbb, 0.06)")
        .with_override("color_notebook_tab_hover_text", "#a9b7c6")
        .with_override("color_notebook_tab_active", "transparent")
        .with_override("color_notebook_tab_active_border", "#4b6eaf")
        .with_override("notebook_tab_active_border_width", "2px")
        .with_override("color_tab_strip_scroller_bg", "#3c3f41")
        .with_override("tab_strip_scroller_border", "1px solid #323232")
        // Popover/menus
        .with_override("color_popover_bg", "#45494a")
        .with_override("popover_border", "1px solid #515151")
        .with_override("color_popover_button_text", "#bbbbbb")
        .with_override("color_popover_button_hover", "#4b6eaf")
        .with_override("color_popover_button_hover_text", "#ffffff")
        // Scrollbar
        .with_override("color_scrollbar_trough", "transparent")
        .with_override("color_scrollbar_slider", "alpha(#888888, 0.3)")
        .with_override("color_scrollbar_slider_hover", "alpha(#888888, 0.5)")
        // Status bar
        .with_override("color_status_bar_bg", "#3c3f41")
        .with_override("color_status_item", "#787878")
        .with_override("color_status_item_strong", "#a9b7c6")
        // Separators
        .with_override("color_separator_fill", "#323232")
        .with_override("separator_alpha", "1.0")
        .with_override("separator_size", "1px")
        // List selection – IntelliJ-style muted blue
        .with_override("dense_row_selected_bg", "#2d5c88")
        .with_override("dense_row_selected_text", "#ffffff")
        .with_override("dense_row_hover_bg", "alpha(#2d5c88, 0.3)");

    theme
}

fn install_app_css(_theme: &ThemeSpec) {
    let Some(display) = gdk::Display::default() else {
        return;
    };

    let css = "
        label.repo-state-clean {
            color: #6a8759;
        }

        label.repo-state-warn {
            color: #bbb529;
        }

        label.repo-state-error {
            color: #cc7832;
        }

        row,
        list row,
        listview row,
        .dense-row,
        .boxed-list row {
            margin: 0;
            margin-top: 0;
            margin-bottom: 0;
            padding: 0;
            border: 0;
            border-top: 0;
            border-bottom: 0;
        }

        .boxed-list {
            border: 0;
            border-radius: 0;
            box-shadow: none;
            background: transparent;
            border-spacing: 0;
        }

        list separator {
            min-height: 0;
            background: transparent;
        }

        row.repo-selected {
            background: #2d5c88;
        }

        row.repo-selected box,
        row.repo-selected label {
            background: transparent;
            color: #ffffff;
        }

        row:hover,
        list row:hover {
            background: alpha(#2d5c88, 0.3);
        }

        row:hover box,
        row:hover label {
            background: transparent;
        }

        .workbench-tab-strip > .tab-header,
        .drag-preview,
        notebook header tab {
            border-radius: 0;
            margin: 0;
        }
        ";

    let provider = CssProvider::new();
    provider.load_from_data(&css);
    style_context_add_provider_for_display(
        &display,
        &provider,
        STYLE_PROVIDER_PRIORITY_USER + 1,
    );
}
