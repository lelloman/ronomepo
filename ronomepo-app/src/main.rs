use std::env;
use std::fs;
use std::path::PathBuf;

use gtk::gio::prelude::ApplicationExtManual;
use maruzzella::{
    build_application, default_product_spec, load_static_plugin, plugin_tab, BottomPanelLayout,
    ButtonAppearance, ButtonStyle, CommandSpec, InputAppearance, MaruzzellaConfig, MenuItemSpec,
    MenuRootSpec, PanelResizePolicy, SurfaceAppearance, SurfaceLevel, TabGroupSpec,
    TabStripAppearance, TabStripStyle, TextAppearance, TextRole, ThemeSpec, Tone,
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
            appearance_id: "ronomepo-toolbar-primary".to_string(),
        },
        ToolbarItemSpec {
            id: "pull".to_string(),
            icon_name: Some("go-down-symbolic".to_string()),
            label: Some("Pull".to_string()),
            command_id: "ronomepo.workspace.pull".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ronomepo-toolbar-primary".to_string(),
        },
        ToolbarItemSpec {
            id: "push".to_string(),
            icon_name: Some("go-up-symbolic".to_string()),
            label: Some("Push".to_string()),
            command_id: "ronomepo.workspace.push".to_string(),
            payload: Vec::new(),
            secondary: false,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ronomepo-toolbar-primary".to_string(),
        },
        ToolbarItemSpec {
            id: "monorepo-overview".to_string(),
            icon_name: Some("view-grid-symbolic".to_string()),
            label: Some("Monorepo Overview".to_string()),
            command_id: "ronomepo.workspace.open_overview".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ronomepo-toolbar-primary".to_string(),
        },
        ToolbarItemSpec {
            id: "commit-check".to_string(),
            icon_name: Some("dialog-warning-symbolic".to_string()),
            label: Some("Commit Check".to_string()),
            command_id: "ronomepo.workspace.open_commit_check".to_string(),
            payload: Vec::new(),
            secondary: true,
            display_mode: ToolbarDisplayMode::IconOnly,
            appearance_id: "ronomepo-toolbar-primary".to_string(),
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
    .with_panel_appearance("ronomepo-side-panel")
    .with_panel_header_appearance("ronomepo-panel-header")
    .with_tab_strip_appearance("ronomepo-side-tabs")
    .with_text_appearance("body");
    product.layout.right_panel = TabGroupSpec::new("panel-right", None, Vec::new())
        .with_panel_appearance("ronomepo-side-panel")
        .with_panel_header_appearance("ronomepo-panel-header")
        .with_tab_strip_appearance("ronomepo-side-tabs")
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
    .with_panel_appearance("ronomepo-bottom-panel")
    .with_panel_header_appearance("ronomepo-panel-header")
    .with_tab_strip_appearance("ronomepo-console-tabs")
    .with_text_appearance("body");
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
        .with_panel_appearance("ronomepo-workbench")
        .with_panel_header_appearance("ronomepo-workbench-header")
        .with_tab_strip_appearance("ronomepo-editor-tabs")
        .with_text_appearance("body"),
    );

    let config = MaruzzellaConfig::new("com.lelloman.ronomepo")
        .with_persistence_id("ronomepo")
        .with_theme(app_theme())
        .with_product(product)
        .with_builtin_plugin(embedded_ronomepo_plugin);

    let application = build_application(config);
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
    theme.density.min_side_panel_width = 200;
    theme.density.min_bottom_panel_height = 200;

    theme = theme
        .with_surface_appearance(
            "app-shell",
            SurfaceAppearance::new(Tone::Neutral, SurfaceLevel::Sunken, TextRole::Body)
                .borderless(),
        )
        .with_surface_appearance(
            "topbar",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Raised, TextRole::BodyStrong),
        )
        .with_surface_appearance(
            "menu",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Flat, TextRole::BodyStrong)
                .borderless(),
        )
        .with_surface_appearance(
            "toolbar",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Flat, TextRole::Body),
        )
        .with_surface_appearance(
            "status",
            SurfaceAppearance::new(Tone::Secondary, SurfaceLevel::Raised, TextRole::Meta),
        )
        .with_surface_appearance(
            "ronomepo-side-panel",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Raised, TextRole::Body),
        )
        .with_surface_appearance(
            "ronomepo-panel-header",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Flat, TextRole::SectionLabel),
        )
        .with_surface_appearance(
            "ronomepo-bottom-panel",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Raised, TextRole::Body),
        )
        .with_surface_appearance(
            "ronomepo-workbench",
            SurfaceAppearance::new(Tone::Neutral, SurfaceLevel::Sunken, TextRole::Body)
                .borderless(),
        )
        .with_surface_appearance(
            "ronomepo-workbench-header",
            SurfaceAppearance::new(Tone::Primary, SurfaceLevel::Flat, TextRole::TabLabel),
        )
        .with_button_appearance(
            "primary",
            ButtonAppearance::new(Tone::Accent, ButtonStyle::Solid, TextRole::BodyStrong),
        )
        .with_button_appearance(
            "secondary",
            ButtonAppearance::new(Tone::Primary, ButtonStyle::Soft, TextRole::Body),
        )
        .with_button_appearance(
            "ghost",
            ButtonAppearance::new(Tone::Neutral, ButtonStyle::Ghost, TextRole::Body),
        )
        .with_button_appearance(
            "ronomepo-toolbar-primary",
            ButtonAppearance::new(Tone::Accent, ButtonStyle::Ghost, TextRole::BodyStrong),
        )
        .with_button_appearance(
            "ronomepo-toolbar-ghost",
            ButtonAppearance::new(Tone::Primary, ButtonStyle::Ghost, TextRole::Body),
        )
        .with_text_appearance(
            "title",
            TextAppearance {
                role: TextRole::Title,
                tone: Tone::Primary,
            },
        )
        .with_text_appearance(
            "subtitle",
            TextAppearance {
                role: TextRole::Subtitle,
                tone: Tone::Secondary,
            },
        )
        .with_text_appearance(
            "body",
            TextAppearance {
                role: TextRole::Body,
                tone: Tone::Primary,
            },
        )
        .with_text_appearance(
            "meta",
            TextAppearance {
                role: TextRole::Meta,
                tone: Tone::Neutral,
            },
        )
        .with_text_appearance(
            "code",
            TextAppearance {
                role: TextRole::Code,
                tone: Tone::Primary,
            },
        )
        .with_input_appearance(
            "search",
            InputAppearance::new(Tone::Secondary, SurfaceLevel::Sunken, TextRole::Body),
        )
        .with_tab_strip_appearance(
            "ronomepo-side-tabs",
            TabStripAppearance::new(Tone::Primary, TabStripStyle::Utility, TextRole::TabLabel),
        )
        .with_tab_strip_appearance(
            "ronomepo-editor-tabs",
            TabStripAppearance::new(Tone::Neutral, TabStripStyle::Editor, TextRole::TabLabel),
        )
        .with_tab_strip_appearance(
            "ronomepo-console-tabs",
            TabStripAppearance::new(Tone::Primary, TabStripStyle::Console, TextRole::TabLabel),
        )
        // Keep token overrides for stateful shell details not fully expressed by appearance ids.
        .with_override("color_separator_fill", "#323232")
        .with_override("separator_alpha", "1.0")
        .with_override("separator_size", "1px")
        .with_override("dense_row_selected_bg", "#2d5c88")
        .with_override("dense_row_selected_text", "#ffffff")
        .with_override("dense_row_hover_bg", "alpha(#2d5c88, 0.3)");

    theme
}
