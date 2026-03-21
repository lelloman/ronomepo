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
    CommandSpec, MaruzzellaConfig, MenuRootSpec, TabGroupSpec, ThemeSpec, ToolbarItemSpec,
    WorkbenchNodeSpec,
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
    product.branding.status_text =
        "Desktop workspace for many sibling Git repositories".to_string();
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
            id: "help".to_string(),
            label: "Help".to_string(),
        },
    ];
    product.menu_items.clear();
    product.commands = vec![
        CommandSpec {
            id: "ronomepo.workspace.refresh".to_string(),
            title: "Refresh Workspace".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.clone_missing".to_string(),
            title: "Clone Missing".to_string(),
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
            id: "ronomepo.workspace.push_force".to_string(),
            title: "Push Force".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.apply_hooks".to_string(),
            title: "Apply Hooks".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.open_overview".to_string(),
            title: "Monorepo Overview".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.check_history".to_string(),
            title: "Check History".to_string(),
        },
        CommandSpec {
            id: "ronomepo.workspace.line_stats".to_string(),
            title: "Line Stats".to_string(),
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
            id: "clone-missing".to_string(),
            icon_name: Some("folder-download-symbolic".to_string()),
            label: Some("Clone Missing".to_string()),
            command_id: "ronomepo.workspace.clone_missing".to_string(),
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
            id: "apply-hooks".to_string(),
            icon_name: Some("emblem-synchronizing-symbolic".to_string()),
            label: Some("Apply Hooks".to_string()),
            command_id: "ronomepo.workspace.apply_hooks".to_string(),
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
    );
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
        vec![plugin_tab(
            "monorepo-overview",
            "workbench-main",
            "Monorepo Overview",
            "com.lelloman.ronomepo.monorepo_overview",
            "The Ronomepo monorepo overview could not be created.",
            false,
        )],
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
        ",
        success = "#7fdc8a",
        warning = theme.palette.accent_strong,
        error = "#ff6b6b",
    );

    let provider = CssProvider::new();
    provider.load_from_data(&css);
    style_context_add_provider_for_display(
        &display,
        &provider,
        STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}
