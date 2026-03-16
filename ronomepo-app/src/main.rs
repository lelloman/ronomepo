use std::path::PathBuf;

use maruzzella::{
    default_product_spec, plugin_tab, run, text_tab, BottomPanelLayout, MaruzzellaConfig,
    TabGroupSpec, ThemeSpec, WorkbenchNodeSpec,
};

fn main() {
    let plugin_path = plugin_path();
    if !plugin_path.exists() {
        eprintln!(
            "Ronomepo plugin not found at {}\nBuild it first with: cargo build -p ronomepo-plugin",
            plugin_path.display()
        );
        return;
    }

    let mut product = default_product_spec();
    product.branding.title = "Ronomepo".to_string();
    product.branding.search_placeholder = "Search repositories".to_string();
    product.branding.status_text =
        "Desktop workspace for many sibling Git repositories".to_string();

    product.layout.bottom_panel_layout = BottomPanelLayout::CenterOnly;
    product.layout.left_panel = TabGroupSpec::new(
        "panel-left",
        Some("workspace"),
        vec![text_tab(
            "workspace",
            "panel-left",
            "Workspace",
            "Workspace path, filters, and import guidance will live here.",
            false,
        )],
    );
    product.layout.right_panel = TabGroupSpec::new(
        "panel-right",
        Some("details"),
        vec![text_tab(
            "details",
            "panel-right",
            "Details",
            "Repository details and selection-aware inspectors will be added here.",
            false,
        )],
    );
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
    );
    product.layout.workbench = WorkbenchNodeSpec::Group(TabGroupSpec::new(
        "workbench-main",
        Some("repositories"),
        vec![plugin_tab(
            "repositories",
            "workbench-main",
            "Repositories",
            "com.lelloman.ronomepo.repositories",
            "The Ronomepo repositories view could not be created.",
            false,
        )],
    ));

    let config = MaruzzellaConfig::new("com.lelloman.ronomepo")
        .with_persistence_id("ronomepo")
        .with_theme(app_theme())
        .with_product(product)
        .with_plugin_path(plugin_path);

    run(config);
}

fn plugin_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("target");
    path.push("debug");
    path.push(format!(
        "{}ronomepo_plugin{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    path
}

fn app_theme() -> ThemeSpec {
    let mut theme = ThemeSpec::default();
    theme.typography.font_family = "\"IBM Plex Sans\", \"Noto Sans\", sans-serif".to_string();
    theme.typography.mono_font_family = "\"JetBrains Mono\", monospace".to_string();
    theme.palette.bg_0 = "#f1efe7".to_string();
    theme.palette.bg_1 = "#e5dfd1".to_string();
    theme.palette.workbench = "#f7f5ed".to_string();
    theme.palette.panel_left = "#ebe4d4".to_string();
    theme.palette.panel_right = "#e9e1cf".to_string();
    theme.palette.panel_bottom = "#e0d7c5".to_string();
    theme.palette.border = "#b6ad99".to_string();
    theme.palette.border_strong = "#8a7d68".to_string();
    theme.palette.text_0 = "#2f281f".to_string();
    theme.palette.text_1 = "#5e5447".to_string();
    theme.palette.text_2 = "#7f7568".to_string();
    theme.palette.accent = "#256d5a".to_string();
    theme.palette.accent_strong = "#174c3f".to_string();
    theme.density.radius_medium = 10;
    theme.density.radius_large = 14;
    theme.density.toolbar_height = 42;
    theme.density.tab_height = 30;
    theme
}

