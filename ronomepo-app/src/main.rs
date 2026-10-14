use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use gtk::gio::prelude::ApplicationExtManual;
use maruzzella::{
    build_application, default_product_spec, load_static_plugin, plugin_tab, BottomPanelLayout,
    CommandSpec, MaruzzellaConfig, MenuItemSpec, MenuRootSpec, PanelResizePolicy, TabGroupSpec,
    ToolbarDisplayMode, ToolbarItemSpec, WorkbenchNodeSpec,
};
use ronomepo_core::normalize_workspace_root;
use ronomepo_plugin::{persisted_config_root, PERSISTENCE_ID};

fn main() {
    let workspace_root = match parse_args(env::args_os().skip(1)) {
        Ok(CliCommand::Run(workspace_root)) => workspace_root,
        Ok(CliCommand::Help) => {
            print!("{USAGE}");
            return;
        }
        Ok(CliCommand::Version) => {
            println!("ronomepo {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Err(message) => exit_with_usage_error(&message),
    };
    let workspace_root =
        workspace_root.unwrap_or_else(|| env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    if !workspace_root.is_dir() {
        exit_with_usage_error(&format!(
            "workspace {} is not a directory",
            workspace_root.display()
        ));
    }
    let workspace_root = normalize_workspace_root(workspace_root);

    configure_gtk_input_method();
    clear_empty_xdg_dirs();
    reset_stale_persisted_layouts();
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
            id: "monorepo-fucina-policy".to_string(),
            root_id: "monorepo".to_string(),
            label: "Fucina Policy".to_string(),
            command_id: "ronomepo.workspace.open_fucina_policy".to_string(),
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
            id: "ronomepo.workspace.open_fucina_policy".to_string(),
            title: "Fucina Policy".to_string(),
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
    // The panels keep the default product's appearances; only their content
    // changes.
    product.layout.left_panel = TabGroupSpec {
        active_tab_id: Some("repositories".to_string()),
        tabs: vec![plugin_tab(
            "repositories",
            "panel-left",
            "Repositories",
            "com.lelloman.ronomepo.repo_monitor",
            "The Ronomepo repository monitor could not be created.",
            false,
        )],
        ..product.layout.left_panel
    }
    .with_tab_strip_hidden();
    product.layout.bottom_panel = TabGroupSpec {
        active_tab_id: Some("operations".to_string()),
        tabs: vec![plugin_tab(
            "operations",
            "panel-bottom",
            "Operations",
            "com.lelloman.ronomepo.operations",
            "The Ronomepo operations view could not be created.",
            false,
        )],
        ..product.layout.bottom_panel
    }
    .with_tab_strip_hidden();
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
        .with_tab_strip_appearance("editor"),
    );

    let config = MaruzzellaConfig::new("com.lelloman.ronomepo")
        .with_persistence_id(PERSISTENCE_ID)
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

/// The XDG base directory spec says an empty variable counts as unset, but
/// Maruzzella would treat an empty XDG_CONFIG_HOME as a relative path and put
/// configs and layouts under the current directory.
fn clear_empty_xdg_dirs() {
    for name in ["XDG_CONFIG_HOME", "XDG_STATE_HOME"] {
        if env::var_os(name).is_some_and(|value| value.is_empty()) {
            env::remove_var(name);
        }
    }
}

/// Removes stale layouts from the main layout slot and from every scoped slot
/// (`<persistence id>--<slot>`) Maruzzella keeps next to it.
fn reset_stale_persisted_layouts() {
    let slot_prefix = format!("{PERSISTENCE_ID}--");
    let Ok(entries) = fs::read_dir(persisted_config_root()) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == PERSISTENCE_ID || name.starts_with(&slot_prefix) {
            reset_stale_persisted_layout(&entry.path().join("layout.json"));
        }
    }
}

fn reset_stale_persisted_layout(path: &Path) {
    let Ok(raw) = fs::read_to_string(path) else {
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

const USAGE: &str = "\
Usage: ronomepo-app [--workspace <dir> | <dir>]

Opens the Ronomepo workspace in <dir> (default: the current directory).

Options:
  -w, --workspace <dir>  Workspace directory
  -h, --help             Print this help
  -V, --version          Print the version
";

#[derive(Debug, PartialEq, Eq)]
enum CliCommand {
    Run(Option<PathBuf>),
    Help,
    Version,
}

fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<CliCommand, String> {
    let mut args = args.into_iter();
    let mut workspace = None;
    let mut set_workspace = |path: PathBuf| {
        if path.as_os_str().is_empty() {
            return Err("the workspace path is empty".to_string());
        }
        if workspace.replace(path).is_some() {
            return Err("only one workspace can be given".to_string());
        }
        Ok(())
    };
    let mut options_done = false;

    while let Some(arg) = args.next() {
        if options_done {
            set_workspace(PathBuf::from(arg))?;
            continue;
        }
        let Some(text) = arg.to_str() else {
            set_workspace(PathBuf::from(arg))?;
            continue;
        };
        match text {
            "-h" | "--help" => return Ok(CliCommand::Help),
            "-V" | "--version" => return Ok(CliCommand::Version),
            "-w" | "--workspace" => {
                let value = args
                    .next()
                    .ok_or_else(|| format!("{text} requires a directory"))?;
                set_workspace(PathBuf::from(value))?;
            }
            "--" => options_done = true,
            _ => {
                if let Some(value) = text.strip_prefix("--workspace=") {
                    set_workspace(PathBuf::from(value))?;
                } else if text.starts_with('-') && text != "-" {
                    return Err(format!("unknown option {text}"));
                } else {
                    set_workspace(PathBuf::from(arg))?;
                }
            }
        }
    }

    Ok(CliCommand::Run(workspace))
}

fn exit_with_usage_error(message: &str) -> ! {
    eprint!("ronomepo-app: {message}\n\n{USAGE}");
    std::process::exit(2);
}

fn embedded_ronomepo_plugin() -> Result<maruzzella::LoadedPlugin, maruzzella::PluginLoadError> {
    load_static_plugin(
        "builtin:ronomepo-plugin",
        ronomepo_plugin::maruzzella_plugin_entry,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<CliCommand, String> {
        parse_args(args.iter().map(OsString::from))
    }

    #[test]
    fn workspace_can_be_given_as_option_or_positional() {
        let expected = Ok(CliCommand::Run(Some(PathBuf::from("/repos"))));
        assert_eq!(parse(&["--workspace", "/repos"]), expected);
        assert_eq!(parse(&["--workspace=/repos"]), expected);
        assert_eq!(parse(&["-w", "/repos"]), expected);
        assert_eq!(parse(&["/repos"]), expected);
        assert_eq!(parse(&["--", "/repos"]), expected);
        assert_eq!(parse(&[]), Ok(CliCommand::Run(None)));
    }

    #[test]
    fn bad_arguments_are_rejected_instead_of_becoming_the_workspace() {
        assert_eq!(parse(&["--help"]), Ok(CliCommand::Help));
        assert_eq!(parse(&["-V"]), Ok(CliCommand::Version));
        assert!(parse(&["--workspace"]).is_err());
        assert!(parse(&["--workspace="]).is_err());
        assert!(parse(&["--verbose"]).is_err());
        assert!(parse(&["/a", "/b"]).is_err());
        assert!(parse(&["--workspace", "/a", "/b"]).is_err());
        assert_eq!(
            parse(&["--", "--odd-dir"]),
            Ok(CliCommand::Run(Some(PathBuf::from("--odd-dir"))))
        );
    }
}
