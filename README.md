# Ronomepo

Ronomepo is a desktop app for managing a "monorepo": a directory that contains many sibling Git repositories plus a small amount of shared metadata.

This project replaces an existing setup based on:

- `repos.txt` as the source list of repositories
- `mono` as the command-line orchestrator
- a convention where each repository is cloned as a sibling directory

The first version keeps that operating model, but moves it into a desktop UI hosted inside [Maruzzella](../maruzzella) as a plugin.

## Problem

The current setup is effective but narrow:

- repository definitions live in a flat text file
- the `mono` script is the only interaction surface
- status and batch operations are terminal-oriented
- there is no structured place for richer metadata, grouping, notes, filters, or workspace-specific actions

Ronomepo turns that workflow into a persistent desktop application without changing the core idea: one workspace, many independent Git repositories.

## What "Monorepo" Means Here

This is not a single Git repository containing many packages.

In Ronomepo, a "monorepo" is:

- one root directory
- many child directories
- each child directory is its own Git repository
- one shared inventory describing which repositories belong to the workspace
- optional shared behavior, such as common hooks or batch operations

This sibling-repository workspace model is what Ronomepo is designed to replace.

## Goals

- Replace `repos.txt` with a structured workspace manifest.
- Replace the `mono` script with a desktop-first workflow.
- Preserve the current sibling-directory layout so migration is simple.
- Provide fast visibility into repository health: missing, clean, dirty, ahead, behind, detached, no upstream.
- Support batch actions such as clone, pull, push, fetch, hook setup, and opening a repository.
- Keep the app local-first and file-based.
- Build on top of Maruzzella instead of inventing a new shell.

## Non-Goals For The First Cut

- Rewriting Git plumbing
- Managing nested package workspaces inside a repository
- Acting as a hosted forge or code review tool
- Supporting arbitrary remote workspace backends
- Reorganizing existing repositories on disk

## Product Shape

Ronomepo will ship as a Maruzzella plugin-backed desktop app.

That means:

- Maruzzella provides the shell, panes, tabs, layout persistence, menus, and command dispatch.
- Ronomepo provides the domain model, repository scanning, Git operations, and views.
- The initial product can be developed as a focused plugin while still feeling like a standalone desktop tool.

The workbench currently exposes:

- a repository list with status, filters and sorting
- per-repository overview pages (Git state, repo manifest, capabilities)
- toolbar actions for Refresh, Pull, Push, Monorepo Overview and Commit Check
- per-repository actions (pull, push, push without commit checks, clone,
  apply hooks) from the repository context menu and the repository overview
- an Operations panel with the log of every Git operation
- workspace metadata loaded from a manifest file

## Legacy Mapping

The current `mono` script already defines the minimum viable feature set:

- `clone`: clone all repositories listed in `repos.txt`
- `pull`: pull clean repositories, skip dirty ones
- `push`: push repositories that are ahead, with protection around commit check rules
- `status`: compute branch, cleanliness, and ahead/behind state
- `setup-hooks`: apply shared hooks through `core.hooksPath`

Ronomepo should reach parity with these workflows before expanding into richer desktop-specific features.

## Workspace Model

The workspace inventory is `ronomepo.json` at the workspace root. It contains:

- `name`: workspace name
- `root`: workspace root path
- `repos`: repository entries with `id`, `name`, `dir_name` (directory
  under the root), `remote_url` and `enabled`
- `shared_hooks_path` (optional): directory applied as `core.hooksPath`;
  defaults to `<root>/hooks` when that directory exists
- `commit_check_rules` (optional): rules the push preflight evaluates
  against unpushed commits; seeded with defaults on first use

See [examples/sample-workspace/ronomepo.json](./examples/sample-workspace/ronomepo.json).
When no manifest exists, the legacy `repos.txt` in the workspace root can be
imported to bootstrap one.

Ronomepo also keeps derived state under `<root>/.ronomepo/` (for example
`capability-state.json` with the latest capability check results).

## Subrepo Capability Manifest

Managed repositories can now expose a repo-local Ronomepo manifest at the repository root:

- `ronomepo.repo.json`

This manifest is separate from the workspace inventory. It tells Ronomepo and AI agents how to navigate the repository through standardized actions.

Formal references:

- [Repo manifest specification](./docs/REPO_MANIFEST_SPEC.md)
- [JSON Schema](./docs/repo-manifest.schema.json)

Core ideas:

- a repository contains one or more `items`
- each item has a Ronomepo `type`, such as `cargo`, `gradle`, `gradle_android`, `python`, or `node`
- Ronomepo knows the built-in behavior for each supported type
- custom behavior can be declared with explicit action commands
- repo-level execution across multiple items must be declared explicitly

The standardized action model is:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`
- `deploy`

If a repository has multiple applicable items for the same action, repo-level execution must declare aggregation rules instead of relying on inference.

See [examples/sample-subrepo/ronomepo.repo.json](./examples/sample-subrepo/ronomepo.repo.json) for a concrete example.

## UI Direction

The UI should stay operational and dense rather than decorative.

Core surfaces:

- repository explorer tab
- repository detail tab
- operations/log tab
- workspace settings tab
- command palette actions exposed through Maruzzella

The most important view is a sortable, filterable repository table with compact Git state summaries and obvious batch actions.

### Fucina policy control surface

When the workspace contains `repos.toml` and `fucina_policy.py`, choose
**Monorepo → Fucina Policy** to inspect every repository's role, publication
policy, declared remotes, lifecycle state, and backup policy. Validation,
remote/ref audits, and configuration previews run through the policy engine.
Applying configuration always follows a successful preview and an explicit
confirmation; the engine remains responsible for every safety decision. The
policy table fills the workbench tab while detailed command output is kept in
the Operations bottom panel.

## Integration With Maruzzella

Ronomepo should use Maruzzella through its plugin runtime and shell API:

- register commands for workspace actions
- contribute menus for repo and workspace actions
- mount plugin-backed GTK views into workbench tabs
- persist plugin-owned configuration separately from shell layout

This is a good fit for Maruzzella's current state because it already supports:

- plugin loading and dependency resolution
- plugin command registration and dispatch
- plugin-backed GTK views
- shell layout persistence

## Local-First Approach

All core data should remain understandable on disk:

- workspace manifest for repository inventory
- optional operation logs or history
- shell layout persisted by Maruzzella
- plugin configuration persisted through Maruzzella's plugin config support

The app should still be scriptable later, but the desktop workflow is the primary product surface.

## Development Priorities

1. Workspace manifest and importer from `repos.txt`
2. Git status engine matching current `mono status` behavior
3. Desktop views for repository list and details
4. Batch operations with clear error reporting
5. Migration path from an existing `repos.txt` + `mono` workspace

## Current Status

The app is functional. The Cargo workspace is split into three crates:

- `ronomepo-core`: workspace and repo manifests, Git status scanning, Git
  operations, commit checks, capabilities and attention signals
- `ronomepo-plugin`: the Maruzzella plugin (state, background workers,
  repository watchers and GTK views)
- `ronomepo-app`: the desktop binary hosting the plugin

## Building And Running

The Cargo workspace patches the `maruzzella` crates to a sibling checkout
(`[patch.crates-io]` in `Cargo.toml`), so a Maruzzella checkout must exist at
`../maruzzella` next to this repository.

```sh
cargo run -p ronomepo-app -- --workspace ~/path/to/workspace
```

The workspace root is taken from `--workspace <path>`, `--workspace=<path>` or
the first positional argument, and defaults to the current directory. A
leading `~` is expanded.

Environment variables:

- `RONOMEPO_WORKSPACE_ROOT`: set by the app for the plugin from the
  argument above (when the plugin is hosted elsewhere it can be set directly)
- `RONOMEPO_USE_SYSTEM_INPUT_METHOD`: keep the system GTK input method; by
  default Ronomepo forces GTK's simple input context to avoid IBus stalls
- `RONOMEPO_PROFILE`: when set to anything other than `0`, log runtime
  profiling summaries (scheduler, watchers, in-flight jobs) to the
  diagnostics log
- `RONOMEPO_EDITOR`, then `VISUAL`, then `EDITOR`: editor used by
  "Open In Editor"

Git operations never prompt: credential prompts are disabled, ssh runs in
batch mode, and each clone, pull or push is cancelled after five minutes.

Files outside the workspace:

- `$XDG_CONFIG_HOME/ronomepo/` (default `~/.config/ronomepo/`): shell layout
  (`layout.json`) and plugin configuration (`plugins.json`)
- `$XDG_STATE_HOME/ronomepo/ronomepo.log` (default
  `~/.local/state/ronomepo/ronomepo.log`): diagnostics log, rotated to
  `ronomepo.log.1` at 10 MiB
