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

The first useful workbench should expose:

- a repository list
- per-repository status details
- batch actions for clone, pull, push, and hook setup
- diagnostics for missing directories or failed Git commands
- workspace metadata loaded from a manifest file

## Legacy Mapping

The current `mono` script already defines the minimum viable feature set:

- `clone`: clone all repositories listed in `repos.txt`
- `pull`: pull clean repositories, skip dirty ones
- `push`: push repositories that are ahead, with protection around commit check rules
- `status`: compute branch, cleanliness, and ahead/behind state
- `setup-hooks`: apply shared hooks through `core.hooksPath`

Ronomepo should reach parity with these workflows before expanding into richer desktop-specific features.

## Proposed Workspace Model

Ronomepo should use a manifest file stored at the workspace root, likely something like `ronomepo.json`.

A first-pass schema should include:

- workspace name
- workspace root path
- shared hooks path
- repository entries
- per-repository id, display name, local directory, remote URL
- optional tags or groups
- optional enabled/disabled flag

The legacy `repos.txt` can be imported automatically during bootstrap.

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

This repository currently contains only planning work. The implementation will start from documentation, data model design, and a thin Maruzzella-hosted plugin skeleton.

See [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) for the phased delivery plan.
