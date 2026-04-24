# Repo Manifest Specification

This document defines the version `1` contract for `ronomepo.repo.json`.

The machine-readable schema lives at [repo-manifest.schema.json](./repo-manifest.schema.json). This document defines the semantics, defaults, and invariants that repositories should follow when authoring manifests.

## Purpose

`ronomepo.repo.json` is a repo-local capability manifest.

It answers these questions in a standardized way:

- what typed items exist in this repository
- which standardized actions Ronomepo can run
- which artifacts the repository produces
- how repo-level actions are aggregated when multiple items participate
- where built-in behavior stops and custom commands begin

The manifest is intended to be readable and writable by both humans and agents.

## File Location

The manifest file name is:

- `ronomepo.repo.json`

It must live at the repository root.

## Top-Level Object

The manifest root is a JSON object with these fields:

- `schema_version`: required integer, must be `1`
- `repo_id`: optional string, stable logical identifier for the repository
- `items`: required array of item definitions
- `repo_actions`: optional array of repo-level action commands
- `aggregation`: optional array of repo-level aggregation rules

Unknown top-level properties are not allowed.

## Standardized Actions

Ronomepo reserves these action names:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`
- `deploy`

These names are the stable public contract. Repositories do not invent new action names inside the manifest.

## Items

An `item` is a typed work unit inside the repository.

Required item fields:

- `id`: stable unique identifier within the repository
- `type`: Ronomepo handler type
- `path`: path to the item root

Optional item fields:

- `config`: handler-specific configuration payload
- `artifacts`: explicit artifact declarations
- `actions`: explicit per-item action overrides

Item invariants:

- `id` must be unique within the manifest
- `type` must be non-empty
- `path` is resolved relative to the repository root unless absolute

Current built-in item types:

- `cargo`
- `gradle`
- `gradle_android`
- `python`
- `node`

Unknown item types are allowed by the schema, but Ronomepo only provides built-in behavior for the known types above. Unknown types require explicit commands for any action that should be executable.

## Action Commands

An action command is used in either:

- `items[].actions`
- `repo_actions`

Fields:

- `action`: one standardized action name
- `command`: argv array, first element is the program
- `workdir`: optional working directory
- `env`: optional string-to-string environment map
- `timeout_seconds`: optional positive integer timeout
- `output`: optional output mode

Output modes:

- `text`
- `json`
- `json_lines`

Defaults:

- `env` defaults to an empty object
- `output` defaults to `text`
- `workdir` defaults to the item root for item actions and the repo root for repo actions

Invariants:

- `command` must contain at least one non-empty string
- a given scope must not declare the same `action` more than once
- item-level action overrides take precedence over built-in handler behavior
- repo-level actions take precedence over aggregation-based repo planning

## Artifacts

Artifacts are declared under `items[].artifacts`.

Fields:

- `name`: stable artifact name within the item
- `kind`: artifact class such as `binary`, `archive`, or `package`
- `path`: optional concrete path
- `pattern`: optional glob-like pattern
- `build_action`: optional standardized action that produces the artifact

Invariants:

- each artifact must declare at least one of `path` or `pattern`
- artifact `path` and `pattern` are resolved relative to the item root unless absolute

Artifacts can be:

- explicitly declared in the manifest
- provided implicitly by Ronomepo built-in handlers

Ronomepo merges both sources when listing artifacts.

## Aggregation

Aggregation rules define repo-level behavior for standardized actions when multiple items participate.

Fields:

- `action`: one standardized action name
- `item_ids`: ordered list of participating item ids
- `execution`: `sequential` or `parallel`
- `failure_policy`: `fail_fast` or `continue`
- `merge`: `combined` or `per_item`

Defaults:

- `execution` defaults to `sequential`
- `failure_policy` defaults to `fail_fast`
- `merge` defaults to `combined`

Invariants:

- `item_ids` must be non-empty
- every `item_id` must reference an existing item
- a manifest must not declare more than one aggregation rule for the same action

Repo-level planning rules:

- if a matching `repo_actions` command exists, Ronomepo uses it directly
- if only one applicable item exists for an action, Ronomepo may plan directly against that item
- if more than one applicable item exists, the manifest must declare an aggregation rule for that action
- Ronomepo must not guess multi-item repo-level behavior

## Built-In Handler Semantics

Built-in handlers expose default behavior for known item types.

### `cargo`

Built-in actions:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`

Current defaults:

- build/test/clean use `cargo` with `--manifest-path`
- artifact listing includes `target/debug/*` and `target/release/*`
- dependency freshness checks for `Cargo.lock`

### `gradle`

Built-in actions:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`

Current defaults:

- build/test/clean use `./gradlew`
- artifact listing includes `build/libs/*` and `build/distributions/*`
- dependency freshness checks for `gradle.lockfile` or `gradle/libs.versions.toml`

### `gradle_android`

Built-in actions:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`

Current defaults:

- build uses `./gradlew assemble`
- test uses `./gradlew test`
- clean uses `./gradlew clean`
- artifact listing includes `build/outputs/**/*`
- dependency freshness checks for `gradle.lockfile` or `gradle/libs.versions.toml`

### `python`

Built-in actions:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`
- `deploy`

Current defaults:

- build uses `python -m build`
- test uses `python -m pytest`
- clean uses a Python cleanup command for common build/test directories
- deploy uses `python -m twine upload dist/*`
- artifact listing includes `dist/*`
- dependency freshness checks for one of:
  - `uv.lock`
  - `poetry.lock`
  - `requirements.txt`
  - `requirements-dev.txt`

### `node`

Built-in actions:

- `list_artifacts`
- `verify_dependencies_freshness`
- `build` when a matching package script exists
- `test` when a matching package script exists
- `clean` when a matching package script exists
- `deploy` when a matching package script exists

Current defaults:

- package manager defaults to `npm`
- default script names are:
  - `build`
  - `test`
  - `clean`
  - `deploy`
- `config.package_manager` may be `npm`, `pnpm`, or `yarn`
- `config.scripts` may override the script name used for a standardized action
- artifact listing includes `dist/*` and `build/*`
- dependency freshness checks for one of:
  - `package-lock.json`
  - `npm-shrinkwrap.json`
  - `pnpm-lock.yaml`
  - `yarn.lock`
  - `bun.lockb`

These handler semantics are versioned by Ronomepo code, not by the JSON Schema alone. Repositories that need different behavior should declare explicit action overrides.

## Validation Model

Validation happens in two layers:

1. JSON Schema validation
2. Ronomepo semantic validation

JSON Schema validates shape, required fields, enums, and basic value constraints.

Ronomepo semantic validation additionally enforces:

- supported `schema_version`
- unique item ids
- no duplicate action commands within the same scope
- no duplicate aggregation rules per action
- aggregation references only known item ids
- repo-level multi-item actions require explicit aggregation

## Example

```json
{
  "schema_version": 1,
  "repo_id": "sample-product",
  "items": [
    {
      "id": "desktop-app",
      "type": "cargo",
      "path": ".",
      "artifacts": [
        {
          "name": "desktop-binary",
          "kind": "binary",
          "path": "target/release/sample-product",
          "build_action": "build"
        }
      ]
    },
    {
      "id": "python-tools",
      "type": "python",
      "path": "tools",
      "actions": [
        {
          "action": "test",
          "command": ["tox", "-q"],
          "timeout_seconds": 120
        }
      ]
    }
  ],
  "repo_actions": [
    {
      "action": "deploy",
      "command": ["./scripts/deploy.sh", "--prod"],
      "workdir": "."
    }
  ],
  "aggregation": [
    {
      "action": "test",
      "item_ids": ["desktop-app", "python-tools"],
      "execution": "parallel",
      "failure_policy": "continue",
      "merge": "combined"
    },
    {
      "action": "list_artifacts",
      "item_ids": ["desktop-app", "python-tools"],
      "execution": "sequential",
      "failure_policy": "fail_fast",
      "merge": "combined"
    }
  ]
}
```
