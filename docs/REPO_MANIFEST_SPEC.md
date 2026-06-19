# Repo Manifest Specification

This document defines the version `2` contract for `ronomepo.repo.json`. Ronomepo still accepts version `1` manifests for compatibility, but repo-defined actions and capabilities require `schema_version: 2`.

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

- `schema_version`: required integer, `1` or `2`; use `2` for actions and capabilities
- `repo_id`: optional string, stable logical identifier for the repository
- `items`: required array of item definitions
- `actions`: optional array of schema v2 repo-defined action nodes
- `capabilities`: optional array of schema v2 capability declarations
- `repo_actions`: optional legacy array of repo-level action commands for standardized actions
- `aggregation`: optional array of repo-level aggregation rules

Unknown top-level properties are not allowed.

Schema v1 compatibility rules:

- `schema_version: 1` may use `items`, `repo_actions`, and `aggregation`
- `actions` and `capabilities` require `schema_version: 2`
- Ronomepo scans both versions, but new repositories should use version `2`

## Standardized Actions

Ronomepo reserves these action names:

- `list_artifacts`
- `build`
- `test`
- `clean`
- `verify_dependencies_freshness`
- `deploy`

These names are the stable public contract. Repositories do not invent new names for legacy `items[].actions`, `repo_actions`, or `aggregation`. Schema v2 `actions[]` may use repo-local path-like ids because those are addressable action nodes, not standardized action names.

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

## Addressable Repo Nodes

Schema v2 introduces repo-local node ids. Node ids are path-like strings without a leading slash. They are Ronomepo addresses, not filesystem paths.

Examples:

- `build`
- `server`
- `server/build`
- `server/dependencies/outdated`

Node ids must not be empty, start or end with `/`, contain empty path segments, or contain `.` / `..` segments. Item ids and repo-defined action ids share one namespace and must not collide. Capability ids are stable capability instance ids; they identify capability state records in the registry.

## Repo-Defined Actions

Schema v2 `actions[]` defines executable action nodes that can be run directly or referenced by capabilities.

Fields:

- `id`: stable repo-local node id
- `command`: argv array, first element is the program
- `workdir`: optional working directory relative to the repository root unless absolute
- `env`: optional string-to-string environment map
- `timeout_seconds`: optional positive integer timeout
- `output`: optional output mode, defaulting to `text`

Repo-defined actions are for concrete execution mechanics. They do not carry capability policy by themselves.

## Capabilities

Schema v2 `capabilities[]` declares what operational contracts a repo exposes. A capability declaration is an instance, so a repository may declare multiple `integrity.build`, `integrity.tests`, or `dependencies.outdated` capabilities for different items or roots.

Fields:

- `id`: stable capability instance id inside the repo
- `capability`: catalog capability id, such as `integrity.build` or `dependencies.outdated`
- `status`: `implemented`, `not_applicable`, or `unsupported`
- `item_id`: optional item this capability applies to
- `root`: optional project root, relative to the repository root unless absolute
- `action_ref`: optional action reference used when `status` is `implemented`
- `standard_action`: optional shorthand for a standard action reference
- `reason`: required when `status` is `not_applicable`
- `schedule`: optional future scheduling policy payload; currently stored but not interpreted

Implemented capabilities must declare exactly one of `action_ref` or `standard_action`. `not_applicable` satisfies required capability policy only when it includes a reason. `unsupported` is valid syntax but remains a policy issue because the repo explicitly says the required automation is not available.

Built-in required capability catalog for item types `cargo`, `node`, `python`, `gradle`, and `gradle_android`:

- `integrity.build`
- `integrity.test_build`
- `integrity.tests`
- `dependencies.outdated`

Optional built-in capability:

- `observability.production_logs`

Custom capability ids are allowed so repo manifests can evolve before Ronomepo has a built-in catalog entry. Custom capabilities are not required by default.

## Action References

Capabilities reference executable actions through one of these forms.

Standard action reference:

```json
{ "kind": "standard", "name": "cargo.test", "item_id": "server" }
```

Repo-defined action reference:

```json
{ "kind": "repo_action", "id": "server/dependencies/outdated" }
```

Group reference:

```json
{
  "kind": "group",
  "execution": "parallel",
  "failure_policy": "fail_fast",
  "merge": "combined",
  "items": [
    { "kind": "standard", "name": "cargo.build", "item_id": "server" },
    { "kind": "standard", "name": "gradle_android.build", "item_id": "android" }
  ]
}
```

Supported standard action reference names are `<item-type>.<action>`, where item type is one of `cargo`, `node`, `python`, `gradle`, or `gradle_android`. Supported action suffixes are `list_artifacts`, `build`, `test`, `tests`, `clean`, `verify_dependencies_freshness`, `dependencies_outdated`, and `deploy`.

## Capability Results And Registry

Actions referenced by capabilities may emit JSON using the minimum result schema:

```json
{
  "status": "ok",
  "summary": "Tests passed",
  "findings": []
}
```

Valid result statuses are `ok`, `warning`, `failed`, `error`, `unknown`, `running`, and `stale`. A finding must include `severity` and `message`; optional fields include `id`, `file`, `line`, `url`, and `suggested_action`.

Ronomepo stores latest capability state in `.ronomepo/capability-state.json` under the workspace root. The registry stores capability-dependent observed state only. The repo manifest and Git state remain observed from their source files/worktrees.

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
  "schema_version": 2,
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
  "actions": [
    {
      "id": "python-tools/dependencies/outdated",
      "command": ["python", "scripts/check_outdated_dependencies.py"],
      "workdir": "tools",
      "output": "json",
      "timeout_seconds": 120
    }
  ],
  "capabilities": [
    {
      "id": "desktop-app/build",
      "capability": "integrity.build",
      "status": "implemented",
      "item_id": "desktop-app",
      "standard_action": "cargo.build"
    },
    {
      "id": "desktop-app/test-build",
      "capability": "integrity.test_build",
      "status": "implemented",
      "item_id": "desktop-app",
      "standard_action": "cargo.test"
    },
    {
      "id": "desktop-app/tests",
      "capability": "integrity.tests",
      "status": "implemented",
      "item_id": "desktop-app",
      "standard_action": "cargo.test"
    },
    {
      "id": "desktop-app/dependencies/outdated",
      "capability": "dependencies.outdated",
      "status": "implemented",
      "item_id": "desktop-app",
      "standard_action": "cargo.dependencies_outdated"
    },
    {
      "id": "python-tools/build",
      "capability": "integrity.build",
      "status": "not_applicable",
      "item_id": "python-tools",
      "reason": "The tools package is script-only and has no build step."
    },
    {
      "id": "python-tools/test-build",
      "capability": "integrity.test_build",
      "status": "not_applicable",
      "item_id": "python-tools",
      "reason": "Python tests are interpreted and do not have a separate compile step."
    },
    {
      "id": "python-tools/tests",
      "capability": "integrity.tests",
      "status": "implemented",
      "item_id": "python-tools",
      "standard_action": "python.test"
    },
    {
      "id": "python-tools/dependencies/outdated-capability",
      "capability": "dependencies.outdated",
      "status": "implemented",
      "item_id": "python-tools",
      "action_ref": {
        "kind": "repo_action",
        "id": "python-tools/dependencies/outdated"
      }
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
