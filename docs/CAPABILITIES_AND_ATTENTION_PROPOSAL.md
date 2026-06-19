# Capabilities, Registry, And Attention ProposalThis document captures the proposed direction for expanding Ronomepo from a
Git-state monitor into a broader repository operations hub.

It started as a design note. The first implementation slice is now reflected in
`REPO_MANIFEST_SPEC.md`: schema v2 repo-defined actions, capability declarations,
capability policy assessment, and a JSON capability-state registry. Scheduling,
custom reactions, and richer attention modelling remain future work.

## Motivation

Ronomepo currently knows how to monitor workspace-level Git state: missing
repositories, dirty worktrees, ahead/behind state, detached heads, and sync
status.

The next step is to monitor the internal health and operational surface of each
managed repository. Examples include:

- outdated dependencies
- broken builds
- broken test builds
- failing tests
- missing required repo automation
- future production or operations signals
- future planning and active-work signals

The goal is to abstract repetitive single-repo maintenance into a consistent
workspace-level model. Ronomepo should be able to answer:

- what can this repo do?
- what must every repo declare?
- which capabilities are implemented, missing, or explicitly not applicable?
- what has recently been checked?
- what needs attention?
- what automated follow-up could be triggered?

## Core Model

The proposed flow is:

```text
Capability Catalog
  -> Repo Action Definitions
  -> Repo Capability Declarations
  -> Scheduled Runs / Observations
  -> Findings
  -> Attention Signals
  -> Optional Reactions
```

The important separation is:

- actions describe what can be executed
- capabilities describe what a repo can expose or answer and which actions satisfy
  that contract
- the registry stores capability-dependent observed state and latest results
- attention signals summarize opportunities, problems, and urgent work
- reactions define optional future automation in response to states

## Capability Catalog

Capabilities are defined by Ronomepo, not invented independently by each repo.
The catalog is versioned centrally and represents the known operational surface
Ronomepo can understand.

Capabilities are broader than integrity checks. Integrity is one capability
family, but future families may include production monitoring, release
operations, planning, security, documentation, or support tooling.

Example catalog shape:

```json
{
  "schema_version": 1,
  "capabilities": [
    {
      "id": "integrity.build",
      "title": "Build",
      "family": "integrity",
      "requirement": {
        "level": "required",
        "scope": "item",
        "applies_to": {
          "item_types": ["cargo", "node", "python", "gradle", "gradle_android"]
        }
      },
      "kind": "check",
      "result_schema": "integrity_result_v1"
    },
    {
      "id": "integrity.test_build",
      "title": "Test Build",
      "family": "integrity",
      "requirement": {
        "level": "required",
        "scope": "item",
        "applies_to": {
          "item_types": ["cargo", "node", "python", "gradle", "gradle_android"]
        }
      },
      "kind": "check",
      "result_schema": "integrity_result_v1"
    },
    {
      "id": "integrity.tests",
      "title": "Tests",
      "family": "integrity",
      "requirement": {
        "level": "required",
        "scope": "item",
        "applies_to": {
          "item_types": ["cargo", "node", "python", "gradle", "gradle_android"]
        }
      },
      "kind": "check",
      "result_schema": "integrity_result_v1"
    },
    {
      "id": "dependencies.outdated",
      "title": "Outdated Dependencies",
      "family": "maintenance",
      "requirement": {
        "level": "required",
        "scope": "item",
        "applies_to": {
          "item_types": ["cargo", "node", "python", "gradle", "gradle_android"]
        }
      },
      "kind": "check",
      "result_schema": "findings_result_v1"
    },
    {
      "id": "observability.production_logs",
      "title": "Production Logs",
      "family": "operations",
      "requirement": {
        "level": "optional",
        "scope": "repo"
      },
      "kind": "query",
      "result_schema": "log_stream_v1"
    }
  ]
}
```

### Requirement Metadata

Capabilities should carry requirement metadata in the built-in catalog. This
metadata tells Ronomepo whether a capability is required, recommended, or
optional, and which manifest scopes it applies to.

Suggested requirement fields:

- `level`: `required`, `recommended`, or `optional`
- `scope`: `repo` or `item`
- `applies_to`: optional condition object, such as matching item types

Conditional requirements should support item-type matching. For example,
`dependencies.outdated` can be required for `cargo`, `node`, `python`, `gradle`,
and `gradle_android` items without being required for every possible repo node.

Validation should be mechanical:

```text
for each catalog capability:
  find eligible repo or item scopes from requirement metadata
  for each eligible scope:
    require implemented or not_applicable
    otherwise emit a missing capability issue
```

`not_applicable` must be declared at the same scope where the requirement
applies. If a capability is required for each Cargo item, the not-applicable
declaration must point at the relevant Cargo item, not only the whole repo.

### Required Capabilities

Ronomepo should be strict about required capabilities.

For every required capability, each repo must declare one of these statuses for
each relevant repo, item, or project scope:

- `implemented`
- `not_applicable`
- `unsupported`

An absent required capability is a Ronomepo-level issue.

`not_applicable` is valid, but must include a reason. For example, a simple
Python script with no third-party dependencies may declare dependency updates as
not applicable.

`unsupported` remains visible as an issue or limitation, but it is better than
silence because the repo has explicitly described its state. Ronomepo should not
provide a soft "deferred" status that becomes a free pass for avoiding required
automation. If a required capability is not implemented and is not truly
not-applicable, it should remain an issue.

## Standard Ronomepo Actions

Ronomepo should provide standard actions for common repo shapes.

The repo should not need to repeat boilerplate for standard projects such as:

- standard Cargo project
- standard Node project
- standard Python project
- standard Gradle project
- standard Gradle Android project

For example, a Cargo project should be able to satisfy `dependencies.outdated`
by referencing Ronomepo standard action `cargo.dependencies_outdated` instead of
defining a custom command every time.

Possible capability declaration shape:

```json
{
  "id": "server/dependencies/outdated",
  "capability": "dependencies.outdated",
  "status": "implemented",
  "root": "server",
  "action_ref": {
    "kind": "standard",
    "name": "cargo.dependencies_outdated"
  }
}
```

Or, more compactly:

```json
{
  "id": "server/dependencies/outdated",
  "capability": "dependencies.outdated",
  "status": "implemented",
  "root": "server",
  "standard_action": "cargo.dependencies_outdated"
}
```

This keeps the repo manifest strict without making every repository verbose.

Standard actions should be part of the built-in Ronomepo catalog. Custom repo
actions should still be supported for repos that do not fit the standard
behavior.

Standard action references must include the project root they apply to, either
directly through `root` or indirectly through an item reference. This matters
because one Git repository may contain several code projects, such as an Android
app next to a Rust server.

## Addressable Repo Tree

Repo-level definitions should share one address namespace. The namespace uses
path-like ids without a leading slash. These are Ronomepo addresses, not
filesystem paths.

Examples inside one repo:

```text
build
server
server/build
server/test
server/dependencies/outdated
android
android/build
android/test
android/dependencies/outdated
```

A node id is unique within the repo manifest. If `server/build` is an action
node, another unrelated item or action cannot also claim `server/build`. This
lets Ronomepo treat projects, actions, action groups, and future operational
objects as addressable repo nodes. Capability declarations reference these node
ids and attach policy/state to them; they do not need to create a second node
when they map one-to-one to an action.

Actions are executable nodes in this tree. For example:

- `build`: repo-level build action
- `server`: project node
- `server/build`: server build action
- `server/test`: server test action

From the workspace level, Ronomepo can prefix the repo id to produce a global
address:

```text
repoA/build
repoA/server/build
repoB/android/dependencies/outdated
```

This gives Ronomepo one stable addressing model for workspace navigation, table
state, command palette entries, registry keys, attention signals, and future
automation.

## Repo Action Definitions

Repo action definitions are executable surfaces. They are the concrete operations
that a user, Ronomepo, or automation can run.

Actions can be used directly in the UI and can also satisfy capability
instances. A repo-defined action may wrap a command, script, provider query, or
other executable mechanism.

Example repo-defined action:

```json
{
  "id": "android/dependencies/outdated",
  "command": ["./scripts/check-android-outdated-dependencies.sh"],
  "workdir": "android",
  "output": "json",
  "timeout_seconds": 300
}
```

Standard Ronomepo actions do not need to be re-declared as repo actions. A
capability can reference standard actions directly by name.

## Repo Capability Declarations

The repo-local manifest should grow a capability declaration section. The exact
field name is open, but this proposal uses `capabilities`.

Capability declarations are instances, not just capability ids. A repo may
declare multiple instances of the same capability, each with a stable instance
`id`, a catalog capability name, and a root or item scope. This is required
because repo does not equal code project.

A capability instance does not define concrete execution by itself. It declares
the operational contract and points at executable actions. The referenced action
can be:

- a standard Ronomepo action, such as `cargo.test` or `cargo.dependencies_outdated`
- a repo-defined action declared elsewhere in the manifest
- a group of standard or repo-defined actions

Each implemented capability instance should declare:

- `id`: stable id for this capability instance inside the repo
- `capability`: catalog capability id, such as `integrity.build`
- `item_id` or `root`: the code project or directory this instance applies to
- `action_ref`: the standard action, repo action, or action group that fulfills it
  (or a shorthand such as `standard_action`)

A repository may therefore expose multiple `integrity.build` instances, multiple
`integrity.tests` instances, and multiple `dependencies.outdated` instances. For
example, one repo may have a Gradle Android app under `android` and a Cargo
server under `server`, each with separate build, test, and dependency capability
instances.

Required capability policy should account for this scope model. A required
capability is not just a single repo-wide checkbox; Ronomepo must know whether it
applies to each declared project/item, whether that scope is implemented, or
whether that scope is explicitly not applicable.

Example:

```json
{
  "schema_version": 1,
  "repo_id": "sample-product",
  "items": [
    {
      "id": "android-app",
      "type": "gradle_android",
      "path": "android"
    },
    {
      "id": "rust-server",
      "type": "cargo",
      "path": "server"
    }
  ],
  "actions": [
    {
      "id": "android/dependencies/outdated",
      "command": ["./scripts/check-android-outdated-dependencies.sh"],
      "workdir": "android",
      "output": "json"
    }
  ],
  "capabilities": [
    {
      "id": "android/build",
      "capability": "integrity.build",
      "status": "implemented",
      "item_id": "android-app",
      "action_ref": {
        "kind": "standard",
        "name": "gradle_android.build"
      },
      "schedule": {
        "policy": "on_change"
      }
    },
    {
      "id": "server/build",
      "capability": "integrity.build",
      "status": "implemented",
      "item_id": "rust-server",
      "action_ref": {
        "kind": "standard",
        "name": "cargo.build"
      },
      "schedule": {
        "policy": "on_change"
      }
    },
    {
      "id": "server/dependencies/outdated",
      "capability": "dependencies.outdated",
      "status": "implemented",
      "root": "server",
      "action_ref": {
        "kind": "standard",
        "name": "cargo.dependencies_outdated"
      }
    },
    {
      "id": "android/dependencies/outdated-capability",
      "capability": "dependencies.outdated",
      "status": "implemented",
      "root": "android",
      "action_ref": {
        "kind": "repo_action",
        "id": "android/dependencies/outdated"
      }
    }
  ]
}
```

A capability may reference an existing standard action:

```json
{
  "id": "server/test-capability",
  "capability": "integrity.tests",
  "status": "implemented",
  "item_id": "rust-server",
  "action_ref": {
    "kind": "standard",
    "name": "cargo.test"
  }
}
```

Or it may reference a repo-defined action:

```json
{
  "id": "android/dependencies/outdated-capability",
  "capability": "dependencies.outdated",
  "status": "implemented",
  "root": "android",
  "action_ref": {
    "kind": "repo_action",
    "id": "android/dependencies/outdated"
  }
}
```

Or it may reference a group:

```json
{
  "id": "build",
  "capability": "integrity.build",
  "status": "implemented",
  "root": ".",
  "action_ref": {
    "kind": "group",
    "execution": "parallel",
    "items": [
      { "kind": "standard", "name": "gradle_android.build", "root": "android" },
      { "kind": "standard", "name": "cargo.build", "root": "server" }
    ]
  }
}
```

Future capability kinds, such as production logs, should still point at actions.
The concrete provider or command belongs to the action definition, not the
capability declaration.

## Capability Versus Action

Actions and capabilities should remain distinct. They are the two repo-level
primitives Ronomepo needs.

An action is executable. It is something the user, Ronomepo, or automation can
run manually or programmatically, such as:

- build this item
- run these tests
- check dependency versions
- clean artifacts
- deploy
- open production logs

A capability is a product or operations contract. It says that a repo can answer
an operational question or participate in a required management surface, such as:

- can this repo build?
- can this repo run tests?
- can this repo report outdated dependencies?
- can this repo expose production logs?

A capability does not define concrete commands, providers, or execution details.
It references actions. The referenced action may be standard, repo-defined, or a
group. This keeps policy and state separate from execution mechanics.

Required policy, `not_applicable`, scheduling, result interpretation, registry
state, and attention derivation belong to capabilities. Commands, providers,
working directories, environment variables, and direct user invocation belong to
actions.

## Integrity Capabilities

The first useful capability family should be `integrity`.

Initial candidates:

- `integrity.build`: verifies the main build succeeds
- `integrity.test_build`: verifies tests compile or can be prepared
- `integrity.tests`: verifies tests pass
- `dependencies.outdated`: reports dependency versions that need attention

Ronomepo already has enough action machinery for build and test checks in many
repos. The dependency case should be one capability: `dependencies.outdated`. It
answers whether newer dependency versions are available and should not be split
into metadata-presence or lockfile-consistency checks for this proposal. Because
"outdated" is ecosystem-specific and often repo-specific, it needs a clear
action result contract.

## Result Contract

Actions referenced by capabilities should produce a small, stable result format when possible.

The minimum result schema is:

```json
{
  "status": "ok",
  "summary": "Tests passed",
  "findings": []
}
```

Required top-level fields:

- `status`: `ok`, `warning`, `failed`, or `error`
- `summary`: short human-readable explanation
- `findings`: list of concrete findings, empty when there are none

The minimum finding schema is:

```json
{
  "severity": "warning",
  "message": "Dependency serde is outdated"
}
```

Optional finding fields may include:

- `id`
- `file`
- `line`
- `url`
- `suggested_action`

This is enough for Ronomepo to store capability state, show a table summary,
derive attention signals, and distinguish a real finding from a check execution
error.

Basic successful result:

```json
{
  "status": "ok",
  "summary": "All dependencies are current",
  "findings": []
}
```

Result with findings:

```json
{
  "status": "warning",
  "summary": "3 outdated dependencies",
  "findings": [
    {
      "id": "serde",
      "severity": "warning",
      "message": "serde 1.0.203 available, current 1.0.197",
      "file": "Cargo.toml"
    }
  ]
}
```

Suggested result statuses:

- `ok`: check ran and found no issue
- `warning`: check ran and found non-urgent findings
- `failed`: check ran and found a real integrity problem
- `error`: check crashed, timed out, or returned invalid output
- `unknown`: no result is known
- `running`: check is currently running
- `stale`: previous result no longer matches the current repo state

`failed` and `error` should remain distinct. A failing test is a repo integrity
problem. A crashed test command or invalid result payload is an execution
problem.

## Repo State Model

The state of a repository should be composed from three sources:

- repo-local manifest state: what the repo states about itself through
  `ronomepo.repo.json`
- Git state: missing, dirty, clean, ahead, behind, diverged, detached, and other
  Git signals Ronomepo already monitors
- capability-dependent state: observed state produced by capability instances,
  such as build state, test state, dependency state, or future operations state

The repo-local manifest and Git state are observed directly from files and Git.
They should not be owned by the registry.

The registry should only store capability-dependent state. For example, if a
repo declares a `build` capability instance, Ronomepo should maintain a build
state for that instance. Some capability states require periodic or triggered
checks before the overall repo state is complete.

The repository table and detail views should read a composed repo state built
from all three sources, not from the registry alone.

## Registry

The registry is Ronomepo-owned runtime state for capability instances. It should
not live inside each subrepo, and it should not duplicate manifest state or Git
state.

The first implementation should use an in-memory struct synced to a
workspace-local JSON file under `.ronomepo/`. That is enough for the current
scale of operations.

The registry should store:

- latest capability state per repo, item/root, and capability instance
- scheduled run state
- findings emitted by capability runs

The registry should be state-oriented. It should answer:

- what is the latest known state for this capability instance?
- is the state fresh or stale?
- is a check currently running?
- when was the state last updated?
- what findings came from the latest run?

Example registry entry:

```json
{
  "repo_id": "sample-product",
  "item_id": "rust-server",
  "root": "server",
  "capability": "integrity.tests",
  "capability_instance_id": "server/test-capability",
  "status": "ok",
  "summary": "Tests passed",
  "checked_at": "2026-06-17T10:00:00Z",
  "commit": "abc123",
  "duration_ms": 18420
}
```

Results should be tied to the commit or working-tree state they checked. If the
repo changes, old results should become stale.

## Attention Model

Ronomepo should expose a generic "needs attention" model.

Git state, failed checks, outdated dependencies, missing required capabilities,
production errors, and future planning work should all normalize into attention
signals.

The table should consume attention summaries, not raw check output. Raw output
belongs in detail views and logs.

Example attention signal:

```json
{
  "id": "sample-product:server/dependencies/outdated",
  "repo_id": "sample-product",
  "source": "capability",
  "source_id": "dependencies.outdated",
  "capability_instance_id": "server/dependencies/outdated",
  "root": "server",
  "type": "maintenance",
  "status": "open",
  "level": "should_do",
  "urgency": "medium",
  "impact": "medium",
  "summary": "4 dependencies are outdated",
  "suggested_actions": ["update_dependencies"],
  "observed_at": "2026-06-17T10:00:00Z",
  "stale_after": "2026-06-18T10:00:00Z"
}
```

Suggested attention levels:

- `info`: useful context
- `opportunity`: work that may be worth doing
- `should_do`: normal maintenance or quality work
- `urgent`: prompt action is needed
- `blocked`: repo cannot be trusted or operated normally

Suggested attention types:

- `git`
- `integrity`
- `maintenance`
- `security`
- `operations`
- `planning`
- `configuration`

This lets the UI show a compact "Needs Attention" column while preserving
detail about urgency, impact, and work type.

## Scheduling

Scheduling must be flexible and overrideable.

Configuration layers should be considered in this order:

1. Ronomepo global defaults
2. capability catalog defaults
3. workspace overrides
4. repo overrides
5. item or capability declaration overrides

Example schedule:

```json
{
  "schedule": {
    "policy": "interval",
    "interval": "1d",
    "jitter": "30m",
    "run_when": ["idle", "online"],
    "max_duration_seconds": 300,
    "automatic": true
  }
}
```

Candidate scheduling policies:

- `manual`
- `on_change`
- `interval`
- `daily`
- `weekly`
- `on_startup`
- `on_attention_state`

The first implementation can be conservative. For example:

- support `manual`
- support `on_change` for cheap/local checks
- support `interval` or `daily` for dependency checks
- store enough schedule metadata to avoid blocking future automation

## Reactions

Future Ronomepo versions should support custom reactions in response to certain
states.

Example future use case:

- `dependencies.outdated` flags a repo
- Ronomepo creates an attention signal
- a configured reaction starts an AI coding agent
- the agent updates dependencies and opens a PR

Reactions should be configured separately from capabilities. Capabilities say
what can be observed or done. Reactions say what Ronomepo may do when a state is
observed.

Example reaction shape:

```json
{
  "triggers": [
    {
      "id": "auto-update-deps",
      "when": {
        "attention_type": "maintenance",
        "source_id": "dependencies.outdated",
        "level_at_least": "should_do"
      },
      "action": {
        "kind": "agent_task",
        "template": "update_dependencies_and_open_pr"
      },
      "approval": "required"
    }
  ]
}
```

Approval policy should be explicit. Some reactions may be safe to run
automatically, but agent-driven code changes and PR creation should likely
require approval at first.

## UI Direction

The repository table should show capability and attention state alongside Git
state.

Possible columns:

- Git
- Sync
- Capabilities
- Integrity
- Needs Attention

Examples:

- `Capabilities: OK`
- `Capabilities: Missing 2`
- `Integrity: OK`
- `Integrity: Tests Failed`
- `Needs Attention: Deps`
- `Needs Attention: Urgent`

The repo detail view should show:

- declared capability instances
- required capabilities that are missing
- not-applicable capabilities and reasons
- latest run result for each capability instance
- raw findings
- schedule status
- next run time
- stale result warnings

## Design Decisions

Current decisions from the design discussion:

- Actions and capabilities are separate repo-level primitives. Actions are
  executable; capabilities declare operational contracts and reference actions.
- Repo-local definitions use one path-like address namespace without a leading
  slash. Actions are addressable nodes in that tree.
- Existing `items[].actions` and `repo_actions` should be unified conceptually
  as scoped action nodes in the addressable repo tree.
- Actions and capabilities should live directly in `ronomepo.repo.json`,
  alongside existing item definitions.
- The core capability catalog and standard action catalog should be built into
  Ronomepo.
- The model should leave room for custom actions and custom capabilities, but
  the core catalogs should not be workspace-specific.
- Required capability policy should be global initially. The abstraction should
  still make future workspace-specific policy possible without major rewrites.
- Required capability policy is defined by catalog requirement metadata, including
  requirement level, scope, and conditional applicability such as item type.
- Capability declarations are instances. A repo may declare multiple instances
  of the same catalog capability, scoped by `item_id` or `root`.
- Standard action references inside capability declarations must identify the
  project root they apply to.
- Workspace-level addresses are formed by prefixing the repo id to the
  repo-local address, such as `repoA/build` or `repoA/server/build`.
- The minimum capability result schema is `status`, `summary`, and `findings`.
- Dependency update checking is one capability, `dependencies.outdated`; this
  proposal does not split metadata presence or lockfile consistency into separate
  capabilities.
- Repo state should be composed from repo-local manifest state, Git state, and
  capability-dependent state.
- The registry should only own capability-dependent state, not manifest state,
  Git state, or derived attention state.
- The first registry implementation should use an in-memory struct synced to a
  workspace-local JSON file under `.ronomepo/`.

## Open Questions

- How should result staleness be calculated for dirty worktrees?
- Which checks are safe to run automatically by default?
- How should Ronomepo prevent expensive scheduled checks from disrupting local
  development?

## First Implementation Slice

A practical first slice could be:

1. Add action and capability catalog types in core.
2. Add repo action definitions and capability instance declarations to the repo
   manifest.
3. Validate required capabilities as implemented, not applicable, or
   unsupported.
4. Add an in-memory capability-state registry synced to workspace-local JSON.
5. Implement capability result records for capabilities backed by:
   - build actions
   - test actions
   - outdated dependency actions
6. Derive basic attention signals from:
   - missing required capabilities
   - failed builds
   - failing tests
   - outdated dependencies
   - stale results
7. Add table summaries for capabilities, integrity, and attention.

The first slice should avoid full reaction automation. It only needs to produce
trustworthy capability state and attention signals.
