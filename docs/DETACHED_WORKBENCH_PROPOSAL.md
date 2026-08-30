# Detached Workbenches

## Status

The design is accepted and implemented as a Maruzzella shell capability. Ronomepo
uses it through normal workbench tab specs and the Maruzzella ABI v2 host API; it
does not own a separate window-management layer.

## User Contract

- Only workbench tabs are detachable. Side and bottom panels stay in the main shell.
- A workbench tab menu offers `Move to New Window`.
- A detached tab menu also offers `Move Back to Main Window`.
- Every detached window is a full workbench with tabs and nested splits, but no
  duplicate top bar, menus, or side/bottom panels.
- Moving a tab performs clean teardown and recreation from `TabSpec`; live GTK
  objects are never reparented across windows.
- Closing a detached window merges its complete workbench tree into its recorded
  parent, falling back to the main workbench if the parent no longer exists.
- Closing the main window quits the whole application after an always-present
  confirmation dialog.
- Detached workbenches, their trees, active tabs, sizes, and maximized state are
  persisted and restored automatically.
- If a restored plugin view is unavailable, its tab remains with the normal
  missing-view placeholder.

## Surface Model

The main workbench has the stable surface id `main`. Each detach allocates a stable
id such as `detached-workbench-1` and persists:

```rust
struct DetachedWorkbenchSpec {
    id: String,
    workbench: WorkbenchNodeSpec,
    return_target: SurfaceReturnTarget,
    geometry: SurfaceWindowGeometry,
}
```

`SurfaceReturnTarget` records the source surface, group, and tab index. This lets
reattachment restore tab order and lets a detached-surface close graft a complete
split tree beside its source group. Empty groups and one-child splits are
normalized in the pure model before GTK is updated.

The main layout owns the detached-surface list and next id. Each detached surface
also has a layout file so its current split state can be saved without pretending
it is a full application shell.

## Host Routing and ABI v2

Each plugin view receives a host API whose `host_context` is bound to its creating
surface. ABI v2 adds contextual variants of `open_view`, `focus_view`,
`is_view_open`, and `update_view_title`.

Routing rules are:

1. View uniqueness is global. An existing view is focused and its owning window
   is presented.
2. A new workbench view requested by a view-local host opens in the caller's surface.
3. Non-workbench placements route to the main shell.
4. Calls without surface context route to the main shell.

ABI v1 plugins are rejected because the view-factory and host structures expanded.
Plugins built with the v2 SDK use contextual callbacks and default to `Ready`
lifecycle behavior when they do not need a guard.

## Teardown Lifecycle

ABI v2 factories may provide `prepare_teardown(request)`. Reasons are tab close,
detach, reattach, detached-surface close, and application quit. Responses are:

- `Ready`: proceed immediately
- `Confirm`: show the supplied reason and allow continue or cancel
- `Blocked`: explain why the operation cannot proceed

Maruzzella preflights every affected tab before changing specs or destroying GTK
objects. Application quit always asks even when every view is ready. The built-in
Maruzzella editor and Ronomepo text editor use the callback to protect unsaved
buffers; Ronomepo clears dirty state only when the exact saved content reaches disk.

## Test-Driven Implementation

The implementation was driven from model and contract tests before GTK integration:

- detach/move-back preserves original tab order
- closing a split surface grafts the tree and retargets descendants
- lifecycle defaults to `Ready` without a callback
- callback decisions and messages cross the ABI boundary
- Ronomepo's editor confirms only for dirty instances

Both complete workspaces must compile and pass their test suites before release.

## Deferred UX

- drag-out and cross-window drag gestures
- a `Move to Existing Window` chooser
- duplicating views instead of moving them
- detachable side or bottom panels
- cross-process workbenches
