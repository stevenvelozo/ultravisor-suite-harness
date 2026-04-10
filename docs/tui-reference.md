# TUI Reference

The interactive terminal UI is a blessed screen driven by a Pict application. It has five views plus a persistent status footer. All navigation is keyboard-driven.

## Global Keybindings

These keys work from any view:

| Key | Action |
|---|---|
| `1` | Clean & Execute |
| `2` | Run Suite |
| `3` | View Results |
| `4` | Dataset Picker |
| `5` | Server Log |
| `m` | Return to Main Menu |
| `l` | Refresh log from the current view's perspective |
| `q` | Quit (gracefully stops servers) |
| `Ctrl+C` | Force quit (still triggers graceful shutdown via SIGINT handler) |

## View: Main Menu

The landing view. Shows:

- The three child servers and their health status (Facto, Integration, Ultravisor)
- The currently selected dataset preset and its dataset count
- The most recent run's pass / fail counts, if any
- The global keybinding cheatsheet

Status indicators:

| Indicator | Meaning |
|---|---|
| `*` green | Server is healthy |
| `o` yellow | Server is starting or health-checking |
| `[ ]` red | Server failed to start or crashed |

When every server is green, you can press `1` or `2` to run. Until then, those keys are no-ops and the menu stays on this view.

## View: Suite Runner

Shown automatically when `1` (Clean & Execute) or `2` (Run Suite) is pressed. Replaces the main menu with a live progress pane.

Layout:

- **Header** -- current preset, dataset count, elapsed time
- **Progress list** -- one row per dataset, updated in place as each stage completes
- **Status line** -- the currently-running stage for the in-progress dataset

Each row cycles through states:

```
[ ] datahub-country-codes              queued
[~] datahub-country-codes              scanning...
[~] datahub-country-codes              parsing (142 rows)...
[~] datahub-country-codes              loading...
[~] datahub-country-codes              verifying...
[[x]] datahub-country-codes              PASS  parsed=248 loaded=248 verified=248
```

A failing dataset shows `[[ ]]` with the failing stage and error text. The runner does not abort on a single failure -- it continues through the full preset and reports everything at the end.

When the run completes, the view does **not** automatically jump to the results view. You stay in the runner so you can scroll through the final progress log. Press `3` to see the summary table or `m` to return to the main menu.

## View: Results

The pass / fail summary table for the most recent run, read from `HarnessTestResult` in `./data/harness.db`.

Columns:

| Column | Description |
|---|---|
| Dataset | Dataset key |
| Status | `PASS` or `FAIL` |
| Parsed | Row count emitted by the parser |
| Loaded | Row count reported by the bulk-create beacon |
| Verified | Row count returned by the independent `SELECT COUNT(*)` |
| Error | Truncated error text when Status is `FAIL` |

A run where every row is `PASS` and `parsed == loaded == verified` is a clean pass. Any other state is a failure, and the Error column explains why.

Pressing `m` returns to the main menu. Pressing `5` jumps to the captured log for deeper inspection.

## View: Dataset Picker

Lets you choose which datasets the next run will operate on.

Keybindings specific to this view:

| Key | Action |
|---|---|
| `s` | Select Small preset |
| `M` | Select Medium preset |
| `L` | Select Large preset |
| `↑` / `↓` | Move the cursor through the dataset list |
| `space` | Toggle the dataset under the cursor |
| `a` | Select all |
| `n` | Select none |
| `m` | Save the current selection and return to the main menu |

The picker shows every key registered in `DATASET_REGISTRY`, with checkmarks next to currently-selected entries. Changes are applied immediately to the application's `selectedDatasets` state so the next `1` or `2` keypress uses the new selection.

Selection survives across runs within a single TUI session. It is not persisted to disk -- quitting and relaunching resets to the default preset.

## View: Log

The captured stdout / stderr from all three child servers plus the harness itself.

The log is a ring buffer of the most recent 2000 lines (set in `harness.js`). Lines are timestamped and prefixed with the source process so you can tell Facto output from Integration output at a glance. The view opens scrolled to the bottom; `PgUp` / `PgDn` and arrow keys scroll through history, and pressing `l` refreshes to the newest line.

This view is the single most useful debugging tool in the harness. When a dataset fails, switch here first -- the underlying exception almost always shows up in the captured log before it shows up in the Results table.

## Footer

A one-line status bar is always visible across the bottom of the screen showing:

- Current view name
- Elapsed time since harness startup
- Server status summary (compact `F:* I:* U:*`)
- Hint text for the most relevant keybindings in the current view

## State Management

View state lives on the `HarnessApplication` instance under `this.uiState`. Key fields:

| Field | Purpose |
|---|---|
| `currentView` | Name of the view currently rendered |
| `currentPreset` | `'small' \| 'medium' \| 'large' \| 'custom'` |
| `selectedDatasets` | Array of dataset keys for the next run |
| `runInProgress` | True while the orchestrator is running; blocks `1` / `2` keypresses |
| `lastRunId` | ID of the most recent `HarnessTestRun` row; used by `View-Results` |
| `serverStatus` | `{ facto, integration, ultravisor }` health indicators |

None of this state is persisted across restarts.

## Blessed / Pict Integration Notes

The TUI is built with `pict-terminalui`, which is a blessed wrapper that mounts blessed boxes as Pict views. Each view is a Pict view subclass with:

- A `render()` method that writes into its assigned blessed box
- A `handleKey(key)` method that responds to the global key router
- A `solve()` method that refreshes whatever AppData it depends on before rendering

This means every view can use the rest of the Pict ecosystem (templates, solvers, service injection) while still participating in blessed's render loop. The main menu, suite runner, results, dataset picker, and log views are all ordinary Pict views with blessed-flavored destinations.
