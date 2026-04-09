# Purpose

## The Problem

The Retold data pipeline is not a single module. It is a sequence of independently-versioned packages that cooperate to take an arbitrary file (CSV, JSON, TSV, fixed-width) and land its rows as queryable records in a warehouse:

| Layer | Module | Role |
|---|---|---|
| File I/O | `meadow-integration` | Auto-detect format, stream-parse, emit row objects |
| Mapping | `meadow-integration` | Apply `Mappings` + `GUIDTemplate` to derive entities |
| Transform | `meadow-integration` | Flatten nested objects, sanitize types |
| Workflow | `ultravisor` | Read an operation JSON, dispatch tasks in order |
| Dispatch | `ultravisor-beacon` | Invoke remote beacon capabilities over HTTP |
| Warehouse | `retold-facto` | Sources, Datasets, IngestJobs, Records, Projections |
| ORM | `meadow` | Schema, query DSL, bulk operations |
| Connection | `meadow-connection-sqlite` | SQLite driver (also: MySQL, MSSQL, Postgres) |
| HTTP | `orator` + `orator-serviceserver-restify` | REST endpoint framework |
| App | `pict` + `pict-application` | Dependency injection, service lifecycle |

Each of these modules has its own test suite. Those suites verify the module in isolation -- usually with mocked boundaries. Isolated tests are fast, deterministic, and essential, but they have a predictable blind spot: **cross-module regressions**.

A typical cross-module regression looks like this:

- `retold-facto` renames `createSource` to `CreateSource`. Its own tests pass because they call the new name.
- `ultravisor` still dispatches `beacon-factodata-createsource` (lowercase). Its own tests mock the beacon registry, so they pass.
- `meadow-integration` never touched `retold-facto` directly, so its tests pass.
- Every isolated suite is green. But a real pipeline run fails at the first dispatch.

This class of failure is invisible until somebody runs the whole thing end-to-end with a real file, a real warehouse, a real dispatch, and a real assertion against row count. **That is what this harness is for.**

## What Full-Suite Testing Means Here

"Full suite" has two orthogonal meanings inside this harness:

1. **Full software stack** -- every module listed in the table above is instantiated in-process. No mocks, no stubs, no fakes. The harness is a single Node process that hosts three Pict child applications (Facto, Meadow Integration, Ultravisor) on ports `8420-8422` and sends real HTTP between them.
2. **Full dataset suite** -- a curated collection of real public datasets covering the file formats, row counts, column shapes, and mapping styles that the pipeline is meant to handle. The Large preset includes 14+ datasets ranging from 150 rows (`iso-10383-mic-codes`) to 200,000+ rows (`tiger-relationship-files`).

A "full suite run" is the cross product: every dataset in the selected preset is driven through every module in the stack, and the `SELECT COUNT(*)` at the end must match the parser's emitted row count. Any deviation is a failure.

## What the Harness Protects Against

| Failure Mode | Caught By |
|---|---|
| Breaking API rename in any module | Real HTTP dispatch fails the first operation step |
| Silent schema change in Facto | `createdataset` rejects or `SELECT COUNT(*)` returns wrong type |
| Mapping regression in `meadow-integration` | Parsed count vs. verified count mismatch |
| Workflow JSON drift in `ultravisor` | Operation dispatch fails before bulk insert |
| Beacon protocol version skew | Beacon handshake fails during server startup |
| Connector regression in `meadow-connection-sqlite` | Bulk insert throws or returns wrong row count |
| Pipeline performance regression | Suite runtime exceeds expectations on a known dataset |
| File-format edge case (BOM, CRLF, stray commas) | Specific dataset fails at the parse step |
| Multi-entity mapping regression | Bookstore fixture fails to produce all three entity datasets |
| Port-binding / process-cleanup bugs | Harness fails to start, surfacing the issue immediately |

Any one of these would be expensive to catch in a downstream application. The harness catches them in under a minute on the small preset.

## What the Harness Is Not

- **Not a replacement for module unit tests.** Each module still owns its own Mocha TDD suite. Those run in milliseconds and pinpoint failures to a single file. The harness runs in seconds-to-minutes and pinpoints failures to a module boundary.
- **Not a performance benchmark.** It reports runtime, but there are no baselines or alerting. Use it to notice when something is wildly slower than usual, not to measure microsecond-level regressions.
- **Not a substitute for application acceptance tests.** Downstream applications still need their own tests against their own data. The harness confirms that the shared pipeline works; whether your application uses it correctly is still your responsibility.
- **Not a fixture generator.** Datasets in the Large preset must already exist in `modules/dist/facto-library/`. The harness does not download them.

## How It Is Expected to Be Used

The harness supports three workflows:

### 1. Pre-commit smoke test (local development)

Before committing a change to any pipeline module, run:

```bash
cd modules/apps/ultravisor-suite-harness
npm run test:small
```

This runs the 3-dataset small preset in headless mode. It takes ~30 seconds and catches the most common breakages (API renames, schema drift, obvious dispatch failures).

### 2. Pre-release validation (release engineer)

Before publishing a new version of any pipeline module, run:

```bash
npm run test:large
```

This runs all 14+ datasets. It takes several minutes and catches edge cases that only show up on longer / weirder / bigger files.

### 3. Bug reproduction (debugging)

When a downstream application reports a data-pipeline bug, open the TUI:

```bash
npm start
```

Press `4` to pick the dataset closest to the one that broke, then press `1` to run it clean. Watch `View-SuiteRunner` for the failing step. If the bug reproduces, switch to `View-Log` (`5`) to inspect the captured server output. If the fix is in the harness fixtures (e.g., a mapping), iterate locally; if it is in one of the modules, patch upstream and re-run.

### 4. Continuous integration

CI invokes `npm run test:medium` or `npm run test:large` on every PR and on the main branch after merge. Failures block the merge. The small preset is sometimes used for PRs that only touch documentation or non-pipeline modules.

## Why a Terminal UI?

The harness was originally a pure CLI. The blessed + Pict UI was added because:

- **Live feedback beats log-tailing.** Watching a progress pane update row-by-row is more informative than reading a 10,000-line log file after the fact.
- **Captured server stdout lives inside the same session.** Switching to `View-Log` (`5`) shows exactly what Facto / Integration / Ultravisor printed during the run, with no need to split terminals.
- **Dataset pickers replace memorized flags.** New contributors can discover presets and toggle them without reading `--help`.
- **Results are queryable.** The same `./data/harness.db` that the run wrote to is available from `View-Results` without a second tool.

The TUI is never required. Every feature is also accessible via CLI flags for automation, and the headless mode is deliberately kept in sync.
