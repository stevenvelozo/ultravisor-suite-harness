# Ultravisor Suite Harness

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

An end-to-end test harness for the [Ultravisor](https://github.com/stevenvelozo/ultravisor) / [Facto](https://github.com/stevenvelozo/retold-facto) / [Meadow Integration](https://github.com/stevenvelozo/meadow-integration) data pipeline. The harness boots a full in-process stack (Facto data warehouse, Meadow Integration parser, Ultravisor workflow engine) and drives real-world datasets from the `facto-library` through the `scan → parse → map → transform → load → verify` pipeline, then reports pass/fail per dataset.

The harness ships with a [blessed](https://github.com/chjj/blessed) terminal UI built on top of Pict for interactive development and a `--headless` mode suitable for CI.

## Features

- **Full-Stack In-Process** -- Boots Facto (`:8420`), Meadow Integration (`:8421`), and Ultravisor (`:8422`) as Pict child applications in the same Node process
- **Real Workload Testing** -- Drives ISO codes, IANA TLDs, RAL colors, BLS titles, IEEE OUI, OurAirports, Tiger Census, Project Gutenberg, and more through the full pipeline
- **Multi-Entity Extraction** -- Bookstore fixture exercises the `TabularTransform` multi-mapping path (one CSV → Book / Author / BookAuthorJoin datasets)
- **Interactive TUI** -- Blessed + Pict layout with main menu, suite runner, results table, dataset picker, and captured server log
- **Headless Mode** -- `node harness.js --headless --datasets=...` prints progress, results summary, and exits 0/1 for CI
- **Dataset Presets** -- Small / Medium / Large presets let you scale from 30-second smoke test to multi-minute full sweep
- **Port Cleanup & Graceful Shutdown** -- Automatically reclaims ports 8420-8422 from stale runs and shuts down cleanly on Ctrl+C
- **Persistent Results** -- Each run is written to `./data/harness.db` (`HarnessTestRun` + `HarnessTestResult` tables) for later inspection

## Installation

```bash
cd modules/apps/ultravisor-suite-harness
npm install
```

The harness expects the `facto-library` dataset cache at `./modules/dist/facto-library/` relative to the repo root. Large presets assume those datasets have already been fetched.

## Quick Start

```bash
# Interactive blessed UI
npm start

# Headless run with the default preset
npm run headless

# CI-friendly validation runs
npm run test:small     # 3 datasets
npm run test:medium    # 8 datasets
npm run test:large     # 14+ datasets
```

Once the TUI is open, use the number-key shortcuts:

| Key | Action |
|---|---|
| `1` | Clean & Execute -- stop servers, wipe `./data/`, restart, run the selected preset |
| `2` | Run Suite -- run the selected preset without cleaning (reuse existing DB state) |
| `3` | View Results -- pass/fail summary table from the last run |
| `4` | Dataset Picker -- choose small / medium / large preset |
| `5` | Server Log -- captured stdout / stderr from the three child servers |
| `m` | Return to main menu from any view |
| `q` | Quit |

In the **Dataset Picker** view, press `s` for small, `M` for medium, `L` for large.

## Headless Usage

```bash
# Default preset
node harness.js --headless

# Explicit dataset list
node harness.js --headless --datasets=datahub-country-codes,datahub-currency-codes

# Run a single multi-entity fixture
node harness.js --headless --datasets=bookstore
```

Headless mode exits with code `0` when every dataset passes, `1` when any dataset fails. It prints a summary table identical to the one shown in the `View Results` screen of the TUI.

## What the Suite Tests

For each selected dataset the harness executes the full pipeline:

1. **Scan** -- locate the input file in `modules/dist/facto-library/<name>/data/` (or the local `fixtures/` folder for multi-entity scenarios)
2. **Parse** -- `meadow-integration FileParser` streams the file and auto-detects CSV / JSON
3. **Map** -- identity mapping for single-entity datasets, or custom `Mappings` JSON for multi-entity fixtures like `bookstore`
4. **Transform** -- `meadow-integration TabularTransform` flattens nested objects and sanitizes values
5. **Load** -- Ultravisor dispatches the `facto-ingest` operation which calls Facto beacons (`createsource`, `createdataset`, `createingestjob`, `bulkcreaterecords`, `updateingestjob`)
6. **Verify** -- `SELECT COUNT(*)` against the resulting Facto dataset is compared to the parsed row count

A dataset passes only when all six steps complete and the verified count matches the parsed count.

## Architecture Summary

- **`harness.js`** -- entry point; intercepts stdout/stderr, parses CLI args, cleans stale ports, launches either the blessed TUI or the headless orchestrator
- **`source/Harness-Application.js`** -- the Pict application that owns the blessed layout, navigation, dataset presets, and runtime state
- **`source/services/Service-ServerManager.js`** -- starts, stops, and health-checks the three in-process servers
- **`source/services/Service-TestOrchestrator.js`** -- owns `DATASET_REGISTRY`, runs the per-dataset pipeline, dispatches `facto-ingest` operations through Ultravisor
- **`source/services/Service-DataManager.js`** -- manages the `./data/` directory and the `harness.db` SQLite file (`HarnessTestRun`, `HarnessTestResult`)
- **`source/views/`** -- blessed + Pict views (`View-MainMenu`, `View-SuiteRunner`, `View-Results`, `View-DatasetPicker`, `View-Log`)
- **`operations/`** -- Ultravisor operation JSON (`facto-ingest.json`, `facto-full-ingest.json`, `facto-projection-import.json`, `facto-projection-deploy.json`)
- **`fixtures/`** -- local test data, including `bookstore/` multi-entity mappings

See [docs/architecture.md](docs/architecture.md) for diagrams.

## Data Storage

All runtime state lives under `./data/` (git-ignored):

- `./data/harness.db` -- harness-owned SQLite DB with suite-run history
- `./data/facto.db` -- Facto warehouse DB
- `./data/target.db` -- Meadow Integration transform workspace
- Any additional databases spun up by child services

Pressing `1` (**Clean & Execute**) wipes this directory before a run. Pressing `2` (**Run Suite**) leaves existing state alone -- useful for isolating pipeline failures from data-setup failures.

## Documentation

- [Overview](docs/README.md)
- [Purpose](docs/purpose.md) -- why full-suite testing exists and what it protects against
- [Usage](docs/usage.md) -- developer and CI workflows
- [Architecture](docs/architecture.md) -- diagrams and component map
- [Pipeline](docs/pipeline.md) -- the six stages in detail
- [Datasets](docs/datasets.md) -- the registry and preset definitions
- [TUI Reference](docs/tui-reference.md) -- every view and keybinding

## Related Packages

- [ultravisor](https://github.com/stevenvelozo/ultravisor) -- workflow execution engine
- [ultravisor-beacon](https://github.com/stevenvelozo/ultravisor-beacon) -- beacon protocol client
- [retold-facto](https://github.com/stevenvelozo/retold-facto) -- data warehouse
- [meadow-integration](https://github.com/stevenvelozo/meadow-integration) -- file parsing and tabular transform
- [meadow](https://github.com/stevenvelozo/meadow) -- ORM and query DSL
- [orator](https://github.com/stevenvelozo/orator) -- REST API server
- [pict](https://github.com/stevenvelozo/pict) -- MVC application framework

## License

MIT

## Contributing

Pull requests welcome. See the [Retold Contributing Guide](https://github.com/stevenvelozo/retold/blob/main/docs/contributing.md) for the code of conduct, contribution process, and testing requirements.
