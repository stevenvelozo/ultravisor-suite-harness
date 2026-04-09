# Ultravisor Suite Harness

> End-to-end pipeline validation for the Ultravisor / Facto / Meadow Integration stack

The Ultravisor Suite Harness is a single-process test rig that exercises the full Retold data pipeline -- from file parsing through workflow dispatch to warehouse persistence -- against real public datasets. It is the integration-level counterpart to the unit tests that live in each individual module: instead of mocking boundaries, it stands the entire stack up in one Node process and runs real datasets through it.

The harness exists because the Retold data pipeline is composed of many independently-versioned modules (`ultravisor`, `ultravisor-beacon`, `retold-facto`, `meadow-integration`, `meadow`, `meadow-connection-sqlite`, `orator`, `pict`, and their dependents). Each module ships with its own tests, but any meaningful user workflow crosses at least four of them. This harness is the one place where a breaking change in any module can be caught before it reaches downstream applications.

Two modes are supported:

- **Interactive TUI** -- a blessed terminal UI built on Pict with a main menu, live suite runner, results table, dataset picker, and captured server log. Use it for iterative development, reproducing customer data bugs, and exploring fixture output.
- **Headless CLI** -- `node harness.js --headless --datasets=...` prints progress to stdout and exits `0` on success or `1` on any failure. Use it in CI, pre-commit hooks, and scripted smoke tests.

## Features

- **Full-Stack In-Process** -- Facto (`:8420`), Meadow Integration (`:8421`), and Ultravisor (`:8422`) all run as child Pict applications in the same Node process
- **Real Workloads** -- 14+ curated datasets spanning ISO codes, government registries, academic catalogs, and open data repositories
- **Multi-Entity Extraction** -- The Bookstore fixture parses one CSV and emits three separate entity datasets via `TabularTransform`
- **Clean & Execute** -- One-keystroke fresh run: stops servers, wipes `./data/`, reinitializes databases, restarts servers, runs the suite
- **Persistent Results** -- Every run is recorded to `./data/harness.db` for later inspection and trend tracking
- **Preset Scaling** -- Small (3 datasets, ~30s), Medium (8 datasets, ~3m), Large (14+ datasets, ~10m+)
- **Port Hygiene** -- Reclaims ports `8420-8422` from stale processes before each start; graceful shutdown on Ctrl+C
- **Captured Log** -- Stdout / stderr from all three child servers are buffered and viewable inside the TUI

## When to Use It

Reach for the harness when:

- You've just upgraded one of the pipeline modules (`ultravisor`, `retold-facto`, `meadow-integration`, `meadow`, `orator`) and want to confirm nothing broke end-to-end
- You're debugging a reported data-pipeline failure and want to reproduce it against a known dataset
- You're adding a new beacon capability to `retold-facto` and want to validate it under the full workflow dispatch path
- You're landing changes to `ultravisor` operation definitions and need to confirm the dispatched sequence still resolves
- You're integrating a new dataset into the `facto-library` and want to sanity-check parsing and loading
- You want a reproducible baseline for pipeline throughput before and after a performance change

Do not reach for the harness when you just need a unit test -- those belong in the individual modules. The harness is explicitly for cross-module integration.

## Learn More

- [Purpose](purpose.md) -- why full-suite testing exists and what it protects against
- [Usage](usage.md) -- developer and CI workflows, flags, and environment expectations
- [Architecture](architecture.md) -- component map, process layout, and data flow diagrams
- [Pipeline](pipeline.md) -- the six stages in detail, with examples from the Bookstore fixture
- [Datasets](datasets.md) -- the dataset registry, preset definitions, and how to add a new dataset
- [TUI Reference](tui-reference.md) -- every blessed view, keybinding, and state transition
