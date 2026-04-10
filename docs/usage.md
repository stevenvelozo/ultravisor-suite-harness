# Usage

This guide covers every way the harness is expected to be invoked, from a first-time local run through to CI integration.

## Prerequisites

- Node.js 18+ (matches the rest of the Retold stack)
- The `facto-library` dataset cache available at `./modules/dist/facto-library/` relative to the repo root for any dataset other than `bookstore`
- Ports `8420`, `8421`, `8422` free (the harness will attempt to reclaim them automatically, but other listeners on these ports will conflict)
- Terminal capable of running blessed (any modern xterm-compatible terminal) if you plan to use the interactive UI

## Installation

```bash
cd modules/apps/ultravisor-suite-harness
npm install
```

The `data/` folder is git-ignored and will be created automatically on first run.

## Quick Reference

| Command | Mode | Preset | Notes |
|---|---|---|---|
| `npm start` | Interactive TUI | User choice | Default entry point for development |
| `npm run headless` | Headless | Default (large) | No UI, prints summary, exits 0/1 |
| `npm test` | Headless | Small | Alias for `test/validate-harness.js` |
| `npm run test:small` | Headless | Small (3 datasets) | ~30 seconds |
| `npm run test:medium` | Headless | Medium (8 datasets) | ~2-3 minutes |
| `npm run test:large` | Headless | Large (14+ datasets) | ~10+ minutes |

## Interactive Mode

Start the blessed UI:

```bash
npm start
```

On launch you'll see the **Main Menu** with server status indicators and the currently selected dataset preset. The menu is keyboard-driven:

| Key | Action |
|---|---|
| `1` | Clean & Execute |
| `2` | Run Suite |
| `3` | View Results |
| `4` | Dataset Picker |
| `5` | Server Log |
| `m` | Return to main menu |
| `l` | Refresh log from any view |
| `q` | Quit |

### Typical Interactive Session

1. Press `4` to open the Dataset Picker.
2. Press `s` for small, `M` for medium, `L` for large, or navigate the list to select specific datasets.
3. Press `m` to return to the main menu.
4. Press `1` to run Clean & Execute. The Suite Runner view replaces the main menu and shows live per-dataset progress.
5. When the run finishes, press `3` to view the results summary.
6. If a dataset failed, press `5` to inspect the captured server log for that window.

### Clean & Execute vs Run Suite

| Key | Steps |
|---|---|
| `1` Clean & Execute | stop servers -> `rm -rf ./data/` -> recreate SQLite DBs -> restart servers -> run suite |
| `2` Run Suite | skip the clean step; run the suite against whatever state is already in `./data/` |

Use `1` for a deterministic clean run. Use `2` when you want to iterate quickly on a mapping or operation definition without repaying the startup cost.

## Headless Mode

Headless mode is the same orchestrator wired to stdout. It is suitable for CI, pre-commit hooks, and any context where interactive input is impossible.

```bash
# Default preset, default output
node harness.js --headless

# Explicit dataset list (comma-separated, no spaces)
node harness.js --headless --datasets=datahub-country-codes,datahub-currency-codes

# Just the bookstore multi-entity fixture
node harness.js --headless --datasets=bookstore
```

Headless output follows this shape:

```
=== Ultravisor Suite Harness (headless) ===
Datasets: datahub-country-codes, datahub-currency-codes

[OK] Data directory cleaned
[OK] Servers running (Facto :8420  Integration :8421  Ultravisor :8422)
[OK] Harness DB initialized

[1/2] datahub-country-codes -- scanning...
[PASS] datahub-country-codes -- verified 248 records
[2/2] datahub-currency-codes -- scanning...
[PASS] datahub-currency-codes -- verified 170 records

=== Results ===
  datahub-country-codes    PASS   parsed= 248 loaded= 248 verified= 248
  datahub-currency-codes   PASS   parsed= 170 loaded= 170 verified= 170

Summary: 2 passed, 0 failed

=== ALL TESTS PASSED ===
```

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Every selected dataset passed (parsed count == loaded count == verified count) |
| `1` | One or more datasets failed, or startup failed |

## CLI Flags

| Flag | Default | Purpose |
|---|---|---|
| `--headless` | off | Disable the blessed UI and print progress to stdout; exit with the suite's pass/fail code |
| `--datasets=<list>` | preset default | Comma-separated dataset keys; overrides any preset |

The validation runner (`test/validate-harness.js`) additionally accepts:

| Flag | Default | Purpose |
|---|---|---|
| `--preset=small\|medium\|large` | `small` | Select a named preset |
| `--datasets=<list>` | preset default | Same as above |

## Dataset Selection Precedence

1. An explicit `--datasets=` list wins over everything else.
2. Otherwise, the preset (small / medium / large) is used.
3. Otherwise, the harness falls back to its built-in default preset.

Dataset keys must match entries in `DATASET_REGISTRY` in `source/services/Service-TestOrchestrator.js`. See [Datasets](datasets.md) for the full list.

## Environment Expectations

- **Working directory.** The harness resolves `FACTO_LIBRARY_PATH = './modules/dist/facto-library'` relative to the repo root. Run it from `modules/apps/ultravisor-suite-harness` (npm scripts handle `cd` for you).
- **Ports.** `8420-8422` must be either free or owned by a previous harness run (the harness will clean them up). Permanent collisions with other services will need to be resolved upstream.
- **Disk.** `./data/` is wiped on every Clean & Execute. Do not keep anything important in it.
- **File permissions.** The harness creates SQLite files inside `./data/` on startup; the process needs write permission there.

## CI Integration

A minimal CI job for a pipeline-module PR:

```yaml
- name: Install harness
  run: |
    cd modules/apps/ultravisor-suite-harness
    npm install

- name: Run medium preset
  run: |
    cd modules/apps/ultravisor-suite-harness
    npm run test:medium
```

For nightly builds, substitute `test:large`. For documentation-only PRs, `test:small` is sufficient.

The harness does not emit JUnit XML; CI systems that require it should wrap the headless command and parse the `=== Results ===` block. Alternatively, read `./data/harness.db` directly after the run:

```bash
sqlite3 ./data/harness.db "SELECT Dataset, Status, Parsed, Loaded, Verified FROM HarnessTestResult ORDER BY CreatedAt DESC LIMIT 20;"
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Startup hangs for 30 seconds then aborts | Stale process holding `8420-8422`, or a previous run that did not shut down cleanly | Check with `lsof -i :8420`; kill the process manually, then retry |
| `ENOENT: no such file or directory` scanning a dataset | `facto-library` not populated | Fetch the dataset into `modules/dist/facto-library/<name>/data/` or run a smaller preset |
| Dataset parses but verification fails | Schema mismatch between parser output and Facto expectations | Switch to `View-Log` (`5`) to inspect the failing bulk insert; if it is a recent module upgrade, bisect |
| `EADDRINUSE :8420` after Ctrl+C | Port cleanup skipped due to abnormal exit | Kill the orphaned process; port cleanup runs on normal startup |
| Headless mode prints blessed escape codes | Piping through a non-TTY wrapper that strips the `--headless` flag | Confirm the flag is reaching the child process |
| Bookstore fixture fails but CSV-only datasets pass | `TabularTransform` / multi-mapping regression in `meadow-integration` | Inspect each mapping in `fixtures/bookstore/` and the Integration server log |
