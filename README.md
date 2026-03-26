# Ultravisor Suite Harness

End-to-end pipeline test harness for the Ultravisor/Facto stack.
Runs the full `parse → map → transform → load` pipeline against real datasets
from the facto-library, using a local SQLite database for storage.

## Usage

```sh
cd modules/apps/ultravisor-suite-harness
npm install
node harness.js
```

## Key bindings

| Key     | Action                                          |
|---------|-------------------------------------------------|
| `1`     | **Clean & Execute** — wipe `data/`, fresh DB, run suite |
| `2`     | **Run Suite** — run against existing database   |
| `3`     | **View Results** — pass/fail summary table      |
| `4`     | **Dataset Picker** — choose preset datasets     |
| `m`     | Return to main menu (from any view)             |
| `q`     | Quit                                            |

In the **Dataset Picker** view:

| Key | Preset                                          |
|-----|-------------------------------------------------|
| `s` | Small (3 datasets — country codes, currency, language) |
| `M` | Medium (5 datasets — adds iana-tlds, ral-colors) |

## Data storage

All data is stored in `./data/harness.db` (SQLite).
The `data/` folder is gitignored.

**Clean option**: pressing `1` runs `rm -rf ./data/` and recreates it with a
fresh database before running the suite. Pressing `2` skips the clean step.

## Pipeline steps

For each selected dataset the harness runs:

1. **Scan** — locate the data file in `modules/apps/dist/facto-library/<name>/data/`
2. **Parse** — `meadow-integration FileParser` (auto-detects CSV/JSON/etc.)
3. **Map** — identity (1:1 field pass-through)
4. **Transform** — flatten nested objects, sanitize values
5. **Load** — bulk-insert into `HarnessRecord` table (SQLite)
6. **Verify** — `SELECT COUNT(*)` and compare to parsed count

## Datasets

Default (small preset):

- `datahub-country-codes` — ISO country codes CSV (~250 rows)
- `datahub-currency-codes` — ISO currency codes CSV (~170 rows)
- `datahub-language-codes` — ISO language codes CSV (~180 rows)

Medium preset adds:

- `iana-tlds` — IANA top-level domains TXT
- `ral-colors` — RAL color codes CSV
