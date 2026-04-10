# Datasets

The harness drives a curated set of real public datasets through the full pipeline. Every dataset is registered in `DATASET_REGISTRY` inside `source/services/Service-TestOrchestrator.js` and grouped into three presets by size.

## Presets

Presets are additive: Medium includes everything in Small, Large includes everything in Medium.

### Small (3 datasets, ~30 seconds)

| Key | Rows | Purpose |
|---|---|---|
| `datahub-country-codes` | ~250 | ISO 3166 country codes CSV -- the reference tiny dataset |
| `datahub-currency-codes` | ~170 | ISO 4217 currency codes CSV |
| `datahub-language-codes` | ~180 | ISO 639 language codes CSV |

Small is the smoke-test preset. It runs fast enough to live in a pre-commit hook and still exercises every pipeline stage.

### Medium (8 datasets, ~2-3 minutes)

Everything in Small plus:

| Key | Rows | Purpose |
|---|---|---|
| `iana-tlds` | ~1,500 | IANA top-level domains -- simple text list |
| `ral-colors` | ~1,600 | RAL industrial color codes CSV |
| `bls-sic-titles` | ~500 | US Bureau of Labor Statistics SIC industry titles |
| `bls-soc-2018` | ~800 | US BLS Standard Occupational Classification 2018 |
| `bookstore` | 130k+ | Multi-entity fixture exercising `TabularTransform` (Book / Author / BookAuthorJoin) |

Medium is the default preset for PR validation. It adds realistic row counts and the multi-entity code path.

### Large (14+ datasets, ~10+ minutes)

Everything in Medium plus:

| Key | Rows | Purpose |
|---|---|---|
| `iso-10383-mic-codes` | ~150 | ISO market identifier codes |
| `ipeds` | 4,000+ | US higher-education institution data |
| `nflverse` | 14,000+ | NFL combine / roster data |
| `ieee-oui` | 27,000+ | IEEE organizationally-unique identifiers |
| `ourairports` | 40,000+ | Global airport database |
| `tiger-relationship-files` | 200,000+ | US Census Tiger geography relationships |
| `project-gutenberg-catalog` | 70,000+ | Project Gutenberg book catalog |

Large is the release-engineering preset. It includes the biggest files and the longest tails of format edge cases.

## DATASET_REGISTRY Shape

Every dataset is registered with a small descriptor:

```javascript
'<dataset-key>':
{
	files:         [ 'data/path-relative-to-library.csv' ],
	format:        'csv' | 'json' | 'tsv' | 'txt',
	fixtureSource: true  // optional -- load from ./fixtures/ instead of facto-library
	mappings:            // optional -- array of mapping descriptors for multi-entity
	[
		{ file: 'path/to/mapping.json', entity: 'EntityName' }
	]
}
```

| Field | Required | Purpose |
|---|---|---|
| `files` | yes | List of files to parse. Most datasets have one; multi-file datasets are loaded sequentially. |
| `format` | yes | Hint passed to `meadow-integration FileParser`. The parser still auto-detects, but the hint short-circuits detection and catches mismatches. |
| `fixtureSource` | no | When `true`, the files are resolved against `./fixtures/` instead of `./modules/dist/facto-library/`. Used by the `bookstore` multi-entity fixture. |
| `mappings` | no | When present, the dataset runs the multi-entity path: the source file is parsed once and each mapping produces a separate entity dataset in Facto. Without this, the harness uses an identity mapping. |

## File Resolution

Non-fixture datasets resolve to:

```
./modules/dist/facto-library/<dataset-key>/<file>
```

e.g. `datahub-country-codes` -> `./modules/dist/facto-library/datahub-country-codes/data/country-codes.csv`.

Fixture datasets (anything with `fixtureSource: true`) resolve to:

```
./fixtures/<file>
```

The harness is run from the `ultravisor-suite-harness` directory (npm scripts handle the `cd`), so both paths are resolved relative to the module root.

## The Bookstore Fixture

`bookstore` is the only multi-entity dataset in the registry and the primary regression guard for `TabularTransform`. It consists of:

- `fixtures/books.csv` -- 130k+ row CSV with 23 columns sourced from the Goodreads 10k dataset
- `fixtures/bookstore/mapping_books_book.json` -- Book entity mapping
- `fixtures/bookstore/mapping_books_author.json` -- Author entity mapping (splits the comma-delimited authors column into separate Author records)
- `fixtures/bookstore/mapping_books_BookAuthorJoin.json` -- Join-table mapping producing one record per (book, author) pair

Each mapping is a plain JSON object with `Entity`, `GUIDTemplate`, and `Mappings` keys. Mapping expressions may reference parsed columns (`{title}`), literals (`"Unknown"`), or comprehension helpers (`round(original_publication_year)`).

The harness passes `bookstore` only when all three entity datasets land in Facto with matching row counts.

## Adding a New Dataset

To add a dataset to the harness:

1. **Drop the file** into `./modules/dist/facto-library/<your-key>/data/your-file.csv` (or commit it under `./fixtures/` if it is a fixture the harness should ship).
2. **Register it** in `DATASET_REGISTRY` inside `source/services/Service-TestOrchestrator.js`:

	```javascript
	'your-key':
	{
		files:  ['data/your-file.csv'],
		format: 'csv'
	}
	```

3. **Add it to a preset** by appending the key to `PRESET_SMALL`, `PRESET_MEDIUM`, or `PRESET_LARGE` in `source/Harness-Application.js`. Pick the smallest preset the dataset fits in -- a 500-row CSV belongs in Small, a million-row file belongs in Large.
4. **Sanity check** the new dataset with `node harness.js --headless --datasets=your-key`. Confirm it passes cleanly before adding it to a preset.
5. **Commit** the registry change. If you are shipping a new fixture, commit the fixture file too.

## Adding a Multi-Entity Dataset

To add a multi-entity dataset (following the `bookstore` pattern):

1. Create the input file under `./fixtures/`.
2. Create one mapping JSON per entity under `./fixtures/<your-key>/`.
3. Register the dataset in `DATASET_REGISTRY` with `fixtureSource: true` and a `mappings` array.
4. Use Small or Medium as the initial preset -- multi-entity datasets exercise more code per row and are slower per row than identity datasets.

## Removing a Dataset

Datasets are sometimes dropped from upstream mirrors. To remove a dataset from the harness:

1. Remove its key from `PRESET_SMALL` / `PRESET_MEDIUM` / `PRESET_LARGE`.
2. Remove its entry from `DATASET_REGISTRY`.
3. Optionally delete the cached copy under `./modules/dist/facto-library/<key>/`.

Prefer removing over silently ignoring -- a dataset in the registry that nobody runs is noise.
