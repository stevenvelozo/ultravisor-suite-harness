# Ultravisor Suite Harness

> End-to-end pipeline validation for the Ultravisor / Facto / Meadow Integration stack

Boots the full data-pipeline stack in-process and drives real-world datasets through the `scan -> parse -> map -> transform -> load -> verify` pipeline, reporting pass / fail per dataset in either a blessed terminal UI or a CI-friendly headless mode.

- **Full-Stack In-Process** -- Facto, Meadow Integration, and Ultravisor all run as child Pict applications in the same Node
- **Real Workloads** -- ISO codes, IANA TLDs, RAL colors, IEEE OUI, OurAirports, Tiger Census, Project Gutenberg, and more
- **Multi-Entity Extraction** -- Exercises the `TabularTransform` multi-mapping path via the Bookstore fixture
- **Interactive TUI** -- Main menu, suite runner, results, dataset picker, and captured server log
- **Headless CI Mode** -- `--headless --datasets=...` prints a summary and exits 0 / 1
- **Scalable Presets** -- Small / Medium / Large presets from 30-second smoke test to multi-minute full sweep

[Overview](README.md)
[Purpose](purpose.md)
[Usage](usage.md)
[Architecture](architecture.md)
[GitHub](https://github.com/stevenvelozo/ultravisor-suite-harness)
