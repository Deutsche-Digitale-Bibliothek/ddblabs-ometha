# Changelog

All notable changes to Ometha are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


---

## [2.2.0] - 2026-06-16

### Added

- Adaptive throttle mechanism that monitors response time and adjusts timeout accordingly every 5s: If median response time is >10% above current timeout --> +5s, otherwise -5s. Maximum set at 120s.
- `conf` mode: added `batchsize`, `numberofprocesses` and `maxtimeout` parameters

## Changed

- Number of harvested files per subfolder can be controlled via `batchsize`. Added to `PRM` and included in `GUI` and `TUI mode`.
- Improved recovery workflow in harvester: both checkpoint writers (crash upon error and checkpoint every 1,000 IDs) now create YAML files instead of txt. The new recovery workflow is therefore: Checkpoint YAML written to output folder → `uv run ometha conf -c resumption_token_latest.yaml` continues with all given parameters including Resumption Token.
---

## [2.1.0] – 2026-06-10

### Added
- Adaptive throttle mechanism measures the amount of harvested files per minute and adjusts the number of parallel downloads every 5 seconds.

### Fixed
- `conf` mode: remove `assert_status_hook` from `start_process` and move it a layer above in order to prevent incorrect pickling and referring to a then pickled internal function. 
- `conf` mode: conflicting configuaration of assert_status_hook and the retry mechanism. The retry mechanism should trigger a retry in case of a 503 for example, but assert_status_hook was throwing an error before it had a chance to do so. Therefore, the hook was removed from the session and and a status check added right after the request.
- `conf` mode with `--a`: a function was added to catch inconsistent date formats in YAML files. Both from date and until date either need to be date and time or just date, a mix of both is a violation of the OAI-PMH protocol. The function adds `T00:00:00Z` to the from date or `T23:59:59Z` to the until date in order to make sure both have the same level of granularity and Ometha harvests last days completely.

### Changed
- Harvested files are grouped into subfolders of 10,000 files each in order to preserve OS performance.

---

## [2.1.0] – 2026-03-28

### Added
- Natural language date input for `--fromdate` / `--untildate`: accepts relative expressions like `1d`, `6h`, `20m`, `3w`, `1mo` in addition to ISO8601 dates
- `conf` mode: `--no-log` flag to suppress logfile creation (useful for cron jobs with external logging)
- `conf` mode: `--cleanup-on-empty` flag to remove the output folder when no records were harvested
- `ometha gui` subcommand launches the GUI — replaces the separate `ometha-gui` entry point, enabling `uvx ometha gui`

### Fixed
- `conf` mode: date keys `from-Datum`/`until-Datum` and `fromdate`/`untildate` are now validated and resolved through the same path as CLI dates — invalid values exit with an actionable error message instead of being silently ignored
- `conf` mode: `outputfolder: null` in YAML config now correctly falls back to the current working directory
- Missing IDs no longer treated as a critical error
- `PRM["mode"]` was not set in CLI mode, causing potential downstream issues

### Changed
- Invalid date input now exits immediately with a descriptive error message
- Removed unused `running_mode` variable in `main.py`
- Ruff linting (`--fix`) added to pre-commit hook

---

## [2.0.0] – 2026-03-27

### Added
- Complete test suite with 170 tests covering CLI, TUI, and harvester logic (`tests/test_oai_mock.py`, `tests/test_cli.py`, `tests/test_tui.py`)
- Reusable in-process OAI-PMH 2.0 mock for tests (`tests/oai_mock.py`) – no real network access needed
- Retry logic in `get_identifier()`: 3 attempts with exponential backoff (20s / 40s / 80s) for transient network errors
- ResumptionToken is periodically saved to disk every 1000 IDs and always saved before aborting on error, allowing harvesting to be resumed
- Pre-commit hook that runs the full test suite before every commit
- `CLAUDE.md` with architecture decisions and contribution conventions
- `tests/TESTING.md` with full documentation of the test suite

### Fixed
- **Config mode (`conf`)**: YAML key `set` renamed to `sets`; added fallback keys `url` (for `baseurl`), `mprefix` (for `metadataPrefix`), `name` (for `datengeber`) for backwards compatibility with older config files
- **Config mode**: Crash when `baseurl` or `metadataPrefix` was `None` before string cleanup
- **Config mode**: `sets: [komplett]` is now correctly treated as "no set filter"
- **IDs mode**: YAML key `set` renamed to `sets`; sets list is now properly converted to the expected dict structure
- **`harvest_files()`**: Broken OAI error detection regex (`<\\error` → `</error>`) – error responses such as `idDoesNotExist` were previously saved as files instead of counted as failed downloads
- **`parse_set_values()`**: Crash when called with `None`
- **Windows**: Colons in folder names replaced with underscores
- **`get_identifier()` spinner**: `spinner.start()` was incorrectly called on every paginated page; moved to before the loop

### Changed
- Replaced unmaintained `halo` spinner library (last release 2020, Python 3.12 `DeprecationWarning`) with `yaspin`
- Migrated from `pip` / `setup.py` / `Pipfile` to `uv` / `pyproject.toml`
- Removed unused `aiohttp` dependency

---

## [1.9.6] – 2023

### Fixed
- Windows compatibility: colons in folder names replaced with underscores
- Timestring format adjusted for Windows folder naming conventions

---

## [1.9.5] and earlier

See git history.
