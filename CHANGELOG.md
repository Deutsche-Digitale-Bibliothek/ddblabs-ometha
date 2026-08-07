# Changelog

All notable changes to Ometha are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


---
## [2.5.0] - 2026-XX-XX

### Fixed
- Fixed a crash (`UnboundLocalError`) that occurred when a harvesting run
  hit an invalid/empty XML response and the user pressed Enter without
  typing a character at the "Drücken Sie Enter zum Beenden" prompt;
  `log_critical_and_print_and_exit` now always exits as its name promises,
  regardless of the exact input given

## [2.4.0] - 2026-08-06

### Added

- Added docstrings to the fixtures for the `TUI mode` unit tests.
- Provide first unit tests for `main.py`.

### Changed
- `--gentle` now also disables identifier pipelining: all identifiers are
  collected upfront (and written to the ID file) before downloads start,
  instead of streaming identifiers into the worker pool as they arrive.
  This avoids running ListIdentifiers and GetRecord requests concurrently
  against endpoints that are already being treated conservatively
- Refactored `collect_ids()` out of `start_process()` into a standalone,
  testable function

### Fixed
- Output folder paths starting with `~` are now correctly expanded to the
  user's home directory instead of creating a literal `~` subfolder
- Ometha crashed with `TypeError: divmod expected 2 arguments, got 3` after
  completing a harvesting run; runtime is now calculated correctly using two
  nested `divmod` calls

## [2.3.0] - 2026-07-31

### Added
- New parameter `--deleted` (CLI) / `deleted` (YAML config) to control how records
  with `status="deleted"` are handled:
  - `keep` (default): records are saved normally, preserving existing behavior
  - `skip`: records are discarded and only logged
  - `folder`: records are moved to a `_deleted/` subfolder in the output directory
  Available in all modes: CLI, TUI, GUI, and YAML config
- Records without a grandchild node under `//metadata` (e.g. empty metadata,
  HTML fragments) are automatically moved to an `_invalid/` subfolder and logged
  as a warning. This check runs after the deleted-record check, so deleted records
  are never misclassified as invalid. A summary count is printed at the end of
  each harvesting run.
- New `--gentle` flag (CLI/TUI) and toggle (GUI) for sensitive OAI endpoints:
  sets conservative defaults of 1 thread, 30s timeout, and 20s rate limit.
  All three values remain individually overridable via `--parallel`, `--timeout`,
  and `--ratelimit`
- New `--ratelimit` parameter (seconds between requests) usable independently
  of `--gentle`; enforced globally across all worker threads via a shared lock,
  so the interval is respected regardless of parallelization level

### Changed
- `harvest_files` logs a summary of skipped/moved deleted records at the end of each run
- TUI: gentle mode is now prompted before parallel downloads, preventing users
  from entering a thread count that would immediately be overridden by the
  gentle mode default
- Harvesting now uses a producer-consumer pipeline: `GetRecord` downloads start
  after the first `ListIdentifiers` page instead of waiting for all identifiers
  to be collected first, significantly reducing time-to-first-download on large
  repositories
- `get_identifier` is now a thin wrapper around the new `get_identifier_pages`
  generator, which yields one page of identifiers at a time; existing callers
  (GUI, ID-file mode) are unaffected
- Intersection sets still collect all identifiers upfront (required for the
  cross-set deduplication), multiple additive sets collect and deduplicate
  before harvesting; single set and no-set cases benefit fully from pipelining

### Fixed
- `test_oai_mock`: `harvest_files` tests now use the mocked session instead of a bare `requests.Session()`, fixing tests that silently produced no output
- `test_oai_mock`: file assertions now use `rglob` to find files in `part_*/` subfolders created by `harvest_files`
- `test_oai_mock`: date value assertions account for `yaml.safe_dump` quoting strings as `'2025-01-01'`
- `test_oai_mock`: resumption token file search now uses `.yaml` extension instead of `.txt`
- `test_cli`: default timeout updated from `0` to `20`; added `batchsize` tests for all subcommands
- `test_tui`: all input sequences updated to include `batchsize` prompt after `n_procs`
- GUI: when multiple sets were selected, only the first set was harvested;
  each set is now collected in a separate ListIdentifiers run and the results
  are merged and deduplicated
- GUI: on Windows, the colon in the drive letter (e.g. `C:`) was replaced
  by `_` when building the output folder path; only the timestamp portion
  is now sanitized

---


## [2.2.0] - 2026-06-26


### Added
- Output files are now split into numbered subfolders (`part_0001`, `part_0002`, ...) to avoid excessively large directories. Subfolder size defaults to 10,000 files and is configurable via `--batchsize` (CLI), the interactive prompt, the GUI, or `batchsize` in a config YAML.
- Parallel download count now adapts automatically at runtime based on measured throughput (files/sec), increasing workers when an endpoint responds well and decreasing them when throughput drops. The value passed via `--parallel`/`-p` (or the config file/GUI equivalent) now acts as a hard ceiling for the adaptive pool instead of a fixed value.
- Request timeout now adapts automatically at runtime based on measured response latency, increasing in steps when an endpoint is slow and decreasing again when it speeds up. The configured timeout value acts as the floor; a new `--maxtimeout` option (default 120s) sets the ceiling.
- End-of-run summary now logs min/max/average worker count and min/max/average timeout used during the harvest.
- Failed downloads and failed IDs (timeouts, connection errors, HTTP errors) are now merged and retried exactly once at the end of a run, with reduced parallelism, to recover records from endpoints that generate content on demand. A final list of any still-failing IDs is written to `_ometha_final_failed_ids.yaml`.
- Resumption-token checkpoints (both the periodic checkpoint and the one written on a fatal error) are now written as complete, ready-to-use config YAMLs — including base URL, metadata prefix, sets, date range, output folder, and the new adaptive-pool/timeout/batch-size settings — instead of plain-text notes. They can be resumed directly with `ometha conf -c <file>`.
- ID-list YAML files (successful, failed, final-failed) now also record `numberofprocesses`, `maxtimeout`, and `batchsize` for full reproducibility.
- Default request timeout changed from 0 (no timeout) to 20 seconds.
### Fixed
- Fixed a crash (`AttributeError: Can't get local object 'start_process.<locals>.assert_status_hook'`) during long-running auto-mode harvests, caused by a response hook defined as a local function, which cannot be pickled when the session is passed across threads. The hook is now defined at module level.
- Removed the response status hook from the session entirely, since it fired before the `HTTPAdapter` retry logic could run, effectively disabling automatic retries on 429/5xx responses. Status codes are now checked explicitly after each request, after retries have been exhausted.
- Improved the "HTTP error" message during identifier harvesting to include the actual status code and URL instead of a generic message.
- Fixed resumption-token harvesting returning 0 IDs on several endpoints, caused by a double `&&` in the generated URL (`...ListIdentifiers&&resumptionToken=...`). Some OAI-PMH endpoints do not tolerate the malformed query string.
- Fixed resumption tokens with surrounding/internal whitespace not being recognized as "no more pages", which could prematurely stop or otherwise misbehave during identifier harvesting.
- `from` and `until` dates are now normalized to the same granularity before being sent to an endpoint (OAI-PMH requires matching granularity). A date-only value is padded to a full datetime (`00:00:00Z` / `23:59:59Z`) if the other bound includes a time component, and vice versa. Previously, mixed granularity (e.g. `from=2020-02-13&until=2026-06-09T14:51:05Z`) caused some endpoints to reject the request.
- Fixed a `NameError: name 'export_type' is not defined` crash during file harvesting introduced while refactoring the download worker.
- Fixed a potential division-by-zero when calculating the retry worker count if there were failed downloads but no failed IDs (or vice versa).
- Fixed output folders being created with a mangled path (e.g. `C_\Users\...` instead of `C:\Users\...`) on Windows, caused by a colon-stripping step that was applied to the entire output path instead of just the timestamp component.
- Fixed auto mode writing `until-Datum` instead of `untildate` to the config YAML
### Changed
- Refactored the file-download worker pool from a fixed-size `multiprocessing.dummy.ThreadPool` to a `ThreadPoolExecutor` with a semaphore-controlled adaptive concurrency limit, enabling live resizing without restarting the pool.
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
