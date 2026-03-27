# Changelog

All notable changes to Ometha are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
