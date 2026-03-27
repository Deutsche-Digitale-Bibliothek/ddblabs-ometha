# CLAUDE.md – Ometha

Hinweise für Claude Code beim Arbeiten in diesem Repository.

## Projekt

Ometha ist ein OAI-PMH 2.0 Harvester (CLI + TUI) ursprünglich für den Einsatz in der Fachstelle Bibliothek der Deutsche Digitale Bibliothek programmiert. Python ≥ 3.12, Paketmanager: `uv`.

## Befehle

```bash
# Abhängigkeiten installieren
uv sync --extra dev

# Tests ausführen (TERM=dumb wegen Halo-Spinner)
TERM=dumb uv run pytest tests/ -v

# Nur Mock-Tests
TERM=dumb uv run pytest tests/test_oai_mock.py -v

# Linting
uv run ruff check ometha/
```

## Zentrales Harvesting-Prinzip

Ometha harvestet **niemals** über `ListRecords`. Der Ablauf ist immer zweistufig:

1. **`ListIdentifiers`** → Liste aller IDs sammeln (`get_identifier()`)
2. **`GetRecord`** für jede ID einzeln → XML/JSON speichern (`harvest_files()`)

Das ermöglicht parallelisierte Downloads, gezielte Fehlerbehandlung pro ID und
Wiederaufnahme mit ResumptionToken. Dieser Ansatz ist bewusst gewählt und soll
nicht geändert werden – auch nicht wenn `ListRecords` auf den ersten Blick
einfacher erscheint. Viele OAI Schniitstellen haben Limitierungen bei `ListRecords` (z.B. fehlende ResumptionToken, unzuverlässige Paginierung, Timeouts bei großen Korpora), die durch den zweistufigen Ansatz umgangen werden.

## Architektur

```
ometha/
├── main.py       # Einstiegspunkt, Session-Setup, Ablaufsteuerung
├── cli.py        # argparse – liefert PRM-Dict
├── harvester.py  # get_identifier(), harvest_files(), Kernlogik
├── helpers.py    # Konstanten (NAMESPACE, PRM-Template), XML-Parsing
└── tui.py        # Interaktiver Modus (Textual)
```

Zentrales Datenmodell ist das `PRM`-Dict (definiert in `helpers.py`).
Alle Funktionen nehmen `PRM` als Parameter.

## Tests & Mock

- Mock-Schnittstelle: `tests/oai_mock.py` → `OAIMock`, `OAIRecord`
- Test-Suiten: `tests/test_oai_mock.py`, `tests/test_cli.py`, `tests/test_tui.py`
- Dokumentation: `tests/TESTING.md`

**Regel: Jede Codeänderung muss von Tests begleitet sein.**
- Neue Features → neue Tests
- Bugfixes → Regressionstest der den Bug reproduziert
- Refactorings → alle bestehenden Tests müssen weiterhin grünen
- Vor einem Commit immer `TERM=dumb uv run pytest tests/ -v` ausführen

Neue Tests immer gegen den Mock schreiben, nie gegen echte OAI-Endpunkte.

```python
def test_beispiel(requests_mock, session, prm_base):
    mock = OAIMock.with_dc_records(5)
    mock.register(requests_mock)
    # ...
```

## Konventionen

- Fehlermeldungen auf Deutsch (Konsistenz mit bestehendem Code)
- Logging via `loguru` (`logger.info/warning/critical`)
- Keine `print()`-Calls außer über `print_and_log()` aus `helpers.py`
- HTTP immer über die übergebene `requests.Session` – nie direkt `requests.get()`
- Neue Abhängigkeiten: `uv add <paket>`, nicht `pip install`
- Präferenz für `uv` über `pip` in allen Kontexten

## Bekannte Eigenheiten

- `get_identifier()` enthält Halo-Spinner → in Tests `TERM=dumb` setzen
- `isinvalid_xml_content()` erwartet XML ohne führende Whitespaces vor `<?xml`
- `PRM["n_procs"]` kann aus YAML-Config als Liste ankommen (bekannter Bug,
  siehe Kommentar in `main.py`)
- `harvest_files()` nutzt `ThreadPool` – Mocks müssen thread-safe sein
  (`requests-mock` ist es)
