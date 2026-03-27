# Testing Ometha

## Setup

```bash
uv sync --extra dev
```

## Tests ausführen

```bash
# Alle Tests (TERM=dumb wegen Halo-Spinner)
TERM=dumb uv run pytest tests/ -v

# Einzelne Test-Dateien
TERM=dumb uv run pytest tests/test_oai_mock.py -v
TERM=dumb uv run pytest tests/test_cli.py -v
TERM=dumb uv run pytest tests/test_tui.py -v

# Einzelne Klasse
TERM=dumb uv run pytest tests/test_oai_mock.py::TestGetIdentifierIntegration -v

# Mit Coverage
TERM=dumb uv run pytest tests/ --cov=ometha --cov-report=term-missing
```

> **Hinweis CI:** `get_identifier()` verwendet einen Halo-Spinner. Bei
> fehlender TTY (GitHub Actions etc.) `TERM=dumb` setzen.

## Teststruktur

```
tests/
├── oai_mock.py       # Wiederverwendbarer OAI-PMH 2.0 Mock
├── test_oai_mock.py  # Integration: OAI-PMH-Protokoll + Harvester-Kernlogik
├── test_cli.py       # CLI-Optionen und parseargs()
└── test_tui.py       # Interaktiver Modus (interactiveMode)
```

## Übersicht: Was wird getestet

### `test_oai_mock.py` – 45 Tests

Integration der Harvester-Kernlogik gegen den OAI-PMH-Mock.

| Klasse | Tests | Was wird geprüft |
|---|---|---|
| `TestIdentify` | 2 | Identify-Verb, Basis-URL im Response |
| `TestListMetadataFormats` | 2 | Default- und Custom-Formate |
| `TestListSets` | 2 | Set-Ausgabe, `noSetHierarchy`-Fehler |
| `TestListIdentifiers` | 6 | Alle IDs, Set-Filter, Paginierung, from/until-Filter, `noRecordsMatch` |
| `TestGetRecord` | 4 | Existierender Record, `idDoesNotExist`, fehlende Argumente, Deleted Records |
| `TestListRecords` | 3 | Metadata-Inhalt, Custom-XML, `cannotDisseminateFormat` |
| `TestErrors` | 3 | `badVerb`, fehlender Verb, `badResumptionToken` |
| `TestOmethaXmlParsing` | 3 | lxml-Kompatibilität der Mock-Responses, ResumptionToken lesbar |
| `TestGetIdentifierIntegration` | 6 | `get_identifier()` direkt: Paginierung, Typen, Set-Filter, große Korpora |
| `Test503Retry` | 4 | 503-Simulation, Retry nach erstem Fehler, nur einmalig |
| `TestHarvestFiles` | 4 | Datei-Output für alle IDs, fehlende IDs → `failed_download`, JSON-Export, Deleted Records |
| `TestDatefilterEdgeCases` | 6 | Grenzwerte from/until inklusiv, `noRecordsMatch`, Mischgranularität |
| `TestTimeoutHandling` | 8 | Timeout/ConnectionError → `failed_ids`; HTTP 500 → `failed_download`; Retry nach Timeout; alle Retries erschöpft → `sys.exit()`; ResumptionToken wird bei Abbruch gespeichert |

### `test_cli.py` – 80 Tests

Testet `parseargs()` und `parse_set_values()` durch Mocken von `sys.argv`.
YAML-Lesevorgänge (conf/ids-Modus) werden via `unittest.mock.patch` gemockt.

| Klasse | Tests | Was wird geprüft |
|---|---|---|
| `TestParseSetValues` | 6 | Einzel-Set, Komma-Trennung, Slash-Trennung (Additive/Intersection), Whitespace, None |
| `TestDefaultCommand` | 28 | Alle Flags (`-b`, `-m`, `-d`, `-f`, `-u`, `-s`, `--resumptiontoken`, `-p`, `-t`, `-o`, `-e`, `--debug`), Defaults, ungültige Datumsangaben → `None`, Kombinationen |
| `TestConfCommand` | 16 | YAML-Lesen, Fallback-Keys (`url`, `mprefix`, `name`), `--auto`/`-a`, `--exporttype`, debug aus YAML, `komplett`-Sets herausgefiltert, Whitespace-Bereinigung von URL/Prefix |
| `TestAutoCommand` | 13 | URL-Parsing (Base-URL, metadataPrefix, set, from, until aus Query-String), alle Common-Flags, `auto_m=True` |
| `TestIdsCommand` | 14 | ID-Datei-Pfad, YAML-Lesen, `komplett` → `None`, alle Common-Flags, `--datengeber` |

### `test_tui.py` – 37 Tests

Testet `interactiveMode()` durch Mocken von `builtins.input` (Liste von Antworten).
HTTP-Requests (Option S) werden per `requests_mock` abgefangen.

| Klasse | Tests | Was wird geprüft |
|---|---|---|
| `TestOptionN` | 22 | Alle PRM-Felder gesetzt, Defaults bei leerem Input (TIMESTR, cwd, 16), Validierungsschleifen für URL/Prefix/Exportformat, from/until, Set-Eingabe, Groß-/Kleinschreibung, leere Hauptmenü-Eingabe → Default N |
| `TestOptionE` | 2 | `sys.exit()` wird ausgelöst, Kleinbuchstabe |
| `TestOptionR` | 5 | ResumptionToken, URL, Prefix gesetzt; Retry bei leerem Token |
| `TestOptionI` | 5 | baseurl/prefix/idfile-Pfad aus YAML-Datei gelesen; Retry bei nicht-existenter Datei |
| `TestOptionS` | 3 | Sets-Listing + `sys.exit(0)` bei gefundenen Sets; keine Sets → kein Exit-0; Kleinbuchstabe |

## OAI-PMH Mock (`tests/oai_mock.py`)

Der Mock simuliert eine vollständige OAI-PMH 2.0-Schnittstelle in-process
über `requests-mock`. Kein echter HTTP-Server nötig.

### Schnellstart

```python
from tests.oai_mock import OAIMock, OAIRecord

def test_mein_harvester(requests_mock, session, prm_base):
    mock = OAIMock.with_dc_records(10)
    mock.register(requests_mock)
    # Dein Code läuft gegen mock.base_url
    resp = requests.get(f"{mock.base_url}?verb=Identify")
    assert resp.status_code == 200
```

### OAIMock – Parameter

| Parameter | Typ | Default | Beschreibung |
|---|---|---|---|
| `base_url` | str | `http://mock-oai.test/oai` | Simulierte Basis-URL |
| `records` | list[OAIRecord] | `[]` | Records im Repository |
| `sets` | dict[str, str] | `{}` | `{setSpec: setName}` |
| `metadata_formats` | list[str] | `["oai_dc"]` | Unterstützte Prefixe |
| `page_size` | int | `100` | Records pro ResumptionToken-Seite |
| `simulate_503` | bool | `False` | Erster Request liefert 503 |

### OAIRecord – Felder

```python
OAIRecord(
    identifier="oai:example.org:12345",
    datestamp="2024-06-01T00:00:00Z",
    sets=["ddc:500", "meine:collection"],
    metadata_prefix="oai_dc",
    metadata_xml="<oai_dc:dc>...</oai_dc:dc>",  # None → Default DC
    deleted=False,
)
```

### Unterstützte OAI-Verben

| Verb | Unterstützt | Besonderheiten |
|---|---|---|
| `Identify` | ✅ | |
| `ListMetadataFormats` | ✅ | |
| `ListSets` | ✅ | `noSetHierarchy` wenn sets leer |
| `ListIdentifiers` | ✅ | ResumptionToken, from/until, set |
| `ListRecords` | ✅ | ResumptionToken, from/until, set |
| `GetRecord` | ✅ | `idDoesNotExist` bei unbekannter ID |

### Fehler-Simulation

```python
# noRecordsMatch
mock = OAIMock(records=[])

# cannotDisseminateFormat
mock = OAIMock(metadata_formats=["oai_dc"])
# → Request mit metadataPrefix=mods → Fehler

# badResumptionToken
requests.get(f"{base_url}?verb=ListIdentifiers&resumptionToken=invalid")

# 503 Retry-After
mock = OAIMock(simulate_503=True)
```
