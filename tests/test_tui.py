"""
Tests für das interaktive TUI (ometha.tui.interactiveMode).

Wird aufgerufen wenn Ometha ohne CLI-Argumente gestartet wird.
Alle input()-Aufrufe werden via unittest.mock.patch gemockt.

Menü-Optionen:
  N – Normales Harvesting
  I – Harvesting per ID-File
  R – Fortsetzen mit ResumptionToken
  S – Sets der Schnittstelle anzeigen
  E – Programm beenden
"""

import os
import sys
import pytest
import requests
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ometha.tui import interactiveMode


# ---------------------------------------------------------------------------
# Fixture: PRM zurücksetzen und minimale Session bereitstellen
# ---------------------------------------------------------------------------


def reset_prm():
    import ometha.helpers as h

    for key in h.PRM:
        h.PRM[key] = None


@pytest.fixture(autouse=True)
def clean_prm():
    reset_prm()
    yield
    reset_prm()


@pytest.fixture
def session():
    return requests.Session()


def mock_inputs(*values):
    """Gibt einen patch-Kontextmanager zurück, der input() der Reihe nach bedient."""
    return patch("builtins.input", side_effect=list(values))


# ---------------------------------------------------------------------------
# Option N – Normales Harvesting
# ---------------------------------------------------------------------------

# Eingabe-Reihenfolge für Option N (add_common_args + N-spezifische Felder):
#   1. Hauptmenü          → "N"
#   2. Datengeber         → "testDG"
#   3. Ausgabeordner      → "/tmp"
#   4. Parallele Downloads→ "4"
#   5. Exportformat       → "xml"
#   6. Timeout            → "0"
#   7. Base-URL           → "http://oai.example.org/"
#   8. Metadata Prefix    → "oai_dc"
#   9. Sets               → "" (keine Eingrenzung)
#  10. Fromdate           → ""
#  11. Untildate          → ""

N_INPUTS = [
    "N",
    "testDG",
    "/tmp",
    "4",
    "xml",
    "0",
    "http://oai.example.org/",
    "oai_dc",
    "",
    "",
    "",
]


class TestOptionN:
    def test_prm_baseurl_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"

    def test_prm_prefix_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["pref"] == "oai_dc"

    def test_prm_datengeber_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["dat_geb"] == "testDG"

    def test_prm_outputfolder_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["out_f"] == "/tmp"

    def test_prm_n_procs_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["n_procs"] == 4

    def test_prm_exp_type_xml(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["exp_type"] == "xml"

    def test_prm_timeout_set(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["timeout"] == 0.0

    def test_empty_fromdate_is_none(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["f_date"] is None

    def test_empty_untildate_is_none(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["u_date"] is None

    def test_fromdate_provided(self, session):
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "2023-01-01",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["f_date"] == "2023-01-01"

    def test_untildate_provided(self, session):
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "2023-12-31",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["u_date"] == "2023-12-31"

    def test_set_provided(self, session):
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "ddc:500",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["sets"][0]["additive"] == ["ddc:500"]

    def test_json_exporttype(self, session):
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "json",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["exp_type"] == "json"

    def test_datengeber_default_wenn_leer(self, session):
        """Leeres Datengeber-Feld → TIMESTR-Default."""
        inputs = [
            "N",
            "",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        # TIMESTR ist ein nicht-leerer String
        assert prm["dat_geb"] is not None
        assert len(prm["dat_geb"]) > 0

    def test_outputfolder_default_ist_cwd(self, session):
        """Leeres Ordner-Feld → aktuelles Arbeitsverzeichnis."""
        inputs = [
            "N",
            "testDG",
            "",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["out_f"] == os.getcwd()

    def test_n_procs_default_wenn_leer(self, session):
        """Leeres Parallele-Downloads-Feld → None (auto-scaling)."""
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["n_procs"] is None

    def test_invalid_exporttype_retried(self, session):
        """Ungültiges Exportformat → TUI fragt erneut."""
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "lido",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["exp_type"] == "xml"

    def test_invalid_url_retried(self, session):
        """Ungültige URL → TUI fragt erneut, bis valide URL kommt."""
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "keine-url",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"

    def test_empty_prefix_retried(self, session):
        """Leeres Prefix-Feld → TUI fragt erneut."""
        inputs = [
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["pref"] == "oai_dc"

    def test_lowercase_n_accepted(self, session):
        """Kleinbuchstabe 'n' soll wie 'N' behandelt werden."""
        inputs = [
            "n",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"

    def test_empty_input_defaults_to_n(self, session):
        """Leere Hauptmenü-Eingabe → Default 'N'."""
        inputs = [
            "",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"

    def test_invalid_menu_option_retried(self, session):
        """Ungültige Menüoption → TUI fragt erneut."""
        inputs = [
            "X",
            "N",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "",
            "",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"


# ---------------------------------------------------------------------------
# Option E – Programm beenden
# ---------------------------------------------------------------------------


class TestOptionE:
    def test_exit_called(self, session):
        with mock_inputs("E"):
            with pytest.raises(SystemExit):
                interactiveMode(session)

    def test_lowercase_e_exits(self, session):
        with mock_inputs("e"):
            with pytest.raises(SystemExit):
                interactiveMode(session)


# ---------------------------------------------------------------------------
# Option R – ResumptionToken
# ---------------------------------------------------------------------------

# Eingabe-Reihenfolge für Option R:
#   1. Hauptmenü          → "R"
#   2. Datengeber         → "testDG"
#   3. Ausgabeordner      → "/tmp"
#   4. Parallele Downloads→ "4"
#   5. Exportformat       → "xml"
#   6. Timeout            → "0"
#   7. Base-URL           → "http://oai.example.org/"
#   8. Metadata Prefix    → "oai_dc"
#   9. ResumptionToken    → "abc123"

R_INPUTS = [
    "R",
    "testDG",
    "/tmp",
    "4",
    "xml",
    "0",
    "http://oai.example.org/",
    "oai_dc",
    "abc123",
]


class TestOptionR:
    def test_resumption_token_set(self, session):
        with mock_inputs(*R_INPUTS):
            prm = interactiveMode(session)
        assert prm["res_tok"] == "abc123"

    def test_baseurl_set(self, session):
        with mock_inputs(*R_INPUTS):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://oai.example.org/"

    def test_prefix_set(self, session):
        with mock_inputs(*R_INPUTS):
            prm = interactiveMode(session)
        assert prm["pref"] == "oai_dc"

    def test_empty_token_retried(self, session):
        """Leerer ResumptionToken → TUI fragt erneut."""
        inputs = [
            "R",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "",
            "tok456",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["res_tok"] == "tok456"

    def test_lowercase_r_accepted(self, session):
        inputs = [
            "r",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "http://oai.example.org/",
            "oai_dc",
            "tok789",
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["res_tok"] == "tok789"


# ---------------------------------------------------------------------------
# Option I – ID-File
# ---------------------------------------------------------------------------

# ID-File-Format (erzeugt von create_id_file):
# Zeile 0: Information: ...
# Zeile 1: date: ...
# Zeile 2: baseurl: http://...
# Zeile 3: sets: ...
# Zeile 4: metadataPrefix: oai_dc

ID_FILE_CONTENT = (
    "Information: Ometha ID-Liste\n"
    "date: 2024-01-01\n"
    "baseurl: http://ids.example.org/oai\n"
    "sets: setA\n"
    "metadataPrefix: oai_dc\n"
    "datengeber: testDG\n"
    "ids:\n"
    "- 'oai:mock:001'\n"
)


class TestOptionI:
    def test_baseurl_from_idfile(self, session, tmp_path):
        id_file = tmp_path / "ids.yaml"
        id_file.write_text(ID_FILE_CONTENT, encoding="utf-8")

        inputs = ["I", "testDG", "/tmp", "4", "xml", "0", str(id_file)]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://ids.example.org/oai"

    def test_prefix_from_idfile(self, session, tmp_path):
        id_file = tmp_path / "ids.yaml"
        id_file.write_text(ID_FILE_CONTENT, encoding="utf-8")

        inputs = ["I", "testDG", "/tmp", "4", "xml", "0", str(id_file)]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["pref"] == "oai_dc"

    def test_idfile_path_stored(self, session, tmp_path):
        id_file = tmp_path / "ids.yaml"
        id_file.write_text(ID_FILE_CONTENT, encoding="utf-8")

        inputs = ["I", "testDG", "/tmp", "4", "xml", "0", str(id_file)]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["id_f"] == str(id_file)

    def test_nonexistent_file_retried(self, session, tmp_path):
        """Nicht-existente Datei → TUI fragt erneut."""
        id_file = tmp_path / "ids.yaml"
        id_file.write_text(ID_FILE_CONTENT, encoding="utf-8")

        inputs = [
            "I",
            "testDG",
            "/tmp",
            "4",
            "xml",
            "0",
            "/does/not/exist.yaml",
            str(id_file),
        ]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["id_f"] == str(id_file)

    def test_lowercase_i_accepted(self, session, tmp_path):
        id_file = tmp_path / "ids.yaml"
        id_file.write_text(ID_FILE_CONTENT, encoding="utf-8")

        inputs = ["i", "testDG", "/tmp", "4", "xml", "0", str(id_file)]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["b_url"] == "http://ids.example.org/oai"


# ---------------------------------------------------------------------------
# Option S – Sets anzeigen
# ---------------------------------------------------------------------------


class TestOptionS:
    """
    Option S ruft get_sets_mprefs() auf, das HTTP-Requests macht
    und bei gefundenen Sets sys.exit(0) aufruft.
    """

    def test_lists_sets_and_exits(self, session, requests_mock):
        from tests.oai_mock import OAIMock

        mock = OAIMock(
            base_url="http://oai.example.org/oai",
            sets={"ddc:500": "Naturwissenschaften", "ddc:600": "Technik"},
        )
        mock.register(requests_mock)

        inputs = ["S", "http://oai.example.org/oai"]
        with mock_inputs(*inputs):
            with pytest.raises(SystemExit) as exc:
                interactiveMode(session)
        assert exc.value.code == 0

    def test_no_sets_does_not_exit_zero(self, session, requests_mock):
        """Keine Sets → log_critical_and_print_and_exit (kein sys.exit(0))."""
        from tests.oai_mock import OAIMock

        mock = OAIMock(
            base_url="http://oai.example.org/oai",
            sets={},
        )
        mock.register(requests_mock)

        # log_critical_and_print_and_exit im ui-Modus ruft einmal input() auf
        # ("Drücken Sie Enter zum Beenden..."), daher ein extra "" am Ende
        inputs = ["S", "http://oai.example.org/oai", ""]
        with mock_inputs(*inputs):
            # Das Programm läuft durch (kein sys.exit(0))
            interactiveMode(session)  # kein Fehler und kein Exit(0)

    def test_lowercase_s_accepted(self, session, requests_mock):
        from tests.oai_mock import OAIMock

        mock = OAIMock(
            base_url="http://oai.example.org/oai",
            sets={"setA": "Set A"},
        )
        mock.register(requests_mock)

        inputs = ["s", "http://oai.example.org/oai"]
        with mock_inputs(*inputs):
            with pytest.raises(SystemExit) as exc:
                interactiveMode(session)
        assert exc.value.code == 0


# ---------------------------------------------------------------------------
# PRM["mode"] im TUI-Modus
# ---------------------------------------------------------------------------


class TestPrmModeTui:
    """interactiveMode() muss PRM["mode"] = "ui" setzen."""

    def test_mode_ui_gesetzt_option_n(self, session):
        with mock_inputs(*N_INPUTS):
            prm = interactiveMode(session)
        assert prm["mode"] == "ui"

    def test_mode_ui_gesetzt_option_r(self, session):
        inputs = ["R", "testDG", "/tmp", "4", "xml", "0", "http://oai.example.org/", "oai_dc", "sometoken"]
        with mock_inputs(*inputs):
            prm = interactiveMode(session)
        assert prm["mode"] == "ui"
