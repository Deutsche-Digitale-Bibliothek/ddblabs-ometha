"""
Tests für alle CLI-Optionen und Kombinationen.

Getestete Subcommands:
  - default: --baseurl, --metadataprefix, --datengeber, --set, --fromdate,
             --untildate, --resumptiontoken, --parallel, --timeout,
             --outputfolder, --debug, --exporttype
  - conf:    --conf, --auto, --exporttype, --debug
  - auto:    --url (mit Query-Parametern)
  - ids:     --idfile, --datengeber
  - parse_set_values(): alleinstehende Hilfsfunktion

Technik: sys.argv wird per monkeypatch gesetzt, read_yaml_file wird gemockt,
das globale PRM-Dict wird vor jedem Test zurückgesetzt.
"""

import os
import re
import sys
from datetime import datetime, timedelta

import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ---------------------------------------------------------------------------
# Hilfsfunktionen / Fixtures
# ---------------------------------------------------------------------------


def reset_prm():
    """Setzt das globale PRM-Dict auf None-Werte zurück."""
    import ometha.helpers as h
    import ometha.cli as cli_mod

    for key in h.PRM:
        h.PRM[key] = None
    # cli.PRM ist dasselbe Objekt – kein weiterer Import nötig


@pytest.fixture(autouse=True)
def clean_prm():
    """Vor jedem Test PRM zurücksetzen, damit Tests voneinander unabhängig sind."""
    reset_prm()
    yield
    reset_prm()


def parse_with(argv: list):
    """
    Setzt sys.argv und ruft parseargs() auf.
    Gibt das zurückgegebene PRM-Dict zurück.
    """
    from ometha.cli import parseargs

    with patch("sys.argv", ["ometha"] + argv):
        return parseargs()


# ---------------------------------------------------------------------------
# parse_set_values()
# ---------------------------------------------------------------------------


class TestParseSetValues:
    """Einzel-Funktion ohne CLI-Aufruf."""

    def setup_method(self):
        from ometha.cli import parse_set_values

        self.psv = parse_set_values

    def test_single_set(self):
        result = self.psv("ddc:500")
        assert result == {"additive": ["ddc:500"], "intersection": []}

    def test_comma_separated_additive(self):
        result = self.psv("setA,setB,setC")
        assert result["additive"] == ["setA", "setB", "setC"]
        assert result["intersection"] == []

    def test_slash_separates_additive_and_intersection(self):
        result = self.psv("setA,setB/setC,setD")
        assert result["additive"] == ["setA", "setB"]
        assert result["intersection"] == ["setC", "setD"]

    def test_slash_with_single_values(self):
        result = self.psv("setA/setB")
        assert result["additive"] == ["setA"]
        assert result["intersection"] == ["setB"]

    def test_whitespace_stripped(self):
        result = self.psv(" setA , setB ")
        assert result["additive"] == ["setA", "setB"]

    def test_none_returns_empty_dicts(self):
        result = self.psv(None)
        assert result == {"additive": [], "intersection": []}


# ---------------------------------------------------------------------------
# `default` Subcommand
# ---------------------------------------------------------------------------


class TestDefaultCommand:
    def test_minimal_required_args(self):
        prm = parse_with(["default", "-b", "http://oai.example.org/", "-m", "oai_dc"])
        assert prm["b_url"] == "http://oai.example.org/"
        assert prm["pref"] == "oai_dc"

    def test_long_flags_baseurl_and_prefix(self):
        prm = parse_with(
            [
                "default",
                "--baseurl",
                "http://oai.example.org/",
                "--metadataprefix",
                "mods",
            ]
        )
        assert prm["b_url"] == "http://oai.example.org/"
        assert prm["pref"] == "mods"

    def test_datengeber_short(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-d", "meinDG"]
        )
        assert prm["dat_geb"] == "meinDG"

    def test_datengeber_long(self):
        prm = parse_with(
            [
                "default",
                "-b",
                "http://x.org/",
                "-m",
                "oai_dc",
                "--datengeber",
                "langer_name",
            ]
        )
        assert prm["dat_geb"] == "langer_name"

    def test_datengeber_default_is_timestr(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        # Der Default ist TIMESTR – ein nicht-leerer String
        assert prm["dat_geb"] is not None
        assert len(prm["dat_geb"]) > 0

    def test_fromdate_valid(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "2023-06-01"]
        )
        assert prm["f_date"] == "2023-06-01"

    def test_fromdate_invalid_is_none(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "not-a-date"]
        )
        assert prm["f_date"] is None

    def test_untildate_valid(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-u", "2024-12-31"]
        )
        assert prm["u_date"] == "2024-12-31"

    def test_untildate_invalid_is_none(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-u", "31.12.2024"]
        )
        assert prm["u_date"] is None

    def test_fromdate_and_untildate_together(self):
        prm = parse_with(
            [
                "default",
                "-b",
                "http://x.org/",
                "-m",
                "oai_dc",
                "-f",
                "2022-01-01",
                "-u",
                "2022-12-31",
            ]
        )
        assert prm["f_date"] == "2022-01-01"
        assert prm["u_date"] == "2022-12-31"

    def test_resumptiontoken(self):
        prm = parse_with(
            [
                "default",
                "-b",
                "http://x.org/",
                "-m",
                "oai_dc",
                "--resumptiontoken",
                "abc123token",
            ]
        )
        assert prm["res_tok"] == "abc123token"

    def test_no_resumptiontoken_is_none(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["res_tok"] is None

    def test_set_single(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "ddc:500"]
        )
        assert prm["sets"] is not None
        # -s liefert eine Liste von dicts
        sets = prm["sets"]
        assert isinstance(sets, list)
        assert sets[0]["additive"] == ["ddc:500"]

    def test_set_multiple(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "setA,setB"]
        )
        assert prm["sets"][0]["additive"] == ["setA", "setB"]

    def test_set_with_intersection(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "setA/setB"]
        )
        assert prm["sets"][0]["additive"] == ["setA"]
        assert prm["sets"][0]["intersection"] == ["setB"]

    def test_debug_flag(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc", "--debug"])
        assert prm["debug"] is True

    def test_no_debug_is_false(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["debug"] is False

    def test_parallel_short(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc", "-p", "8"])
        assert prm["n_procs"] == 8

    def test_parallel_default(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["n_procs"] is None

    def test_timeout_short(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc", "-t", "30"])
        assert prm["timeout"] == 30

    def test_timeout_default(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["timeout"] == 0

    def test_outputfolder_short(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-o", "/tmp/myfolder"]
        )
        assert prm["out_f"] == "/tmp/myfolder"

    def test_outputfolder_default_is_cwd(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["out_f"] == os.getcwd()

    def test_exporttype_json(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-e", "json"]
        )
        assert prm["exp_type"] == "json"

    def test_exporttype_default_xml(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["exp_type"] == "xml"

    def test_all_options_combined(self):
        prm = parse_with(
            [
                "default",
                "-b",
                "http://oai.example.org/",
                "-m",
                "oai_dc",
                "-d",
                "meinDG",
                "-f",
                "2023-01-01",
                "-u",
                "2023-12-31",
                "-s",
                "setA,setB",
                "--resumptiontoken",
                "tok42",
                "-p",
                "4",
                "-t",
                "5",
                "-o",
                "/tmp/harvest",
                "-e",
                "json",
                "--debug",
            ]
        )
        assert prm["b_url"] == "http://oai.example.org/"
        assert prm["pref"] == "oai_dc"
        assert prm["dat_geb"] == "meinDG"
        assert prm["f_date"] == "2023-01-01"
        assert prm["u_date"] == "2023-12-31"
        assert prm["sets"][0]["additive"] == ["setA", "setB"]
        assert prm["res_tok"] == "tok42"
        assert prm["n_procs"] == 4
        assert prm["timeout"] == 5
        assert prm["out_f"] == "/tmp/harvest"
        assert prm["exp_type"] == "json"
        assert prm["debug"] is True

    def test_conf_mode_false_for_default(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["conf_m"] is not True

    def test_auto_mode_false_for_default(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["auto_m"] is not True


# ---------------------------------------------------------------------------
# Natural language date parsing
# ---------------------------------------------------------------------------

ISODATEREGEX = r"^\d{4}-\d{2}-\d{2}$"


class TestParseNaturalDate:
    """Tests für parse_natural_date() aus helpers.py."""

    def setup_method(self):
        from ometha.helpers import parse_natural_date

        self.pnd = parse_natural_date

    def _assert_iso_date(self, result):
        assert result is not None
        assert re.match(ISODATEREGEX, result), f"Kein ISO8601-Datum: {result!r}"

    def test_days(self):
        result = self.pnd("1d")
        self._assert_iso_date(result)
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert result == expected

    def test_multiple_days(self):
        result = self.pnd("7d")
        expected = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        assert result == expected

    def test_hours(self):
        result = self.pnd("2h")
        self._assert_iso_date(result)
        expected = (datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d")
        assert result == expected

    def test_minutes(self):
        result = self.pnd("20m")
        self._assert_iso_date(result)
        expected = (datetime.now() - timedelta(minutes=20)).strftime("%Y-%m-%d")
        assert result == expected

    def test_weeks(self):
        result = self.pnd("3w")
        expected = (datetime.now() - timedelta(weeks=3)).strftime("%Y-%m-%d")
        assert result == expected

    def test_months(self):
        result = self.pnd("1mo")
        self._assert_iso_date(result)
        expected = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        assert result == expected

    def test_invalid_returns_none(self):
        assert self.pnd("not-a-date") is None

    def test_plain_number_returns_none(self):
        assert self.pnd("42") is None

    def test_unknown_unit_returns_none(self):
        assert self.pnd("5y") is None

    def test_whitespace_stripped(self):
        result = self.pnd(" 1d ")
        self._assert_iso_date(result)


class TestNaturalDateViaCLI:
    """Integrationstests: natürlichsprachige Datumsangaben über CLI."""

    def test_fromdate_natural_days(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "1d"]
        )
        assert prm["f_date"] is not None
        assert re.match(ISODATEREGEX, prm["f_date"])
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        assert prm["f_date"] == expected

    def test_fromdate_natural_hours(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "6h"]
        )
        assert prm["f_date"] is not None
        assert re.match(ISODATEREGEX, prm["f_date"])

    def test_fromdate_natural_minutes(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "20m"]
        )
        assert prm["f_date"] is not None
        assert re.match(ISODATEREGEX, prm["f_date"])

    def test_untildate_natural_weeks(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-u", "2w"]
        )
        assert prm["u_date"] is not None
        assert re.match(ISODATEREGEX, prm["u_date"])

    def test_untildate_natural_months(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-u", "1mo"]
        )
        assert prm["u_date"] is not None
        assert re.match(ISODATEREGEX, prm["u_date"])

    def test_iso_date_still_works(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "2024-01-15"]
        )
        assert prm["f_date"] == "2024-01-15"

    def test_invalid_date_still_none(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-f", "gestern"]
        )
        assert prm["f_date"] is None


# ---------------------------------------------------------------------------
# PRM["mode"] – wird in allen Subcommands auf "cli" gesetzt
# ---------------------------------------------------------------------------


class TestPrmMode:
    def test_mode_cli_default(self):
        prm = parse_with(["default", "-b", "http://x.org/", "-m", "oai_dc"])
        assert prm["mode"] == "cli"

    def test_mode_cli_auto(self):
        prm = parse_with(["auto", "-u", "http://x.org/?metadataPrefix=oai_dc"])
        assert prm["mode"] == "cli"

    def test_mode_cli_conf(self):
        data = {
            "baseurl": "http://x.org/",
            "sets": [],
            "metadataPrefix": "oai_dc",
            "datengeber": "dg",
            "timeout": 0,
            "debug": False,
            "outputfolder": "/tmp",
            "from-Datum": None,
            "until-Datum": None,
            "fromdate": None,
            "untildate": None,
        }

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert prm["mode"] == "cli"

    def test_mode_cli_ids(self):
        data = {"baseurl": "http://x.org/", "sets": ["setA"], "metadataPrefix": "oai_dc"}

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["ids", "-i", "ids.yaml"])
        assert prm["mode"] == "cli"


# ---------------------------------------------------------------------------
# "komplett" im default-Modus
# ---------------------------------------------------------------------------


class TestKomplettInDefaultMode:
    def test_komplett_filtered_from_additive(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "komplett"]
        )
        assert prm["sets"][0]["additive"] == []

    def test_komplett_case_insensitive(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "Komplett"]
        )
        assert prm["sets"][0]["additive"] == []

    def test_regular_set_not_filtered(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "ddc:500"]
        )
        assert prm["sets"][0]["additive"] == ["ddc:500"]

    def test_komplett_in_set_list_removed(self):
        prm = parse_with(
            ["default", "-b", "http://x.org/", "-m", "oai_dc", "-s", "ddc:500,komplett"]
        )
        assert "komplett" not in prm["sets"][0]["additive"]
        assert "ddc:500" in prm["sets"][0]["additive"]


# ---------------------------------------------------------------------------
# conf-Modus: beide Datums-Key-Namen
# ---------------------------------------------------------------------------


class TestConfDateKeys:
    """Conf-Modus muss sowohl "from-Datum"/"until-Datum" als auch
    "fromdate"/"untildate" aus der YAML lesen können."""

    def _parse_conf_with_yaml(self, yaml_data):
        def mock_read_yaml(path, keys, default=None):
            return [yaml_data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            return parse_with(["conf", "-c", "config.yaml"])

    def _base_yaml(self):
        return {
            "baseurl": "http://x.org/",
            "sets": [],
            "metadataPrefix": "oai_dc",
            "datengeber": "dg",
            "timeout": 0,
            "debug": False,
            "outputfolder": "/tmp",
            "from-Datum": None,
            "until-Datum": None,
            "fromdate": None,
            "untildate": None,
        }

    def test_auto_key_from_datum(self):
        data = {**self._base_yaml(), "from-Datum": "2025-01-01"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["f_date"] == "2025-01-01"

    def test_auto_key_until_datum(self):
        data = {**self._base_yaml(), "until-Datum": "2025-12-31"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["u_date"] == "2025-12-31"

    def test_manual_key_fromdate(self):
        data = {**self._base_yaml(), "fromdate": "2024-06-01"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["f_date"] == "2024-06-01"

    def test_manual_key_untildate(self):
        data = {**self._base_yaml(), "untildate": "2024-06-30"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["u_date"] == "2024-06-30"

    def test_auto_key_takes_priority_over_manual(self):
        data = {**self._base_yaml(), "from-Datum": "2025-01-01", "fromdate": "2020-01-01"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["f_date"] == "2025-01-01"

    def test_datetime_granularity_accepted(self):
        data = {**self._base_yaml(), "from-Datum": "2025-06-01T10:00:00Z"}
        prm = self._parse_conf_with_yaml(data)
        assert prm["f_date"] == "2025-06-01T10:00:00Z"


# ---------------------------------------------------------------------------
# `conf` Subcommand
# ---------------------------------------------------------------------------

YAML_CONF_MINIMAL = {
    "baseurl": "http://conf.example.org/oai",
    "sets": ["komplett"],
    "from-Datum": None,
    "until-Datum": None,
    "metadataPrefix": "oai_dc",
    "datengeber": "testDG",
    "timeout": 0,
    "debug": False,
    "outputfolder": "/tmp",
}


class TestConfCommand:
    """
    Testet parseargs() im conf-Modus.
    read_yaml_file wird gemockt, damit keine echte Datei benötigt wird.
    """

    def _parse_conf(self, extra_argv=None, yaml_data=None):
        data = yaml_data or YAML_CONF_MINIMAL

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        argv = ["conf", "-c", "config.yaml"] + (extra_argv or [])
        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            return parse_with(argv)

    def test_conf_mode_is_set(self):
        prm = self._parse_conf()
        assert prm["conf_m"] is True

    def test_conf_f_is_path(self):
        prm = self._parse_conf()
        assert prm["conf_f"] == "config.yaml"

    def test_auto_mode_default_false(self):
        prm = self._parse_conf()
        assert prm["auto_m"] is False

    def test_auto_mode_flag(self):
        prm = self._parse_conf(extra_argv=["--auto"])
        assert prm["auto_m"] is True

    def test_auto_short_flag(self):
        prm = self._parse_conf(extra_argv=["-a"])
        assert prm["auto_m"] is True

    def test_debug_from_yaml_true(self):
        """In conf-Modus kommt debug aus der YAML-Datei, nicht vom --debug Flag."""
        data = {**YAML_CONF_MINIMAL, "debug": True}
        prm = self._parse_conf(yaml_data=data)
        assert prm["debug"] is True

    def test_debug_from_yaml_false(self):
        prm = self._parse_conf()  # YAML_CONF_MINIMAL hat debug: False
        assert prm["debug"] is False

    def test_exporttype_json(self):
        prm = self._parse_conf(extra_argv=["-e", "json"])
        assert prm["exp_type"] == "json"

    def test_baseurl_read_from_yaml(self):
        prm = self._parse_conf()
        assert prm["b_url"] == "http://conf.example.org/oai"

    def test_prefix_read_from_yaml(self):
        prm = self._parse_conf()
        assert prm["pref"] == "oai_dc"

    def test_datengeber_read_from_yaml(self):
        prm = self._parse_conf()
        assert prm["dat_geb"] == "testDG"

    def test_komplett_sets_becomes_empty_additive(self):
        """sets: [komplett] soll als additive=[] behandelt werden."""
        prm = self._parse_conf()
        # komplett wird herausgefiltert
        sets = prm["sets"]
        assert isinstance(sets, list)
        assert sets[0]["additive"] == []

    def test_named_sets_preserved(self):
        data = {**YAML_CONF_MINIMAL, "sets": ["ddc:500", "ddc:600"]}
        prm = self._parse_conf(yaml_data=data)
        assert "ddc:500" in prm["sets"][0]["additive"]
        assert "ddc:600" in prm["sets"][0]["additive"]

    def test_fallback_url_key(self):
        """Wenn 'baseurl' fehlt, soll 'url' als Fallback genutzt werden."""
        data = {**YAML_CONF_MINIMAL}
        data.pop("baseurl")
        data["url"] = "http://fallback.example.org/oai"

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert prm["b_url"] == "http://fallback.example.org/oai"

    def test_fallback_mprefix_key(self):
        """Wenn 'metadataPrefix' fehlt, soll 'mprefix' als Fallback genutzt werden."""
        data = {**YAML_CONF_MINIMAL}
        data.pop("metadataPrefix")
        data["mprefix"] = "mods"

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert prm["pref"] == "mods"

    def test_fallback_name_key(self):
        """Wenn 'datengeber' fehlt, soll 'name' als Fallback genutzt werden."""
        data = {**YAML_CONF_MINIMAL}
        data.pop("datengeber")
        data["name"] = "fallbackName"

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert prm["dat_geb"] == "fallbackName"

    def test_outputfolder_from_yaml(self):
        data = {**YAML_CONF_MINIMAL, "outputfolder": "/data/harvest"}

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert prm["out_f"] == "/data/harvest"

    def test_n_procs_is_none_when_not_in_yaml(self):
        """numberofprocesses nicht in YAML → None (auto-scaling in main.py)."""
        prm = self._parse_conf()
        assert prm["n_procs"] is None

    def test_numberofprocesses_from_yaml(self):
        """numberofprocesses in YAML wird als n_procs übernommen."""
        data = {**YAML_CONF_MINIMAL, "numberofprocesses": 8}
        prm = self._parse_conf(yaml_data=data)
        assert prm["n_procs"] == 8

    def test_trailing_whitespace_stripped_from_baseurl(self):
        data = {**YAML_CONF_MINIMAL, "baseurl": "http://x.org/oai/ "}

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        # re.sub(r"/\s$", "", ...) entfernt abschließenden Slash+Leerzeichen
        assert not prm["b_url"].endswith(" ")

    def test_trailing_whitespace_stripped_from_prefix(self):
        data = {**YAML_CONF_MINIMAL, "metadataPrefix": "oai_dc "}

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["conf", "-c", "config.yaml"])
        assert not prm["pref"].endswith(" ")

    def test_no_log_flag(self):
        prm = self._parse_conf(extra_argv=["--no-log"])
        assert prm["no_log"] is True

    def test_no_log_default_false(self):
        prm = self._parse_conf()
        assert prm["no_log"] is False

    def test_cleanup_on_empty_flag(self):
        prm = self._parse_conf(extra_argv=["--cleanup-on-empty"])
        assert prm["cleanup_empty"] is True

    def test_cleanup_on_empty_default_false(self):
        prm = self._parse_conf()
        assert prm["cleanup_empty"] is False

    def test_from_datum_read_from_yaml(self):
        data = {**YAML_CONF_MINIMAL, "from-Datum": "2025-01-01"}
        prm = self._parse_conf(yaml_data=data)
        assert prm["f_date"] == "2025-01-01"

    def test_until_datum_read_from_yaml(self):
        data = {**YAML_CONF_MINIMAL, "until-Datum": "2025-12-31"}
        prm = self._parse_conf(yaml_data=data)
        assert prm["u_date"] == "2025-12-31"

    def test_from_datum_datetime_read_from_yaml(self):
        """Datetime-Granularität (YYYY-MM-DDThh:mm:ssZ) muss akzeptiert werden."""
        data = {**YAML_CONF_MINIMAL, "from-Datum": "2025-06-01T10:00:00Z"}
        prm = self._parse_conf(yaml_data=data)
        assert prm["f_date"] == "2025-06-01T10:00:00Z"

    def test_from_datum_none_when_missing(self):
        prm = self._parse_conf()
        assert prm["f_date"] is None

    def test_until_datum_none_when_missing(self):
        prm = self._parse_conf()
        assert prm["u_date"] is None


# ---------------------------------------------------------------------------
# `auto` Subcommand
# ---------------------------------------------------------------------------


class TestAutoCommand:
    def test_minimal_url(self):
        prm = parse_with(
            [
                "auto",
                "-u",
                "http://oai.example.org/?verb=ListIdentifiers&metadataPrefix=oai_dc",
            ]
        )
        assert prm["b_url"] == "http://oai.example.org/"
        assert prm["pref"] == "oai_dc"
        assert prm["auto_m"] is True

    def test_url_with_set(self):
        prm = parse_with(
            [
                "auto",
                "-u",
                "http://oai.example.org/?verb=ListIdentifiers&metadataPrefix=oai_dc&set=ddc:500",
            ]
        )
        assert prm["sets"] == ["ddc:500"]

    def test_url_with_from_date(self):
        prm = parse_with(
            [
                "auto",
                "-u",
                "http://oai.example.org/?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2023-01-01",
            ]
        )
        assert prm["f_date"] == ["2023-01-01"]

    def test_url_with_until_date(self):
        prm = parse_with(
            [
                "auto",
                "-u",
                "http://oai.example.org/?verb=ListIdentifiers&metadataPrefix=oai_dc&until=2023-12-31",
            ]
        )
        assert prm["u_date"] == ["2023-12-31"]

    def test_url_without_query_params(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/oai"])
        assert prm["b_url"] == "http://oai.example.org/oai"
        assert prm["pref"] is None

    def test_auto_mode_flag(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/"])
        assert prm["auto_m"] is True

    def test_parallel_option(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/", "-p", "8"])
        assert prm["n_procs"] == 8

    def test_timeout_option(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/", "-t", "60"])
        assert prm["timeout"] == 60

    def test_outputfolder_option(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/", "-o", "/tmp/auto"])
        assert prm["out_f"] == "/tmp/auto"

    def test_exporttype_option(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/", "-e", "json"])
        assert prm["exp_type"] == "json"

    def test_debug_flag(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/", "--debug"])
        assert prm["debug"] is True

    def test_dat_geb_is_timestamp(self):
        prm = parse_with(["auto", "-u", "http://oai.example.org/"])
        # dat_geb wird auf time.strftime(...) gesetzt – nicht None
        assert prm["dat_geb"] is not None
        assert len(prm["dat_geb"]) > 0

    def test_base_url_path_preserved(self):
        """Pfadkomponenten der URL sollen erhalten bleiben."""
        prm = parse_with(
            ["auto", "-u", "http://oai.example.org/oai/request?metadataPrefix=oai_dc"]
        )
        assert prm["b_url"] == "http://oai.example.org/oai/request"


# ---------------------------------------------------------------------------
# `ids` Subcommand
# ---------------------------------------------------------------------------

YAML_IDS_MINIMAL = {
    "baseurl": "http://ids.example.org/oai",
    "sets": ["setA"],
    "metadataPrefix": "oai_dc",
}


class TestIdsCommand:
    """
    Testet parseargs() im ids-Modus.
    read_yaml_file wird gemockt.
    """

    def _parse_ids(self, extra_argv=None, yaml_data=None):
        data = yaml_data or YAML_IDS_MINIMAL

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        argv = ["ids", "-i", "ids.yaml"] + (extra_argv or [])
        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            return parse_with(argv)

    def test_id_f_is_set(self):
        prm = self._parse_ids()
        assert prm["id_f"] == "ids.yaml"

    def test_baseurl_from_yaml(self):
        prm = self._parse_ids()
        assert prm["b_url"] == "http://ids.example.org/oai"

    def test_prefix_from_yaml(self):
        prm = self._parse_ids()
        assert prm["pref"] == "oai_dc"

    def test_datengeber_default(self):
        prm = self._parse_ids()
        assert prm["dat_geb"] is not None

    def test_datengeber_custom(self):
        prm = self._parse_ids(extra_argv=["-d", "meinDG"])
        assert prm["dat_geb"] == "meinDG"

    def test_sets_from_yaml(self):
        prm = self._parse_ids()
        sets = prm["sets"]
        assert isinstance(sets, list)
        assert sets[0]["additive"] == ["setA"]

    def test_komplett_sets_become_none(self):
        data = {**YAML_IDS_MINIMAL, "sets": ["komplett"]}
        prm = self._parse_ids(yaml_data=data)
        assert prm["sets"] is None

    def test_parallel_option(self):
        prm = self._parse_ids(extra_argv=["-p", "4"])
        assert prm["n_procs"] == 4

    def test_timeout_option(self):
        prm = self._parse_ids(extra_argv=["-t", "10"])
        assert prm["timeout"] == 10

    def test_outputfolder_option(self):
        prm = self._parse_ids(extra_argv=["-o", "/tmp/ids_out"])
        assert prm["out_f"] == "/tmp/ids_out"

    def test_exporttype_option(self):
        prm = self._parse_ids(extra_argv=["-e", "json"])
        assert prm["exp_type"] == "json"

    def test_debug_flag(self):
        prm = self._parse_ids(extra_argv=["--debug"])
        assert prm["debug"] is True

    def test_idfile_long_flag(self):
        data = YAML_IDS_MINIMAL

        def mock_read_yaml(path, keys, default=None):
            return [data.get(k, default) for k in keys]

        with patch("ometha.cli.read_yaml_file", side_effect=mock_read_yaml):
            prm = parse_with(["ids", "--idfile", "myids.yaml"])
        assert prm["id_f"] == "myids.yaml"
