import json
import re
from importlib.resources import files

import pytest

from ometha.i18n import detect_lang, t

_ENV = ("OMETHA_LANG", "LC_ALL", "LC_MESSAGES", "LANG")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for v in _ENV:
        monkeypatch.delenv(v, raising=False)


def _cat(lang):
    return json.loads(files("ometha").joinpath("locales", f"{lang}.json").read_text("utf-8"))


def test_catalogs_have_same_keys_and_placeholders():
    en, de = _cat("en"), _cat("de")
    assert en.keys() == de.keys()
    ph = lambda s: sorted(re.findall(r"\{(\w+)", s))  # noqa: E731
    for k in en:
        assert ph(en[k]) == ph(de[k]), k


def test_env_override_and_system_locale(monkeypatch):
    monkeypatch.setenv("LANG", "de_DE.UTF-8")
    assert detect_lang() == "de"
    monkeypatch.setenv("OMETHA_LANG", "en")
    assert detect_lang() == "en"
    monkeypatch.setenv("OMETHA_LANG", "fr")
    assert detect_lang() == "en"


def test_unknown_key_returns_key(monkeypatch):
    monkeypatch.setenv("OMETHA_LANG", "de")
    assert t("no.such.key") == "no.such.key"


def test_translation_switches_language(monkeypatch):
    key = next(k for k in _cat("en") if _cat("en")[k] != _cat("de")[k])
    monkeypatch.setenv("OMETHA_LANG", "en")
    en = t(key, **{p: "x" for p in re.findall(r"\{(\w+)", _cat("en")[key])})
    monkeypatch.setenv("OMETHA_LANG", "de")
    de = t(key, **{p: "x" for p in re.findall(r"\{(\w+)", _cat("de")[key])})
    assert en != de


def test_labels_follow_language(monkeypatch):
    from ometha.helpers import FEHLER

    monkeypatch.setenv("OMETHA_LANG", "de")
    assert "Fehler:" in f"{FEHLER}"
    monkeypatch.setenv("OMETHA_LANG", "en")
    assert "Error:" in f"{FEHLER}"
