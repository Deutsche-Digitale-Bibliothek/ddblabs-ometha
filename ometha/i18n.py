"""Minimal JSON-based translations. Language: OMETHA_LANG, then system locale, else English."""

import json
import locale
import os
from functools import lru_cache
from importlib.resources import files

SUPPORTED = ("en", "de")


def detect_lang() -> str:
    for var in ("OMETHA_LANG", "LC_ALL", "LC_MESSAGES", "LANG"):
        val = os.environ.get(var, "")
        if val:
            code = val.lower()[:2]
            return code if code in SUPPORTED else "en"
    try:
        code = (locale.getlocale()[0] or "")[:2].lower()
    except ValueError:
        code = ""
    return code if code in SUPPORTED else "en"


@lru_cache(maxsize=None)
def _load(lang: str) -> dict:
    return json.loads(
        files("ometha").joinpath("locales", f"{lang}.json").read_text("utf-8")
    )


def t(key: str, /, **kw) -> str:
    """Translate key; fall back to English, then to the key itself."""
    text = _load(detect_lang()).get(key) or _load("en").get(key) or key
    return text.format(**kw) if kw else text
