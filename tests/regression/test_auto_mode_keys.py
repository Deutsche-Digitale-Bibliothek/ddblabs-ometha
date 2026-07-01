import yaml
from unittest.mock import patch
from ometha.harvester import change_date
import ometha.helpers as h

"""
Regression: Auto-Modus schrieb 'until-Datum' statt 'untildate' (gefixt in v2.1.1)
"""


def test_auto_mode_writes_untildate_not_legacy_key(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("baseurl: https://example.com\n")

    h.PRM["conf_m"] = True
    h.PRM["auto_m"] = True
    h.PRM["conf_f"] = str(config)
    try:
        change_date("2024-06-01", str(config), key="untildate")
    finally:
        h.PRM["conf_m"] = None
        h.PRM["auto_m"] = None
        h.PRM["conf_f"] = None

    raw = yaml.safe_load(config.read_text())
    assert "untildate" in raw
    assert "until-Datum" not in raw
