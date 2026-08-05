"""Tests für main.py – insbesondere collect_ids() und das Gentle-Mode-Verhalten
beim Deaktivieren des Pipelinings."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from ometha.main import collect_ids


@pytest.fixture
def prm_base():
    """Minimales PRM-Dict für collect_ids-Tests."""
    return {
        "b_url": "http://mock-oai.test/oai",
        "pref": "oai_dc",
        "gentle": False,
        "mode": "cli",
        "debug": False,
    }


@pytest.fixture
def session():
    return MagicMock()


class TestCollectIdsNormalMode:
    """Ohne Gentle Mode muss der Generator unverändert durchgereicht werden (Pipelining aktiv)."""

    def test_returns_generator_not_list(self, prm_base, session):
        def fake_generator(PRM, set, session):
            yield ["id1", "id2"]
            yield ["id3"]

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm_base, None, session)
        # Kein Pipelining-Bruch: das Ergebnis muss iterierbar sein, aber keine Liste
        assert not isinstance(result, list)

    def test_generator_yields_correct_pages(self, prm_base, session):
        def fake_generator(PRM, set, session):
            yield ["id1", "id2"]
            yield ["id3"]

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm_base, None, session)
            pages = list(result)
        assert pages == [["id1", "id2"], ["id3"]]

    def test_set_argument_passed_through(self, prm_base, session):
        mock_gen = MagicMock(return_value=iter([["id1"]]))
        with patch("ometha.main.generate_id_harvesting_url", mock_gen):
            collect_ids(prm_base, "setA", session)
        mock_gen.assert_called_once_with(prm_base, "setA", session)


class TestCollectIdsGentleMode:
    """Im Gentle Mode muss vollständig eingesammelt werden (kein Pipelining)."""

    def test_returns_flat_list(self, prm_base, session):
        prm = {**prm_base, "gentle": True}

        def fake_generator(PRM, set, session):
            yield ["id1", "id2"]
            yield ["id3"]

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm, None, session)
        assert isinstance(result, list)

    def test_all_ids_collected_across_pages(self, prm_base, session):
        prm = {**prm_base, "gentle": True}

        def fake_generator(PRM, set, session):
            yield ["id1", "id2"]
            yield ["id3"]

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm, None, session)
        assert result == ["id1", "id2", "id3"]

    def test_single_page_still_returns_list(self, prm_base, session):
        prm = {**prm_base, "gentle": True}

        def fake_generator(PRM, set, session):
            yield ["only_id"]

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm, None, session)
        assert result == ["only_id"]

    def test_empty_generator_returns_empty_list(self, prm_base, session):
        prm = {**prm_base, "gentle": True}

        def fake_generator(PRM, set, session):
            return iter([])

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            result = collect_ids(prm, None, session)
        assert result == []

    def test_set_argument_passed_through(self, prm_base, session):
        prm = {**prm_base, "gentle": True}
        mock_gen = MagicMock(return_value=iter([["id1"]]))
        with patch("ometha.main.generate_id_harvesting_url", mock_gen):
            collect_ids(prm, "setB", session)
        mock_gen.assert_called_once_with(prm, "setB", session)


class TestCollectIdsGentleModeExhaustsGenerator:
    """Regression: im Gentle Mode muss der Generator vollständig konsumiert werden,
    bevor collect_ids zurückkehrt – kein verzögertes Auswerten."""

    def test_generator_fully_exhausted_before_return(self, prm_base, session):
        prm = {**prm_base, "gentle": True}
        call_log = []

        def fake_generator(PRM, set, session):
            call_log.append("page1")
            yield ["id1"]
            call_log.append("page2")
            yield ["id2"]
            call_log.append("done")

        with patch("ometha.main.generate_id_harvesting_url", side_effect=fake_generator):
            collect_ids(prm, None, session)

        assert call_log == ["page1", "page2", "done"]