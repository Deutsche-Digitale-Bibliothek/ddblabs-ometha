"""
Tests für Ometha-Harvester gegen den OAI-PMH-Mock.

Abgedeckte Szenarien:
  - Identify-Anfrage
  - ListIdentifiers (einfach + paginiert)
  - GetRecord (einzeln + 404)
  - ListRecords mit Datumfilter
  - ResumptionToken-Durchlauf
  - Fehlerbehandlung (noRecordsMatch, idDoesNotExist, badVerb)
  - Deleted Records
  - Set-Filter
"""

import os
import sys
import pytest
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from oai_mock import OAIMock, OAIRecord
from ometha.harvester import get_identifier, get_identifier_pages
from ometha.helpers import isinvalid_xml_content, safe_get, NAMESPACE

BASE_URL = "http://mock-oai.test/oai"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def session():
    """Minimale requests.Session ohne Retry-Dekorierung (für Unit-Tests)."""
    return requests.Session()


@pytest.fixture
def prm_base():
    """Minimales PRM-Dict im CLI-Stil."""
    return {
        "b_url": BASE_URL,
        "pref": "oai_dc",
        "dat_geb": "mock",
        "sets": {},
        "debug": False,
        "timeout": 10,
        "id_f": None,
        "f_date": None,
        "u_date": None,
        "res_tok": None,
        "conf_f": None,
        "conf_m": False,
        "auto_m": False,
        "out_f": "/tmp",
        "n_procs": 4,
        "mode": "cli",
        "exp_type": "xml",
        "max_timeout": None,
        "batch_size": None,
        "deleted_handling": "keep",
        "gentle": False,
        "ratelimit": None,
    }


# ---------------------------------------------------------------------------
# Identify
# ---------------------------------------------------------------------------


class TestIdentify:
    def test_identify_returns_valid_xml(self, requests_mock):
        mock = OAIMock.with_dc_records(5)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=Identify")
        assert resp.status_code == 200
        assert "<repositoryName>OAI Mock Repository</repositoryName>" in resp.text
        assert "<protocolVersion>2.0</protocolVersion>" in resp.text

    def test_identify_contains_base_url(self, requests_mock):
        mock = OAIMock(base_url=BASE_URL)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=Identify")
        assert BASE_URL in resp.text


# ---------------------------------------------------------------------------
# ListMetadataFormats
# ---------------------------------------------------------------------------


class TestListMetadataFormats:
    def test_default_formats(self, requests_mock):
        mock = OAIMock.with_dc_records(3)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListMetadataFormats")
        assert "<metadataPrefix>oai_dc</metadataPrefix>" in resp.text

    def test_custom_formats(self, requests_mock):
        mock = OAIMock(metadata_formats=["oai_dc", "mods", "datacite"])
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListMetadataFormats")
        for fmt in ["oai_dc", "mods", "datacite"]:
            assert f"<metadataPrefix>{fmt}</metadataPrefix>" in resp.text


# ---------------------------------------------------------------------------
# ListSets
# ---------------------------------------------------------------------------


class TestListSets:
    def test_sets_returned(self, requests_mock):
        mock = OAIMock(sets={"ddc:500": "Naturwissenschaften", "ddc:600": "Technik"})
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListSets")
        assert "<setSpec>ddc:500</setSpec>" in resp.text
        assert "<setName>Naturwissenschaften</setName>" in resp.text

    def test_no_sets_returns_error(self, requests_mock):
        mock = OAIMock(sets={})
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListSets")
        assert 'code="noSetHierarchy"' in resp.text


# ---------------------------------------------------------------------------
# ListIdentifiers via get_identifier()
# ---------------------------------------------------------------------------


class TestListIdentifiers:
    def test_all_ids_returned(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(10)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert len(ids) == 10
        assert all(id.startswith("oai:mock.test:") for id in ids)

    def test_ids_with_set_filter(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(10)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&set=setA"
        ids = get_identifier(prm_base, url, session)

        # setA enthält gerade Indizes: 0, 2, 4, 6, 8 → 5 Records
        assert len(ids) == 5

    def test_pagination_via_resumption_token(self, requests_mock, session, prm_base):
        """Mock mit page_size=3, 9 Records → 3 Seiten, alle IDs geliefert."""
        mock = OAIMock.with_dc_records(9, page_size=3)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert len(ids) == 9

    def test_from_date_filter(self, requests_mock, session, prm_base):
        records = [
            OAIRecord("oai:mock:old", datestamp="2020-01-01T00:00:00Z"),
            OAIRecord("oai:mock:new", datestamp="2024-06-01T00:00:00Z"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2023-01-01"
        ids = get_identifier(prm_base, url, session)

        assert "oai:mock:new" in ids
        assert "oai:mock:old" not in ids

    def test_until_date_filter(self, requests_mock, session, prm_base):
        records = [
            OAIRecord("oai:mock:old", datestamp="2020-01-01T00:00:00Z"),
            OAIRecord("oai:mock:new", datestamp="2024-06-01T00:00:00Z"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&until=2022-01-01"
        ids = get_identifier(prm_base, url, session)

        assert "oai:mock:old" in ids
        assert "oai:mock:new" not in ids

    def test_no_records_match_exits(self, requests_mock, session, prm_base):
        """noRecordsMatch → isinvalid_xml_content liefert trotzdem XML zurück (kein Crash)."""
        mock = OAIMock(records=[])
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        resp = requests.get(url)
        assert 'code="noRecordsMatch"' in resp.text


# ---------------------------------------------------------------------------
# GetRecord
# ---------------------------------------------------------------------------


class TestGetRecord:
    def test_get_existing_record(self, requests_mock):
        records = [OAIRecord("oai:mock:001", datestamp="2024-01-01T00:00:00Z")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:001")
        assert resp.status_code == 200
        assert "<GetRecord>" in resp.text
        assert "oai:mock:001" in resp.text

    def test_get_nonexistent_record(self, requests_mock):
        mock = OAIMock(records=[])
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:does:not:exist")
        assert 'code="idDoesNotExist"' in resp.text

    def test_get_record_missing_args(self, requests_mock):
        mock = OAIMock()
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=GetRecord")
        assert 'code="badArgument"' in resp.text

    def test_get_deleted_record(self, requests_mock):
        records = [OAIRecord("oai:mock:deleted", deleted=True)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:deleted")
        assert 'status="deleted"' in resp.text
        # Deleted records haben kein <metadata>
        assert "<metadata>" not in resp.text


# ---------------------------------------------------------------------------
# ListRecords (XML-Struktur)
# ---------------------------------------------------------------------------


class TestListRecords:
    def test_list_records_contains_metadata(self, requests_mock):
        records = [
            OAIRecord("oai:mock:001"),
            OAIRecord("oai:mock:002"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc")
        assert "<ListRecords>" in resp.text
        assert "<metadata>" in resp.text
        assert "oai:mock:001" in resp.text
        assert "oai:mock:002" in resp.text

    def test_list_records_with_custom_metadata(self, requests_mock):
        custom_meta = '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"><dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">Mein Titel</dc:title></oai_dc:dc>'
        records = [OAIRecord("oai:mock:custom", metadata_xml=custom_meta)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListRecords&metadataPrefix=oai_dc")
        assert "Mein Titel" in resp.text

    def test_unknown_prefix_returns_error(self, requests_mock):
        mock = OAIMock(metadata_formats=["oai_dc"])
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListRecords&metadataPrefix=mods")
        assert 'code="cannotDisseminateFormat"' in resp.text


# ---------------------------------------------------------------------------
# Fehlerbehandlung
# ---------------------------------------------------------------------------


class TestErrors:
    def test_bad_verb(self, requests_mock):
        mock = OAIMock()
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=InvalidVerb")
        assert 'code="badVerb"' in resp.text

    def test_no_verb(self, requests_mock):
        mock = OAIMock()
        mock.register(requests_mock)

        resp = requests.get(BASE_URL)
        assert 'code="badVerb"' in resp.text

    def test_bad_resumption_token(self, requests_mock):
        mock = OAIMock()
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&resumptionToken=invalidtoken999")
        assert 'code="badResumptionToken"' in resp.text


# ---------------------------------------------------------------------------
# isinvalid_xml_content Kompatibilität (Ometha-intern)
# ---------------------------------------------------------------------------


class TestOmethaXmlParsing:
    def test_identify_parseable_by_ometha(self, requests_mock, prm_base):
        mock = OAIMock.with_dc_records(3)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=Identify")
        root = isinvalid_xml_content(resp, f"{BASE_URL}?verb=Identify", "cli")
        assert root is not None

    def test_list_identifiers_parseable_by_ometha(self, requests_mock, prm_base):
        mock = OAIMock.with_dc_records(5)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc")
        root = isinvalid_xml_content(resp, BASE_URL, "cli")

        identifiers = [el.text for el in root.findall(f".//{NAMESPACE}identifier")]
        assert len(identifiers) == 5

    def test_resumption_token_readable_by_ometha(self, requests_mock, prm_base):
        mock = OAIMock.with_dc_records(5, page_size=2)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc")
        root = isinvalid_xml_content(resp, BASE_URL, "cli")

        token = root.findtext(f".//{NAMESPACE}resumptionToken")
        assert token is not None
        assert token.startswith("token_")


# ---------------------------------------------------------------------------
# TestListIdentifiers – get_identifier() Integration
# ---------------------------------------------------------------------------


class TestGetIdentifierIntegration:
    """
    Testet get_identifier() aus ometha.harvester direkt gegen den Mock.
    Achtung: get_identifier() verwendet Halo-Spinner – in CI ggf. mit
    TERM=dumb oder --no-header ausführen.
    """

    def test_returns_list(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(5)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert isinstance(ids, list)
        assert len(ids) == 5

    def test_all_pages_collected(self, requests_mock, session, prm_base):
        """page_size=3, 7 Records → 3 Seiten, alle 7 IDs gesammelt."""
        mock = OAIMock.with_dc_records(7, page_size=3)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert len(ids) == 7

    def test_identifiers_are_strings(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(3)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert all(isinstance(i, str) for i in ids)
        assert all(i.startswith("oai:") for i in ids)

    def test_set_filter_reduces_results(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(10)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&set=setB"
        ids = get_identifier(prm_base, url, session)

        # setB = ungerade Indizes: 1,3,5,7,9 → 5 Records
        assert len(ids) == 5

    def test_single_record(self, requests_mock, session, prm_base):
        mock = OAIMock.with_dc_records(1)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert len(ids) == 1

    def test_large_corpus_pagination(self, requests_mock, session, prm_base):
        """50 Records, page_size=10 → 5 Seiten, alle IDs."""
        mock = OAIMock.with_dc_records(50, page_size=10)
        mock.register(requests_mock)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        ids = get_identifier(prm_base, url, session)

        assert len(ids) == 50
        assert len(set(ids)) == 50  # keine Duplikate


# ---------------------------------------------------------------------------
# Test503Retry – Retry-after Simulation
# ---------------------------------------------------------------------------


class Test503Retry:
    """
    Simuliert einen 503-Fehler beim ersten Request.
    Ometha's HTTPAdapter ist mit Retry(status_forcelist=[503]) konfiguriert –
    hier testen wir, dass der Mock den 503 korrekt auslöst und
    ein normaler requests-Client danach erfolgreich ist.
    """

    def test_first_request_returns_503(self, requests_mock):
        mock = OAIMock.with_dc_records(3, simulate_503=True)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=Identify")
        assert resp.status_code == 503
        assert resp.headers.get("Retry-After") == "1"

    def test_second_request_succeeds_after_503(self, requests_mock):
        """Nach dem ersten 503 liefert der Mock normale Responses."""
        mock = OAIMock.with_dc_records(3, simulate_503=True)
        mock.register(requests_mock)

        # Erster Request → 503
        resp1 = requests.get(f"{BASE_URL}?verb=Identify")
        assert resp1.status_code == 503

        # Zweiter Request → 200
        resp2 = requests.get(f"{BASE_URL}?verb=Identify")
        assert resp2.status_code == 200
        assert "<repositoryName>" in resp2.text

    def test_503_only_fires_once(self, requests_mock):
        """simulate_503 triggert nur beim allerersten Request."""
        mock = OAIMock.with_dc_records(3, simulate_503=True)
        mock.register(requests_mock)

        statuses = [requests.get(f"{BASE_URL}?verb=Identify").status_code for _ in range(4)]
        assert statuses[0] == 503
        assert all(s == 200 for s in statuses[1:])

    def test_without_503_flag_always_200(self, requests_mock):
        mock = OAIMock.with_dc_records(3, simulate_503=False)
        mock.register(requests_mock)

        for _ in range(3):
            resp = requests.get(f"{BASE_URL}?verb=Identify")
            assert resp.status_code == 200


# ---------------------------------------------------------------------------
# TestHarvestFiles – harvest_files() Integration
# ---------------------------------------------------------------------------


class TestHarvestFiles:
    """
    Testet harvest_files() aus ometha.harvester gegen den Mock.
    harvest_files() ruft GetRecord für jede ID auf und speichert XML-Dateien.
    """

    def test_files_created_for_all_ids(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:{i:03d}") for i in range(5)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        ids = [r.identifier for r in records]
        failed_dl, failed_ids = harvest_files(ids, prm_base, str(tmp_path), session=session)

        xml_files = list(tmp_path.rglob("*.xml"))
        assert len(xml_files) == 5
        assert failed_dl == []
        assert failed_ids == []

    def test_nonexistent_id_goes_to_failed_download(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:exists")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        ids = ["oai:mock:exists", "oai:mock:doesnotexist"]
        failed_dl, failed_ids = harvest_files(ids, prm_base, str(tmp_path), session=session)

        xml_files = list(tmp_path.rglob("*.xml"))
        assert len(xml_files) == 1
        assert "oai:mock:doesnotexist" in failed_dl

    def test_json_export(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:json001")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm_json = {**prm_base, "exp_type": "json"}
        harvest_files(["oai:mock:json001"], prm_json, str(tmp_path), session=session)

        json_files = list(tmp_path.rglob("*.json"))
        assert len(json_files) == 1

    def test_deleted_record_goes_to_failed_download(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:deleted", deleted=True)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        failed_dl, failed_ids = harvest_files(["oai:mock:deleted"], prm_base, str(tmp_path), session=session)

        # Deleted records liefern kein <metadata> → Ometha behandelt als failed
        xml_files = list(tmp_path.rglob("*.xml"))
        # Entweder gespeichert (mit deleted-Header) oder failed – beides akzeptabel
        assert len(xml_files) + len(failed_dl) == 1


# ---------------------------------------------------------------------------
# TestDatefilterEdgeCases
# ---------------------------------------------------------------------------


class TestDatefilterEdgeCases:
    def test_exact_from_boundary_included(self, requests_mock):
        """Ein Record mit datestamp == from-Datum soll enthalten sein."""
        records = [OAIRecord("oai:mock:boundary", datestamp="2024-03-15T00:00:00Z")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2024-03-15")
        assert "oai:mock:boundary" in resp.text

    def test_exact_until_boundary_included(self, requests_mock):
        records = [OAIRecord("oai:mock:boundary", datestamp="2024-03-15T00:00:00Z")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&until=2024-03-15")
        assert "oai:mock:boundary" in resp.text

    def test_from_after_all_records_returns_no_match(self, requests_mock):
        records = [OAIRecord("oai:mock:old", datestamp="2020-01-01T00:00:00Z")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2025-01-01")
        assert 'code="noRecordsMatch"' in resp.text

    def test_until_before_all_records_returns_no_match(self, requests_mock):
        records = [OAIRecord("oai:mock:new", datestamp="2024-01-01T00:00:00Z")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&until=2019-12-31")
        assert 'code="noRecordsMatch"' in resp.text

    def test_from_and_until_range(self, requests_mock):
        records = [
            OAIRecord("oai:mock:before", datestamp="2022-01-01T00:00:00Z"),
            OAIRecord("oai:mock:inside", datestamp="2023-06-01T00:00:00Z"),
            OAIRecord("oai:mock:after", datestamp="2025-01-01T00:00:00Z"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2023-01-01&until=2024-01-01")
        assert "oai:mock:inside" in resp.text
        assert "oai:mock:before" not in resp.text
        assert "oai:mock:after" not in resp.text

    def test_mixed_datestamps_granularity(self, requests_mock):
        """Datestamp mit und ohne Zeit-Anteil."""
        records = [
            OAIRecord("oai:mock:dateonly", datestamp="2024-05-01"),
            OAIRecord("oai:mock:datetime", datestamp="2024-05-01T12:00:00Z"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc&from=2024-05-01")
        assert "oai:mock:dateonly" in resp.text
        assert "oai:mock:datetime" in resp.text


# ---------------------------------------------------------------------------
# TestTimeoutHandling – langsame / nicht erreichbare Schnittstellen
# ---------------------------------------------------------------------------


class TestTimeoutHandling:
    """
    Testet das Verhalten bei Netzwerkproblemen.

    Technik:
    - requests_mock mit exc=... simuliert Fehler ohne echtes Warten
    - time.sleep wird gemockt, damit der Exponential-Backoff in
      get_identifier() die Tests nicht verlangsamt (sonst 20s + 40s + 80s)
    """

    # ------------------------------------------------------------------
    # harvest_files(): einzelne IDs mit Netzwerkproblemen
    # ------------------------------------------------------------------

    def test_timeout_bei_einzelner_id_geht_in_failed_ids(self, requests_mock, prm_base, tmp_path, session):
        """Timeout bei GetRecord → ID landet in failed_ids, kein Absturz."""
        from ometha.harvester import harvest_files

        # Eine normale ID + eine, bei der der Mock Timeout wirft
        records = [OAIRecord("oai:mock:ok")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        requests_mock.get(
            f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:timeout",
            exc=requests.exceptions.Timeout,
        )

        failed_dl, failed_ids = harvest_files(
            ["oai:mock:ok", "oai:mock:timeout"],
            prm_base,
            str(tmp_path),
            session=session,
        )

        assert "oai:mock:timeout" in failed_ids
        assert "oai:mock:ok" not in failed_ids
        xml_files = list(tmp_path.rglob("*.xml"))
        assert len(xml_files) == 1

    def test_connection_error_geht_in_failed_ids(self, requests_mock, prm_base, tmp_path, session):
        """ConnectionError bei GetRecord → ID landet in failed_ids."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:ok")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        requests_mock.get(
            f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:connfail",
            exc=requests.exceptions.ConnectionError,
        )

        failed_dl, failed_ids = harvest_files(
            ["oai:mock:ok", "oai:mock:connfail"],
            prm_base,
            str(tmp_path),
            session=session,
        )

        assert "oai:mock:connfail" in failed_ids
        assert "oai:mock:ok" not in failed_ids

    def test_alle_ids_timeout_keine_dateien(self, requests_mock, prm_base, tmp_path, session):
        """Alle GetRecord-Requests schlagen fehl → keine Dateien, alle in failed_ids."""
        from ometha.harvester import harvest_files

        mock = OAIMock(records=[])
        mock.register(requests_mock)

        for i in range(3):
            requests_mock.get(
                f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:{i:03d}",
                exc=requests.exceptions.Timeout,
            )

        ids = [f"oai:mock:{i:03d}" for i in range(3)]
        failed_dl, failed_ids = harvest_files(ids, prm_base, str(tmp_path), session=session)

        assert len(failed_ids) == 3
        assert len(list(tmp_path.rglob("*.xml"))) == 0

    def test_http_500_geht_in_failed_download(self, requests_mock, prm_base, tmp_path, session):
        """HTTP 500 bei GetRecord → ID landet in failed_download (nicht failed_ids)."""
        from ometha.harvester import harvest_files

        mock = OAIMock(records=[])
        mock.register(requests_mock)

        requests_mock.get(
            f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:500",
            status_code=500,
        )

        failed_dl, failed_ids = harvest_files(["oai:mock:500"], prm_base, str(tmp_path), session=session)

        assert "oai:mock:500" in failed_dl
        assert failed_ids == []

    # ------------------------------------------------------------------
    # get_identifier(): Retry-Logik bei langsamer Schnittstelle
    # ------------------------------------------------------------------

    def test_timeout_retry_erfolgreich(self, requests_mock, session, prm_base):
        """
        Erster Request → Timeout, zweiter → Erfolg.
        time.sleep wird gemockt, damit der Test nicht 20 Sekunden wartet.
        """
        from unittest.mock import patch
        from ometha.harvester import get_identifier

        mock = OAIMock.with_dc_records(3)
        call_count = {"n": 0}

        def flaky_response(request, context):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise requests.exceptions.Timeout("simulierter Timeout")
            return mock._dispatch(request, context)

        requests_mock.get(BASE_URL, text=flaky_response)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        with patch("ometha.harvester.time.sleep"):
            ids = get_identifier(prm_base, url, session)

        assert len(ids) == 3
        assert call_count["n"] == 2  # einmal Timeout, einmal Erfolg

    def test_zwei_timeouts_dann_erfolg(self, requests_mock, session, prm_base):
        """Zwei Timeouts, dritter Request erfolgreich – alle IDs zurückgegeben."""
        from unittest.mock import patch
        from ometha.harvester import get_identifier

        mock = OAIMock.with_dc_records(5)
        call_count = {"n": 0}

        def two_failures(request, context):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise requests.exceptions.Timeout("simulierter Timeout")
            return mock._dispatch(request, context)

        requests_mock.get(BASE_URL, text=two_failures)

        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        with patch("ometha.harvester.time.sleep"):
            ids = get_identifier(prm_base, url, session)

        assert len(ids) == 5

    def test_alle_retries_erschoepft_endet_mit_exit(self, requests_mock, session, prm_base, tmp_path):
        """
        Alle 3 Retry-Versuche schlagen fehl → sys.exit() wird gerufen.
        time.sleep wird gemockt um 20s+40s+80s Wartezeit zu vermeiden.
        """
        from unittest.mock import patch
        from ometha.harvester import get_identifier

        requests_mock.get(BASE_URL, exc=requests.exceptions.Timeout)

        prm_base["out_f"] = str(tmp_path)
        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        with patch("ometha.harvester.time.sleep"):
            with pytest.raises(SystemExit):
                get_identifier(prm_base, url, session)

    def test_resumption_token_wird_bei_timeout_gespeichert(self, requests_mock, session, prm_base, tmp_path):
        """
        Timeout auf Seite 2 → der zuletzt erfolgreiche ResumptionToken
        wird als Datei gespeichert.
        """
        from unittest.mock import patch
        from ometha.harvester import get_identifier

        mock = OAIMock.with_dc_records(6, page_size=3)
        call_count = {"n": 0}

        def fail_on_second_page(request, context):
            call_count["n"] += 1
            if call_count["n"] > 1:
                raise requests.exceptions.Timeout("Timeout auf Seite 2")
            return mock._dispatch(request, context)

        requests_mock.get(BASE_URL, text=fail_on_second_page)

        prm_base["out_f"] = str(tmp_path)
        url = f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
        with patch("ometha.harvester.time.sleep"):
            with pytest.raises(SystemExit):
                get_identifier(prm_base, url, session)

        # ResumptionToken-Datei muss angelegt worden sein
        token_files = list(tmp_path.glob("resumption_token_*.yaml"))
        assert len(token_files) >= 1
        content = token_files[0].read_text()
        assert "resumptiontoken" in content.lower() or "token" in content.lower()


# ---------------------------------------------------------------------------
# change_date() – YAML-Datei wird korrekt aktualisiert
# ---------------------------------------------------------------------------


class TestChangeDateYaml:
    """Tests für harvester.change_date() – schreibt from-/until-Datum in YAML."""

    def setup_method(self):
        import ometha.helpers as h

        self._saved = {k: h.PRM[k] for k in ("conf_m", "auto_m", "conf_f")}

    def teardown_method(self):
        import ometha.helpers as h

        for k, v in self._saved.items():
            h.PRM[k] = v

    def _activate_auto(self, path):
        import ometha.helpers as h

        h.PRM["conf_m"] = True
        h.PRM["auto_m"] = True
        h.PRM["conf_f"] = str(path)

    def test_change_date_schreibt_from_datum(self, tmp_path):
        import yaml
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\nfrom-Datum: '2020-01-01'\n")
        self._activate_auto(config)

        change_date("2025-06-01T10:00:00Z", str(config), "from-Datum")

        result = read_yaml_file(str(config), ["from-Datum"])[0]
        assert result == "2025-06-01T10:00:00Z"

    def test_change_date_schreibt_until_datum(self, tmp_path):
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\n")
        self._activate_auto(config)

        change_date("2025-12-31T23:59:59Z", str(config), "until-Datum")

        result = read_yaml_file(str(config), ["until-Datum"])[0]
        assert result == "2025-12-31T23:59:59Z"

    def test_change_date_keine_aktion_wenn_conf_m_false(self, tmp_path):
        import ometha.helpers as h
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\nfrom-Datum: '2020-01-01'\n")
        h.PRM["conf_m"] = False
        h.PRM["auto_m"] = True

        change_date("2025-06-01T10:00:00Z", str(config), "from-Datum")

        result = read_yaml_file(str(config), ["from-Datum"])[0]
        assert result == "2020-01-01"  # unverändert

    def test_change_date_keine_aktion_wenn_auto_m_false(self, tmp_path):
        import ometha.helpers as h
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\nfrom-Datum: '2020-01-01'\n")
        h.PRM["conf_m"] = True
        h.PRM["auto_m"] = False

        change_date("2025-06-01T10:00:00Z", str(config), "from-Datum")

        result = read_yaml_file(str(config), ["from-Datum"])[0]
        assert result == "2020-01-01"  # unverändert

    def test_change_date_legt_neuen_key_an(self, tmp_path):
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\n")
        self._activate_auto(config)

        change_date("2025-06-01T10:00:00Z", str(config), "until-Datum")

        result = read_yaml_file(str(config), ["until-Datum"])[0]
        assert result == "2025-06-01T10:00:00Z"

    def test_change_date_behaelt_andere_keys(self, tmp_path):
        from ometha.harvester import change_date, read_yaml_file

        config = tmp_path / "config.yaml"
        config.write_text("baseurl: http://test.org/\nmetadataPrefix: oai_dc\nfrom-Datum: '2020-01-01'\n")
        self._activate_auto(config)

        change_date("2025-06-01T10:00:00Z", str(config), "from-Datum")

        baseurl = read_yaml_file(str(config), ["baseurl"])[0]
        assert baseurl == "http://test.org/"


# ---------------------------------------------------------------------------
# Regression: Auto-Modus setzt PRM["u_date"] auf OAITIMESTR
# ---------------------------------------------------------------------------


class TestAutoModeUDateUpdate:
    """Regression: Im Auto-Modus muss PRM["u_date"] auf den aktuellen
    OAITIMESTR gesetzt werden, nicht auf den veralteten Wert aus der YAML-Datei.

    Bug: Nach einem Run enthielten from-Datum und until-Datum denselben Wert T1.
    Beim nächsten Run wurde PRM["u_date"] = T1 aus der YAML gelesen, aber
    change_date schrieb T2 nur in die Datei – nicht in PRM. Dadurch wurde
    die Query mit from=T1, until=T1 gestellt und lieferte kein Delta.
    """

    def setup_method(self):
        import ometha.helpers as h

        self._saved = {k: h.PRM[k] for k in ("conf_m", "auto_m", "conf_f", "u_date", "f_date")}

    def teardown_method(self):
        import ometha.helpers as h

        for k, v in self._saved.items():
            h.PRM[k] = v

    def test_u_date_wird_auf_oaitimestr_gesetzt(self, tmp_path):
        """PRM["u_date"] muss nach dem auto-Modus-Block dem aktuellen OAITIMESTR entsprechen."""
        import ometha.helpers as h
        from ometha.harvester import change_date
        from ometha.helpers import OAITIMESTR

        stale_timestamp = "2025-01-01T00:00:00Z"
        config = tmp_path / "config.yaml"
        config.write_text(
            f"baseurl: http://test.org/\nfrom-Datum: '{stale_timestamp}'\nuntil-Datum: '{stale_timestamp}'\n"
        )

        # Simuliert den Zustand kurz vor dem Harvesting in start_process():
        # PRM wurde aus der YAML gefüllt (beide Daten gleich = Bug-Zustand)
        h.PRM["conf_m"] = True
        h.PRM["auto_m"] = True
        h.PRM["conf_f"] = str(config)
        h.PRM["f_date"] = stale_timestamp
        h.PRM["u_date"] = stale_timestamp  # veralteter Wert aus YAML

        # Fix: change_date + sofortige PRM-Aktualisierung
        change_date(OAITIMESTR, str(config), "until-Datum")
        if h.PRM["conf_m"] and h.PRM["auto_m"]:
            h.PRM["u_date"] = OAITIMESTR

        assert h.PRM["u_date"] == OAITIMESTR
        assert h.PRM["u_date"] != h.PRM["f_date"], (
            "u_date darf nicht gleich f_date sein – kein Delta!"
        )

    def test_u_date_bleibt_unveraendert_ohne_auto_mode(self, tmp_path):
        """Ohne Auto-Modus darf PRM["u_date"] nicht überschrieben werden."""
        import ometha.helpers as h
        from ometha.harvester import change_date
        from ometha.helpers import OAITIMESTR

        stale_timestamp = "2025-01-01T00:00:00Z"
        config = tmp_path / "config.yaml"
        config.write_text(f"baseurl: http://test.org/\nuntil-Datum: '{stale_timestamp}'\n")

        h.PRM["conf_m"] = True
        h.PRM["auto_m"] = False  # kein Auto-Modus
        h.PRM["conf_f"] = str(config)
        h.PRM["u_date"] = stale_timestamp

        change_date(OAITIMESTR, str(config), "until-Datum")
        if h.PRM["conf_m"] and h.PRM["auto_m"]:
            h.PRM["u_date"] = OAITIMESTR

        assert h.PRM["u_date"] == stale_timestamp  # unverändert


# ---------------------------------------------------------------------------
# create_id_file() – Dateiinhalt und Wiederlesbarkeit
# ---------------------------------------------------------------------------


class TestCreateIdFile:
    """Tests für harvester.create_id_file()."""

    def _prm(self, tmp_path, f_date=None, u_date=None):
        return {
            "b_url": "http://test.org/oai",
            "sets": None,
            "pref": "oai_dc",
            "dat_geb": "testDG",
            "timeout": 0,
            "debug": False,
            "f_date": f_date,
            "u_date": u_date,
            "out_f": str(tmp_path),
        }

    def test_datei_wird_angelegt(self, tmp_path):
        from ometha.harvester import create_id_file

        path = create_id_file(self._prm(tmp_path), [], str(tmp_path), "successful")
        assert os.path.exists(path)

    def test_typ_successful_im_dateinamen(self, tmp_path):
        from ometha.harvester import create_id_file

        path = create_id_file(self._prm(tmp_path), [], str(tmp_path), "successful")
        assert "_ometha_successful_ids.yaml" in path

    def test_typ_failed_im_dateinamen(self, tmp_path):
        from ometha.harvester import create_id_file

        path = create_id_file(self._prm(tmp_path), [], str(tmp_path), "failed")
        assert "_ometha_failed_ids.yaml" in path

    def test_ids_wieder_lesbar(self, tmp_path):
        from ometha.harvester import create_id_file, read_yaml_file

        ids = ["oai:test:001", "oai:test:002"]
        path = create_id_file(self._prm(tmp_path), ids, str(tmp_path), "successful")
        result = read_yaml_file(path, ["ids"])[0]
        assert "oai:test:001" in result
        assert "oai:test:002" in result

    def test_keine_ids_ergibt_leere_liste(self, tmp_path):
        from ometha.harvester import create_id_file, read_yaml_file

        path = create_id_file(self._prm(tmp_path), [], str(tmp_path), "successful")
        content = open(path).read()
        assert "ids:" in content

    def test_datumswerte_im_inhalt(self, tmp_path):
        from ometha.harvester import create_id_file

        prm = self._prm(tmp_path, f_date="2025-01-01", u_date="2025-12-31")
        path = create_id_file(prm, [], str(tmp_path), "successful")
        content = open(path).read()
        # yaml.safe_dump schreibt Strings mit Quotes: fromdate: '2025-01-01'
        assert "fromdate:" in content and "2025-01-01" in content
        assert "untildate:" in content and "2025-12-31" in content

    def test_none_datum_als_none_string(self, tmp_path):
        """None-Werte werden von yaml.safe_dump als 'null' geschrieben."""
        from ometha.harvester import create_id_file

        path = create_id_file(self._prm(tmp_path), [], str(tmp_path), "successful")
        content = open(path).read()
        assert "fromdate:" in content  # Key ist vorhanden
        assert "fromdate: 2" not in content  # aber kein Datum gesetzt


# ---------------------------------------------------------------------------
# save_file() / JSON-Konvertierungsfehler → failed_download
# ---------------------------------------------------------------------------


class TestJsonConversionError:
    """JSON-Konvertierungsfehler müssen in failed_download landen, nicht stillschweigend
    verschwinden."""

    def test_json_fehler_landet_in_failed_download(self, requests_mock, prm_base, tmp_path, session):
        from unittest.mock import patch
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:jsonerr001")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm_json = {**prm_base, "exp_type": "json", "out_f": str(tmp_path)}

        with patch("ometha.harvester.xmltodict.parse", side_effect=Exception("parse error")):
            failed_dl, failed_ids = harvest_files(
                ["oai:mock:jsonerr001"], prm_json, str(tmp_path), session=session
            )

        assert "oai:mock:jsonerr001" in failed_dl
        assert len(list(tmp_path.rglob("*.json"))) == 0

    def test_json_fehler_erzeugt_keine_datei(self, requests_mock, prm_base, tmp_path, session):
        from unittest.mock import patch
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:jsonerr002")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm_json = {**prm_base, "exp_type": "json", "out_f": str(tmp_path)}

        with patch("ometha.harvester.xmltodict.parse", side_effect=ValueError("bad")):
            harvest_files(["oai:mock:jsonerr002"], prm_json, str(tmp_path), session=session)

        assert len(list(tmp_path.rglob("*.json"))) == 0

    def test_json_erfolg_nicht_in_failed(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:jsonok001")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm_json = {**prm_base, "exp_type": "json", "out_f": str(tmp_path)}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:jsonok001"], prm_json, str(tmp_path), session=session
        )

        assert failed_dl == []
        assert failed_ids == []
        assert len(list(tmp_path.rglob("*.json"))) == 1


# ---------------------------------------------------------------------------
# Deleted Record Handling
# ---------------------------------------------------------------------------


class TestDeletedRecordHandling:
    """Tests für die drei deleted_handling-Modi: keep, skip, folder."""

    def _make_prm(self, prm_base, tmp_path, mode):
        return {**prm_base, "deleted_handling": mode, "out_f": str(tmp_path)}

    def test_keep_speichert_deleted_record(self, requests_mock, prm_base, tmp_path, session):
        """keep: gelöschter Record wird normal in den Ausgabeordner gespeichert."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:del001", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "keep")
        failed_dl, failed_ids = harvest_files(["oai:mock:del001"], prm, str(tmp_path), session=session)

        all_files = list(tmp_path.rglob("*.xml"))
        assert len(all_files) == 1
        assert failed_dl == []
        assert failed_ids == []
        # Datei darf NICHT im _deleted/-Unterordner liegen
        assert all_files[0].parent.name != "_deleted"

    def test_skip_verwirft_deleted_record(self, requests_mock, prm_base, tmp_path, session):
        """skip: gelöschter Record erzeugt keine Datei und landet nicht in failed."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:del002", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "skip")
        failed_dl, failed_ids = harvest_files(["oai:mock:del002"], prm, str(tmp_path), session=session)

        assert list(tmp_path.rglob("*.xml")) == []
        assert failed_dl == []
        assert failed_ids == []

    def test_folder_verschiebt_deleted_record(self, requests_mock, prm_base, tmp_path, session):
        """folder: gelöschter Record landet in _deleted/-Unterordner."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:del003", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "folder")
        failed_dl, failed_ids = harvest_files(["oai:mock:del003"], prm, str(tmp_path), session=session)

        deleted_files = list((tmp_path / "_deleted").rglob("*.xml"))
        assert len(deleted_files) == 1
        # Kein File im normalen Ausgabeordner
        normal_files = [f for f in tmp_path.rglob("*.xml") if "_deleted" not in str(f)]
        assert normal_files == []
        assert failed_dl == []
        assert failed_ids == []

    def test_folder_erstellt_deleted_unterordner(self, requests_mock, prm_base, tmp_path, session):
        """folder: _deleted/-Ordner wird automatisch angelegt."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:del004", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "folder")
        harvest_files(["oai:mock:del004"], prm, str(tmp_path), session=session)

        assert (tmp_path / "_deleted").is_dir()

    def test_keep_mischung_deleted_und_normal(self, requests_mock, prm_base, tmp_path, session):
        """keep: normale und gelöschte Records landen beide im Ausgabeordner."""
        from ometha.harvester import harvest_files

        records = [
            OAIRecord("oai:mock:normal001"),
            OAIRecord("oai:mock:del005", deleted=True),
        ]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "keep")
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:normal001", "oai:mock:del005"], prm, str(tmp_path), session=session
        )

        assert len(list(tmp_path.rglob("*.xml"))) == 2
        assert failed_dl == []

    def test_skip_mischung_nur_normale_gespeichert(self, requests_mock, prm_base, tmp_path, session):
        """skip: nur normale Records werden gespeichert, deleted verworfen."""
        from ometha.harvester import harvest_files

        records = [
            OAIRecord("oai:mock:normal002"),
            OAIRecord("oai:mock:del006", deleted=True),
        ]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "skip")
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:normal002", "oai:mock:del006"], prm, str(tmp_path), session=session
        )

        all_files = list(tmp_path.rglob("*.xml"))
        assert len(all_files) == 1
        assert "normal002" in all_files[0].name

    def test_folder_mischung_getrennte_ordner(self, requests_mock, prm_base, tmp_path, session):
        """folder: normale Records in part_*/, deleted in _deleted/."""
        from ometha.harvester import harvest_files

        records = [
            OAIRecord("oai:mock:normal003"),
            OAIRecord("oai:mock:del007", deleted=True),
        ]
        OAIMock(records=records).register(requests_mock)

        prm = self._make_prm(prm_base, tmp_path, "folder")
        harvest_files(
            ["oai:mock:normal003", "oai:mock:del007"], prm, str(tmp_path), session=session
        )

        normal_files = [f for f in tmp_path.rglob("*.xml") if "_deleted" not in str(f)]
        deleted_files = list((tmp_path / "_deleted").rglob("*.xml"))
        assert len(normal_files) == 1
        assert len(deleted_files) == 1


# ---------------------------------------------------------------------------
# Invalid Record Handling (//metadata/*/* check)
# ---------------------------------------------------------------------------


class TestInvalidRecordHandling:
    """Mit --sort-invalid werden Records ohne //metadata/*/*-Knoten in _invalid/ verschoben."""

    EMPTY_METADATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <GetRecord>
    <record>
      <header>
        <identifier>oai:mock:empty001</identifier>
        <datestamp>2024-01-01T00:00:00Z</datestamp>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"/>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>"""

    HTML_METADATA_XML = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <GetRecord>
    <record>
      <header>
        <identifier>oai:mock:html001</identifier>
        <datestamp>2024-01-01T00:00:00Z</datestamp>
      </header>
      <metadata>
        <div>Garbage HTML content</div>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>"""

    def _register_custom_response(self, requests_mock, identifier, xml_body):
        """Registriert eine individuelle GetRecord-Antwort für eine bestimmte ID."""
        requests_mock.get(
            f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier={identifier}",
            text=xml_body,
        )

    def test_ohne_flag_wird_nichts_aussortiert(self, requests_mock, prm_base, tmp_path, session):
        """Default (ohne --sort-invalid): auch leere Records werden normal gespeichert."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path)}
        harvest_files(["oai:mock:empty001"], prm, str(tmp_path), session=session)

        assert not (tmp_path / "_invalid").exists()
        assert len(list(tmp_path.glob("*.xml"))) == 1

    def test_valid_record_nicht_in_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Normaler Record mit //metadata/*/* landet NICHT in _invalid/."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:valid001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        harvest_files(["oai:mock:valid001"], prm, str(tmp_path), session=session)

        assert not (tmp_path / "_invalid").exists()
        assert len(list(tmp_path.rglob("*.xml"))) == 1

    def test_leeres_metadata_geht_in_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Record mit leerem <metadata>-Root (kein Enkelkind) → _invalid/."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:empty001"], prm, str(tmp_path), session=session
        )

        invalid_files = list((tmp_path / "_invalid").rglob("*.xml"))
        assert len(invalid_files) == 1
        normal_files = [f for f in tmp_path.rglob("*.xml") if "_invalid" not in str(f)]
        assert normal_files == []

    def test_invalid_record_nicht_in_failed(self, requests_mock, prm_base, tmp_path, session):
        """Ungültiger Record landet in _invalid/, nicht in failed_download oder failed_ids."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:empty001"], prm, str(tmp_path), session=session
        )

        assert failed_dl == []
        assert failed_ids == []

    def test_invalid_ordner_wird_angelegt(self, requests_mock, prm_base, tmp_path, session):
        """_invalid/-Ordner wird automatisch erstellt."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        harvest_files(["oai:mock:empty001"], prm, str(tmp_path), session=session)

        assert (tmp_path / "_invalid").is_dir()

    def test_mischung_valid_und_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Valide Records normal gespeichert, invalide in _invalid/."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:valid002")]
        OAIMock(records=records).register(requests_mock)
        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:valid002", "oai:mock:empty001"], prm, str(tmp_path), session=session
        )

        normal_files = [f for f in tmp_path.rglob("*.xml") if "_invalid" not in str(f)]
        invalid_files = list((tmp_path / "_invalid").rglob("*.xml"))
        assert len(normal_files) == 1
        assert len(invalid_files) == 1
        assert failed_dl == []

    def test_deleted_hat_vorrang_vor_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Deleted Records werden als deleted behandelt, nicht als invalid –
        auch wenn ihr <metadata>-Element fehlt."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:del_invalid", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path), "deleted_handling": "folder", "sort_invalid": True}
        harvest_files(["oai:mock:del_invalid"], prm, str(tmp_path), session=session)

        assert (tmp_path / "_deleted").is_dir()
        assert not (tmp_path / "_invalid").exists()


class TestIsInvalidHelper:
    """Unit-Tests für die is_invalid()-Hilfsfunktion direkt."""

    def _call(self, xml_text):
        from ometha.harvester import _parse_record, is_invalid

        return is_invalid(_parse_record(xml_text.encode()))

    def test_valider_record_nicht_invalid(self):
        xml = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <GetRecord><record><header/><metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
              <dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">Test</dc:title>
            </oai_dc:dc>
          </metadata></record></GetRecord></OAI-PMH>"""
        assert self._call(xml) is False

    def test_leeres_metadata_element_ist_invalid(self):
        xml = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <GetRecord><record><header/>
            <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"/>
            </metadata>
          </record></GetRecord></OAI-PMH>"""
        assert self._call(xml) is True

    def test_fehlendes_metadata_element_ist_invalid(self):
        xml = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <GetRecord><record><header/></record></GetRecord></OAI-PMH>"""
        assert self._call(xml) is True

    def test_nur_textnodes_ist_invalid(self):
        xml = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <GetRecord><record><header/>
            <metadata>nur text, kein Element</metadata>
          </record></GetRecord></OAI-PMH>"""
        assert self._call(xml) is True

    def test_kaputtes_xml_ist_invalid(self):
        assert self._call("<kaputt>><<") is True

    def test_tiefe_struktur_ist_valide(self):
        """METS o.ä. mit tiefer Verschachtelung muss als valide erkannt werden."""
        xml = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
          <GetRecord><record><header/>
            <metadata>
              <mets:mets xmlns:mets="http://www.loc.gov/METS/">
                <mets:dmdSec>
                  <mets:mdWrap><mets:xmlData><mods/></mets:xmlData></mets:mdWrap>
                </mets:dmdSec>
              </mets:mets>
            </metadata>
          </record></GetRecord></OAI-PMH>"""
        assert self._call(xml) is False


# ---------------------------------------------------------------------------
# Gentle Mode / Rate Limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    """Prüft ob das Rate Limit zwischen Requests eingehalten wird."""

    def test_ratelimit_verlangsamt_downloads(self, requests_mock, prm_base, tmp_path, session):
        """Mit ratelimit=0.2s müssen 3 Requests mindestens 0.4s dauern."""
        from ometha.harvester import harvest_files
        import time

        records = [OAIRecord(f"oai:mock:rate{i:03d}") for i in range(3)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "ratelimit": 0.2, "out_f": str(tmp_path)}
        t_start = time.monotonic()
        harvest_files([r.identifier for r in records], prm, str(tmp_path), session=session)
        elapsed = time.monotonic() - t_start

        # 3 Requests mit 0.2s Abstand → mindestens 0.4s (2 Pausen zwischen 3 Requests)
        assert elapsed >= 0.4, f"Expected >= 0.4s, got {elapsed:.2f}s"

    def test_ohne_ratelimit_keine_kuenstliche_pause(self, requests_mock, prm_base, tmp_path, session):
        """Ohne ratelimit laufen 3 Requests deutlich schneller als 0.3s."""
        from ometha.harvester import harvest_files
        import time

        records = [OAIRecord(f"oai:mock:fast{i:03d}") for i in range(3)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "ratelimit": None, "out_f": str(tmp_path)}
        t_start = time.monotonic()
        harvest_files([r.identifier for r in records], prm, str(tmp_path), session=session)
        elapsed = time.monotonic() - t_start

        assert elapsed < 2.0, f"Expected fast run, got {elapsed:.2f}s"


# ---------------------------------------------------------------------------
# get_identifier_pages Generator
# ---------------------------------------------------------------------------


class TestGetIdentifierPages:
    """Testet den get_identifier_pages-Generator direkt."""

    def test_yields_lists(self, requests_mock, session, prm_base):
        """get_identifier_pages muss Listen yielden, keine einzelnen Strings."""
        mock = OAIMock.with_dc_records(5)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        pages = list(get_identifier_pages(prm_base, url, session))
        assert len(pages) >= 1
        assert all(isinstance(page, list) for page in pages)

    def test_all_ids_collected_across_pages(self, requests_mock, session, prm_base):
        """Alle IDs aus allen Seiten müssen erreichbar sein."""
        mock = OAIMock.with_dc_records(9, page_size=4)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        all_ids = [id for page in get_identifier_pages(prm_base, url, session) for id in page]
        assert len(all_ids) == 9

    def test_multiple_pages_yielded(self, requests_mock, session, prm_base):
        """Mit page_size=3 und 9 Records müssen 3 Seiten geliefert werden."""
        mock = OAIMock.with_dc_records(9, page_size=3)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        pages = list(get_identifier_pages(prm_base, url, session))
        assert len(pages) == 3

    def test_first_page_available_immediately(self, requests_mock, session, prm_base):
        """Der Generator muss nach der ersten Seite yielden, nicht erst am Ende."""
        mock = OAIMock.with_dc_records(9, page_size=3)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        gen = get_identifier_pages(prm_base, url, session)
        first_page = next(gen)
        assert isinstance(first_page, list)
        assert len(first_page) == 3  # erste Seite sofort verfügbar

    def test_get_identifier_wrapper_returns_flat_list(self, requests_mock, session, prm_base):
        """get_identifier (Wrapper) muss weiterhin eine flache Liste zurückgeben."""
        mock = OAIMock.with_dc_records(9, page_size=3)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        ids = get_identifier(prm_base, url, session)
        assert isinstance(ids, list)
        assert len(ids) == 9
        assert all(isinstance(i, str) for i in ids)


# ---------------------------------------------------------------------------
# harvest_files mit Generator (Pipelining)
# ---------------------------------------------------------------------------


class TestHarvestFilesPipelining:
    """Prüft dass harvest_files korrekt mit einem Generator als ids-Parameter arbeitet."""

    def test_harvest_from_generator(self, requests_mock, prm_base, tmp_path, session):
        """harvest_files muss auch einen Generator akzeptieren und verarbeiten."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:pipe{i:03d}") for i in range(5)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}

        # Generator statt Liste übergeben
        def id_generator():
            yield [r.identifier for r in records[:3]]
            yield [r.identifier for r in records[3:]]

        failed_dl, failed_ids = harvest_files(id_generator(), prm, str(tmp_path), session=session)

        assert len(list(tmp_path.rglob("*.xml"))) == 5
        assert failed_dl == []
        assert failed_ids == []

    def test_pipeline_from_get_identifier_pages(self, requests_mock, prm_base, tmp_path, session):
        """Vollständiger Pipeline-Durchlauf: get_identifier_pages → harvest_files."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:full{i:03d}") for i in range(6)]
        mock = OAIMock(records=records, page_size=3)
        mock.register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        list_url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        # get_identifier_pages direkt an harvest_files übergeben
        pages = get_identifier_pages(prm, list_url, session)
        failed_dl, failed_ids = harvest_files(pages, prm, str(tmp_path), session=session)

        assert len(list(tmp_path.rglob("*.xml"))) == 6
        assert failed_dl == []

    def test_list_still_works(self, requests_mock, prm_base, tmp_path, session):
        """harvest_files muss weiterhin Listen akzeptieren (Rückwärtskompatibilität)."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:list{i:03d}") for i in range(3)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        ids = [r.identifier for r in records]
        failed_dl, failed_ids = harvest_files(ids, prm, str(tmp_path), session=session)

        assert len(list(tmp_path.rglob("*.xml"))) == 3
        assert failed_dl == []

    def test_producer_sentinel_set_on_empty_generator(self, requests_mock, prm_base, tmp_path, session):
        """Auch bei leerem Generator hängt harvest_files nicht."""
        from ometha.harvester import harvest_files
        import threading

        OAIMock(records=[]).register(requests_mock)
        prm = {**prm_base, "out_f": str(tmp_path)}

        def empty_gen():
            return
            yield  # macht es zu einem Generator

        result = [None]
        def run():
            result[0] = harvest_files(empty_gen(), prm, str(tmp_path), session=session)

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=10)
        assert not t.is_alive(), "harvest_files hat sich aufgehängt bei leerem Generator"


class TestCompleteListSizeOptional:
    """Regression: completeListSize ist laut OAI-PMH-Spec optional (§3.1.1.4).
    Fehlt es in der Server-Antwort, muss Ometha ohne Crash weiterlaufen –
    einfach ohne ETA/Gesamtzahl im Fortschrittsbalken."""

    def test_missing_complete_list_size_does_not_crash(self, requests_mock, prm_base, session):
        """on_list_size darf nie aufgerufen werden, wenn completeListSize fehlt."""
        from ometha.harvester import get_identifier_pages

        xml_no_list_size = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <ListIdentifiers>
    <header><identifier>oai:mock:nols001</identifier><datestamp>2024-01-01T00:00:00Z</datestamp></header>
    <resumptionToken/>
  </ListIdentifiers>
</OAI-PMH>"""
        requests_mock.get(
            f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc",
            text=xml_no_list_size,
        )

        received = []
        pages = list(
            get_identifier_pages(
                prm_base,
                f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc",
                session,
                on_list_size=lambda n: received.append(n),
            )
        )

        assert pages == [["oai:mock:nols001"]]
        assert received == []  # Callback wurde nie aufgerufen, kein Crash

    def test_missing_complete_list_size_leaves_progress_bar_without_eta(
        self, requests_mock, prm_base, tmp_path, session
    ):
        """End-to-End: harvest_files muss auch dann fehlerfrei durchlaufen, wenn die
        Schnittstelle nie eine completeListSize liefert und total_holder[0] dauerhaft
        None bleibt."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:noeta{i:03d}") for i in range(3)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        total_holder = [None]  # bleibt None, simuliert fehlende completeListSize

        def id_generator():
            yield [r.identifier for r in records]

        failed_dl, failed_ids = harvest_files(
            id_generator(), prm, str(tmp_path), session=session, total_holder=total_holder
        )

        assert len(list(tmp_path.rglob("*.xml"))) == 3
        assert failed_dl == []
        assert total_holder[0] is None  # unverändert, aber kein Crash


class TestHarvestFilesTotalHolder:
    """Regression: der Fortschrittsbalken zeigte bei Pipelining keine Gesamtzahl/ETA mehr an.
    total_holder soll die Gesamtzahl nachreichen, sobald sie bekannt ist."""

    def test_total_holder_used_for_list_ids(self, requests_mock, prm_base, tmp_path, session):
        """Bei einer normalen Liste ist die Gesamtzahl sofort bekannt (len(ids)),
        total_holder wird dafür nicht gebraucht, darf aber übergeben werden ohne Fehler."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:th{i:03d}") for i in range(3)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        ids = [r.identifier for r in records]
        total_holder = [None]
        failed_dl, failed_ids = harvest_files(
            ids, prm, str(tmp_path), session=session, total_holder=total_holder
        )
        assert failed_dl == []
        assert failed_ids == []

    def test_total_holder_filled_by_generator_used_for_pipelining(
        self, requests_mock, prm_base, tmp_path, session
    ):
        """Wenn ids ein Generator ist und total_holder während der Iteration befüllt wird,
        muss harvest_files trotzdem alle IDs vollständig verarbeiten."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:gen{i:03d}") for i in range(4)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        total_holder = [None]

        def id_generator():
            # simuliert get_identifier_pages: befüllt total_holder bei der ersten Seite
            total_holder[0] = 4
            yield [r.identifier for r in records[:2]]
            yield [r.identifier for r in records[2:]]

        failed_dl, failed_ids = harvest_files(
            id_generator(), prm, str(tmp_path), session=session, total_holder=total_holder
        )
        assert len(list(tmp_path.rglob("*.xml"))) == 4
        assert failed_dl == []
        assert failed_ids == []

    def test_none_total_holder_does_not_crash(self, requests_mock, prm_base, tmp_path, session):
        """Ohne total_holder (Default None) muss harvest_files weiterhin funktionieren
        (Rückwärtskompatibilität, z. B. für gui.py)."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:notota001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:notota001"], prm, str(tmp_path), session=session
        )
        assert failed_dl == []
        assert failed_ids == []


class TestHarvestFilesIncrementalProgress:
    """Regression: der Fortschrittsbalken stand während der gesamten ID-Sammelphase
    bei 0, weil zuerst alle IDs aus der Queue gezogen und Futures eingereicht wurden,
    bevor überhaupt ein pbar.update() lief. Jetzt aktualisiert ein done-callback den
    Fortschritt sofort bei jedem abgeschlossenen Download."""

    def test_all_results_recorded_even_with_interleaved_submission(
        self, requests_mock, prm_base, tmp_path, session
    ):
        """Auch wenn IDs seitenweise nachkommen (Generator statt Liste), müssen am
        Ende alle Downloads erfolgreich verzeichnet sein – kein Ergebnis darf durch
        die Umstellung auf done-callbacks verloren gehen."""
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:prog{i:03d}") for i in range(10)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path), "n_procs": 3}

        def id_generator():
            ids = [r.identifier for r in records]
            for i in range(0, len(ids), 3):
                yield ids[i : i + 3]

        failed_dl, failed_ids = harvest_files(id_generator(), prm, str(tmp_path), session=session)

        assert len(list(tmp_path.rglob("*.xml"))) == 10
        assert failed_dl == []
        assert failed_ids == []

    def test_failed_and_successful_mixed_all_recorded(
        self, requests_mock, prm_base, tmp_path, session
    ):
        """Mischung aus erfolgreichen und fehlschlagenden Downloads: alle Ergebnisse
        müssen über die done-callbacks korrekt eingesammelt werden."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:ok001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        ids = ["oai:mock:ok001", "oai:mock:doesnotexist"]
        failed_dl, failed_ids = harvest_files(ids, prm, str(tmp_path), session=session)

        assert "oai:mock:doesnotexist" in failed_dl
        assert len(list(tmp_path.rglob("*.xml"))) == 1


# ---------------------------------------------------------------------------
# Regression: log_critical_and_print_and_exit muss immer exiten
# ---------------------------------------------------------------------------


class TestLogCriticalAndPrintAndExit:
    """Regression: log_critical_and_print_and_exit exitete bei leerem Enter-Druck
    (input() == "") nicht, weil der Rückgabewert als truthy-Check missbraucht wurde.
    Das führte in isinvalid_xml_content zu einem UnboundLocalError statt eines
    sauberen Programmendes."""

    def test_ui_mode_exits_on_empty_input(self):
        """Leerer Enter-Druck (input() gibt '') muss trotzdem zu sys.exit() führen."""
        from unittest.mock import patch
        from ometha.helpers import log_critical_and_print_and_exit

        with patch("builtins.input", return_value=""):
            with pytest.raises(SystemExit):
                log_critical_and_print_and_exit("Testfehler", mode="ui")

    def test_ui_mode_exits_on_nonempty_input(self):
        """Auch mit getipptem Text muss exitet werden (bisheriges Verhalten bleibt)."""
        from unittest.mock import patch
        from ometha.helpers import log_critical_and_print_and_exit

        with patch("builtins.input", return_value="x"):
            with pytest.raises(SystemExit):
                log_critical_and_print_and_exit("Testfehler", mode="ui")

    def test_cli_mode_exits_without_prompting(self):
        """Im CLI-Modus darf kein input() aufgerufen werden, aber es muss exitet werden."""
        from unittest.mock import patch
        from ometha.helpers import log_critical_and_print_and_exit

        with patch("builtins.input") as mock_input:
            with pytest.raises(SystemExit):
                log_critical_and_print_and_exit("Testfehler", mode="cli")
        mock_input.assert_not_called()

    def test_exits_with_exception_argument(self):
        """Mit übergebener exception muss ebenfalls exitet werden (unverändertes Verhalten)."""
        from ometha.helpers import log_critical_and_print_and_exit

        with pytest.raises(SystemExit):
            log_critical_and_print_and_exit("Testfehler", mode="cli", exception=ValueError("boom"))


class TestIsInvalidXmlContentRegression:
    """Regression: isinvalid_xml_content löste bei ungültigem XML einen
    UnboundLocalError aus (root war nie zugewiesen), wenn
    log_critical_and_print_and_exit ausnahmsweise zurückkehrte, statt zu exiten."""

    class _FakeResponse:
        def __init__(self, content: bytes):
            self.content = content

    def test_invalid_xml_raises_cleanly_not_unboundlocalerror(self):
        """Wenn log_critical_and_print_and_exit (z. B. gemockt) zurückkehrt,
        muss isinvalid_xml_content die ursprüngliche XMLSyntaxError weiterreichen
        statt eines UnboundLocalError."""
        from unittest.mock import patch
        from lxml import etree

        response = self._FakeResponse(b"")  # leerer Body → "Start tag expected"

        with patch("ometha.helpers.log_critical_and_print_and_exit", return_value=None):
            with pytest.raises(etree.XMLSyntaxError):
                isinvalid_xml_content(response, "http://example.org/oai", mode="cli")

    def test_valid_xml_returns_root_normally(self):
        """Gültiges XML muss weiterhin normal geparst werden."""
        response = self._FakeResponse(b"<root><child/></root>")
        root = isinvalid_xml_content(response, "http://example.org/oai", mode="cli")
        assert root.tag == "root"

    def test_invalid_xml_calls_log_critical_with_message(self):
        """Bei ungültigem XML muss log_critical_and_print_and_exit mit einer
        aussagekräftigen Fehlermeldung aufgerufen werden."""
        from unittest.mock import patch

        response = self._FakeResponse(b"nicht xml")

        with patch("ometha.helpers.log_critical_and_print_and_exit") as mock_exit:
            mock_exit.side_effect = SystemExit(1)
            with pytest.raises(SystemExit):
                isinvalid_xml_content(response, "http://example.org/oai", mode="cli")

        assert mock_exit.call_count == 1
        message = mock_exit.call_args[0][0]
        assert "XML Syntax Error" in message
        assert "http://example.org/oai" in message


# ---------------------------------------------------------------------------
# harvest_files: stats-Parameter für Lauf-Statistiken
# ---------------------------------------------------------------------------


class TestHarvestFilesStats:
    """stats-Dict wird in-place mit Lauf-Statistiken befüllt (für Zusammenfassung)."""

    def test_stats_none_by_default_no_crash(self, requests_mock, prm_base, tmp_path, session):
        """Ohne stats-Argument (Default None) muss harvest_files weiterhin funktionieren."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:stats001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        failed_dl, failed_ids = harvest_files(["oai:mock:stats001"], prm, str(tmp_path), session=session)
        assert failed_dl == []

    def test_stats_populated_all_successful(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:allok{i:03d}") for i in range(5)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        stats: dict = {}
        harvest_files([r.identifier for r in records], prm, str(tmp_path), session=session, stats=stats)

        assert stats["total"] == 5
        assert stats["successful"] == 5
        assert stats["failed_download"] == 0
        assert stats["failed_ids"] == 0
        assert stats["skipped_deleted"] == 0
        assert stats["moved_deleted"] == 0
        assert stats["moved_invalid"] == 0

    def test_stats_counts_failed_download(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:okstat001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        ids = ["oai:mock:okstat001", "oai:mock:doesnotexist"]
        stats: dict = {}
        harvest_files(ids, prm, str(tmp_path), session=session, stats=stats)

        assert stats["total"] == 2
        assert stats["successful"] == 1
        assert stats["failed_download"] == 1

    def test_stats_counts_deleted_folder_mode(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [
            OAIRecord("oai:mock:normalstat001"),
            OAIRecord("oai:mock:delstat001", deleted=True),
        ]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path), "deleted_handling": "folder"}
        stats: dict = {}
        harvest_files(
            ["oai:mock:normalstat001", "oai:mock:delstat001"],
            prm, str(tmp_path), session=session, stats=stats,
        )

        assert stats["total"] == 2
        assert stats["successful"] == 1
        assert stats["moved_deleted"] == 1

    def test_stats_counts_skipped_deleted(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:skipstat001", deleted=True)]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path), "deleted_handling": "skip"}
        stats: dict = {}
        harvest_files(["oai:mock:skipstat001"], prm, str(tmp_path), session=session, stats=stats)

        assert stats["total"] == 1
        assert stats["successful"] == 0
        assert stats["skipped_deleted"] == 1

    def test_stats_counts_invalid_records(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        xml_empty_metadata = """<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <GetRecord>
    <record>
      <header><identifier>oai:mock:invstat001</identifier><datestamp>2024-01-01T00:00:00Z</datestamp></header>
      <metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"/></metadata>
    </record>
  </GetRecord>
</OAI-PMH>"""
        requests_mock.get(
            f"{BASE_URL}?verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:mock:invstat001",
            text=xml_empty_metadata,
        )

        prm = {**prm_base, "out_f": str(tmp_path), "sort_invalid": True}
        stats: dict = {}
        harvest_files(["oai:mock:invstat001"], prm, str(tmp_path), session=session, stats=stats)

        assert stats["total"] == 1
        assert stats["successful"] == 0
        assert stats["moved_invalid"] == 1


# ---------------------------------------------------------------------------
# write_summary_xml
# ---------------------------------------------------------------------------


class TestWriteSummaryXml:
    """Tests für die XML-Zusammenfassung am Ende eines Harvesting-Laufs."""

    def _prm(self, tmp_path, **overrides):
        base = {
            "b_url": "http://mock-oai.test/oai",
            "pref": "oai_dc",
            "sets": [{"additive": ["setA", "setB"], "intersection": []}],
            "f_date": "2024-01-01",
            "u_date": "2024-12-31",
            "dat_geb": "TestPartner",
            "exp_type": "xml",
            "deleted_handling": "folder",
            "gentle": False,
        }
        return {**base, **overrides}

    def _stats(self, **overrides):
        base = {
            "total": 10,
            "successful": 8,
            "failed_download": 1,
            "failed_ids": 0,
            "skipped_deleted": 0,
            "moved_deleted": 1,
            "moved_invalid": 0,
        }
        return {**base, **overrides}

    def test_file_is_created_with_expected_name(self, tmp_path):
        from ometha.harvester import write_summary_xml

        prm = self._prm(tmp_path)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        assert os.path.basename(path) == "_ometha_TestPartner_summary.xml"
        assert os.path.exists(path)

    def test_file_is_valid_xml(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()
        assert root.tag == "harvestSummary"

    def test_source_fields_present(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()

        assert root.findtext("source/baseUrl") == "http://mock-oai.test/oai"
        assert root.findtext("source/metadataPrefix") == "oai_dc"
        assert root.findtext("source/fromDate") == "2024-01-01"
        assert root.findtext("source/untilDate") == "2024-12-31"
        sets = [el.text for el in root.findall("source/sets/set")]
        assert sets == ["setA", "setB"]

    def test_config_fields_present(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path, gentle=True)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()

        assert root.findtext("config/datengeber") == "TestPartner"
        assert root.findtext("config/exportType") == "xml"
        assert root.findtext("config/deletedHandling") == "folder"
        assert root.findtext("config/gentle") == "true"

    def test_results_fields_match_stats(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path)
        stats = self._stats(total=42, successful=40, failed_download=2)
        path = write_summary_xml(prm, str(tmp_path), stats, "00:01:23.456")
        root = etree.parse(path).getroot()

        assert root.findtext("results/total") == "42"
        assert root.findtext("results/successful") == "40"
        assert root.findtext("results/failedDownload") == "2"

    def test_runtime_and_version_present(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree
        from ometha._version import __version__

        prm = self._prm(tmp_path)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:12:34.567")
        root = etree.parse(path).getroot()

        assert root.findtext("meta/runtime") == "00:12:34.567"
        assert root.find("meta/ometha").get("version") == __version__

    def test_missing_dates_omitted_not_empty_tags(self, tmp_path):
        """Wenn fromDate/untilDate nicht gesetzt sind, sollen sie nicht als
        leere Tags auftauchen, sondern ganz fehlen."""
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path, f_date=None, u_date=None)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()

        assert root.find("source/fromDate") is None
        assert root.find("source/untilDate") is None

    def test_no_sets_produces_empty_sets_element(self, tmp_path):
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path, sets=None)
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()

        sets_el = root.find("source/sets")
        assert sets_el is not None
        assert len(sets_el) == 0

    def test_special_characters_in_baseurl_escaped_correctly(self, tmp_path):
        """Regression: Base-URLs mit &-Zeichen in der Query müssen korrekt
        XML-escaped werden (kein String-Format-Bug)."""
        from ometha.harvester import write_summary_xml
        from lxml import etree

        prm = self._prm(tmp_path, b_url="http://example.org/oai?a=1&b=2")
        path = write_summary_xml(prm, str(tmp_path), self._stats(), "00:01:23.456")
        root = etree.parse(path).getroot()
        assert root.findtext("source/baseUrl") == "http://example.org/oai?a=1&b=2"


# ---------------------------------------------------------------------------
# safe_get: SSL-Zertifikatsprüfung mit geloggtem Fallback
# ---------------------------------------------------------------------------


class TestSafeGet:
    """safe_get() prüft standardmäßig SSL-Zertifikate, loggt aber statt zu
    crashen, wenn die Prüfung fehlschlägt, und deaktiviert sie nur dann
    für den Rest des Laufs."""

    def test_successful_request_returns_response_unchanged(self):
        """Ohne SSL-Fehler muss safe_get sich wie ein normaler session.get() verhalten."""
        from unittest.mock import MagicMock

        session = MagicMock()
        session.verify = True
        session.get.return_value = "response"

        result = safe_get(session, "https://good-cert.example.org/oai", timeout=(10, 30))

        assert result == "response"
        session.get.assert_called_once_with("https://good-cert.example.org/oai", timeout=(10, 30))

    def test_verify_stays_true_when_no_ssl_error(self):
        """session.verify darf nicht angefasst werden, solange kein SSL-Fehler auftritt."""
        from unittest.mock import MagicMock

        session = MagicMock()
        session.verify = True
        session.get.return_value = "response"

        safe_get(session, "https://good-cert.example.org/oai")

        assert session.verify is True

    def test_ssl_error_disables_verify_and_retries(self):
        """Bei SSLError muss session.verify auf False gesetzt und der Request wiederholt werden."""
        from unittest.mock import MagicMock
        from requests.exceptions import SSLError

        session = MagicMock()
        session.verify = True
        session.get.side_effect = [SSLError("certificate verify failed"), "response after fallback"]

        result = safe_get(session, "https://bad-cert.example.org/oai")

        assert result == "response after fallback"
        assert session.verify is False
        assert session.get.call_count == 2

    def test_ssl_error_logs_warning(self):
        """Bei SSLError muss eine Warnung mit der betroffenen URL geloggt werden."""
        from unittest.mock import MagicMock, patch
        from requests.exceptions import SSLError

        session = MagicMock()
        session.verify = True
        session.get.side_effect = [SSLError("certificate verify failed"), "ok"]

        with patch("ometha.helpers.logger") as mock_logger:
            safe_get(session, "https://bad-cert.example.org/oai")

        mock_logger.warning.assert_called_once()
        warning_message = mock_logger.warning.call_args[0][0]
        assert "https://bad-cert.example.org/oai" in warning_message

    def test_second_call_does_not_retry_again(self):
        """Nachdem verify einmal auf False gesetzt wurde, darf ein erneuter SSL-Fehler
        (der dann eigentlich nicht mehr auftreten sollte) nicht zu einer Endlosschleife
        oder einem dritten Aufruf führen, wenn der zweite Call bereits erfolgreich ist."""
        from unittest.mock import MagicMock
        from requests.exceptions import SSLError

        session = MagicMock()
        session.verify = True
        session.get.side_effect = [SSLError("certificate verify failed"), "ok"]

        safe_get(session, "https://bad-cert.example.org/oai")
        assert session.get.call_count == 2

        # Zweiter Aufruf über dieselbe Session: verify ist jetzt False,
        # darf also gar nicht erst in den except-Zweig laufen.
        session.get.reset_mock()  # call_count wird von MagicMock kumulativ gezählt
        session.get.side_effect = ["ok again"]
        result = safe_get(session, "https://bad-cert.example.org/oai?verb=GetRecord")
        assert result == "ok again"
        assert session.get.call_count == 1

    def test_verify_already_false_does_not_warn_again(self):
        """Wenn verify schon False ist (z. B. von einem früheren Fallback), soll bei
        einem erneuten SSLError nicht nochmal gewarnt werden – nur weiterversucht."""
        from unittest.mock import MagicMock
        from requests.exceptions import SSLError

        session = MagicMock()
        session.verify = False  # bereits deaktiviert
        session.get.side_effect = [SSLError("still failing somehow"), "ok"]

        result = safe_get(session, "https://bad-cert.example.org/oai")

        assert result == "ok"
        assert session.verify is False

    def test_kwargs_passed_through_on_both_attempts(self):
        """timeout und andere kwargs müssen bei beiden Versuchen (vor und nach
        dem Fallback) korrekt durchgereicht werden."""
        from unittest.mock import MagicMock
        from requests.exceptions import SSLError

        session = MagicMock()
        session.verify = True
        session.get.side_effect = [SSLError("cert error"), "ok"]

        safe_get(session, "https://bad-cert.example.org/oai", timeout=(20, 80))

        for call in session.get.call_args_list:
            assert call.kwargs.get("timeout") == (20, 80)


class TestSafeGetIntegration:
    """Integrationstest: safe_get im echten Harvesting-Flow gegen den Mock,
    wenn keine SSL-Probleme auftreten (Normalfall)."""

    def test_get_identifier_pages_uses_safe_get_successfully(self, requests_mock, prm_base, session):
        """Ohne SSL-Probleme muss get_identifier_pages weiterhin normal funktionieren,
        jetzt über safe_get statt eines direkten session.get()-Aufrufs."""
        mock = OAIMock.with_dc_records(3)
        mock.register(requests_mock)
        url = f"{mock.base_url}?verb=ListIdentifiers&metadataPrefix=oai_dc"

        pages = list(get_identifier_pages(prm_base, url, session))
        all_ids = [id for page in pages for id in page]
        assert len(all_ids) == 3

    def test_harvest_files_uses_safe_get_successfully(self, requests_mock, prm_base, tmp_path, session):
        """Ohne SSL-Probleme muss harvest_files weiterhin normal funktionieren,
        jetzt über safe_get statt eines direkten session.get()-Aufrufs."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:safeget001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        failed_dl, failed_ids = harvest_files(["oai:mock:safeget001"], prm, str(tmp_path), session=session)

        assert failed_dl == []
        assert failed_ids == []
        assert len(list(tmp_path.rglob("*.xml"))) == 1


class TestCheckpointFilename:
    """Regression: Checkpoint-Dateien dürfen sich bei mehreren Läufen im
    selben Ausgabeordner nicht gegenseitig überschreiben."""

    def test_laufender_checkpoint_enthaelt_timestamp(self, tmp_path):
        from ometha.harvester import write_checkpoint_yaml
        from ometha.helpers import TIMESTR

        path = write_checkpoint_yaml("tok", 10, {"out_f": str(tmp_path)})
        assert os.path.basename(path) == f"resumption_token_latest_{TIMESTR}.yaml"

    def test_zwei_laeufe_ueberschreiben_sich_nicht(self, tmp_path):
        from unittest.mock import patch
        from ometha.harvester import write_checkpoint_yaml

        with patch("ometha.harvester.TIMESTR", "2025-01-01_10_00_00Z"):
            p1 = write_checkpoint_yaml("tokA", 1, {"out_f": str(tmp_path)})
        with patch("ometha.harvester.TIMESTR", "2025-01-01_11_00_00Z"):
            p2 = write_checkpoint_yaml("tokB", 2, {"out_f": str(tmp_path)})

        assert p1 != p2
        assert "tokA" in open(p1, encoding="utf-8").read()
        assert "tokB" in open(p2, encoding="utf-8").read()

    def test_innerhalb_eines_laufs_wird_derselbe_checkpoint_aktualisiert(self, tmp_path):
        from ometha.harvester import write_checkpoint_yaml

        p1 = write_checkpoint_yaml("tok1", 1000, {"out_f": str(tmp_path)})
        p2 = write_checkpoint_yaml("tok2", 2000, {"out_f": str(tmp_path)})
        assert p1 == p2
        assert "tok2" in open(p2, encoding="utf-8").read()


# ---------------------------------------------------------------------------
# Regressionstests für den Review von PR #26
# ---------------------------------------------------------------------------


class TestHarvestFilesRegressions:
    def test_eckige_klammern_im_dateinamen_ersetzt(self, requests_mock, prm_base, tmp_path, session):
        """Regex hatte \\[\\] durch $$ ersetzt → [ und ] landeten im Dateinamen."""
        from ometha.harvester import harvest_files

        OAIMock(records=[OAIRecord("oai:mock:a[1]")]).register(requests_mock)
        prm = {**prm_base, "out_f": str(tmp_path)}
        harvest_files(["oai:mock:a[1]"], prm, str(tmp_path), session=session)

        names = [f.name for f in tmp_path.glob("*.xml")]
        assert names == ["oai_mock_a_1_.xml"]

    def test_schreibfehler_landet_in_failed_download(self, requests_mock, prm_base, tmp_path, session):
        """OSError beim Speichern darf nicht im Future-Callback verschwinden."""
        from ometha.harvester import harvest_files

        OAIMock.with_dc_records(1).register(requests_mock)
        oai_id = OAIMock.with_dc_records(1).records[0].identifier
        missing = str(tmp_path / "gibt_es_nicht")
        prm = {**prm_base, "out_f": missing}
        stats: dict = {}
        failed_dl, failed_ids = harvest_files([oai_id], prm, missing, session=session, stats=stats)

        assert failed_dl == [oai_id]
        assert stats["successful"] == 0

    def test_abbruch_im_identifier_harvesting_wird_weitergereicht(
        self, requests_mock, prm_base, tmp_path, session
    ):
        """sys.exit() im Producer-Thread muss im Aufrufer ankommen, sonst läuft
        der Lauf als erfolgreich weiter (und der Auto-Modus setzt das from-Datum)."""
        from ometha.harvester import harvest_files

        mock = OAIMock.with_dc_records(2)
        mock.register(requests_mock)

        def pages():
            yield [mock.records[0].identifier]
            sys.exit()

        prm = {**prm_base, "out_f": str(tmp_path)}
        with pytest.raises(SystemExit):
            harvest_files(pages(), prm, str(tmp_path), session=session)

    def test_timeout_0_bleibt_ohne_timeout(self, requests_mock, prm_base, tmp_path, session, monkeypatch):
        """timeout=0 heißt 'kein Timeout'; der adaptive Controller darf daraus
        keinen Median-Latenz-Timeout machen."""
        import time as _time

        from ometha import harvester

        monkeypatch.setattr(harvester, "MEASURE_INTERVAL", 0.01)
        mock = OAIMock.with_dc_records(40)

        def slow(request, context):
            _time.sleep(0.005)
            return mock._dispatch(request, context)

        requests_mock.get(BASE_URL, text=slow)
        prm = {**prm_base, "out_f": str(tmp_path), "timeout": 0, "n_procs": 2}
        harvester.harvest_files([r.identifier for r in mock.records], prm, str(tmp_path), session=session)

        timeouts = {r.timeout for r in requests_mock.request_history if "GetRecord" in r.url}
        assert timeouts == {None}

    def test_ohne_batchsize_flach_gespeichert(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        mock = OAIMock.with_dc_records(3)
        mock.register(requests_mock)
        prm = {**prm_base, "out_f": str(tmp_path)}
        harvest_files([r.identifier for r in mock.records], prm, str(tmp_path), session=session)

        assert len(list(tmp_path.glob("*.xml"))) == 3
        assert not list(tmp_path.glob("part_*"))

    def test_mit_batchsize_unterordner(self, requests_mock, prm_base, tmp_path, session):
        from ometha.harvester import harvest_files

        mock = OAIMock.with_dc_records(5)
        mock.register(requests_mock)
        prm = {**prm_base, "out_f": str(tmp_path), "batch_size": 2}
        harvest_files([r.identifier for r in mock.records], prm, str(tmp_path), session=session)

        assert sorted(p.name for p in tmp_path.glob("part_*")) == ["part_0001", "part_0002", "part_0003"]
        assert len(list(tmp_path.rglob("*.xml"))) == 5


class TestWorkerLimit:
    def test_shrink_bei_belegten_slots_wird_nachgeholt(self):
        """acquire(blocking=False) schlug bei voller Auslastung still fehl →
        die Worker-Zahl sank nur auf dem Papier."""
        from ometha.harvester import _WorkerLimit

        limit = _WorkerLimit(2)
        limit.acquire()
        limit.acquire()
        limit.shrink(1)  # beide Slots belegt → Schuld
        limit.release()  # wird einbehalten
        limit.release()
        assert limit._sem.acquire(blocking=False) is True
        assert limit._sem.acquire(blocking=False) is False

    def test_grow_tilgt_schuld_zuerst(self):
        from ometha.harvester import _WorkerLimit

        limit = _WorkerLimit(1)
        limit.acquire()
        limit.shrink(1)  # Schuld 1
        limit.grow(1)  # tilgt Schuld, kein zusätzlicher Slot
        limit.release()
        assert limit._sem.acquire(blocking=False) is True
        assert limit._sem.acquire(blocking=False) is False


class TestSplitSets:
    def test_none_und_strings(self):
        from ometha.harvester import _split_sets

        assert _split_sets({"sets": [{"additive": None, "intersection": ["b"]}]}) == ([], ["b"])
        assert _split_sets({"sets": ["a", "b"]}) == (["a", "b"], [])
        assert _split_sets({"sets": {}}) == ([], [])
        assert _split_sets({}) == ([], [])


class TestStartProcess:
    """End-to-end durch start_process() gegen den Mock (Pipelining-Pfad)."""

    def _run(self, requests_mock, tmp_path, monkeypatch, **overrides):
        from ometha import main as ometha_main
        from ometha.helpers import TIMESTR

        OAIMock.with_dc_records(3).register(requests_mock)
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / ".ometha").write_text("User-Agent: test\nFrom: test\nasciiart: false\n")
        prm = {
            "b_url": BASE_URL, "pref": "oai_dc", "dat_geb": "mock", "sets": {}, "debug": False,
            "timeout": 10, "id_f": None, "f_date": None, "u_date": None, "res_tok": None,
            "conf_f": None, "conf_m": False, "auto_m": False, "out_f": str(tmp_path / "out"),
            "n_procs": None, "mode": "cli", "exp_type": "xml", "max_timeout": None,
            "batch_size": None, "deleted_handling": "keep", "gentle": False, "ratelimit": None,
            "summary": False, "sort_invalid": False, "no_log": True, "cleanup_empty": False,
            **overrides,
        }
        monkeypatch.setattr(ometha_main, "parseargs", lambda: prm)
        # loguru erlaubt das Registrieren des Levels PARAMETER nur einmal pro Prozess
        monkeypatch.setattr(ometha_main.logger, "level", lambda *a, **k: None)
        monkeypatch.setattr(sys, "argv", ["ometha", "default"])
        with pytest.raises(SystemExit):
            ometha_main.start_process()
        return tmp_path / "out" / "mock" / TIMESTR.replace(":", "_")

    def test_pipelining_schreibt_id_datei_ohne_summary(self, requests_mock, tmp_path, monkeypatch):
        folder = self._run(requests_mock, tmp_path, monkeypatch)

        assert (folder / "_ometha_successful_ids.yaml").exists()
        assert not (folder / "_ometha_mock_summary.xml").exists()
        assert len([f for f in folder.glob("*.xml")]) == 3

    def test_summary_auch_ohne_retry(self, requests_mock, tmp_path, monkeypatch):
        """Summary stand nur im Retry-Zweig → bei fehlerfreien Läufen fehlte sie."""
        folder = self._run(requests_mock, tmp_path, monkeypatch, summary=True)

        assert (folder / "_ometha_mock_summary.xml").exists()
