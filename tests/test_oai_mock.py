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

from tests.oai_mock import OAIMock, OAIRecord
from ometha.harvester import get_identifier
from ometha.helpers import isinvalid_xml_content, NAMESPACE

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

    def test_files_created_for_all_ids(self, requests_mock, prm_base, tmp_path):
        from ometha.harvester import harvest_files

        records = [OAIRecord(f"oai:mock:{i:03d}") for i in range(5)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        ids = [r.identifier for r in records]
        failed_dl, failed_ids = harvest_files(ids, prm_base, str(tmp_path), session=requests.Session())

        xml_files = list(tmp_path.glob("*.xml"))
        assert len(xml_files) == 5
        assert failed_dl == []
        assert failed_ids == []

    def test_nonexistent_id_goes_to_failed_download(self, requests_mock, prm_base, tmp_path):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:exists")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        ids = ["oai:mock:exists", "oai:mock:doesnotexist"]
        failed_dl, failed_ids = harvest_files(ids, prm_base, str(tmp_path), session=requests.Session())

        xml_files = list(tmp_path.glob("*.xml"))
        assert len(xml_files) == 1
        assert "oai:mock:doesnotexist" in failed_dl

    def test_json_export(self, requests_mock, prm_base, tmp_path):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:json001")]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        prm_json = {**prm_base, "exp_type": "json"}
        harvest_files(["oai:mock:json001"], prm_json, str(tmp_path), session=requests.Session())

        json_files = list(tmp_path.glob("*.json"))
        assert len(json_files) == 1

    def test_deleted_record_goes_to_failed_download(self, requests_mock, prm_base, tmp_path):
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:deleted", deleted=True)]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        failed_dl, failed_ids = harvest_files(
            ["oai:mock:deleted"], prm_base, str(tmp_path), session=requests.Session()
        )

        # Deleted records liefern kein <metadata> → Ometha behandelt als failed
        xml_files = list(tmp_path.glob("*.xml"))
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
            OAIRecord("oai:mock:after",  datestamp="2025-01-01T00:00:00Z"),
        ]
        mock = OAIMock(records=records)
        mock.register(requests_mock)

        resp = requests.get(
            f"{BASE_URL}?verb=ListIdentifiers&metadataPrefix=oai_dc"
            f"&from=2023-01-01&until=2024-01-01"
        )
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
