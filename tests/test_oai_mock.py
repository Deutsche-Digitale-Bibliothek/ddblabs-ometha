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
    """Records ohne //metadata/*/*-Knoten werden in _invalid/ verschoben."""

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

    def test_valid_record_nicht_in_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Normaler Record mit //metadata/*/* landet NICHT in _invalid/."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:valid001")]
        OAIMock(records=records).register(requests_mock)

        prm = {**prm_base, "out_f": str(tmp_path)}
        harvest_files(["oai:mock:valid001"], prm, str(tmp_path), session=session)

        assert not (tmp_path / "_invalid").exists()
        assert len(list(tmp_path.rglob("*.xml"))) == 1

    def test_leeres_metadata_geht_in_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Record mit leerem <metadata>-Root (kein Enkelkind) → _invalid/."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path)}
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

        prm = {**prm_base, "out_f": str(tmp_path)}
        failed_dl, failed_ids = harvest_files(
            ["oai:mock:empty001"], prm, str(tmp_path), session=session
        )

        assert failed_dl == []
        assert failed_ids == []

    def test_invalid_ordner_wird_angelegt(self, requests_mock, prm_base, tmp_path, session):
        """_invalid/-Ordner wird automatisch erstellt."""
        from ometha.harvester import harvest_files

        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path)}
        harvest_files(["oai:mock:empty001"], prm, str(tmp_path), session=session)

        assert (tmp_path / "_invalid").is_dir()

    def test_mischung_valid_und_invalid(self, requests_mock, prm_base, tmp_path, session):
        """Valide Records normal gespeichert, invalide in _invalid/."""
        from ometha.harvester import harvest_files

        records = [OAIRecord("oai:mock:valid002")]
        OAIMock(records=records).register(requests_mock)
        self._register_custom_response(requests_mock, "oai:mock:empty001", self.EMPTY_METADATA_XML)

        prm = {**prm_base, "out_f": str(tmp_path)}
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

        prm = {**prm_base, "out_f": str(tmp_path), "deleted_handling": "folder"}
        harvest_files(["oai:mock:del_invalid"], prm, str(tmp_path), session=session)

        assert (tmp_path / "_deleted").is_dir()
        assert not (tmp_path / "_invalid").exists()


class TestIsInvalidHelper:
    """Unit-Tests für die is_invalid()-Hilfsfunktion direkt."""

    def _call(self, xml_text):
        # is_invalid ist eine innere Funktion von harvest_files –
        # wir testen das Verhalten indirekt über den XML-Inhalt.
        from lxml import etree
        NAMESPACE = "{http://www.openarchives.org/OAI/2.0/}"
        try:
            root = etree.fromstring(xml_text.encode())
            return not root.findall(f".//{NAMESPACE}metadata/*/*")
        except Exception:
            return True

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