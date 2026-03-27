"""
oai_mock.py – leichtgewichtiger OAI-PMH 2.0 Mock für pytest.

Nutzung:
    from tests.oai_mock import OAIMock

    def test_something(requests_mock):
        mock = OAIMock(base_url="http://mock-oai.test/oai")
        mock.register(requests_mock)
        # ... dein Harvester-Code gegen mock.base_url
"""

from __future__ import annotations

from dataclasses import dataclass, field
from textwrap import dedent
from typing import Optional
from urllib.parse import parse_qs, urlparse


OAI_NS = "http://www.openarchives.org/OAI/2.0/"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
DC_NS = "http://purl.org/dc/elements/1.1/"


# ---------------------------------------------------------------------------
# Datenmodell
# ---------------------------------------------------------------------------


@dataclass
class OAIRecord:
    identifier: str
    datestamp: str = "2024-01-01T00:00:00Z"
    sets: list[str] = field(default_factory=list)
    metadata_prefix: str = "oai_dc"
    metadata_xml: Optional[str] = None  # raw XML-String des <metadata>-Inhalts
    deleted: bool = False

    def default_dc_metadata(self) -> str:
        return dedent(f"""\
            <oai_dc:dc xmlns:oai_dc="{OAI_DC_NS}"
                       xmlns:dc="{DC_NS}">
              <dc:title>Test Record {self.identifier}</dc:title>
              <dc:identifier>{self.identifier}</dc:identifier>
            </oai_dc:dc>
        """)

    def header_xml(self) -> str:
        set_specs = "\n".join(f"      <setSpec>{s}</setSpec>" for s in self.sets)
        status = ' status="deleted"' if self.deleted else ""
        return dedent(f"""\
            <header{status}>
              <identifier>{self.identifier}</identifier>
              <datestamp>{self.datestamp}</datestamp>
              {set_specs}
            </header>
        """)

    def record_xml(self) -> str:
        if self.deleted:
            return f"<record>{self.header_xml()}</record>"
        meta = self.metadata_xml or self.default_dc_metadata()
        return dedent(f"""\
            <record>
              {self.header_xml()}
              <metadata>
                {meta}
              </metadata>
            </record>
        """)


# ---------------------------------------------------------------------------
# Envelope-Helfer
# ---------------------------------------------------------------------------


def _envelope(verb: str, base_url: str, body: str, request_params: str = "") -> str:
    # Kein führender Whitespace vor <?xml ...?> – lxml/etree.XML() erlaubt das nicht
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<OAI-PMH xmlns="{OAI_NS}" xmlns:xsi="{XSI_NS}"'
        f' xsi:schemaLocation="{OAI_NS} http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">'
        f"<responseDate>2024-01-01T00:00:00Z</responseDate>"
        f'<request verb="{verb}" {request_params}>{base_url}</request>'
        f"{body}"
        "</OAI-PMH>"
    )


def _error_envelope(base_url: str, code: str, message: str) -> str:
    body = f'<error code="{code}">{message}</error>'
    return _envelope("error", base_url, body)


# ---------------------------------------------------------------------------
# Haupt-Mock-Klasse
# ---------------------------------------------------------------------------


class OAIMock:
    """
    Konfigurierbare OAI-PMH 2.0 Mock-Schnittstelle.

    Args:
        base_url:        Die simulierte Basis-URL
        records:         Liste von OAIRecord-Objekten
        sets:            Dict {setSpec: setName}
        metadata_formats: Liste unterstützter Prefixe
        page_size:       Anzahl Records pro ResumptionToken-Seite
        simulate_503:    Wenn True, erstes Request liefert 503 (für Retry-Tests)
    """

    def __init__(
        self,
        base_url: str = "http://mock-oai.test/oai",
        records: list[OAIRecord] | None = None,
        sets: dict[str, str] | None = None,
        metadata_formats: list[str] | None = None,
        page_size: int = 100,
        simulate_503: bool = False,
    ):
        self.base_url = base_url
        self.records: list[OAIRecord] = records or []
        self.sets: dict[str, str] = sets or {}
        self.metadata_formats = metadata_formats or ["oai_dc"]
        self.page_size = page_size
        self.simulate_503 = simulate_503
        self._503_triggered = False

        # Interne Token-Map: token_str -> (verb, offset, filter_params)
        self._tokens: dict[str, dict] = {}
        self._token_counter = 0

    # ------------------------------------------------------------------
    # Registrierung bei requests_mock
    # ------------------------------------------------------------------

    def register(self, requests_mock) -> None:
        """Registriert alle OAI-Verben als requests_mock-Handler."""
        requests_mock.get(self.base_url, text=self._dispatch)
        requests_mock.head(self.base_url, text=lambda req, ctx: "")

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def _dispatch(self, request, context) -> str:
        context.status_code = 200
        context.headers["Content-Type"] = "text/xml; charset=utf-8"

        if self.simulate_503 and not self._503_triggered:
            self._503_triggered = True
            context.status_code = 503
            context.headers["Retry-After"] = "1"
            return ""

        params = parse_qs(urlparse(request.url).query)

        def p(key):
            return params.get(key, [None])[0]

        verb = p("verb")
        if not verb:
            return _error_envelope(self.base_url, "badVerb", "No verb supplied")

        handlers = {
            "Identify": self._handle_identify,
            "ListMetadataFormats": self._handle_list_metadata_formats,
            "ListSets": self._handle_list_sets,
            "ListIdentifiers": self._handle_list_identifiers,
            "ListRecords": self._handle_list_records,
            "GetRecord": self._handle_get_record,
        }

        handler = handlers.get(verb)
        if not handler:
            return _error_envelope(self.base_url, "badVerb", f"Unknown verb: {verb}")

        return handler(params, context)

    # ------------------------------------------------------------------
    # Verb-Handler
    # ------------------------------------------------------------------

    def _handle_identify(self, params, context) -> str:
        body = dedent(f"""\
            <Identify>
              <repositoryName>OAI Mock Repository</repositoryName>
              <baseURL>{self.base_url}</baseURL>
              <protocolVersion>2.0</protocolVersion>
              <adminEmail>mock@example.org</adminEmail>
              <earliestDatestamp>2000-01-01T00:00:00Z</earliestDatestamp>
              <deletedRecord>transient</deletedRecord>
              <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
            </Identify>
        """)
        return _envelope("Identify", self.base_url, body)

    def _handle_list_metadata_formats(self, params, context) -> str:
        formats_xml = "\n".join(
            dedent(f"""\
                <metadataFormat>
                  <metadataPrefix>{fmt}</metadataPrefix>
                  <schema>http://example.org/{fmt}.xsd</schema>
                  <metadataNamespace>http://example.org/{fmt}/</metadataNamespace>
                </metadataFormat>
            """)
            for fmt in self.metadata_formats
        )
        return _envelope(
            "ListMetadataFormats",
            self.base_url,
            f"<ListMetadataFormats>{formats_xml}</ListMetadataFormats>",
        )

    def _handle_list_sets(self, params, context) -> str:
        if not self.sets:
            return _error_envelope(
                self.base_url, "noSetHierarchy", "This repository does not support sets"
            )
        sets_xml = "\n".join(
            f"<set><setSpec>{spec}</setSpec><setName>{name}</setName></set>"
            for spec, name in self.sets.items()
        )
        return _envelope("ListSets", self.base_url, f"<ListSets>{sets_xml}</ListSets>")

    def _filtered_records(self, params: dict) -> list[OAIRecord]:
        """Gibt Records zurück gefiltert nach set, from, until.
        Datumsvergleich normalisiert auf ISO-Datumspräfix (YYYY-MM-DD),
        um Mischformen wie '2024-03-15' vs '2024-03-15T00:00:00Z' korrekt zu behandeln.
        """

        def p(key):
            return (params.get(key) or [None])[0]

        def date_prefix(s: str) -> str:
            return s[:10] if s else s

        set_filter = p("set")
        from_date = p("from")
        until_date = p("until")

        result = self.records
        if set_filter:
            result = [r for r in result if set_filter in r.sets]
        if from_date:
            result = [
                r for r in result if date_prefix(r.datestamp) >= date_prefix(from_date)
            ]
        if until_date:
            result = [
                r for r in result if date_prefix(r.datestamp) <= date_prefix(until_date)
            ]
        return result

    def _make_token(self, verb: str, offset: int, params: dict) -> str:
        self._token_counter += 1
        token = f"token_{verb}_{self._token_counter}"
        self._tokens[token] = {"verb": verb, "offset": offset, "params": params}
        return token

    def _handle_list_identifiers(self, params, context) -> str:
        return self._list_response(params, context, verb="ListIdentifiers")

    def _handle_list_records(self, params, context) -> str:
        return self._list_response(params, context, verb="ListRecords")

    def _list_response(self, params, context, verb: str) -> str:
        def p(key):
            return (params.get(key) or [None])[0]

        # ResumptionToken-Fortsetzung
        res_token = p("resumptionToken")
        if res_token:
            if res_token not in self._tokens:
                return _error_envelope(
                    self.base_url, "badResumptionToken", f"Unknown token: {res_token}"
                )
            token_data = self._tokens[res_token]
            offset = token_data["offset"]
            orig_params = token_data["params"]
        else:
            prefix = p("metadataPrefix")
            if not prefix:
                return _error_envelope(
                    self.base_url, "badArgument", "metadataPrefix is required"
                )
            if prefix not in self.metadata_formats:
                return _error_envelope(
                    self.base_url,
                    "cannotDisseminateFormat",
                    f"Unknown prefix: {prefix}",
                )
            offset = 0
            orig_params = params

        all_records = self._filtered_records(orig_params)
        total = len(all_records)

        if total == 0:
            return _error_envelope(
                self.base_url, "noRecordsMatch", "No records match the given criteria"
            )

        page = all_records[offset : offset + self.page_size]
        next_offset = offset + self.page_size

        if verb == "ListIdentifiers":
            items_xml = "\n".join(r.header_xml() for r in page)
        else:
            items_xml = "\n".join(r.record_xml() for r in page)

        # ResumptionToken wenn weitere Seiten vorhanden
        if next_offset < total:
            new_token = self._make_token(verb, next_offset, orig_params)
            resumption_xml = (
                f'<resumptionToken completeListSize="{total}" cursor="{offset}">'
                f"{new_token}</resumptionToken>"
            )
        else:
            resumption_xml = (
                f'<resumptionToken completeListSize="{total}" cursor="{offset}"/>'
            )

        body = f"<{verb}>\n{items_xml}\n{resumption_xml}\n</{verb}>"
        return _envelope(verb, self.base_url, body)

    def _handle_get_record(self, params, context) -> str:
        def p(key):
            return (params.get(key) or [None])[0]

        identifier = p("identifier")
        prefix = p("metadataPrefix")

        if not identifier or not prefix:
            return _error_envelope(
                self.base_url,
                "badArgument",
                "identifier and metadataPrefix are required",
            )

        record = next((r for r in self.records if r.identifier == identifier), None)
        if not record:
            return _error_envelope(
                self.base_url, "idDoesNotExist", f"No record with id: {identifier}"
            )

        body = f"<GetRecord>{record.record_xml()}</GetRecord>"
        return _envelope("GetRecord", self.base_url, body)

    # ------------------------------------------------------------------
    # Convenience-Methoden
    # ------------------------------------------------------------------

    @classmethod
    def with_dc_records(cls, n: int = 5, **kwargs) -> "OAIMock":
        """Erstellt eine Mock-Instanz mit n einfachen oai_dc-Records."""
        records = [
            OAIRecord(
                identifier=f"oai:mock.test:{i:04d}",
                datestamp=f"2024-{(i % 12) + 1:02d}-01T00:00:00Z",
                sets=["setA"] if i % 2 == 0 else ["setB"],
            )
            for i in range(n)
        ]
        return cls(
            records=records,
            sets={"setA": "Set A", "setB": "Set B"},
            **kwargs,
        )
