"""Tests for parsear_entry() budget amount mapping."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from nacional.licitaciones import parsear_entry, _sanitizar_url


def _parse_entry(filename):
    tree = ET.parse(Path(__file__).parent / "fixtures" / filename)
    return parsear_entry(tree.getroot())


class TestBudgetMapping:
    @pytest.fixture(autouse=True)
    def parsed(self):
        self.result = _parse_entry("entry_complete.xml")

    def test_valor_estimado_contrato(self):
        assert self.result["valor_estimado_contrato"] == 7809917.35

    def test_importe_sin_iva_is_tax_exclusive(self):
        assert self.result["importe_sin_iva"] == 4685950.41

    def test_importe_con_iva_is_total_amount(self):
        assert self.result["importe_con_iva"] == 5670000.00


class TestBudgetMissing:
    _XML = """\
    <entry xmlns="http://www.w3.org/2005/Atom"
           xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2"
           xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2"
           xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2"
           xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2">
        <id>https://contrataciondelestado.es/sindicacion/licitacionesPerfilContratante/NO-BUDGET</id>
        <link href="https://contrataciondelestado.es/wps/poc?uri=deeplink:detalle_licitacion&amp;idEvl=NB001"/>
        <updated>2026-03-25T10:00:00.000+01:00</updated>
        <cac-place-ext:ContractFolderStatus>
            <cbc:ContractFolderID>TEST/2026/NO-BUDGET</cbc:ContractFolderID>
            <cbc-place-ext:ContractFolderStatusCode>PUB</cbc-place-ext:ContractFolderStatusCode>
            <cac-place-ext:LocatedContractingParty>
                <cac:Party>
                    <cac:PartyName>
                        <cbc:Name>Órgano sin presupuesto</cbc:Name>
                    </cac:PartyName>
                </cac:Party>
            </cac-place-ext:LocatedContractingParty>
            <cac:ProcurementProject>
                <cbc:Name>Proyecto sin BudgetAmount</cbc:Name>
                <cbc:TypeCode>2</cbc:TypeCode>
            </cac:ProcurementProject>
        </cac-place-ext:ContractFolderStatus>
    </entry>"""

    def test_budget_fields_none_when_absent(self):
        entry = ET.fromstring(self._XML)
        result = parsear_entry(entry)
        assert result["valor_estimado_contrato"] is None
        assert result["importe_sin_iva"] is None
        assert result["importe_con_iva"] is None


class TestSanitizarUrl:
    def test_decodes_html_entities(self):
        assert _sanitizar_url("https://example.com?a=1&amp;b=2") == "https://example.com?a=1&b=2"

    def test_double_encoded_entities(self):
        assert _sanitizar_url("https://example.com?a=1&amp;amp;b=2") == "https://example.com?a=1&b=2"

    def test_none_returns_none(self):
        assert _sanitizar_url(None) is None

    def test_strips_whitespace(self):
        assert _sanitizar_url("  https://example.com  ") == "https://example.com"

    def test_already_clean_url_unchanged(self):
        assert _sanitizar_url("https://example.com?a=1&b=2") == "https://example.com?a=1&b=2"


class TestDocumentReferences:
    def test_legal_doc_parsed(self):
        result = _parse_entry("entry_complete.xml")
        assert result["doc_legal_nombre"] == "PCAP TEST EXP.pdf"
        assert "DocumentIdParam=TEST_LEGAL" in result["doc_legal_url"]
        assert "&amp;" not in result["doc_legal_url"]

    def test_technical_doc_parsed(self):
        result = _parse_entry("entry_complete.xml")
        assert result["doc_tecnico_nombre"] == "PPT TEST EXP.pdf"
        assert "DocumentIdParam=TEST_TECH" in result["doc_tecnico_url"]
        assert "&amp;" not in result["doc_tecnico_url"]

    def test_additional_docs_parsed(self):
        result = _parse_entry("entry_complete.xml")
        docs = result["docs_adicionales"]
        assert isinstance(docs, list)
        assert len(docs) == 2
        assert docs[0]["nombre"] == "CUADRO PRECIOS 1.pdf"
        assert "DocumentIdParam=TEST_ADD_1" in docs[0]["url"]
        assert "&amp;" not in docs[0]["url"]
        assert docs[0]["hash"] == "ghi789hash="

    def test_additional_doc_without_hash(self):
        result = _parse_entry("entry_complete.xml")
        docs = result["docs_adicionales"]
        assert docs[1]["hash"] is None

    def test_urls_are_sanitized(self):
        result = _parse_entry("entry_complete.xml")
        for url_field in ("doc_legal_url", "doc_tecnico_url", "url"):
            val = result[url_field]
            if val:
                assert "&amp;" not in val, f"{url_field} contains &amp;"


class TestAwardingCriteria:
    def test_criteria_parsed_as_list(self):
        result = _parse_entry("entry_complete.xml")
        assert isinstance(result["criterios_adjudicacion"], list)
        assert len(result["criterios_adjudicacion"]) == 2

    def test_obj_criterion(self):
        result = _parse_entry("entry_complete.xml")
        obj = [c for c in result["criterios_adjudicacion"] if c["tipo"] == "OBJ"]
        assert len(obj) == 1
        assert obj[0]["descripcion"] == "Oferta Económica"
        assert obj[0]["nota"] is None
        assert obj[0]["subtipo_code"] == "1"
        assert obj[0]["peso"] == 60

    def test_subj_criterion_with_note(self):
        result = _parse_entry("entry_complete.xml")
        subj = [c for c in result["criterios_adjudicacion"] if c["tipo"] == "SUBJ"]
        assert len(subj) == 1
        assert subj[0]["descripcion"] == "Memoria técnica"
        assert "propuesta técnica" in subj[0]["nota"]
        assert subj[0]["subtipo_code"] == "99"
        assert subj[0]["peso"] == 40
