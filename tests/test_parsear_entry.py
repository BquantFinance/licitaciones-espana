"""Tests for parsear_entry() budget amount mapping."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from nacional.licitaciones import parsear_entry


def _parse_entry(filename):
    tree = ET.parse(Path(__file__).parent / "fixtures" / filename)
    return parsear_entry(tree.getroot())


class TestBudgetMapping:
    @pytest.fixture(autouse=True)
    def parsed(self):
        self.result = _parse_entry("entry_budget.xml")

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
