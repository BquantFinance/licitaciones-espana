"""
Tests offline de los scripts TED (ted/*.py).

Sin red: las respuestas de la TED Search API v3 y del CSV bulk de data.europa.eu
se simulan. Además de funciones sueltas se ejecutan las rutas "main" documentadas
(descarga, cross-validation, diagnóstico y análisis sector salud) sobre una copia
de los scripts en un árbol temporal con la misma estructura que el repo
(<tmp>/ted, <tmp>/nacional), lanzados desde un cwd distinto a la raíz.
"""

import gzip
import hashlib
import importlib.util
import io
import csv
import json
import logging
import os
import re
import runpy
import shutil
import subprocess
import sys
import threading
import urllib.error
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests

REPO_DIR = Path(__file__).resolve().parent.parent
TED_DIR = REPO_DIR / "ted"


def _load(name, filename):
    spec = importlib.util.spec_from_file_location(name, TED_DIR / filename)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


tm = _load("ted_module_t", "ted_module.py")
rtc = _load("run_ted_crossvalidation_t", "run_ted_crossvalidation.py")
cvp = _load("cross_validation_ted_placsp_t", "cross-validation_ted_placsp.py")

_ORIG_READ_CSV = pd.read_csv
ORIG_DESCARGAR_XML = tm._descargar_xml   # la real (el fixture ted_xml la sustituye en cada test)


# ═══════════════════════════════════════════════════════════════════════════
#  Simuladores HTTP (TED API v3 + CSV bulk)
# ═══════════════════════════════════════════════════════════════════════════

class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code}")


class FakeTedApi:
    """POST /v3/notices/search con paginación PAGE_NUMBER.

    notices: lista de (yyyymmdd, notice). cap: nº máximo de resultados que la
    API sirve por query (límite de paginación, devuelve páginas vacías después).
    fail_pages: páginas que siempre devuelven HTTP 500.
    """

    def __init__(self, notices, cap=None, fail_pages=()):
        self.notices = notices
        self.cap = cap
        self.fail_pages = set(fail_pages)
        self.calls = []

    ultima = None   # la última API simulada que ha respondido (FakeTedXml sirve el XML de sus avisos)

    def __call__(self, url, json=None, timeout=None, headers=None, **kw):
        assert url == "https://api.ted.europa.eu/v3/notices/search"
        FakeTedApi.ultima = self
        self.calls.append(json)
        q = json["query"]
        d_from = re.search(r"publication-date>=(\d{8})", q).group(1)
        d_to = re.search(r"publication-date<=(\d{8})", q).group(1)
        sel = [n for d, n in self.notices if d_from <= d <= d_to]
        page, limit = json["page"], json["limit"]
        if page in self.fail_pages:
            return FakeResponse(500)
        start, end = (page - 1) * limit, page * limit
        if self.cap is not None:
            end = min(end, self.cap)
        chunk = sel[start:end] if start < end else []
        return FakeResponse(200, {"notices": chunk, "totalNoticeCount": len(sel)})


def _notice(pub, buyer_ids=("P4109100J",), buyer_name="Ayuntamiento de Sevilla",
            winners=(), win_names=(), values=(), dates=(), internal_id=None,
            cpv="79000000", offers_codes=(), offers_vals=(), city=("Sevilla",),
            notice_type="can-standard"):
    n = {
        "publication-number": pub,
        "notice-type": notice_type,
        "buyer-name": {"spa": [buyer_name]},
        "buyer-identifier": list(buyer_ids),
        "buyer-country": ["ESP"],
        "buyer-city": list(city),
        "classification-cpv": [cpv],
        "winner-identifier": list(winners),
        "tender-value": list(values),
        "tender-value-cur": ["EUR"] * len(values),
        "winner-decision-date": list(dates),
    }
    if win_names:
        n["winner-name"] = {"spa": list(win_names)}
    if internal_id:
        n["internal-identifier-proc"] = [internal_id]
    if offers_vals:
        n["received-submissions-type-code"] = list(offers_codes)
        n["received-submissions-type-val"] = list(offers_vals)
    return n


_NS_XML = (
    'xmlns="urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2" '
    'xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2" '
    'xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2" '
    'xmlns:efac="http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1" '
    'xmlns:efbc="http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1" '
    'xmlns:efext="http://data.europa.eu/p27/eforms-ubl-extensions/1" '
    'xmlns:ext="urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2"')


def _eforms(resultados, ofertas, contratos, partes, organizaciones, lotes=(), total=None):
    """XML eForms mínimo de un aviso de adjudicación (bytes). Cada lista trae los fragmentos XML de
    sus elementos (efac:LotResult, efac:LotTender, efac:SettledContract, efac:TenderingParty,
    efac:Organization y cac:ProcurementProjectLot)."""
    total = f'<cbc:TotalAmount currencyID="EUR">{total}</cbc:TotalAmount>' if total else ''
    return (f'<?xml version="1.0" encoding="UTF-8"?><ContractAwardNotice {_NS_XML}>'
            '<ext:UBLExtensions><ext:UBLExtension><ext:ExtensionContent><efext:EformsExtension>'
            f'<efac:NoticeResult>{total}{"".join(resultados)}{"".join(ofertas)}{"".join(contratos)}'
            f'{"".join(partes)}</efac:NoticeResult>'
            f'<efac:Organizations>{"".join(organizaciones)}</efac:Organizations>'
            '</efext:EformsExtension></ext:ExtensionContent></ext:UBLExtension></ext:UBLExtensions>'
            f'{"".join(lotes)}</ContractAwardNotice>').encode("utf-8")


def _resultado(res, lote, ofertas=(), contratos=(), estado="selec-w", motivo=None, estadisticas=()):
    return (f'<efac:LotResult><cbc:ID schemeName="result">{res}</cbc:ID>'
            f'<cbc:TenderResultCode listName="winner-selection-status">{estado}</cbc:TenderResultCode>'
            + (f'<efac:DecisionReason><efbc:DecisionReasonCode listName="non-award-justification">{motivo}'
               '</efbc:DecisionReasonCode></efac:DecisionReason>' if motivo else '')
            + ''.join(f'<efac:LotTender><cbc:ID schemeName="tender">{t}</cbc:ID></efac:LotTender>' for t in ofertas)
            + ''.join(f'<efac:ReceivedSubmissionsStatistics><efbc:StatisticsCode listName="received-submission-'
                      f'type">{c}</efbc:StatisticsCode><efbc:StatisticsNumeric>{v}</efbc:StatisticsNumeric>'
                      '</efac:ReceivedSubmissionsStatistics>' for c, v in estadisticas)
            + ''.join(f'<efac:SettledContract><cbc:ID schemeName="contract">{c}</cbc:ID></efac:SettledContract>'
                      for c in contratos)
            + f'<efac:TenderLot><cbc:ID schemeName="Lot">{lote}</cbc:ID></efac:TenderLot></efac:LotResult>')


def _oferta(ten, parte, lote, valor=None, moneda="EUR"):
    importe = (f'<cac:LegalMonetaryTotal><cbc:PayableAmount currencyID="{moneda}">{valor}</cbc:PayableAmount>'
               '</cac:LegalMonetaryTotal>') if valor is not None else ''
    return (f'<efac:LotTender><cbc:ID schemeName="tender">{ten}</cbc:ID>{importe}'
            f'<efac:TenderingParty><cbc:ID schemeName="tendering-party">{parte}</cbc:ID></efac:TenderingParty>'
            f'<efac:TenderLot><cbc:ID>{lote}</cbc:ID></efac:TenderLot></efac:LotTender>')


def _contrato(con, ofertas, fecha=None, referencia=None):
    return (f'<efac:SettledContract><cbc:ID schemeName="contract">{con}</cbc:ID>'
            + (f'<cbc:AwardDate>{fecha}</cbc:AwardDate>' if fecha else '')
            + (f'<efac:ContractReference><cbc:ID>{referencia}</cbc:ID></efac:ContractReference>' if referencia else '')
            + ''.join(f'<efac:LotTender><cbc:ID schemeName="tender">{t}</cbc:ID></efac:LotTender>' for t in ofertas)
            + '</efac:SettledContract>')


def _parte(tpa, miembros, lider=None):
    return (f'<efac:TenderingParty><cbc:ID schemeName="tendering-party">{tpa}</cbc:ID>'
            + ''.join(f'<efac:Tenderer><cbc:ID schemeName="organization">{o}</cbc:ID>'
                      + ('<efbc:GroupLeadIndicator>true</efbc:GroupLeadIndicator>' if o == lider else '')
                      + '</efac:Tenderer>' for o in miembros)
            + '</efac:TenderingParty>')


def _organizacion(org, nombre, nif, tamano=None, ids=None):
    """efac:Organization; ids: [(schemeName, valor)] de sus BT-501 (por defecto, el NIF)."""
    ids = ids if ids is not None else ([("NIF", nif)] if nif else [])
    return ('<efac:Organization><efac:Company>'
            + (f'<efbc:CompanySizeCode listName="economic-operator-size">{tamano}</efbc:CompanySizeCode>'
               if tamano else '')
            + f'<cac:PartyIdentification><cbc:ID schemeName="organization">{org}</cbc:ID></cac:PartyIdentification>'
            f'<cac:PartyName><cbc:Name languageID="SPA">{nombre}</cbc:Name></cac:PartyName>'
            + ''.join(f'<cac:PartyLegalEntity><cbc:CompanyID schemeName="{e}">{v}</cbc:CompanyID></cac:PartyLegalEntity>'
                      for e, v in ids)
            + '</efac:Company></efac:Organization>')


def _lote(lote, titulo=None, estimado=None):
    return (f'<cac:ProcurementProjectLot><cbc:ID schemeName="Lot">{lote}</cbc:ID><cac:ProcurementProject>'
            + (f'<cbc:Name languageID="SPA">{titulo}</cbc:Name>' if titulo else '')
            + (f'<cac:RequestedTenderTotal><cbc:EstimatedOverallContractAmount currencyID="EUR">{estimado}'
               '</cbc:EstimatedOverallContractAmount></cac:RequestedTenderTotal>' if estimado else '')
            + '</cac:ProcurementProject></cac:ProcurementProjectLot>')


def _xml_de_aviso(n):
    """XML eForms de un aviso simulado (_notice): un lote, una oferta ganadora (la cita su contrato)
    y un contrato por posición de sus listas (ganador, importe, fecha), que es lo que describían
    estos avisos de prueba; sin ninguna, un resultado de lote sin oferta."""
    ganadores = list(n.get("winner-identifier", []))
    nombres = list((n.get("winner-name") or {}).get("spa", []))
    valores = list(n.get("tender-value", []))
    fechas = list(n.get("winner-decision-date", []))
    estadisticas = list(zip(n.get("received-submissions-type-code", []), n.get("received-submissions-type-val", [])))
    k = max(len(ganadores), len(nombres), len(valores), len(fechas))
    if k == 0:
        return _eforms([_resultado("RES-0001", "LOT-0001", estado="clos-nw")], [], [], [], [], [_lote("LOT-0001")])
    res, ofs, cons, pts, orgs, lotes = [], [], [], [], [], []
    for i in range(k):
        lote, ten, con, tpa, org = (f"LOT-{i + 1:04d}", f"TEN-{i + 1:04d}", f"CON-{i + 1:04d}",
                                    f"TPA-{i + 1:04d}", f"ORG-{i + 2:04d}")
        hay_ganador = i < len(ganadores) or i < len(nombres)
        res.append(_resultado(f"RES-{i + 1:04d}", lote, [ten], [con], estadisticas=estadisticas))
        ofs.append(_oferta(ten, tpa, lote, valores[i] if i < len(valores) else None))
        cons.append(_contrato(con, [ten], fechas[i] if i < len(fechas) else None))
        pts.append(_parte(tpa, [org] if hay_ganador else []))
        if hay_ganador:
            orgs.append(_organizacion(org, nombres[i] if i < len(nombres) else "",
                                      ganadores[i] if i < len(ganadores) else ""))
        lotes.append(_lote(lote))
    return _eforms(res, ofs, cons, pts, orgs, lotes)


class FakeTedXml:
    """GET https://ted.europa.eu/en/notice/<número>/xml: el XML de un aviso fijado (fijos) o, si no,
    el generado (_xml_de_aviso) para el aviso que sirve la última API simulada; si no, 404.
    codigos: {número: código HTTP} para simular fallos."""

    def __init__(self):
        self.calls, self.fijos, self.codigos = [], {}, {}

    def __call__(self, url):
        numero = re.search(r"/notice/([^/]+)/xml$", url).group(1)
        self.calls.append(numero)
        if numero in self.codigos:
            return self.codigos[numero], b"", {}
        if numero in self.fijos:
            return 200, self.fijos[numero], {}
        api = FakeTedApi.ultima
        aviso = next((n for _, n in (api.notices if api else []) if n.get("publication-number") == numero), None)
        return (200, _xml_de_aviso(aviso), {}) if aviso is not None else (404, b"", {})


@pytest.fixture(autouse=True)
def ted_xml(monkeypatch):
    """XML eForms de los avisos sin red (ver FakeTedXml)."""
    FakeTedApi.ultima = None
    fake = FakeTedXml()
    monkeypatch.setattr(tm, "_descargar_xml", fake)
    return fake


def _filas(n):
    """Filas de un aviso simulado con su XML eForms generado."""
    return tm._parse_api_notice(n, _xml_de_aviso(n))


def _api_notices_2024():
    return [
        ("20240320", _notice("100001-2024", buyer_ids=["P4109100J"], buyer_name="Ayuntamiento de Sevilla",
                             winners=["ESB11111111"], win_names=["EMPRESA UNO SL"], values=["305000"],
                             dates=["2024-03-15+01:00"], internal_id="SEV-2024-001",
                             offers_codes=["t-sme", "tenders"], offers_vals=["1", "4"])),
        ("20240410", _notice("100002-2024", buyer_ids=["P2100000H"], buyer_name="Diputación Provincial de Huelva",
                             values=["495000"], dates=["2024-04-01+02:00"], internal_id="exp-2024-002",
                             city=["Huelva"])),
        ("20240715", _notice("100003-2024", buyer_ids=["ESS2800000A"], buyer_name="Consejería de Hacienda",
                             winners=["A99999999"], win_names=["OTRA EMPRESA SA"], values=["1000000"],
                             dates=["2024-07-01+02:00"], city=["Madrid"])),
        ("20241105", _notice("100004-2024", buyer_ids=["Q2800001B"], buyer_name="Hospital Universitario La Paz",
                             winners=["A10000001", "A10000002"], win_names=["LAB UNO SA", "LAB DOS SA"],
                             values=["250000", "260000"], dates=["2024-10-20+02:00", "2024-10-28+01:00"],
                             cpv="33600000", city=["Madrid"])),
    ]


TED_CSV_2019 = (
    "ID_NOTICE_CAN,YEAR,ISO_COUNTRY_CODE,CAE_NAME,CAE_NATIONALID,CAE_TYPE,CAE_TOWN,TYPE_OF_CONTRACT,"
    "CPV,VALUE_EURO_FIN_1,AWARD_VALUE_EURO_FIN_1,WIN_NAME,WIN_NATIONALID,WIN_COUNTRY_CODE,NUMBER_OFFERS,"
    "DT_DISPATCH,DT_AWARD,CANCELLED\n"
    "2019/S 001-000001,2019,ES,Ayuntamiento de Sevilla,ESP4109100J,3,Sevilla,S,79000000,400000,400000,"
    "EMPRESA TRES SL,ESB33333333,ES,3,2019-02-01,2019-01-20,0\n"
    "2019/S 001-000002,2019,ES,Otro organo,ESP9999999X,3,Jaen,U,33000000,,350000,"
    "EMPRESA SIETE SL,ESB77777777,ES,2,2019-03-01,2019-02-20,0\n"
    "2019/S 001-000003,2019,ES,Organo cancelado,ESP8888888X,3,Leon,W,45000000,900000,,"
    "EMPRESA X,ESB12121212,ES,1,2019-04-01,2019-03-20,1\n"
    "2019/S 001-000004,2019,FR,Mairie de Paris,FR123,3,Paris,S,79000000,500000,500000,"
    "SOCIETE,FR999,FR,4,2019-05-01,2019-04-20,0\n"
)


def _csv_anual_fake(csv_by_year):
    """tm._descargar simulado: sirve el CSV bulk de cada año en su URL «TED 2020» (cualquier otra, 404)."""
    def fake(url, destino):
        m = re.search(r"notices%20(\d{4})\.csv$", url)
        if m and m.group(1) in csv_by_year:
            Path(destino).write_text(csv_by_year[m.group(1)], encoding="utf-8")
            return
        raise requests.exceptions.HTTPError(f"HTTP 404 {url}")
    return fake


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(tm.time, "sleep", lambda s: None)


@pytest.fixture
def ted_http(monkeypatch, no_sleep):
    """Red simulada: CSV bulk 2019 + API v3 2024."""
    api = FakeTedApi(_api_notices_2024())
    monkeypatch.setattr(requests, "post", api)
    monkeypatch.setattr(tm, "_descargar", _csv_anual_fake({"2019": TED_CSV_2019}))
    return api


# ═══════════════════════════════════════════════════════════════════════════
#  Fixtures PLACSP
# ═══════════════════════════════════════════════════════════════════════════

DEP_LOCAL = "Sector Público > Entidades Locales > Andalucía > {}"
DEP_SALUD = "Sector Público > Comunidades y Ciudades Autónomas > Comunidad de Madrid > Servicio Madrileño de Salud"


def _placsp_df():
    base = dict(tipo_registro="LICITACION", estado="Adjudicada", conjunto="licitaciones",
                procedimiento="Abierto", tipo_contrato="Servicios", ano=2024.0,
                fecha_adjudicacion=pd.Timestamp("2024-03-01"), cpv_principal="79000000")
    salud = dict(organo_contratante="Hospital Universitario La Paz", nif_organo="Q2800001B",
                 dependencia=DEP_SALUD, tipo_contrato="Suministros", cpv_principal="33600000")
    rows = [
        # a) E1: NIF adjudicatario + importe
        dict(expediente="1/2024", organo_contratante="Ayuntamiento de Sevilla", nif_organo="P4109100J",
             dependencia=DEP_LOCAL.format("Sevilla"), nif_adjudicatario="B11111111",
             adjudicatario="EMPRESA UNO SL", importe=300_000),
        # b) E2: nº expediente + importe
        dict(expediente="EXP-2024-002", organo_contratante="Diputación Provincial de Huelva",
             nif_organo="P2100000H", dependencia=DEP_LOCAL.format("Huelva"),
             nif_adjudicatario="B44444444", adjudicatario="EMPRESA CUATRO SL", importe=500_000),
        # c) E3: NIF órgano + importe
        dict(expediente="CM-2024-77", organo_contratante="Consejería de Hacienda", nif_organo="S2800000A",
             dependencia="Sector Público > Comunidades y Ciudades Autónomas > Comunidad de Madrid",
             nif_adjudicatario="B22222222", adjudicatario="EMPRESA DOS SL", importe=980_000),
        # d) AGE (Ministerio): umbral 143K -> SARA, sin match -> missing
        dict(expediente="DEF-2024-9", organo_contratante="Junta de Contratación del Ministerio de Defensa",
             nif_organo="S2830001I",
             dependencia="Sector Público > Administración General del Estado > Ministerio de Defensa",
             nif_adjudicatario="B88888888", adjudicatario="EMPRESA OCHO SL", importe=180_000),
        # e) entidad local ('Sector Público > ...' contiene 'ICO '): NO es AGE -> umbral 221K -> no SARA
        dict(expediente="PMD-2024-5", organo_contratante="Patronato Municipal de Deportes de Málaga",
             nif_organo="P7900001A", dependencia=DEP_LOCAL.format("Málaga"),
             nif_adjudicatario="B90000001", adjudicatario="EMPRESA NUEVE SL", importe=180_000),
        # f) concesión de servicios de 1M: umbral de concesiones (5,538M) -> no SARA
        dict(expediente="COR-2024-1", organo_contratante="Ayuntamiento de Córdoba", nif_organo="P1402100J",
             dependencia=DEP_LOCAL.format("Córdoba"), tipo_contrato="Concesión Servicios",
             nif_adjudicatario="B90000002", adjudicatario="CONCESIONARIA SA", importe=1_000_000),
        # g) gestión de servicios públicos (no SARA)
        dict(expediente="GRA-2016-1", organo_contratante="Ayuntamiento de Granada", nif_organo="P1808900C",
             dependencia=DEP_LOCAL.format("Granada"), tipo_contrato="Gestión Servicios Públicos",
             nif_adjudicatario="B90000003", adjudicatario="GESTORA SA", importe=400_000, ano=2016.0,
             fecha_adjudicacion=pd.Timestamp("2016-05-01")),
        # h) sin nº de expediente: no debe cruzarse con avisos TED sin internal_id
        dict(expediente=None, organo_contratante="Ayuntamiento de Jaén", nif_organo="P2305000C",
             dependencia=DEP_LOCAL.format("Jaén"), nif_adjudicatario="B66666666",
             adjudicatario="EMPRESA SEIS SL", importe=352_000),
        # i) mismo nº de expediente que (a) pero OTRO órgano: E6 no debe propagar
        dict(expediente="1/2024", organo_contratante="Ayuntamiento de Cádiz", nif_organo="P1101200D",
             dependencia=DEP_LOCAL.format("Cádiz"), nif_adjudicatario="B55555555",
             adjudicatario="EMPRESA CINCO SL", importe=400_000),
        # j1, j2) sanidad: E1 contra los 2 lotes del aviso 100004-2024
        dict(salud, expediente="LP-2024-1", nif_adjudicatario="A10000001", adjudicatario="LAB UNO SA",
             importe=250_000),
        dict(salud, expediente="LP-2024-2", nif_adjudicatario="A10000002", adjudicatario="LAB DOS SA",
             importe=260_000),
        # j3) sanidad sin match -> missing
        dict(salud, expediente="LP-2024-3", nif_adjudicatario="A10000003", adjudicatario="LAB TRES SA",
             importe=300_000, cpv_principal="33140000"),
        # k) sanidad, negociado sin publicidad sin match -> SARA pero no 'missing'
        dict(salud, expediente="LP-2024-4", nif_adjudicatario="A10000004", adjudicatario="LAB CUATRO SA",
             importe=300_000, procedimiento="Negociado sin publicidad"),
        # l) E1 contra una fila del CSV bulk 2019 (sin campos eForms: win_size, internal_id...)
        dict(expediente="SEV-2019-7", organo_contratante="Ayuntamiento de Sevilla", nif_organo="P4109100J",
             dependencia=DEP_LOCAL.format("Sevilla"), nif_adjudicatario="B33333333",
             adjudicatario="EMPRESA TRES SL", importe=400_000, ano=2019.0,
             fecha_adjudicacion=pd.Timestamp("2019-01-15")),
        # excluidos: contrato menor y privado
        dict(expediente="MEN-1", organo_contratante="Ayuntamiento de Sevilla", nif_organo="P4109100J",
             dependencia=DEP_LOCAL.format("Sevilla"), nif_adjudicatario="B11111111",
             adjudicatario="EMPRESA UNO SL", importe=15_000, conjunto="menores"),
        dict(expediente="PRIV-1", organo_contratante="Ayuntamiento de Sevilla", nif_organo="P4109100J",
             dependencia=DEP_LOCAL.format("Sevilla"), nif_adjudicatario="B99999990",
             adjudicatario="EMPRESA PRIV SL", importe=900_000, tipo_contrato="Privado"),
    ]
    out = []
    for i, r in enumerate(rows):
        r = {**base, **r}
        imp = float(r.pop("importe"))
        # Mismo valor en las 3 columnas de importe: los tests no dependen de cuál
        # se use como proxy del valor estimado (issue #6)
        r.update(id=f"placsp-{i}", importe_adjudicacion=imp, importe_sin_iva=imp,
                 valor_estimado_contrato=imp)
        out.append(r)
    return pd.DataFrame(out)


def _make_repo(tmp_path, scripts=()):
    (tmp_path / "ted").mkdir(parents=True, exist_ok=True)
    (tmp_path / "nacional").mkdir(parents=True, exist_ok=True)
    # run_ted_crossvalidation.py lee PLACSP con nacional.licitaciones.leer_placsp
    shutil.copy(REPO_DIR / "nacional" / "licitaciones.py", tmp_path / "nacional" / "licitaciones.py")
    for s in scripts:
        shutil.copy(TED_DIR / s, tmp_path / "ted" / s)
    # Los scripts de análisis importan ted_module (avisos_para_cruce), que importa comun/
    shutil.copy(TED_DIR / "ted_module.py", tmp_path / "ted" / "ted_module.py")
    shutil.copytree(REPO_DIR / "comun", tmp_path / "comun", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__"))
    return tmp_path


def _run_script(repo, script, monkeypatch):
    """Ejecuta un script como __main__ desde un cwd que NO es la raíz del repo."""
    other_cwd = repo / "otro_cwd"
    other_cwd.mkdir(exist_ok=True)
    monkeypatch.chdir(other_cwd)
    monkeypatch.setattr(sys, "argv", [script])
    return runpy.run_path(str(repo / "ted" / script), run_name="__main__")


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — parser API v3
# ═══════════════════════════════════════════════════════════════════════════

class TestParseApiNotice:
    def test_multilot_record_per_winner(self):
        recs = _filas(_api_notices_2024()[3][1])
        assert len(recs) == 2
        assert [r["win_nationalid"] for r in recs] == ["A10000001", "A10000002"]
        assert [r["tender_value"] for r in recs] == ["250000", "260000"]
        assert [r["lot_index"] for r in recs] == [0, 1]
        assert all(r["year"] == "2024" and r["ted_notice_id"] == "100004-2024" for r in recs)
        assert recs[0]["cae_nationalid"] == "Q2800001B"
        assert recs[0]["cpv"] == "33600000"

    def test_buyer_city_list_is_not_stringified(self):
        rec = _filas(_api_notices_2024()[0][1])[0]
        assert rec["cae_town"] == "Sevilla"

    def test_number_offers_uses_tenders_statistic(self):
        # BT-760 se repite por tipo (t-sme, tenders...): nº de ofertas = 'tenders'
        rec = _filas(_api_notices_2024()[0][1])[0]
        assert rec["number_offers"] == "4"
        assert rec["internal_id_proc"] == "SEV-2024-001"

    def test_number_offers_de_cada_resultado_de_lote(self):
        # Antes la lista de la API iba por posición (y sin códigos se tomaba el primer valor)
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"], estadisticas=[("t-sme", 1), ("tenders", 7)]),
                       _resultado("RES-0002", "LOT-0002", ["TEN-0002"], estadisticas=[("tenders", 2)])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 10), _oferta("TEN-0002", "TPA-0001", "LOT-0002", 20)],
                      [_contrato("CON-0001", ["TEN-0001"]), _contrato("CON-0002", ["TEN-0002"])],
                      [_parte("TPA-0001", ["ORG-0002"])], [_organizacion("ORG-0002", "EMP", "B11111111")])
        assert [r["number_offers"] for r in tm._parse_api_notice(_notice("1-2024"), xml)] == ["7", "2"]

    def test_sin_xml_una_fila_con_los_datos_del_aviso(self):
        recs = tm._parse_api_notice(_notice("1-2024", winners=["B1", "B2"], values=["1", "2"]))
        assert len(recs) == 1 and recs[0]["win_name"] == "" and recs[0]["tender_value"] == ""
        assert recs[0]["_xml_eforms"] == "sin XML" and recs[0]["cae_nationalid"] == "P4109100J"


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — paginación / errores API
# ═══════════════════════════════════════════════════════════════════════════

def _many_notices(n, year=2024):
    out = []
    for i in range(n):
        month = 1 + (i * 12) // n
        date = f"{year}{month:02d}15"
        out.append((date, _notice(f"{200000 + i}-{year}", winners=[f"B{i:08d}"],
                                  win_names=[f"EMP {i}"], values=[str(100000 + i)])))
    return out


class TestApiPagination:
    def test_query_and_page_numbers(self, monkeypatch, no_sleep):
        api = FakeTedApi(_many_notices(250))
        monkeypatch.setattr(requests, "post", api)
        records, hit_limit, complete = tm._download_api_period(2024, "20240101", "20241231", "2024")
        assert len(records) == 250
        assert (hit_limit, complete) == (False, True)
        assert [c["page"] for c in api.calls] == [1, 2, 3]
        body = api.calls[0]
        assert body["limit"] == 100 and body["scope"] == "ALL"
        assert body["paginationMode"] == "PAGE_NUMBER"
        assert "buyer-country=ESP" in body["query"]
        assert "publication-date>=20240101" in body["query"]
        assert "publication-date<=20241231" in body["query"]
        assert "can-standard" in body["query"]
        # Las filas salen del XML: la API ya no pide las listas por posición (ganador, importe...)
        assert "title-proc" in body["fields"] and "change-notice-version-identifier" in body["fields"]
        assert "tender-value" not in body["fields"] and "winner-identifier" not in body["fields"]

    def test_silent_pagination_cap_triggers_quarter_split(self, monkeypatch, tmp_path, no_sleep):
        # La API sirve como máximo 150 resultados por query (páginas vacías después)
        api = FakeTedApi(_many_notices(250), cap=150)
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024, force=True)
        assert len(df) == 250
        assert df["ted_notice_id"].is_unique
        labels = {(re.search(r">=(\d{8})", c["query"]).group(1)) for c in api.calls}
        assert {"20240101", "20240401", "20240701", "20241001"} <= labels
        assert (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    def test_http_errors_are_not_cached_as_complete_year(self, monkeypatch, tmp_path, no_sleep, caplog):
        api = FakeTedApi(_many_notices(250), fail_pages={2})
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        records, hit_limit, complete = tm._download_api_period(2024, "20240101", "20241231", "2024")
        assert len(records) == 100 and complete is False
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            df = tm._download_api_year(2024, force=True)
        assert len(df) == 100
        assert not (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        assert "INCOMPLETA" in caplog.text

    def test_download_does_not_persist_incomplete_consolidated(self, monkeypatch, tmp_path, no_sleep, caplog):
        api = FakeTedApi(_many_notices(250), fail_pages={2})
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm, "_descargar", _csv_anual_fake({}))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            df = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert df is not None and len(df) == 100
        assert df.attrs.get("sin_guardar")   # main() sale con 1
        assert not (tmp_path / "ted_es_can.parquet").exists()
        assert "2024" in caplog.text


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — normalización
# ═══════════════════════════════════════════════════════════════════════════

class TestNormalize:
    def test_dates_with_mixed_utc_offsets_are_parsed(self):
        recs = []
        for _, n in _api_notices_2024():
            recs.extend(_filas(n))
        df = tm._normalize_ted_data(pd.DataFrame(recs))
        # +01:00 (invierno) y +02:00 (verano) mezclados: no deben quedar todo NaT
        assert df["dt_award"].notna().sum() == 5
        assert pd.api.types.is_datetime64_any_dtype(df["dt_award"])
        assert df.loc[df["ted_notice_id"] == "100001-2024", "dt_award"].iloc[0] == pd.Timestamp("2024-03-15")

    def test_numeric_and_importe(self):
        recs = []
        for _, n in _api_notices_2024():
            recs.extend(_filas(n))
        df = tm._normalize_ted_data(pd.DataFrame(recs))
        assert df["importe_ted"].tolist() == [305000.0, 495000.0, 1000000.0, 250000.0, 260000.0]
        assert df["year"].tolist() == [2024] * 5
        assert df.loc[0, "win_nif_clean"] == "B11111111"
        assert df.loc[0, "number_offers"] == 4


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — CLI (download / validate)
# ═══════════════════════════════════════════════════════════════════════════

class TestTedModuleCli:
    def test_default_data_dir_matches_readme_layout(self):
        # README: ted/ted_es_can.parquet y ted/ted_can_{año}_ES*.parquet;
        # run_ted_crossvalidation.py lee ted/ted_es_can.parquet
        assert tm.TEDConfig.DATA_DIR.resolve() == TED_DIR
        assert rtc.TED_PATH.resolve() == TED_DIR / "ted_es_can.parquet"

    def test_download_end_to_end(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download", "--years", "2019,2024"])
        tm.main()
        out = tmp_path / "ted_es_can.parquet"
        assert out.exists()
        assert (tmp_path / "ted_can_2019_ES.parquet").exists()
        assert (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        df = pd.read_parquet(out)
        # CSV: 4 filas -> 1 FR filtrada; la cancelada se conserva (cancelled='1', regla 1 del repo);
        # API: 4 avisos -> 5 registros (lotes)
        assert len(df) == 3 + 5
        cancelada = df[df["ted_notice_id"] == "2019/S 001-000003"]
        assert len(cancelada) == 1 and cancelada["cancelled"].astype(str).tolist() == ["1"]
        assert sorted(df["source"].unique()) == ["api_v3", "csv_bulk"]
        csv = df[df["source"] == "csv_bulk"].set_index("ted_notice_id")
        assert csv.loc["2019/S 001-000001", "importe_ted"] == 400000
        assert csv.loc["2019/S 001-000002", "importe_ted"] == 350000   # VALUE_EURO vacío: usa AWARD_VALUE
        assert csv.loc["2019/S 001-000001", "tipo_contrato"] == "servicios"
        assert csv.loc["2019/S 001-000001", "win_nif_clean"] == "B33333333"
        assert csv.loc["2019/S 001-000001", "year"] == 2019
        for col in ["ted_notice_id", "year", "cae_name", "cae_nationalid", "win_nationalid",
                    "importe_ted", "win_nif_clean", "cpv", "number_offers", "internal_id_proc",
                    "total_value", "estimated_value_proc", "dt_award"]:
            assert col in df.columns, col
        assert df.loc[df["source"] == "api_v3", "dt_award"].notna().all()

    def test_lfs_pointer_cache_is_redownloaded(self, monkeypatch, tmp_path, ted_http, caplog):
        # Clon sin 'git lfs pull': ted/*.parquet son punteros de texto, no parquet
        pointer = "version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 123\n"
        for name in ["ted_es_can.parquet", "ted_can_2019_ES.parquet", "ted_can_2024_ES_api.parquet"]:
            (tmp_path / name).write_text(pointer)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            df = tm.download_ted_spain(years=[2019, 2024])
        assert len(df) == 8   # 3 de 2019 (la cancelada se conserva) + 5 de la API
        assert "Cache ilegible" in caplog.text
        assert len(pd.read_parquet(tmp_path / "ted_es_can.parquet")) == 8

    def test_validate_creates_output_dir(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path / "data")
        monkeypatch.setattr(tm.TEDConfig, "OUTPUT_DIR", tmp_path / "salida")
        tm.download_ted_spain(years=[2019, 2024])
        pipe = pd.DataFrame({
            "_nif": ["B11111111", "B66666666", "B55555555"],
            "_imp_adj": [300_000.0, 352_000.0, 400_000.0],
            "_año": [2024.0, np.nan, 2024.0],
            # leído de CSV: la fecha llega como texto
            "_fecha_adj": ["2024-03-01", "2024-05-01", "2024-06-01"],
            "_es_menor": [False, False, False],
            "_organ": ["Ayuntamiento de Sevilla", "Ayuntamiento de Jaén", "Ayuntamiento de Cádiz"],
            "_adj": ["EMPRESA UNO SL", "EMPRESA SEIS SL", "EMPRESA CINCO SL"],
            "_expediente": ["1/2024", None, "1/2024"],
            "_cpv": ["79000000"] * 3,
        })
        pipe_path = tmp_path / "pipeline.csv"
        pipe.to_csv(pipe_path, index=False)
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "validate", "--pipeline-file", str(pipe_path)])
        tm.main()
        out = tmp_path / "salida" / "v6_0_missing_in_ted.csv"
        assert out.exists()
        missing = pd.read_csv(out)
        # Solo Sevilla casa (E1); Jaén (sin expediente) no se cruza con avisos TED sin internal_id
        assert sorted(missing["_organ"]) == ["Ayuntamiento de Cádiz", "Ayuntamiento de Jaén"]


class TestCrossValidateTedModule:
    def test_enrichment_has_no_nan_strings(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019, 2024])
        ted = pd.read_parquet(tmp_path / "ted_es_can.parquet")      # como en 'validate'
        pipe = pd.DataFrame({"_nif": ["B33333333", "B44444444"], "_imp_adj": [400_000.0, 500_000.0],
                             "_año": [2019.0, 2024.0], "_es_menor": [False, False],
                             "_expediente": ["SEV-2019-7", "EXP-2024-002"]})
        out, _ = tm.cross_validate_ted(pipe, ted, "NAC")
        assert out["_ted_validated"].all()
        assert out["_ted_id"].tolist() == ["2019/S 001-000001", "100002-2024"]
        for col in ["_ted_win_size", "_ted_internal_id", "_ted_direct_award", "_ted_buyer_legal_type"]:
            assert not out[col].isin(["nan", "None"]).any(), col
        assert out.loc[0, "_ted_cpv"] == "79000000"
        assert out.loc[0, "_ted_n_ofertas"] == 3

    def test_null_expediente_does_not_match_notices_without_internal_id(self):
        ted = pd.DataFrame({"ted_notice_id": ["2019/S 1"], "year": [2019], "importe_ted": [350_000.0],
                            "win_nif_clean": ["B77777777"], "internal_id_proc": [None]})
        buf = io.BytesIO()
        ted.to_parquet(buf, index=False)
        buf.seek(0)
        ted = pd.read_parquet(buf)
        pipe = pd.DataFrame({"_nif": ["B66666666"], "_imp_adj": [352_000.0], "_año": [2024.0],
                             "_es_menor": [False], "_expediente": ["None"]})
        out, missing = tm.cross_validate_ted(pipe, ted, "NAC")
        assert not out.loc[0, "_ted_validated"]
        assert len(missing) == 1


# ═══════════════════════════════════════════════════════════════════════════
#  run_ted_crossvalidation.py — reglas SARA
# ═══════════════════════════════════════════════════════════════════════════

# (año, obras, servicios AGE, servicios resto, sectores especiales) — README + Reglamentos UE
_BIENIOS = [
    (2016, 5_225_000, 135_000, 209_000, 418_000),
    (2017, 5_225_000, 135_000, 209_000, 418_000),
    (2018, 5_548_000, 144_000, 221_000, 443_000),
    (2019, 5_548_000, 144_000, 221_000, 443_000),
    (2020, 5_350_000, 139_000, 214_000, 428_000),
    (2021, 5_350_000, 139_000, 214_000, 428_000),
    (2022, 5_382_000, 140_000, 215_000, 431_000),
    (2023, 5_382_000, 140_000, 215_000, 431_000),
    (2024, 5_538_000, 143_000, 221_000, 443_000),
    (2025, 5_538_000, 143_000, 221_000, 443_000),
    (2026, 5_404_000, 140_000, 216_000, 432_000),
    (2027, 5_404_000, 140_000, 216_000, 432_000),
]


class TestSaraRules:
    @pytest.mark.parametrize("year,obras,age,resto,sect", _BIENIOS)
    def test_thresholds_per_bienio(self, year, obras, age, resto, sect):
        f = rtc.get_sara_threshold
        assert f(year, "Obras", False, False) == obras
        assert f(year, "Obras", True, False) == obras
        assert f(year, "Servicios", True, False) == age
        assert f(year, "Suministros", True, False) == age
        assert f(year, "Servicios", False, False) == resto
        assert f(year, "Suministros", False, True) == sect

    def test_concesiones_use_works_threshold(self):
        # Art. 20 LCSP / Directiva 2014/23/UE: concesiones de obras y de servicios
        assert rtc.get_sara_threshold(2024, "Concesión Obras", False, False) == 5_538_000
        assert rtc.get_sara_threshold(2024, "Concesión Servicios", False, False) == 5_538_000
        assert rtc.get_sara_threshold(2021, "Concesión Servicios", True, False) == 5_350_000

    @pytest.mark.parametrize("tipo", ["Gestión Servicios Públicos", "Gestion Servicios Publicos",
                                      "Privado", "Patrimonial", "Administrativo Especial",
                                      "", "nan", "None", None, np.nan])
    def test_non_sara_types(self, tipo):
        assert rtc.get_sara_threshold(2024, tipo, False, False) is None

    @pytest.mark.parametrize("dep,expected", [
        ("Sector Público > Administración General del Estado > Ministerio de Defensa", (True, False)),
        ("SECTOR PUBLICO > ADMINISTRACION GENERAL DEL ESTADO", (True, False)),
        ("Sector Público > Entidades Locales > Andalucía > Sevilla", (False, False)),
        ("Sector Público > Entidades Locales > Consorcio Turístico de la Costa", (False, False)),
        ("Sector Público > Entidades Locales > Castilla y León > Ayuntamiento de Boecillo", (False, False)),
        ("Sector Público > Otras > Tesorería General de la Seguridad Social", (True, False)),
        ("Sector Público > Otras > Instituto de Crédito Oficial (ICO)", (True, False)),
        ("Sector Público > Otras > ADIF", (False, True)),
        ("Sector Público > Entidades Locales > Metro de Málaga", (False, True)),
        (None, (False, False)),
    ])
    def test_classify_buyer(self, dep, expected):
        assert rtc.classify_buyer(dep) == expected


# ═══════════════════════════════════════════════════════════════════════════
#  Pipeline completo: descarga -> cross-validation -> diagnóstico -> salud
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def crossval_repo(tmp_path, monkeypatch, ted_http):
    repo = _make_repo(tmp_path, ["run_ted_crossvalidation.py", "diagnostico_missing_ted.py",
                                 "analisis_sector_salud.py", "cross-validation_ted_placsp.py"])
    # TED descargado con ted_module (red simulada) en <repo>/ted, como documenta el README
    monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", repo / "ted")
    tm.download_ted_spain(years=[2019, 2024])
    assert (repo / "ted" / "ted_es_can.parquet").exists()
    _placsp_df().to_parquet(repo / "nacional" / "licitaciones_espana.parquet", index=False)
    _run_script(repo, "run_ted_crossvalidation.py", monkeypatch)
    return repo


class TestRunTedCrossvalidationE2E:
    def test_outputs_and_documented_names(self, crossval_repo):
        ted = crossval_repo / "ted"
        for name in ["crossval_sara.parquet", "crossval_matched.parquet",
                     "crossval_missing.parquet", "missing_alta_confianza.parquet"]:
            assert (ted / name).exists(), name
        sara = pd.read_parquet(ted / "crossval_sara.parquet")
        for col in ["expediente", "nif_adjudicatario", "_ted_validated", "_ted_missing",
                    "_match_strategy", "_ted_id", "_umbral_sara", "_es_sara"]:
            assert col in sara.columns, col

    def test_matching_results(self, crossval_repo):
        sara = pd.read_parquet(crossval_repo / "ted" / "crossval_sara.parquet")
        sara["_key"] = sara["organo_contratante"] + "|" + sara["expediente"].fillna("<sin>")
        s = sara.set_index("_key")
        assert s["_es_sara"].all()
        assert len(s) == 11
        strat = s["_match_strategy"].to_dict()
        assert strat["Ayuntamiento de Sevilla|1/2024"] == "E1_E2"
        assert strat["Diputación Provincial de Huelva|EXP-2024-002"] == "E1_E2"
        assert strat["Consejería de Hacienda|CM-2024-77"] == "E3_nif_org"
        assert strat["Hospital Universitario La Paz|LP-2024-1"] == "E1_E2"
        assert strat["Hospital Universitario La Paz|LP-2024-2"] == "E1_E2"
        assert strat["Ayuntamiento de Sevilla|SEV-2019-7"] == "E1_E2"
        assert s.loc["Ayuntamiento de Sevilla|1/2024", "_ted_id"] == "100001-2024"
        assert s.loc["Consejería de Hacienda|CM-2024-77", "_ted_id"] == "100003-2024"
        missing = set(s.index[s["_ted_missing"]])
        assert missing == {
            "Junta de Contratación del Ministerio de Defensa|DEF-2024-9",
            "Ayuntamiento de Jaén|<sin>",          # sin expediente: no casa con avisos sin internal_id
            "Ayuntamiento de Cádiz|1/2024",        # mismo expediente que Sevilla, otro órgano: no E6
            "Hospital Universitario La Paz|LP-2024-3",
        }
        neg = s.loc["Hospital Universitario La Paz|LP-2024-4"]
        assert not neg["_ted_validated"] and not neg["_ted_missing"]
        # Totales coherentes: validados + missing + neg. sin pub. no validados = SARA
        assert s["_ted_validated"].sum() == 6
        # Enriquecimiento sin cadenas 'nan'/'None'
        for col in ["_ted_cpv", "_ted_win_size", "_ted_internal_id", "_ted_direct_award"]:
            assert not s[col].isin(["nan", "None", "NaN"]).any(), col
        assert s.loc["Ayuntamiento de Sevilla|1/2024", "_ted_internal_id"] == "SEV-2024-001"
        assert s.loc["Ayuntamiento de Sevilla|1/2024", "_ted_n_ofertas"] == 4
        assert s.loc["Ayuntamiento de Sevilla|SEV-2019-7", "_ted_n_ofertas"] == 3
        assert s.loc["Ayuntamiento de Sevilla|SEV-2019-7", "_ted_win_size"] == ""

    def test_non_sara_rows_excluded(self, crossval_repo):
        sara = pd.read_parquet(crossval_repo / "ted" / "crossval_sara.parquet")
        for exp in ["PMD-2024-5", "COR-2024-1", "GRA-2016-1", "MEN-1", "PRIV-1"]:
            assert exp not in set(sara["expediente"]), exp
        umbral = sara.set_index("expediente")["_umbral_sara"]
        assert umbral["DEF-2024-9"] == 143_000
        assert umbral["EXP-2024-002"] == 221_000
        assert umbral["LP-2024-1"] == 221_000

    def test_missing_file_matches_flags(self, crossval_repo):
        ted = crossval_repo / "ted"
        sara = pd.read_parquet(ted / "crossval_sara.parquet")
        miss = pd.read_parquet(ted / "crossval_missing.parquet")
        matched = pd.read_parquet(ted / "crossval_matched.parquet")
        assert len(miss) == sara["_ted_missing"].sum() == 4
        assert len(matched) == sara["_ted_validated"].sum() == 6

    def test_diagnostico_runs_on_crossval_outputs(self, crossval_repo, monkeypatch, capsys):
        _run_script(crossval_repo, "diagnostico_missing_ted.py", monkeypatch)
        out = capsys.readouterr().out
        assert "Missing cargados: 4" in out
        assert "CLASIFICACION FINAL DE MISSING" in out
        assert (crossval_repo / "data" / "ted" / "missing_alta_confianza.parquet").exists()

    def test_analisis_salud_runs_on_crossval_outputs(self, crossval_repo, monkeypatch, capsys):
        _run_script(crossval_repo, "analisis_sector_salud.py", monkeypatch)
        out = capsys.readouterr().out
        assert "SARA v1 cargado: 11" in out
        assert "Contratos SARA salud: 4" in out
        assert "CPV (2 digitos) en contratos salud" in out
        assert "No se encontro columna CPV" not in out
        # Sin grupos de lotes, la cobertura ajustada debe ser igual a la bruta (2/4)
        m = re.search(r"Ajustado por lotes.*?Matched: (\d+) / (\d+) = ([\d.]+)%", out, re.S)
        assert m and (m.group(1), m.group(2), m.group(3)) == ("2", "4", "50.0")

    def test_legacy_crossvalidation_script_runs(self, crossval_repo, monkeypatch, capsys):
        _run_script(crossval_repo, "cross-validation_ted_placsp.py", monkeypatch)
        out = capsys.readouterr().out
        outdir = crossval_repo / "data" / "ted"
        assert (outdir / "crossval_matched.parquet").exists()
        assert (outdir / "crossval_stats.txt").exists()
        matched = pd.read_parquet(outdir / "crossval_matched.parquet")
        assert set(matched["expediente"]) == {"1/2024", "EXP-2024-002", "LP-2024-1", "LP-2024-2",
                                              "SEV-2019-7"}
        assert "Tamaño ganador: 0" in out
        pct = float(re.search(r"Missing in TED: [\d,]+ \(([\d.]+)% de no-menores", out).group(1))
        assert pct <= 100.0
        # No debe pisar las salidas de run_ted_crossvalidation.py en ted/
        assert len(pd.read_parquet(crossval_repo / "ted" / "crossval_matched.parquet")) == 6


# ═══════════════════════════════════════════════════════════════════════════
#  run_ted_crossvalidation.py — casos sueltos
# ═══════════════════════════════════════════════════════════════════════════

def test_sara_de_un_ano_sin_ted_queda_sin_evaluar(tmp_path, monkeypatch, ted_http):
    """TED solo cubre 2019 y 2024: un SARA de 2025 sin coincidencia no es 'missing'
    (el snapshot no puede decir si se publicó); el resultado va por id y versión."""
    repo = _make_repo(tmp_path, ["run_ted_crossvalidation.py"])
    monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", repo / "ted")
    tm.download_ted_spain(years=[2019, 2024])
    df = _placsp_df().assign(fecha_updated=pd.Timestamp("2025-07-01 10:00", tz="UTC"))
    extra = df[df["expediente"] == "DEF-2024-9"].assign(
        id="placsp-2025", expediente="DEF-2025-1", ano=2025.0,
        fecha_adjudicacion=pd.Timestamp("2025-06-01"))
    pd.concat([df, extra], ignore_index=True).to_parquet(
        repo / "nacional" / "licitaciones_espana.parquet", index=False)
    _run_script(repo, "run_ted_crossvalidation.py", monkeypatch)

    sara = pd.read_parquet(repo / "ted" / "crossval_sara.parquet").set_index("expediente")
    assert {"id", "fecha_updated", "_ano", "_ted_anio_cubierto"} <= set(sara.columns)
    assert sara.loc["DEF-2025-1", "id"] == "placsp-2025"
    assert not sara.loc["DEF-2025-1", "_ted_anio_cubierto"]
    assert not sara.loc["DEF-2025-1", "_ted_missing"] and not sara.loc["DEF-2025-1", "_ted_validated"]
    # El mismo contrato en 2024 (año cubierto) sí es missing
    assert sara.loc["DEF-2024-9", "_ted_anio_cubierto"] and sara.loc["DEF-2024-9", "_ted_missing"]
    missing = pd.read_parquet(repo / "ted" / "crossval_missing.parquet")
    assert "DEF-2025-1" not in set(missing["expediente"]) and "DEF-2024-9" in set(missing["expediente"])
    assert len(missing) == sara["_ted_missing"].sum()


class TestLoadPlacspSemantica:
    """load_placsp sobre la semántica corregida de PLACSP (issue #6 y versiones repetidas)."""

    def _fila(self, id_, importe_vec, importe_pbl=None, **over):
        row = dict(id=id_, tipo_registro="LICITACION", estado="Adjudicada", conjunto="licitaciones",
                   procedimiento="Abierto", tipo_contrato="Servicios", ano=2024.0,
                   fecha_adjudicacion=pd.Timestamp("2024-03-01"), cpv_principal="79000000",
                   expediente=f"EXP-{id_}", organo_contratante="Ayuntamiento de Sevilla",
                   nif_organo="P4109100J", dependencia=DEP_LOCAL.format("Sevilla"),
                   nif_adjudicatario="B11111111", adjudicatario="EMPRESA UNO SL",
                   importe_adjudicacion=importe_pbl if importe_pbl is not None else importe_vec,
                   importe_sin_iva=importe_pbl, valor_estimado_contrato=importe_vec)
        row.update(over)
        return row

    def _cargar(self, tmp_path, filas):
        path = tmp_path / "placsp.parquet"
        pd.DataFrame(filas).to_parquet(path, index=False)
        return rtc.load_placsp(path).set_index("id")

    def test_versiones_repetidas_cuentan_una_vez(self, tmp_path):
        filas = [self._fila("a", 300_000.0, estado="Adjudicada"),
                 self._fila("a", 300_000.0, estado="Resuelta"),
                 self._fila("a", 300_000.0, estado="Resuelta")]
        df = self._cargar(tmp_path, filas)
        assert len(df) == 1 and df.loc["a", "_es_sara"]

    def test_umbral_sobre_valor_estimado(self, tmp_path):
        # Presupuesto sin IVA 150K (< 221K) pero valor estimado 250K (prórrogas): SARA
        df = self._cargar(tmp_path, [self._fila("a", 250_000.0, importe_pbl=150_000.0),
                                     self._fila("b", np.nan, importe_pbl=150_000.0)])
        assert df.loc["a", "_imp_sara"] == 250_000.0 and df.loc["a", "_es_sara"]
        # Sin valor estimado se usa el presupuesto sin IVA
        assert df.loc["b", "_imp_sara"] == 150_000.0 and not df.loc["b", "_es_sara"]

    def test_esquema_publicado_v2026_02(self, tmp_path):
        # Parquet antiguo: sin valor_estimado_contrato e importe_sin_iva = valor estimado
        fila = self._fila("a", 250_000.0)
        fila.pop("valor_estimado_contrato")
        fila["importe_sin_iva"] = 250_000.0
        fila["importe_adjudicacion"] = 200_000.0
        df = self._cargar(tmp_path, [fila])
        assert df.loc["a", "_imp_sara"] == 250_000.0 and df.loc["a", "_es_sara"]
        assert df.loc["a", "_imp_match"] == 200_000.0

    def test_sin_tipo_registro_ni_concesiones_excluidas(self, tmp_path):
        filas = [self._fila("a", 6_000_000.0, tipo_contrato="Concesión Servicios"),
                 self._fila("b", 900_000.0, tipo_contrato="Otros"),
                 self._fila("c", 300_000.0, conjunto="consultas")]
        for f in filas:
            f.pop("tipo_registro")
        df = self._cargar(tmp_path, filas)
        assert list(df.index) == ["a"]
        assert df.loc["a", "_umbral_sara"] == rtc.get_sara_threshold(2024, "Obras", False, False)

    def test_suma_de_lotes_por_organo_sin_multiplicar_el_valor_estimado(self, tmp_path):
        filas = [
            # mismo expediente, valor estimado distinto por lote: 120K + 130K >= 221K -> SARA por lotes
            self._fila("l1", 120_000.0, expediente="LOT-1"),
            self._fila("l2", 130_000.0, expediente="LOT-1"),
            # el mismo valor estimado repetido (el del expediente completo): cuenta una vez
            self._fila("r1", 150_000.0, expediente="REP-1"),
            self._fila("r2", 150_000.0, expediente="REP-1"),
            # mismo nº de expediente en otro órgano: no se suma con LOT-1
            self._fila("o1", 120_000.0, expediente="LOT-1", organo_contratante="Ayuntamiento de Cádiz",
                       dependencia=DEP_LOCAL.format("Cádiz")),
        ]
        df = self._cargar(tmp_path, filas)
        assert df.loc[["l1", "l2"], "_sara_por_lotes"].all()
        assert not df.loc[["r1", "r2", "o1"], "_es_sara"].any()


class TestRunTedUnits:
    def _placsp(self, **over):
        row = dict(_es_sara=True, _imp_match=350_000.0, _ano=2024.0, _nif="B66666666",
                   _expediente=np.nan, _sara_por_lotes=False, _imp_sara=350_000.0,
                   nif_organo="P2305000C", organo_contratante="Ayuntamiento de Jaén")
        row.update(over)
        return pd.DataFrame([row])

    def test_e2_skips_null_expediente_and_ted_without_internal_id(self):
        ted = pd.DataFrame({
            "ted_notice_id": ["2019/S 1", "2019/S 2"], "year": [2019, 2024],
            "importe_ted": [350_000.0, 350_000.0], "win_nif_clean": ["B77777777", ""],
            "internal_id_proc": [None, None],
        })
        buf = io.BytesIO()
        ted.to_parquet(buf, index=False)
        buf.seek(0)
        ted = pd.read_parquet(buf)
        for exp in [np.nan, None, "None"]:
            matched, *_ = rtc.run_e1_e2(self._placsp(_expediente=exp), ted)
            assert matched == [], exp

    def test_e6_propagates_only_within_same_organ(self):
        df = pd.DataFrame([
            dict(_es_sara=True, _es_neg_sin_pub=False, _expediente="1/2024", _imp_match=300_000.0,
                 _ano=2024, organo_contratante="Ayto A", nif_organo=""),
            dict(_es_sara=True, _es_neg_sin_pub=False, _expediente="1/2024", _imp_match=400_000.0,
                 _ano=2024, organo_contratante="Ayto B", nif_organo=""),
            dict(_es_sara=True, _es_neg_sin_pub=False, _expediente="1/2024", _imp_match=100_000.0,
                 _ano=2024, organo_contratante="Ayto A", nif_organo=""),
        ])
        ted = pd.DataFrame({"ted_notice_id": ["X-2024"], "importe_ted": [300_000.0], "year": [2024],
                            "cae_nationalid": [""], "cae_name": ["Otro"]})
        adv = rtc.run_advanced_matching(df, ted, [0], {0: {"ted_id": "X-2024"}}, {"X-2024"})
        assert adv["e6_matched_idx"] == {2}
        assert adv["e6_ted_ids"] == {2: "X-2024"}

    def test_advanced_strategies_each_find_their_match(self):
        base = dict(_es_sara=True, _es_neg_sin_pub=False, _ano=2024, nif_organo="")
        df = pd.DataFrame([
            dict(base, _expediente="R0", organo_contratante="Organo Tres", nif_organo="P1111111A",
                 _imp_match=495_000.0),                                            # E3
            dict(base, _expediente="R1a", organo_contratante="Organo Cuatro", nif_organo="P2222222B",
                 _imp_match=600_000.0),                                            # E4 (lotes)
            dict(base, _expediente="R1b", organo_contratante="Organo Cuatro", nif_organo="P2222222B",
                 _imp_match=410_000.0),                                            # E4 (lotes)
            dict(base, _expediente="R2", organo_contratante="Ayuntamiento de Villanueva del Río",
                 _imp_match=305_000.0),                                            # E5
            dict(base, _expediente="R3", organo_contratante="ADIF - Presidencia",
                 _imp_match=1_990_000.0),                                          # E3b
            dict(base, _expediente="R4", organo_contratante="Consorcio de Bomberos de la Provincia de Malaga",
                 _imp_match=790_000.0),                                            # E7
        ])
        ted = pd.DataFrame({
            "ted_notice_id": ["T0", "T1", "T2", "T3", "T4"],
            "importe_ted": [500_000.0, 1_000_000.0, 300_000.0, 2_000_000.0, 800_000.0],
            "total_value": [np.nan, 1_000_000.0, np.nan, np.nan, np.nan],
            "estimated_value_proc": [np.nan] * 5,
            "year": [2024] * 5,
            "cae_nationalid": ["ESP1111111A", "P2222222B", "", "", ""],
            "cae_name": ["Organo Tres", "Organo Cuatro", "AYUNTAMIENTO DE VILLANUEVA DEL RIO",
                         "Administrador de Infraestructuras Ferroviarias",
                         "Consorcio Provincial de Bomberos de Malaga"],
        })
        adv = rtc.run_advanced_matching(df, ted, [], {}, set())
        tid = lambda t_idx: adv["ted_valid"].loc[t_idx, "ted_notice_id"]  # noqa: E731
        assert [(s, tid(t)) for s, t, _ in adv["e3_matched"]] == [(0, "T0")]
        assert [(sorted(ix), tid(t)) for ix, t, _, _ in adv["e4_matched_groups"]] == [([1, 2], "T1")]
        assert [(s, tid(t)) for s, t, _ in adv["e5_matched"]] == [(3, "T2")]
        assert [(s, tid(t)) for s, t, _ in adv["e3b_matched"]] == [(4, "T3")]
        assert [(s, tid(t)) for s, t, _ in adv["e7_matched"]] == [(5, "T4")]
        assert len(adv["df_missing_final"]) == 0


# ═══════════════════════════════════════════════════════════════════════════
#  diagnostico_missing_ted.py / analisis_sector_salud.py — casos sueltos
# ═══════════════════════════════════════════════════════════════════════════

def test_diagnostico_tries_all_years_for_each_row(tmp_path, monkeypatch, capsys):
    repo = _make_repo(tmp_path, ["diagnostico_missing_ted.py"])
    ted = repo / "ted"
    miss = pd.DataFrame({
        "organo_contratante": ["Ayuntamiento de Alfa", "Ayuntamiento de Beta"],
        "nif_organo": ["P0000001A", "P0000002B"],
        "nif_adjudicatario": ["B1", "B2"], "expediente": ["A-1", "B-1"],
        "importe_adjudicacion": [300_000.0, 500_000.0], "ano": [2024.0, 2024.0],
    })
    miss.to_parquet(ted / "crossval_missing.parquet", index=False)
    miss.to_parquet(ted / "crossval_sara.parquet", index=False)
    miss.iloc[:0].to_parquet(ted / "crossval_matched.parquet", index=False)
    pd.DataFrame({
        "cae_name": ["Ayuntamiento de Alfa", "Ayuntamiento de Beta", "Ayuntamiento de Beta"],
        "cae_nationalid": ["P0000001A", "P0000002B", "P0000002B"],
        "year": [2024, 2024, 2023], "importe_ted": [300_000.0, 999_000.0, 500_000.0],
        "ted_notice_id": ["T1", "T2", "T3"], "win_nif_clean": ["", "", ""],
    }).to_parquet(ted / "ted_es_can.parquet", index=False)
    _run_script(repo, "diagnostico_missing_ted.py", monkeypatch)
    out = capsys.readouterr().out
    # La fila de Beta casa en el año anterior (2023); antes se cortaba tras el 1er año con avisos
    assert "Match por buyer name + importe: 2 " in out
    assert "Match por NIF organo + importe: 2 " in out


def _sara_salud(rows):
    base = dict(_es_sara=True, _ted_validated=False, _ted_missing=False, ano=2024.0,
                _tipo_contrato="Suministros", cpv_principal="33600000", _match_strategy="",
                nif_adjudicatario="A1")
    return pd.DataFrame([{**base, **r} for r in rows])


def test_analisis_salud_ccaa_is_not_substring_based(tmp_path, monkeypatch, capsys):
    repo = _make_repo(tmp_path, ["analisis_sector_salud.py"])
    rows = []
    for i in range(12):
        rows.append(dict(organo_contratante="Hospital Universitari Joan XXIII de Tarragona",
                         expediente=f"T-{i}", importe_adjudicacion=300_000.0 + i * 50_000,
                         _ted_validated=(i < 6), _ted_missing=(i >= 6),
                         _match_strategy="E1_E2" if i < 6 else ""))
    for i in range(12):
        rows.append(dict(organo_contratante="Hospital de las Casas Viejas", expediente=f"C-{i}",
                         importe_adjudicacion=300_000.0 + i * 50_000, _ted_validated=True,
                         _match_strategy="E3_nif_org"))
    rows.append(dict(organo_contratante="Ayuntamiento de Reus", expediente="R-1",
                     importe_adjudicacion=500_000.0, _tipo_contrato="Obras"))
    _sara_salud(rows).to_parquet(repo / "ted" / "crossval_sara.parquet", index=False)
    pd.DataFrame({"importe_ted": [1.0], "year": [2024]}).to_parquet(repo / "ted" / "ted_es_can.parquet")
    _run_script(repo, "analisis_sector_salud.py", monkeypatch)
    out = capsys.readouterr().out
    ccaa = out.split("SALUD POR COMUNIDAD AUTONOMA")[1].split("RESUMEN")[0]
    # 'TARRAGONA' contiene 'ARAGON' y 'CASAS' contiene 'SAS' (Servicio Andaluz de Salud)
    assert "Aragon" not in ccaa
    assert "Andalucia" not in ccaa


def test_legacy_missing_pct_uses_consistent_denominator(capsys):
    # cross-validation_ted_placsp.py: una fila sobre umbral con NIF válido y otra sin NIF,
    # ninguna en TED -> 2 missing de 2 contratos no-menores sobre umbral (antes: 200%)
    placsp = pd.DataFrame({
        "_nif": ["B12345678", ""], "_imp_adj": [300_000.0, 300_000.0], "_año": [2024.0, 2024.0],
        "_expediente": ["A-1", "A-2"], "_es_menor": [False, False], "_sobre_umbral_ue": [True, True],
        "_organ": ["Org", "Org"],
    })
    ted = pd.DataFrame({"importe_ted": [1.0], "year": [2024], "win_nif_clean": [""],
                        "ted_notice_id": ["T"], "internal_id_proc": [""]})
    _, missing = cvp.cross_validate(placsp, ted)
    out = capsys.readouterr().out
    assert len(missing) == 2
    pct = float(re.search(r"Missing in TED: [\d,]+ \(([\d.]+)%", out).group(1))
    assert pct == 100.0


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — completitud: todo lo que ofrece la fuente
# ═══════════════════════════════════════════════════════════════════════════

# Tipos de aviso del tipo de documento CAN en el eForms SDK
# (codelists/notice-type.gc + notice-types/notice-types.json: subtipos 25-40, E4-E6, T02)
EFORMS_CAN_NOTICE_TYPES = {"veat", "can-standard", "can-social", "can-desg",
                           "can-modif", "compl", "can-tran"}


def _csv_hub_fake(csv_by_url):
    """tm._descargar simulado: solo sirve las URL dadas (cualquier otra, 404); las .zip, dentro de un ZIP."""
    llamadas = []

    def fake(url, destino):
        llamadas.append((url, "zip" if url.endswith(".zip") else None))
        if url in csv_by_url:
            if url.endswith(".zip"):
                with zipfile.ZipFile(destino, "w") as z:
                    z.writestr(url.rsplit("/", 1)[-1][:-len(".zip")] + ".csv", csv_by_url[url])
            else:
                Path(destino).write_text(csv_by_url[url], encoding="utf-8")
            return
        raise requests.exceptions.HTTPError(f"HTTP 404 {url}")
    fake.llamadas = llamadas
    return fake


def _api_prohibida(*a, **k):
    raise AssertionError("no debe consultarse la API si hay CSV bulk")


TED_CSV_2021_HUB = (
    "ID_NOTICE_CAN,YEAR,ISO_COUNTRY_CODE,CAE_NAME,CAE_NATIONALID,TYPE_OF_CONTRACT,CPV,"
    "VALUE_EURO_FIN_1,AWARD_VALUE_EURO_FIN_1,WIN_NAME,WIN_NATIONALID,NUMBER_OFFERS,DT_AWARD,CANCELLED\n"
    "2021/S 010-000001,2021,ES,Ayuntamiento de Sevilla,ESP4109100J,S,79000000,400000,380000,"
    "EMPRESA SL,ESB11111111,3,2021-01-10,0\n"
    "2021/S 010-000002,2021,FR,Mairie,FR1,S,79000000,1,1,X,FR2,1,2021-01-10,0\n"
)


class TestTedCompletitud:
    def test_query_pide_todos_los_tipos_de_aviso_can(self, monkeypatch, no_sleep):
        # Antes: solo can-standard, can-social, can-modif y can-desg (faltaban
        # veat -adjudicaciones sin licitación previa-, can-tran y compl)
        assert set(tm.TEDConfig.API_NOTICE_TYPES) == EFORMS_CAN_NOTICE_TYPES
        api = FakeTedApi(_many_notices(5))
        monkeypatch.setattr(requests, "post", api)
        tm._download_api_period(2024, "20240101", "20241231", "2024")
        tipos = re.search(r"notice-type IN \(([^)]*)\)", api.calls[0]["query"]).group(1)
        assert {t.strip() for t in tipos.split(",")} == EFORMS_CAN_NOTICE_TYPES

    def test_campos_pedidos_caben_en_los_limites_de_la_api(self):
        fields = tm.TEDConfig.API_FIELDS
        assert len(fields) == len(set(fields))
        # Límites documentados de la API: 250 avisos y 10.000 "campos" por página
        assert tm.TEDConfig.TED_API_PAGE_SIZE <= 250
        assert len(fields) * tm.TEDConfig.TED_API_PAGE_SIZE < 10_000
        for f in ["publication-date", "notice-subtype", "form-type", "procedure-type",
                  "contract-nature-main-proc", "place-of-performance"]:
            assert f in fields

    def test_avisos_api_con_fecha_tipo_de_contrato_y_procedimiento(self):
        n = _notice("300001-2024", winners=["B1"], win_names=["EMP"], values=["1000"],
                    notice_type="veat")
        n.update({"publication-date": ["2024-03-20+01:00"], "notice-subtype": ["25"],
                  "form-type": ["dir-awa-pre"], "procedure-type": ["neg-wo-call"],
                  "contract-nature-main-proc": ["services"],
                  "place-of-performance": ["ES618", "ESP"]})
        rec = tm._parse_api_notice(n)[0]
        assert rec["notice_type"] == "veat" and rec["notice_subtype"] == "25"
        assert rec["procedure_type"] == "neg-wo-call" and rec["contract_nature"] == "services"
        assert rec["place_of_performance"] == "ES618;ESP"
        df = tm._normalize_ted_data(pd.DataFrame([rec]))
        # Antes todas las filas de la API quedaban como 'otros' y sin fecha de publicación
        assert df.loc[0, "tipo_contrato"] == "servicios"
        assert df.loc[0, "publication_date"] == pd.Timestamp("2024-03-20")

    def test_csv_2020_2023_desde_el_zip_de_data_europa_eu(self, monkeypatch, tmp_path, no_sleep):
        # Las URL "TED 2020" no tienen 2020-2023: esos años caían a la API, que
        # para avisos anteriores a eForms no trae adjudicatario ni importe
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        fake = _csv_hub_fake({url: TED_CSV_2021_HUB})
        monkeypatch.setattr(tm, "_descargar", fake)
        monkeypatch.setattr(requests, "post", _api_prohibida)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm.download_ted_spain(years=[2021], force_redownload=True)
        assert (url, "zip") in fake.llamadas
        assert df["ted_notice_id"].tolist() == ["2021/S 010-000001"]
        assert df.loc[0, "win_nif_clean"] == "B11111111" and df.loc[0, "importe_ted"] == 380000
        assert df.loc[0, "source"] == "csv_bulk"
        assert (tmp_path / "ted_can_2021_ES.parquet").exists()

    def test_csv_sin_columna_de_pais_prueba_la_siguiente_url(self, monkeypatch, tmp_path):
        # Antes: una respuesta 200 que no era el CSV (p.ej. HTML) cortaba la
        # búsqueda y el año pasaba a la API
        primera = (f"{tm.TEDConfig.CSV_BASE_URL}/TED%202020/TED%20-%20Contract%20award"
                   f"%20notices%202021.csv")
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        fake = _csv_hub_fake({primera: "<html>\nportal</html>\n", url: TED_CSV_2021_HUB})
        monkeypatch.setattr(tm, "_descargar", fake)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_csv_year(2021, force=True)
        assert df is not None and df["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001"]

    def test_importe_de_csv_sin_columnas_fin_1(self):
        df = pd.DataFrame({"ID_NOTICE_CAN": ["a", "b"], "YEAR": ["2022", "2022"],
                           "AWARD_VALUE_EURO": ["150000", ""], "VALUE_EURO": ["", "90000"]})
        out = tm._normalize_ted_data(df)
        assert out["importe_ted"].tolist() == [150000.0, 90000.0]

    def test_trimestre_por_encima_del_limite_se_divide_en_meses(self, monkeypatch, tmp_path, no_sleep):
        # 250 avisos, la API solo sirve 50 por consulta: año y trimestres
        # (~62) superan el límite; los meses (~21) no. Antes el trimestre quedaba truncado.
        api = FakeTedApi(_many_notices(250), cap=50)
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024, force=True)
        assert len(df) == 250 and df["ted_notice_id"].is_unique
        assert not df.attrs.get("descarga_incompleta")
        desde = {re.search(r">=(\d{8})", c["query"]).group(1) for c in api.calls}
        assert {"20240201", "20240501", "20240801", "20241101"} <= desde
        assert (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    def test_limite_en_un_solo_dia_es_descarga_incompleta(self, monkeypatch, tmp_path, no_sleep):
        notices = [("20240315", _notice(f"{400000 + i}-2024")) for i in range(30)]
        monkeypatch.setattr(requests, "post", FakeTedApi(notices, cap=10))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024, force=True)
        assert df.attrs.get("descarga_incompleta") is True
        assert not (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    @pytest.mark.parametrize("desde,hasta,esperado", [
        ("20240101", "20241231", [("20240101", "20240331"), ("20240401", "20240630"),
                                  ("20240701", "20240930"), ("20241001", "20241231")]),
        ("20240101", "20240331", [("20240101", "20240131"), ("20240201", "20240229"),
                                  ("20240301", "20240331")]),
        ("20241115", "20250110", [("20241115", "20241130"), ("20241201", "20241231"),
                                  ("20250101", "20250110")]),
        ("20240201", "20240203", [("20240201", "20240201"), ("20240202", "20240202"),
                                  ("20240203", "20240203")]),
        ("20240201", "20240201", []),
    ])
    def test_subperiodos(self, desde, hasta, esperado):
        assert [(a, b) for a, b, _ in tm._subperiods(desde, hasta)] == esperado

    def test_ano_en_curso_no_se_cachea(self, monkeypatch, tmp_path, no_sleep):
        monkeypatch.setattr(tm, "_current_year", lambda: 2024)
        monkeypatch.setattr(requests, "post", FakeTedApi(_many_notices(20)))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024, force=True)
        assert len(df) == 20
        # Si se guardara, la siguiente ejecución daría el año por completo
        assert not (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    def test_cache_guardada_con_el_ano_abierto_se_actualiza(self, monkeypatch, tmp_path, no_sleep):
        import os
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        pd.DataFrame({"ted_notice_id": ["viejo-2024"], "lot_index": [0]}).to_parquet(cache)
        junio_2024 = pd.Timestamp("2024-06-01").timestamp()
        os.utime(cache, (junio_2024, junio_2024))
        api = FakeTedApi(_many_notices(20))
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024)
        assert api.calls and len(df) == 20
        assert "viejo-2024" not in pd.read_parquet(cache)["ted_notice_id"].tolist()
        # Una cache escrita después de cerrar el año sí se reutiliza
        api.calls.clear()
        assert len(tm._download_api_year(2024)) == 20 and not api.calls

    def test_cache_consolidada_sin_los_anos_pedidos_se_reconstruye(self, monkeypatch, tmp_path, ted_http):
        # Antes ted_es_can.parquet se devolvía siempre, aunque le faltaran años
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019], force_redownload=True)
        assert set(pd.read_parquet(tmp_path / "ted_es_can.parquet")["year"]) == {2019}
        df = tm.download_ted_spain(years=[2019, 2024])
        assert set(df["year"]) == {2019, 2024}
        assert set(pd.read_parquet(tmp_path / "ted_es_can.parquet")["year"]) == {2019, 2024}
        # Con todos los años cerrados y presentes se reutiliza sin red
        ted_http.calls.clear()
        tm.download_ted_spain(years=[2019, 2024])
        assert not ted_http.calls

    def test_cache_consolidada_con_ano_abierto_se_reconstruye(self, monkeypatch, tmp_path, ted_http):
        import os
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        out = tmp_path / "ted_es_can.parquet"
        (tmp_path / "ted_can_2024_ES_api.parquet").unlink()
        julio_2024 = pd.Timestamp("2024-07-01").timestamp()
        os.utime(out, (julio_2024, julio_2024))   # guardada con 2024 aún abierto
        ted_http.calls.clear()
        tm.download_ted_spain(years=[2019, 2024])
        assert ted_http.calls   # 2024 se vuelve a pedir a la API

    def test_anos_por_defecto_hasta_el_ano_en_curso(self, monkeypatch):
        monkeypatch.setattr(tm, "_current_year", lambda: 2031)
        assert tm._default_years() == list(range(2006, 2032))
        capturado = {}
        monkeypatch.setattr(tm, "download_ted_spain",
                            lambda years, force_redownload, **kw: capturado.update(years=years, **kw)
                            or pd.DataFrame())   # guardado: main() no sale con 1
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download"])
        tm.main()
        # Antes: '2010-2025' fijo
        assert capturado["years"] == list(range(2006, 2032))
        assert capturado["semillas"] == []

    def test_cache_api_de_version_anterior_se_vuelve_a_descargar(self, monkeypatch, tmp_path, no_sleep):
        # ted_can_2024/2025_ES_api.parquet publicados: sin veat/can-tran/compl ni
        # notice_subtype/publication_date; reutilizarlos dejaría esos avisos fuera
        pd.DataFrame({"ted_notice_id": ["viejo-2024"], "lot_index": [0], "notice_type": ["can-standard"]}
                     ).to_parquet(tmp_path / "ted_can_2024_ES_api.parquet")
        api = FakeTedApi(_many_notices(20))
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_api_year(2024)
        assert api.calls and len(df) == 20 and "notice_subtype" in df.columns

    def test_csv_conserva_todas_las_columnas_de_la_fuente(self, monkeypatch, tmp_path, no_sleep):
        # Antes solo 27 columnas (CSV_COLUMNS_KEEP): se perdían título, nº de
        # contrato, URL del aviso, PYME adjudicataria...
        csv_txt = (TED_CSV_2021_HUB.splitlines()[0] + ",TITLE,CONTRACT_NUMBER,TED_NOTICE_URL,B_CONTRACTOR_SME\n"
                   + TED_CSV_2021_HUB.splitlines()[1]
                   + ",Servicio de limpieza,CT-7,ted.europa.eu/udl?uri=TED:NOTICE:1-2021,Y\n")
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: csv_txt}))
        monkeypatch.setattr(requests, "post", _api_prohibida)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm.download_ted_spain(years=[2021], force_redownload=True)
        fila = df.iloc[0]
        assert (fila["TITLE"], fila["CONTRACT_NUMBER"], fila["B_CONTRACTOR_SME"]) == (
            "Servicio de limpieza", "CT-7", "Y")
        assert "TED_NOTICE_URL" in pd.read_parquet(tmp_path / "ted_can_2021_ES.parquet").columns


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — histórico (sesgo del superviviente, comun/historico.py)
# ═══════════════════════════════════════════════════════════════════════════

META = ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]


def _aviso(num, valor="100000", fecha="20240315"):
    """(fecha de publicación, aviso de la API de 2024) con un ganador."""
    return (fecha, _notice(f"{num}-2024", winners=[f"B{num:08d}"], win_names=[f"EMP {num}"],
                           values=[valor], dates=["2024-03-01+01:00"]))


def _fijar_fecha(ruta, fecha):
    """Fecha de modificación de un fichero: la de su versión en el histórico."""
    t = pd.Timestamp(fecha, tz="UTC").timestamp()
    os.utime(ruta, (t, t))


def _historico(carpeta):
    h = Path(carpeta) / "_historico"
    return sorted(p.name for p in h.iterdir()) if h.is_dir() else []


def _estado(df):
    """{ted_notice_id: [_en_ultima_descarga de cada una de sus filas]}."""
    return {k: sorted(bool(v) for v in g) for k, g in df.groupby("ted_notice_id")["_en_ultima_descarga"]}


def _sha(ruta):
    return hashlib.sha256(Path(ruta).read_bytes()).hexdigest()


def _foto(carpeta):
    """Contenido y fecha de cada parquet de la carpeta (y de _historico/)."""
    return {str(p.relative_to(carpeta)): (_sha(p), p.stat().st_mtime_ns)
            for p in sorted(Path(carpeta).rglob("*.parquet"))}


@pytest.fixture
def ted_2024(monkeypatch, tmp_path, no_sleep):
    """API simulada con avisos de 2024 (lista modificable entre ejecuciones),
    sin CSV, datos en tmp_path y 2026 como año en curso."""
    api = FakeTedApi([_aviso(1), _aviso(2), _aviso(3)])
    monkeypatch.setattr(requests, "post", api)
    monkeypatch.setattr(tm, "_descargar", _csv_anual_fake({}))
    monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
    monkeypatch.setattr(tm, "_current_year", lambda: 2026)
    return api


# CSV bulk con los identificadores reales: ID_NOTICE_CAN = año + número del aviso
TED_CSV_2021_IDS = (
    "ID_NOTICE_CAN,YEAR,ISO_COUNTRY_CODE,CAE_NAME,WIN_NAME,WIN_NATIONALID,AWARD_VALUE_EURO_FIN_1,CANCELLED\n"
    "20211001,2021,ES,Ayuntamiento de Sevilla,EMPRESA SL,ESB11111111,380000,0\n"
    "20211002,2021,ES,Diputación de Huelva,OTRA SL,ESB22222222,500000,0\n"
)


class TestHistoricoTed:
    """Al refrescar un año no se machacan su caché ni el consolidado: lo que TED
    retira o cambia sigue con _en_ultima_descarga=False."""

    def test_aviso_retirado_se_conserva(self, ted_2024, tmp_path):
        tm.download_ted_spain(years=[2024], force_redownload=True)
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        _fijar_fecha(cache, "2025-01-10")
        ted_2024.notices = [_aviso(1), _aviso(2)]            # TED retira el aviso 3
        df = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert _estado(df) == {"1-2024": [True], "2-2024": [True], "3-2024": [False]}
        assert list(df.columns[-3:]) == META
        retirado = df.set_index("ted_notice_id").loc["3-2024"]
        assert retirado["_primera_descarga"] == retirado["_ultima_descarga"] == "2025-01-10T00:00:00+00:00"
        # La caché anterior (con el aviso 3) queda en _historico/ y la actual no lo tiene
        assert "ted_can_2024_ES_api__20250110T000000Z.parquet" in _historico(tmp_path)
        assert "3-2024" not in set(pd.read_parquet(cache)["ted_notice_id"])
        # El consolidado anterior también; el nuevo es lo que se devuelve
        assert any(n.startswith("ted_es_can__") for n in _historico(tmp_path))
        assert _estado(pd.read_parquet(tmp_path / "ted_es_can.parquet")) == _estado(df)

    def test_aviso_cambiado_conserva_la_version_anterior(self, ted_2024, tmp_path):
        tm.download_ted_spain(years=[2024], force_redownload=True)
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2025-01-10")
        ted_2024.notices = [_aviso(1), _aviso(2), _aviso(3, valor="250000")]   # TED corrige un importe
        df = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert _estado(df) == {"1-2024": [True], "2-2024": [True], "3-2024": [False, True]}
        tres = df[df["ted_notice_id"] == "3-2024"].set_index("_en_ultima_descarga")["importe_ted"]
        assert (tres[False], tres[True]) == (100000, 250000)
        # En los cruces cada aviso cuenta una vez: su versión vigente
        for ultima_version in (tm.ultima_version_por_aviso, rtc.ultima_version_por_aviso):
            ultima = ultima_version(df)
            assert ultima["ted_notice_id"].is_unique
            assert ultima.set_index("ted_notice_id").loc["3-2024", "importe_ted"] == 250000

    def test_ejecucion_parcial_no_retira_los_otros_anios(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        _fijar_fecha(tmp_path / "ted_can_2019_ES.parquet", "2020-01-10")
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2025-01-10")
        ted_http.notices = ted_http.notices[:3]              # TED retira 100004-2024 (dos lotes)
        df = tm.download_ted_spain(years=[2024], force_redownload=True)
        # 2019 no se ha vuelto a descargar: sigue entero, vigente y con la fecha de su versión
        de_2019 = df[df["year"] == 2019]
        assert len(de_2019) == 3 and de_2019["_en_ultima_descarga"].all()   # la cancelada se conserva
        assert set(de_2019["_ultima_descarga"]) == {"2020-01-10T00:00:00+00:00"}
        assert not any(n.startswith("ted_can_2019") for n in _historico(tmp_path))
        de_2024 = df[df["year"] == 2024]
        assert _estado(de_2024)["100004-2024"] == [False, False]
        assert de_2024["_en_ultima_descarga"].sum() == 3

    def test_descarga_fallida_o_vacia_no_retira_nada(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        foto = _foto(tmp_path)
        # 1. La API falla (HTTP 500): descarga incompleta, no se guarda nada
        monkeypatch.setattr(requests, "post", FakeTedApi(_api_notices_2024(), fail_pages={1}))
        tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        assert _foto(tmp_path) == foto
        # 2. La API responde sin avisos: 2024 sigue como estaba
        monkeypatch.setattr(requests, "post", FakeTedApi([]))
        df = tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        assert len(df) == 8 and df["_en_ultima_descarga"].all()
        assert _foto(tmp_path) == foto
        # 3. El CSV falla (404) con caché guardada: se sigue usando, sin ir a la API
        monkeypatch.setattr(tm, "_descargar", _csv_anual_fake({}))
        monkeypatch.setattr(requests, "post", _api_prohibida)
        df = tm.download_ted_spain(years=[2019], force_redownload=True)
        assert len(df) == 8 and df["_en_ultima_descarga"].all()
        assert _foto(tmp_path) == foto

    def test_anio_en_curso_no_se_cachea_pero_queda_en_el_historico(self, ted_2024, monkeypatch, tmp_path):
        monkeypatch.setattr(tm, "_current_year", lambda: 2024)
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        en_curso = tmp_path / "ted_can_2024_ES_api_en_curso.parquet"
        tm.download_ted_spain(years=[2024], force_redownload=True)
        assert not cache.exists() and en_curso.exists()
        _fijar_fecha(en_curso, "2024-05-01")
        ted_2024.notices = [_aviso(2), _aviso(3), _aviso(4)]   # retira el 1 y publica el 4
        df = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert not cache.exists()
        assert _estado(df) == {"1-2024": [False], "2-2024": [True], "3-2024": [True], "4-2024": [True]}
        _fijar_fecha(en_curso, "2024-06-01")
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")   # guardado con 2024 abierto
        # Cierra el año: se guarda la caché y lo visto durante el año sigue en el consolidado
        monkeypatch.setattr(tm, "_current_year", lambda: 2025)
        ted_2024.notices.append(_aviso(5))
        df = tm.download_ted_spain(years=[2024])
        assert cache.exists()
        assert _estado(df) == {"1-2024": [False], "2-2024": [True], "3-2024": [True],
                               "4-2024": [True], "5-2024": [True]}
        uno = df.set_index("ted_notice_id").loc["1-2024"]
        assert uno["_primera_descarga"] == uno["_ultima_descarga"] == "2024-05-01T00:00:00+00:00"
        assert df.set_index("ted_notice_id").loc["2-2024", "_primera_descarga"] == "2024-05-01T00:00:00+00:00"

    def test_semilla_solo_anade_los_avisos_que_faltan(self, ted_2024, monkeypatch, tmp_path):
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: TED_CSV_2021_IDS}))
        ted_2024.notices = [_aviso(1), _aviso(2)]
        # Como el ted_es_can.parquet publicado, donde 2020-2023 venían de la API
        # (número-año) y ahora del CSV (año + número): 1001-2021 es 20211001.
        # 20211002 (formato CSV, como 2010-2019 en el publicado) también está
        publicado = pd.DataFrame({
            "ted_notice_id": ["1001-2021", "20211002", "1003-2021", "1-2024", "9-2024", "9-2024", "5-2018"],
            "year": [2021, 2021, 2021, 2024, 2024, 2024, 2018],
            "source": ["api_v3", "csv_bulk"] + ["api_v3"] * 5,
            "lot_index": [0.0, np.nan, 0.0, 0.0, 0.0, 1.0, 0.0],
            "importe_ted": [380000.0, 500000.0, 1.0, 100000.0, 2.0, 3.0, 4.0],
        })
        semilla = tmp_path / "release" / "ted_es_can.parquet"
        semilla.parent.mkdir()
        publicado.to_parquet(semilla, index=False)
        tm.download_ted_spain(years=[2021, 2024], force_redownload=True)
        # Con --semilla no vale el consolidado ya guardado (años cerrados): se reconstruye
        df = tm.download_ted_spain(years=[2021, 2024], semillas=[semilla])
        sembradas = df[df["_origen"].notna()]
        # Solo los avisos que faltan (con todas sus filas); 2018 no se ha descargado
        assert sorted(sembradas["ted_notice_id"]) == ["1003-2021", "9-2024", "9-2024"]
        assert (sembradas["_origen"] == "release v2026.02").all()
        assert not sembradas["_en_ultima_descarga"].any()
        descargadas = df[df["_origen"].isna()]
        assert sorted(descargadas["ted_notice_id"]) == ["1-2024", "2-2024", "20211001", "20211002"]
        assert descargadas["_en_ultima_descarga"].all()
        # Otra vez con la misma semilla: no añade nada ni cambia el consolidado
        out = tmp_path / "ted_es_can.parquet"
        antes = _sha(out)
        otra = tm.download_ted_spain(years=[2021, 2024], force_redownload=True, semillas=[semilla])
        assert len(otra) == len(df) and _sha(out) == antes
        # Sin --semilla las filas sembradas se conservan (vienen del consolidado anterior)
        sin = tm.download_ted_spain(years=[2021, 2024], force_redownload=True)
        assert sorted(sin.loc[sin["_origen"].notna(), "ted_notice_id"]) == ["1003-2021", "9-2024", "9-2024"]

    def test_clave_aviso_iguala_csv_y_api(self):
        ids = ["2020112", "112-2020", "000112-2020", "2019S 001", None]
        assert tm.clave_aviso(ids).tolist() == ["112-2020", "112-2020", "112-2020", "2019S 001", None]

    def test_reejecucion_sin_cambios_no_crea_version(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        foto = _foto(tmp_path)
        ted_http.notices = ted_http.notices[::-1]      # los mismos avisos, en otro orden
        df = tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        assert _foto(tmp_path) == foto and _historico(tmp_path) == []
        assert df["_en_ultima_descarga"].all()

    def test_una_descarga_da_lo_de_antes_mas_las_columnas_meta(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm.download_ted_spain(years=[2019, 2024], force_redownload=True)
        # Lo que daba el código anterior: las descargas concatenadas y normalizadas
        csv = tm._download_csv_year(2019, force=True)
        api = tm._download_api_year(2024, force=True)
        antes = tm._normalize_ted_data(pd.concat([tm._renombrar_csv(csv), api], ignore_index=True))
        assert list(df.columns) == list(antes.columns) + META
        pd.testing.assert_frame_equal(df.drop(columns=META), antes)
        assert df["_en_ultima_descarga"].all()
        assert (df["_primera_descarga"] == df["_ultima_descarga"]).all()

    def test_cache_api_del_parser_anterior_no_se_compara_fila_a_fila(self, ted_2024, tmp_path, capsys):
        # Caché publicada en v2026.02 (sin notice_subtype): sus filas no casan con
        # las del parser actual (cae_town "['Sevilla']"...) y duplicarían los avisos.
        # Los avisos que ya no están en la descarga (8-2024) se conservan (antes se perdían)
        vieja = pd.DataFrame({"ted_notice_id": ["1-2024", "8-2024"], "year": ["2024"] * 2,
                              "lot_index": [0, 0], "cae_town": ["['Sevilla']"] * 2,
                              "notice_type": ["can-standard"] * 2, "source": ["api_v3"] * 2})
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        vieja.to_parquet(cache, index=False)
        _fijar_fecha(cache, "2026-02-06")
        df = tm.download_ted_spain(years=[2024])
        assert _estado(df) == {"1-2024": [True], "2-2024": [True], "3-2024": [True], "8-2024": [False]}
        ocho = df.set_index("ted_notice_id").loc["8-2024"]
        assert ocho["_origen"] == "caché de 2024 del parser anterior" and ocho["cae_town"] == "['Sevilla']"
        assert df.loc[df["ted_notice_id"] != "8-2024", "_origen"].isna().all()
        assert "ted_can_2024_ES_api__20260206T000000Z.parquet" in _historico(tmp_path)
        assert "1 añadidas" in capsys.readouterr().out

    def test_cross_validate_ted_no_valida_dos_contratos_con_un_aviso(self, ted_2024, tmp_path):
        tm.download_ted_spain(years=[2024], force_redownload=True)
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2025-01-10")
        # TED corrige la ciudad del aviso 1 (mismo ganador e importe): dos versiones
        ted_2024.notices[0][1]["buyer-city"] = ["Dos Hermanas"]
        ted = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert _estado(ted)["1-2024"] == [False, True]
        pipe = pd.DataFrame({"_nif": ["B00000001"] * 2, "_imp_adj": [100_000.0] * 2, "_año": [2024.0] * 2,
                             "_fecha_adj": ["2024-03-01"] * 2, "_es_menor": [False] * 2,
                             "_organ": ["Ayuntamiento de Sevilla"] * 2, "_adj": ["EMP 1"] * 2,
                             "_expediente": [None, None], "_cpv": ["79000000"] * 2})
        res, _ = tm.cross_validate_ted(pipe, ted, "NAC")
        assert res["_ted_validated"].sum() == 1


def test_cruce_toma_la_ultima_version_de_cada_aviso(tmp_path, capsys):
    """run_ted_crossvalidation.load_ted: cada aviso una vez (vigente, o la última
    versión de uno retirado; los sembrados del release cuentan)."""
    ted = pd.DataFrame({
        "ted_notice_id": ["A", "A", "B", "B", "C", "D"],
        "importe_ted": [100.0, 120.0, 200.0, 210.0, 300.0, 400.0],
        "win_nationalid": ["B11111111"] * 6,
        "year": [2024] * 6,
        "_ultima_descarga": ["2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00",
                             "2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00",
                             None, "2025-02-01T00:00:00+00:00"],
        "_en_ultima_descarga": [False, True, False, False, False, True],
        "_origen": [None, None, None, None, "release v2026.02", None],
    })
    esperado = [("A", 120.0), ("B", 210.0), ("C", 300.0), ("D", 400.0)]
    ultima = tm.ultima_version_por_aviso(ted)
    assert sorted(zip(ultima["ted_notice_id"], ultima["importe_ted"])) == esperado
    ted.to_parquet(tmp_path / "ted_es_can.parquet", index=False)
    df = rtc.load_ted(tmp_path / "ted_es_can.parquet")
    assert sorted(zip(df["ted_notice_id"], df["importe_ted"])) == esperado
    assert list(df.index) == [0, 1, 2, 3]
    assert "fuera del cruce): 2" in capsys.readouterr().out
    # Sin columnas de histórico (consolidados anteriores) no cambia nada
    viejo = ted.drop(columns=META[1:] + ["_origen"])
    assert tm.ultima_version_por_aviso(viejo) is viejo
    assert rtc.ultima_version_por_aviso(viejo) is viejo


def test_nullable_strings_from_rebuilt_parquet():
    assert rtc.classify_buyer(pd.NA) == (False, False)
    assert rtc.normalize_name(pd.NA) == ""
    assert rtc.clean_nif(pd.NA) == ""


# ═══════════════════════════════════════════════════════════════════════════
#  Regla 2: registros irregulares, avisos cancelados y código de salida
# ═══════════════════════════════════════════════════════════════════════════

TED_CSV_2021_MAL = (
    "ID_NOTICE_CAN,YEAR,ISO_COUNTRY_CODE,CAE_NAME,CAE_NATIONALID,TYPE_OF_CONTRACT,CPV,"
    "VALUE_EURO_FIN_1,AWARD_VALUE_EURO_FIN_1,WIN_NAME,WIN_NATIONALID,NUMBER_OFFERS,DT_AWARD,CANCELLED\n"
    "2021/S 010-000001,2021,ES,Ayuntamiento de Sevilla,ESP4109100J,S,79000000,400000,380000,"
    "EMPRESA SL,ESB11111111,3,2021-01-10,0\n"
    # una coma sin comillas en el nombre: un campo de más (antes se perdía sin guardarse)
    "2021/S 010-000002,2021,ES,Ayuntamiento de Cádiz, Área de Hacienda,ESP1101200I,S,79000000,1,1,"
    "X,ESB2,1,2021-01-11,0\n"
    "2021/S 010-000003,2021,FR,Mairie,FR1,S,79000000,1,1,X,FR2,1,2021-01-10,0,EXTRA\n"
    "2021/S 010-000004,2021,ES,Diputación,ESP1,S,79000000,5,5,NA,ESB4,1,2021-01-12,1\n"
    "\n"
    "2021/S 010-000005,2021,ES,Corto,ESP5,S\n"
)

_CAB_2021, _FILA_2021 = TED_CSV_2021_HUB.splitlines(keepends=True)[:2]


def _csv_2021(monkeypatch, tmp_path, texto, **k):
    """_download_csv_year(2021) con `texto` servido en la URL del hub (en ZIP) y datos en tmp_path."""
    url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
    monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: texto}))
    monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
    return tm._download_csv_year(2021, force=True, **k)


def _irregulares(carpeta, year=2021):
    return _ORIG_READ_CSV(Path(carpeta) / f"ted_can_{year}_registros_irregulares.csv", dtype=str,
                          keep_default_na=False)


def _zip_fake(miembros):
    """tm._descargar simulado: en la URL del hub de 2021, un ZIP con estos ficheros; el resto, 404."""
    url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)

    def fake(u, destino):
        if u != url:
            raise requests.exceptions.HTTPError(f"HTTP 404 {u}")
        with zipfile.ZipFile(destino, "w") as z:
            for nombre, texto in miembros.items():
                z.writestr(nombre, texto)
    return fake


class TestTedRegla2:
    def test_registros_irregulares_se_guardan_aparte(self, monkeypatch, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            df = _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL)
        # Los de España con sus campos; el corto, completado con vacíos; ninguno truncado
        assert df["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001", "2021/S 010-000004", "2021/S 010-000005"]
        filas = df.set_index("ID_NOTICE_CAN")
        assert filas.loc["2021/S 010-000004", "WIN_NAME"] == "NA"   # texto publicado, no nulo
        assert pd.isna(filas.loc["2021/S 010-000005", "CPV"])
        # La fila que sale del registro corto lleva su motivo (regla 1: marcar, no limpiar)
        assert filas.loc["2021/S 010-000005", "_registro_irregular"] == "campos_de_menos"
        assert filas["_registro_irregular"].isna().sum() == 2
        irr = _irregulares(tmp_path)
        # Línea en que empieza cada uno: la cabecera es la 1 y la línea en blanco cuenta
        assert irr["linea"].tolist() == ["3", "4", "7"]
        assert irr["motivo"].tolist() == ["campos_de_mas", "campos_de_mas", "campos_de_menos"]
        assert irr["n_campos"].tolist() == ["15", "15", "6"] and set(irr["n_campos_cabecera"]) == {"14"}
        assert set(irr["url"]) == {tm.TEDConfig.CSV_HUB_URL.format(year=2021)}
        campos = [json.loads(c) for c in irr["campos_json"]]
        assert campos[0][3:5] == ["Ayuntamiento de Cádiz", " Área de Hacienda"] and len(campos[0]) == 15
        assert campos[1][-1] == "EXTRA"
        assert campos[2] == ["2021/S 010-000005", "2021", "ES", "Corto", "ESP5", "S"]
        assert "REGISTROS IRREGULARES" in caplog.text
        assert not list(tmp_path.glob(".*"))   # ni el temporal de la descarga ni el de los irregulares

    def test_cualquier_registro_irregular_del_csv_usado_marca_el_anio(self, monkeypatch, tmp_path):
        anios = []
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL, irregulares=anios)
        assert anios == [2021]
        # Uno solo entre 201 líneas, de otro país, también: el CSV de TED no trae ninguno
        malo = "2021/S 010-000009,2021,FR,Mairie,FR1,S,1,1,1,X,FR2,1,2021-01-10,0,EXTRA\n"
        anios = []
        df = _csv_2021(monkeypatch, tmp_path, _CAB_2021 + _FILA_2021 * 199 + malo, irregulares=anios)
        assert anios == [2021] and len(df) == 199
        assert _irregulares(tmp_path)["linea"].tolist() == ["201"]
        anios = []
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_HUB, irregulares=anios)
        assert anios == []

    def test_url_descartada_con_irregulares_y_otra_buena(self, monkeypatch, tmp_path):
        # La URL «TED 2020» sirve un CSV cuyo único aviso de España tiene un campo de más (sin filas
        # de España buenas) y el hub, uno bueno: el año sale bien y esos irregulares, a _historico/
        url_2020 = f"{tm.TEDConfig.CSV_BASE_URL}/TED%202020/TED%20-%20Contract%20award%20notices%202021.csv"
        malo = _CAB_2021 + "2021/S 010-000002,2021,ES,Ayto de Cádiz, Hacienda,ESP1,S,1,1,1,X,Y,1,2021-01-11,0\n"
        hub = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url_2020: malo, hub: TED_CSV_2021_HUB}))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        anios = []
        df = tm._download_csv_year(2021, force=True, irregulares=anios)
        assert anios == [] and df["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001"]
        assert not (tmp_path / "ted_can_2021_registros_irregulares.csv").exists()
        assert any(n.startswith("ted_can_2021_registros_irregulares") for n in _historico(tmp_path))
        # Sin ninguna URL buena (ni caché), el año sí se marca
        otra = tmp_path / "otra"
        otra.mkdir()
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url_2020: malo}))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", otra)
        anios = []
        assert tm._download_csv_year(2021, force=True, irregulares=anios) is None
        assert anios == [2021] and _irregulares(otra)["motivo"].tolist() == ["campos_de_mas"]

    def test_comilla_sin_cerrar_se_guarda_y_la_descarga_sale_con_1(self, monkeypatch, tmp_path, no_sleep):
        # Una comilla que no se cierra: el módulo csv se traga en ese campo todo lo que sigue, también
        # un aviso de España (antes pandas rechazaba la URL entera: «EOF inside string»)
        texto = (_CAB_2021 + _FILA_2021
                 + '2021/S 010-000002,2021,ES,"Ayuntamiento de Jaén,ESP2,S,1,1,1,X,Y,1,2021-01-10,0\n'
                 + "2021/S 010-000003,2021,ES,Diputación de Jaén,ESP3,S,2,2,2,X,Y,1,2021-01-11,0\n")
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: texto}))
        monkeypatch.setattr(requests, "post", _api_prohibida)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download", "--years", "2021"])
        with pytest.raises(SystemExit) as salida:
            tm.main()
        assert salida.value.code == 1
        consolidado = pd.read_parquet(tmp_path / "ted_es_can.parquet")   # lo descargado sí se guarda
        assert consolidado["_registro_irregular"].tolist().count("campos_de_menos+salto_de_linea") == 1
        irr = _irregulares(tmp_path)
        assert irr["linea"].tolist() == ["3"] and irr["motivo"].tolist() == ["campos_de_menos+salto_de_linea"]
        assert "Diputación de Jaén" in json.loads(irr["campos_json"][0])[3]   # nada se pierde

    def test_campo_en_varias_lineas(self, monkeypatch, tmp_path):
        texto = (_CAB_2021 + '2021/S 010-000007,2021,ES,"Ayuntamiento\nde Jaén",ESP7,S,1,1,1,X,Y,1,2021-01-10,0\n'
                 + _FILA_2021)
        df = _csv_2021(monkeypatch, tmp_path, texto)
        assert df["CAE_NAME"].tolist() == ["Ayuntamiento\nde Jaén", "Ayuntamiento de Sevilla"]
        irr = _irregulares(tmp_path)
        assert irr["linea"].tolist() == ["2"] and irr["motivo"].tolist() == ["salto_de_linea"]

    def test_irregulares_se_guardan_aunque_no_quede_ninguna_fila_de_espana(self, monkeypatch, tmp_path):
        texto = _CAB_2021 + "2021/S 010-000002,2021,ES,Ayto de Cádiz, Hacienda,ESP1,S,1,1,1,X,Y,1,2021-01-11,0\n"
        assert _csv_2021(monkeypatch, tmp_path, texto) is None
        assert _irregulares(tmp_path)["motivo"].tolist() == ["campos_de_mas"]

    def test_una_descarga_sin_irregulares_archiva_los_de_la_anterior(self, monkeypatch, tmp_path):
        ruta = tmp_path / "ted_can_2021_registros_irregulares.csv"

        def en_historico():
            return [n for n in _historico(tmp_path) if n.startswith("ted_can_2021_registros_irregulares")]

        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL)
        antes = ruta.read_bytes()
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_HUB)
        assert not ruta.exists()
        assert len(en_historico()) == 1 and (tmp_path / "_historico" / en_historico()[0]).read_bytes() == antes
        # La misma descarga dos veces: una sola versión nueva
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL)
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL)
        assert ruta.read_bytes() == antes and len(en_historico()) == 1

    def test_zip_con_varios_csv_no_se_lee_a_medias(self, monkeypatch, tmp_path, caplog):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        monkeypatch.setattr(tm, "_descargar", _zip_fake({"a.csv": TED_CSV_2021_HUB, "b.csv": TED_CSV_2021_HUB}))
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            assert tm._download_csv_year(2021, force=True) is None
        assert "se esperaba uno" in caplog.text
        # Un CSV y otros ficheros que no son CSV: se lee el CSV
        monkeypatch.setattr(tm, "_descargar", _zip_fake({"LEEME.txt": "hola", "export_CAN_2021.csv": TED_CSV_2021_HUB}))
        assert tm._download_csv_year(2021, force=True)["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001"]

    def test_el_zip_se_cierra(self, monkeypatch, tmp_path):
        abiertos = []

        class ZipEspia(zipfile.ZipFile):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                abiertos.append(self)

        monkeypatch.setattr(tm.zipfile, "ZipFile", ZipEspia)
        assert len(_csv_2021(monkeypatch, tmp_path, TED_CSV_2021_HUB)) == 1
        # En Windows no se puede borrar el temporal con el ZIP abierto
        assert len(abiertos) == 2 and all(z.fp is None for z in abiertos)

    def test_campo_mayor_que_el_limite_del_modulo_csv(self, monkeypatch, tmp_path):
        largo = "x" * 200_000
        anterior = csv.field_size_limit(131_072)   # el de fábrica, fijado: otro test pudo cambiarlo
        try:
            df = _csv_2021(monkeypatch, tmp_path, _CAB_2021 + _FILA_2021.replace("EMPRESA SL", largo))
            assert df["WIN_NAME"].tolist() == [largo]
            assert csv.field_size_limit() == 131_072   # se restaura
        finally:
            csv.field_size_limit(anterior)

    def test_lineas_en_blanco_antes_de_la_cabecera(self, monkeypatch, tmp_path):
        df = _csv_2021(monkeypatch, tmp_path, "\n\r\n" + TED_CSV_2021_HUB)
        assert df["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001"]

    def test_cabecera_con_nombres_repetidos(self, monkeypatch, tmp_path):
        texto = TED_CSV_2021_HUB.replace("TYPE_OF_CONTRACT,CPV,VALUE_EURO_FIN_1", "CPV,CPV,CPV.1", 1)
        df = _csv_2021(monkeypatch, tmp_path, texto)
        assert list(df.columns[5:8]) == ["CPV", "CPV.2", "CPV.1"]
        assert (tmp_path / "ted_can_2021_ES.parquet").exists()   # antes, to_parquet fallaba con dos 'CPV.1'

    def test_comilla_sin_cerrar_en_el_ultimo_campo_del_fichero(self, monkeypatch, tmp_path):
        texto = _CAB_2021 + _FILA_2021 + '2021/S 010-000008,2021,ES,Ayto,ESP8,S,1,1,1,X,Y,1,2021-01-10,"0\n'
        df = _csv_2021(monkeypatch, tmp_path, texto)
        fila = df.set_index("ID_NOTICE_CAN").loc["2021/S 010-000008"]
        assert fila["CANCELLED"] == "0\n" and fila["_registro_irregular"] == "salto_de_linea"
        assert _irregulares(tmp_path)["linea"].tolist() == ["3"]

    def test_lineas_de_solo_espacios_se_saltan_como_en_pandas(self, monkeypatch, tmp_path):
        df = _csv_2021(monkeypatch, tmp_path, "  \t\n" + _CAB_2021 + " \n" + _FILA_2021 + "\t\t\n")
        assert df["ID_NOTICE_CAN"].tolist() == ["2021/S 010-000001"]
        assert "_registro_irregular" not in df.columns
        assert not (tmp_path / "ted_can_2021_registros_irregulares.csv").exists()

    @pytest.mark.parametrize("en_zip", [False, True])
    def test_bom_crlf_y_bytes_invalidos_como_con_pandas(self, monkeypatch, tmp_path, en_zip):
        # Sin el BOM en el nombre de la primera columna, los \r\n de dentro de un campo tal cual y
        # un byte inválido sustituido, sin tirar el año; en el CSV suelto y dentro del ZIP
        datos = ("\ufeff" + _CAB_2021.replace("\n", "\r\n")
                 + '2021/S 010-000001,2021,ES,"Ayto\r\nde Sevilla",ESP1,S,1,1,1,X,Y,1,2021-01-10,0\r\n'
                 + "2021/S 010-000002,2021,ES,Ayto de C").encode("utf-8") \
            + b"\xff" + "diz,ESP2,S,1,1,1,X,Y,1,2021-01-10,0\r\n".encode("utf-8")
        url = (tm.TEDConfig.CSV_HUB_URL.format(year=2021) if en_zip else
               f"{tm.TEDConfig.CSV_BASE_URL}/TED%202020/TED%20-%20Contract%20award%20notices%202021.csv")

        def fake(u, destino):
            if u != url:
                raise requests.exceptions.HTTPError(f"HTTP 404 {u}")
            if en_zip:
                with zipfile.ZipFile(destino, "w") as z:
                    z.writestr("export_CAN_2021.csv", datos)
            else:
                Path(destino).write_bytes(datos)

        monkeypatch.setattr(tm, "_descargar", fake)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._download_csv_year(2021, force=True)
        assert df.columns[0] == "ID_NOTICE_CAN"
        assert df["CAE_NAME"].tolist() == ["Ayto\r\nde Sevilla", "Ayto de C\ufffddiz"]
        assert df["_registro_irregular"].tolist() == ["salto_de_linea", None]

    def test_irregulares_distintos_dejan_la_version_anterior_en_historico(self, monkeypatch, tmp_path):
        ruta = tmp_path / "ted_can_2021_registros_irregulares.csv"
        _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_MAL)
        primera = ruta.read_bytes()
        otro = _CAB_2021 + _FILA_2021 + "2021/S 010-000009,2021,FR,Mairie,FR1,S,1,1,1,X,FR2,1,2021-01-10,0,EXTRA\n"
        _csv_2021(monkeypatch, tmp_path, otro)
        assert _irregulares(tmp_path)["linea"].tolist() == ["3"]
        viejas = [n for n in _historico(tmp_path) if n.startswith("ted_can_2021_registros_irregulares")]
        assert len(viejas) == 1 and (tmp_path / "_historico" / viejas[0]).read_bytes() == primera

    def test_una_lectura_que_falla_a_medias_no_deja_temporales(self, monkeypatch, tmp_path):
        def rota(texto, irregular):
            irregular(2, "campos_de_mas", ["x", "y"], 1)
            raise RuntimeError("lectura cortada")

        monkeypatch.setattr(tm, "_leer_csv_espana", rota)
        assert _csv_2021(monkeypatch, tmp_path, TED_CSV_2021_HUB) is None
        assert not list(tmp_path.glob(".*"))

    def test_temporales_viejos_de_una_ejecucion_matada(self, monkeypatch, tmp_path, ted_http):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        viejo = tmp_path / ".descarga_ted_can_2019.99999.tmp"
        viejo_irr = tmp_path / ".ted_can_2019_registros_irregulares.csv.99999.tmp"
        reciente = tmp_path / ".descarga_ted_can_2018.99998.tmp"   # de una ejecución en marcha
        for ruta in (viejo, viejo_irr, reciente):
            ruta.write_text("x")
        for ruta in (viejo, viejo_irr):
            _fijar_fecha(ruta, "2000-01-01")
        tm.download_ted_spain(years=[2019])
        assert not viejo.exists() and not viejo_irr.exists() and reciente.exists()

    def test_cancelados_se_conservan_y_el_cruce_los_excluye(self, monkeypatch, tmp_path, capsys):
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2021)
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: TED_CSV_2021_MAL}))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm._normalize_ted_data(tm._renombrar_csv(tm._download_csv_year(2021, force=True)))
        assert len(df) == 3 and (df["cancelled"].astype(str) == "1").sum() == 1
        df.to_parquet(tmp_path / "ted_es_can.parquet")
        cargado = rtc.load_ted(tmp_path / "ted_es_can.parquet")
        assert len(cargado) == 2 and not (cargado["cancelled"].astype(str) == "1").any()
        assert "Avisos cancelados (fuera del cruce): 1" in capsys.readouterr().out

    def test_download_sale_con_1_si_no_guarda_o_si_el_csv_es_irregular(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download", "--years", "2021"])
        sin_guardar = pd.DataFrame({"a": [1]})
        sin_guardar.attrs["sin_guardar"] = True
        irregular = pd.DataFrame({"a": [1]})
        irregular.attrs["csv_irregular"] = [2021]
        for resultado in (None, sin_guardar, irregular):
            monkeypatch.setattr(tm, "download_ted_spain", lambda **k: resultado)
            with pytest.raises(SystemExit) as salida:
                tm.main()
            assert salida.value.code == 1
        monkeypatch.setattr(tm, "download_ted_spain", lambda **k: pd.DataFrame({"a": [1]}))
        tm.main()   # guardado: sin error
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download", "--years", "2021", "--method", "sparql"])
        monkeypatch.setattr(tm, "download_ted_spain_sparql", lambda **k: None)
        with pytest.raises(SystemExit) as salida:
            tm.main()
        assert salida.value.code == 1


@pytest.mark.parametrize("cabecera", [["X", "X", "X.1"], ["X", "X.1", "X"], ["A", "", "B", ""],
                                      ["", "Unnamed: 0", ""], ["A", "A", "A", "A.1", "A.2"],
                                      ["A.1", "A", "A"], ["Unnamed: 1", "", "X"], ["A", " A", "A "]])
def test_nombres_de_columna_como_los_ponia_pandas(cabecera):
    texto = ",".join(cabecera) + "\n" + ",".join(["v"] * len(cabecera)) + "\n"
    trozo = next(iter(_ORIG_READ_CSV(io.StringIO(texto), dtype=str, low_memory=False, chunksize=50_000,
                                     on_bad_lines="skip")))
    assert tm._nombres_unicos(cabecera) == list(trozo.columns)


def _ted_cancelado_despues():
    """Consolidado: el aviso A publicado (versión vieja) y cancelado en la última descarga; B vigente."""
    return pd.DataFrame({
        "ted_notice_id": ["A", "A", "B"],
        "cancelled": ["0", "1", "0"],
        "importe_ted": [300_000.0, 300_000.0, 500_000.0],
        "win_nationalid": ["B11111111", "B11111111", "B22222222"],
        "win_nif_clean": ["B11111111", "B11111111", "B22222222"],
        "cae_name": ["Ayuntamiento de Alfa"] * 3,
        "cae_nationalid": ["P0000001A"] * 3,
        "year": [2024, 2024, 2024],
        "_ultima_descarga": ["2025-01-01T00:00:00+00:00", "2025-02-01T00:00:00+00:00",
                             "2025-02-01T00:00:00+00:00"],
        "_en_ultima_descarga": [False, True, True],
    })


def test_el_cruce_deja_fuera_el_aviso_cancelado_en_una_descarga_posterior(tmp_path):
    ted = _ted_cancelado_despues()
    assert tm.avisos_para_cruce(ted)["ted_notice_id"].tolist() == ["B"]
    # Quitando antes los cancelados, A volvería con su versión vieja
    assert tm.ultima_version_por_aviso(ted[ted["cancelled"] != "1"])["ted_notice_id"].tolist() == ["A", "B"]
    pipe = pd.DataFrame({"_nif": ["B11111111", "B22222222"], "_imp_adj": [300_000.0, 500_000.0],
                         "_año": [2024.0] * 2, "_fecha_adj": ["2024-03-01"] * 2, "_es_menor": [False] * 2,
                         "_organ": ["Ayuntamiento de Alfa"] * 2, "_adj": ["EMP A", "EMP B"],
                         "_expediente": [None, None], "_cpv": ["79000000"] * 2})
    res, _ = tm.cross_validate_ted(pipe, ted, "NAC")
    assert res["_ted_validated"].tolist() == [False, True]
    ted.to_parquet(tmp_path / "ted_es_can.parquet", index=False)
    assert rtc.load_ted(tmp_path / "ted_es_can.parquet")["ted_notice_id"].tolist() == ["B"]
    assert cvp.load_ted(tmp_path / "ted_es_can.parquet")["ted_notice_id"].tolist() == ["B"]
    # Con cancelled numérico igual (float con nulos, p.ej. filas de la API sin el campo: '1.0' como texto)
    ted.assign(cancelled=[0.0, 1.0, np.nan]).to_parquet(tmp_path / "ted_es_can.parquet", index=False)
    assert tm.avisos_para_cruce(pd.read_parquet(tmp_path / "ted_es_can.parquet"))["ted_notice_id"].tolist() == ["B"]
    assert rtc.load_ted(tmp_path / "ted_es_can.parquet")["ted_notice_id"].tolist() == ["B"]


def test_cancelled_con_tipos_que_admiten_nulos(tmp_path):
    """Con string o Int64, un nulo compara como <NA>: no debe dejar fuera los avisos de la API ni
    los sembrados (sin cancelled)."""
    ted = _ted_cancelado_despues()
    for tipo, valores in (("string", ["0", "1", None]), ("Int64", [0, 1, None])):
        con_nulos = ted.assign(cancelled=pd.array(valores, dtype=tipo))
        assert tm.avisos_para_cruce(con_nulos)["ted_notice_id"].tolist() == ["B"], tipo
        con_nulos.to_parquet(tmp_path / "ted_es_can.parquet", index=False)
        assert rtc.load_ted(tmp_path / "ted_es_can.parquet")["ted_notice_id"].tolist() == ["B"], tipo


def _entradas_diagnostico(ted):
    miss = pd.DataFrame({
        "organo_contratante": ["Ayuntamiento de Alfa"], "nif_organo": ["P0000001A"],
        "nif_adjudicatario": ["B1"], "expediente": ["A-1"],
        "importe_adjudicacion": [300_000.0], "ano": [2024.0],
    })
    miss.to_parquet(ted / "crossval_missing.parquet", index=False)
    miss.to_parquet(ted / "crossval_sara.parquet", index=False)
    miss.iloc[:0].to_parquet(ted / "crossval_matched.parquet", index=False)
    _ted_cancelado_despues().to_parquet(ted / "ted_es_can.parquet", index=False)


def test_script_de_analisis_en_un_proceso_limpio(tmp_path):
    """Sin nada importado antes: el script importa el ted_module de su carpeta (y este, comun/)."""
    repo = _make_repo(tmp_path, ["diagnostico_missing_ted.py"])
    _entradas_diagnostico(repo / "ted")
    otro = tmp_path / "otro_cwd"
    otro.mkdir()
    r = subprocess.run([sys.executable, str(repo / "ted" / "diagnostico_missing_ted.py")], cwd=otro,
                       capture_output=True, text=True, timeout=600)
    assert r.returncode == 0, r.stderr[-3000:]
    assert "TED total: 1\n" in r.stdout


def test_scripts_de_analisis_cuentan_cada_aviso_una_vez_y_sin_cancelados(tmp_path, monkeypatch, capsys):
    repo = _make_repo(tmp_path, ["diagnostico_missing_ted.py", "analisis_sector_salud.py"])
    ted = repo / "ted"
    miss = pd.DataFrame({
        "organo_contratante": ["Ayuntamiento de Alfa"], "nif_organo": ["P0000001A"],
        "nif_adjudicatario": ["B1"], "expediente": ["A-1"],
        "importe_adjudicacion": [300_000.0], "ano": [2024.0],
    })
    miss.to_parquet(ted / "crossval_missing.parquet", index=False)
    miss.to_parquet(ted / "crossval_sara.parquet", index=False)
    miss.iloc[:0].to_parquet(ted / "crossval_matched.parquet", index=False)
    _ted_cancelado_despues().to_parquet(ted / "ted_es_can.parquet", index=False)
    _run_script(repo, "diagnostico_missing_ted.py", monkeypatch)
    assert "TED total: 1\n" in capsys.readouterr().out
    _sara_salud([dict(organo_contratante="Hospital de Alfa", expediente="S-1", importe_adjudicacion=300_000.0)]) \
        .to_parquet(ted / "crossval_sara.parquet", index=False)
    _run_script(repo, "analisis_sector_salud.py", monkeypatch)
    assert "TED cargado: 1\n" in capsys.readouterr().out


def test_nif_con_los_textos_que_pandas_leia_como_nulos(tmp_path):
    df = tm._normalize_ted_data(pd.DataFrame({
        "win_nationalid": ["#N/A N/A", "1.#QNAN", "ESB12345678"],
        "cae_nationalid": ["NULL", "n/a", "ESP4109100J"],
    }))
    assert df["win_nif_clean"].tolist() == ["", "", "B12345678"]
    assert df["cae_nif_clean"].tolist() == ["", "", "P4109100J"]
    assert rtc.clean_nif("-1.#IND ") == "" and rtc.clean_nif("ESB12345678") == "B12345678"
    assert rtc._NIF_NULOS == tm._NIF_NULOS
    pd.DataFrame({"win_nationalid": ["#N/A N/A", "ESB12345678"], "importe_ted": [1.0, 2.0]}) \
        .to_parquet(tmp_path / "ted_es_can.parquet", index=False)
    assert rtc.load_ted(tmp_path / "ted_es_can.parquet")["win_nif_clean"].tolist() == ["", "B12345678"]
    # Son los textos que pandas lee como nulos con las opciones por defecto
    textos = [x for x in tm._TEXTOS_NULOS_PANDAS if x]
    leido = _ORIG_READ_CSV(io.StringIO("x\n" + "\n".join(textos) + "\n"), dtype=str)
    assert len(leido) == len(textos) and leido["x"].isna().all()
    try:
        from pandas._libs.parsers import STR_NA_VALUES
    except ImportError:   # API interna de pandas: si cambia de sitio, basta con lo anterior
        return
    assert set(STR_NA_VALUES) == set(tm._TEXTOS_NULOS_PANDAS)


# ═══════════════════════════════════════════════════════════════════════════
#  XML eForms: una fila por oferta ganadora de cada resultado de lote
#  (fallos medidos por el ETL de la web, docs/etl_v2/grupo4.md del repo de la web,
#  y revisión de la PR #45)
# ═══════════════════════════════════════════════════════════════════════════

FIX_EFORMS = REPO_DIR / "tests" / "fixtures" / "ted_eforms"


def _xml_real(numero):
    """XML publicado por TED (descargado el 29-sep-2026)."""
    return gzip.decompress((FIX_EFORMS / f"{numero}.xml.gz").read_bytes())


def _api_real(numero):
    """Respuesta de la API de búsqueda para ese aviso (29-sep-2026, sin links)."""
    avisos = json.loads((FIX_EFORMS / "api.json").read_text(encoding="utf-8"))["notices"]
    return next(n for n in avisos if n["publication-number"] == numero)


def _org_simple():
    return [_parte("TPA-0001", ["ORG-0002"])], [_organizacion("ORG-0002", "EMP", "B11111111")]


def _ganadoras(filas):
    return [f for f in filas if f["tender_id"]]


class TestTedEforms:
    def test_646040_2026_una_fila_por_adjudicacion_y_no_por_posicion(self):
        # La API da winner-name 3 veces (9 nombres), 3 importes y 1 fecha (deduplicada): el parser
        # anterior hacía 9 filas, 6 de ellas con el último NIF (B27200104) y el primer importe
        api = _api_real("646040-2026")
        assert len(api["winner-name"]["spa"]) == 9 and len(api["winner-decision-date"]) == 1
        filas = tm._parse_api_notice(api, _xml_real("646040-2026"))
        got = [(f["lot_id"], f["win_name"], f["win_nationalid"], f["tender_value"], f["number_offers"])
               for f in filas]
        assert got == [("LOT-0002", "SOLRED S.A.", "A79707345", "15119.62", "1"),
                       ("LOT-0004", "ESTRUCTURAS MECANIZADAS DE ASTURIAS S.L.", "B74281171", "62685", "1"),
                       ("LOT-0005", "GASOLEOS VIVEIRO S.L.", "B27200104", "11040.1", "2")]
        # Las tres ofertas ganadoras (las cita el contrato 852/2026) suman el valor del aviso (BT-161)
        assert sum(float(f["tender_value"]) for f in filas) == pytest.approx(88844.72)
        assert {(f["notice_value"], f["notice_value_cur"]) for f in filas} == {("88844.72", "EUR")}
        assert {f["dt_award"] for f in filas} == {"2026-08-12+02:00"}
        assert {f["contract_id"] for f in filas} == {"852/2026"}
        assert {f["ganadora_por"] for f in filas} == {"contrato"}
        assert [f["n_ofertas_descritas"] for f in filas] == ["1", "1", "1"]
        assert [f["estimated_value_lot"] for f in filas] == ["45296.37", "138600", "26116.35"]
        assert [f["internal_id_lot"] for f in filas] == ["2", "4", "5"]
        assert filas[0]["title_lot"].startswith("Lote 2.- Suministro destinado a vehículos")
        assert filas[0]["title_proc"].startswith("Suministro de combustible para los vehículos")
        assert filas[0]["description_proc"].startswith("La necesidad de esta contratación")
        assert [f["win_size"] for f in filas] == ["large", "sme", "sme"]
        assert all(f["_xml_eforms"] == "" and f["n_filas_aviso"] == 3 for f in filas)
        assert [f["lot_index"] for f in filas] == [0, 1, 2]
        assert "value_euro" not in filas[0] and "currency" not in filas[0]

    def test_266280_2024_acuerdo_marco_solo_las_ofertas_con_contrato(self):
        # Cada resultado describe 2-3 ofertas (las recibidas) y el contrato cita una: las ganadoras
        # son ESAOTE, GENERAL ELECTRIC (dos lotes) y FUJIFILM, las mismas que da la API (winner-name)
        filas = tm._parse_api_notice(_api_real("266280-2024"), _xml_real("266280-2024"))
        got = [(f["lot_id"], f["win_nationalid"], f["tender_value"], f["n_ofertas_descritas"]) for f in filas]
        assert got == [("LOT-0003", "A60785573", "132000", "3"), ("LOT-0004", "A28061737", "195000", "2"),
                       ("LOT-0005", "A28061737", "188000", "3"), ("LOT-0009", "B82097940", "140000", "2")]
        assert {f["win_name"].split(" ")[0] for f in filas} == {"ESAOTE", "GENERAL", "FUJIFILM"}
        # SAKURA, SIEMENS y PHILIPS ofertaron y no ganaron: no son adjudicatarias
        assert not any(n in f["win_name"] for f in filas for n in ("SAKURA", "SIEMENS", "PHILIPS"))
        assert filas[0]["framework_max_lot"] == "396000" and filas[0]["framework_max_lot_cur"] == "EUR"

    def test_perdedora_nunca_es_adjudicataria(self):
        # efac:LotResult/efac:LotTender son las ofertas recibidas (OPT-320); la ganadora es la que
        # cita un contrato (BT-3202). En un lote sin adjudicar (clos-nw, open-nw) no hay ninguna,
        # aunque un contrato cite una de sus ofertas
        xml = _eforms(
            [_resultado("RES-0001", "LOT-0001", ["TEN-0001", "TEN-0002", "TEN-0003"], ["CON-0001"],
                        estadisticas=[("tenders", 3)]),
             _resultado("RES-0002", "LOT-0002", ["TEN-0004", "TEN-0005"], estado="clos-nw", motivo="no-signed"),
             _resultado("RES-0003", "LOT-0003", ["TEN-0006"], estado="open-nw")],
            [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 90), _oferta("TEN-0002", "TPA-0002", "LOT-0001", 80),
             _oferta("TEN-0003", "TPA-0003", "LOT-0001", 0), _oferta("TEN-0004", "TPA-0001", "LOT-0002", 5),
             _oferta("TEN-0005", "TPA-0002", "LOT-0002", 6), _oferta("TEN-0006", "TPA-0003", "LOT-0003", 7)],
            [_contrato("CON-0001", ["TEN-0002"], "2026-01-02+01:00", "C1"), _contrato("CON-0002", ["TEN-0004"])],
            [_parte("TPA-0001", ["ORG-0002"]), _parte("TPA-0002", ["ORG-0003"]), _parte("TPA-0003", ["ORG-0004"])],
            [_organizacion("ORG-0002", "PERDEDORA UNO", "B11111111"),
             _organizacion("ORG-0003", "GANADORA", "B22222222"),
             _organizacion("ORG-0004", "PERDEDORA DOS", "B33333333")])
        filas = tm._parse_api_notice(_notice("20-2024"), xml)
        assert [(f["lot_id"], f["tender_id"], f["win_name"], f["n_ofertas_descritas"]) for f in filas] == [
            ("LOT-0001", "TEN-0002", "GANADORA", "3"), ("LOT-0002", "", "", "2"), ("LOT-0003", "", "", "1")]
        assert (filas[0]["tender_value"], filas[0]["contract_id"], filas[0]["dt_award"]) == ("80", "C1", "2026-01-02+01:00")
        # Las filas sin adjudicataria no llevan nada de ninguna oferta
        for f in filas[1:]:
            assert all(f[c] == "" for c in ("win_name", "win_nationalid", "tender_value", "contract_id", "ganadora_por"))
        df = tm._normalize_ted_data(pd.DataFrame(filas))
        assert df["importe_ted"].tolist()[0] == 80.0 and df["importe_ted"].iloc[1:].isna().all()

    def test_ganadora_por_el_contrato_de_su_resultado(self):
        # 536696-2026: el contrato del lote 8 (CON-0002) cita la oferta del lote 5 (TEN-0001) de la
        # misma empresa y ningún contrato cita la del lote 8 (TEN-0002). El resultado del lote 8 es
        # selec-w, describe una sola oferta y cita su contrato: esa oferta es la ganadora
        def aviso(empresa_lote8):
            return _eforms(
                [_resultado("RES-0001", "LOT-0005", ["TEN-0001"], ["CON-0001"]),
                 _resultado("RES-0002", "LOT-0008", ["TEN-0002"], ["CON-0002"])],
                [_oferta("TEN-0001", "TPA-0001", "LOT-0005", 39174.8),
                 _oferta("TEN-0002", empresa_lote8, "LOT-0008", 15881)],
                [_contrato("CON-0001", ["TEN-0001"], "2025-11-27+01:00", "LOTE 5"),
                 _contrato("CON-0002", ["TEN-0001"], "2025-11-24+01:00", "LOTE 8")],
                [_parte("TPA-0001", ["ORG-0002"]), _parte("TPA-0002", ["ORG-0003"])],
                [_organizacion("ORG-0002", "PLATAFORMA FEMAR S.L.", "B91016238"),
                 _organizacion("ORG-0003", "OTRA SL", "B44444444")])
        filas = tm._parse_api_notice(_notice("21-2024"), aviso("TPA-0001"))
        assert [(f["lot_id"], f["tender_id"], f["ganadora_por"], f["contract_id"], f["dt_award"]) for f in filas] == [
            ("LOT-0005", "TEN-0001", "contrato", "LOTE 5", "2025-11-27+01:00"),
            ("LOT-0008", "TEN-0002", "contrato del resultado", "LOTE 8", "2025-11-24+01:00")]
        # Si la oferta del lote 8 es de otra empresa, no hay pruebas de que ganara
        filas = tm._parse_api_notice(_notice("21-2024"), aviso("TPA-0002"))
        assert [(f["lot_id"], f["tender_id"], f["win_name"]) for f in filas] == [
            ("LOT-0005", "TEN-0001", "PLATAFORMA FEMAR S.L."), ("LOT-0008", "", "")]

    def test_543120_2026_dos_resultados_del_mismo_lote_con_un_contrato(self):
        # Dos resultados del lote 3 citan el mismo contrato (CONTR-2023-989191 LOTE 2), que solo cita
        # TEN-0001 (755.161,6 €, el valor del aviso). TEN-0002 (857.348,7 €), de la misma empresa y del
        # mismo lote, no es adjudicataria: antes salían las dos y sumaban 1.612.510,3 €
        filas = tm._parse_api_notice(_notice("543120-2026"), _xml_real("543120-2026"))
        assert [(f["lot_result_id"], f["lot_id"], f["tender_id"], f["ganadora_por"]) for f in filas] == [
            ("RES-0001", "LOT-0003", "", ""), ("RES-0003", "LOT-0003", "TEN-0001", "contrato")]
        g, = _ganadoras(filas)
        assert (g["tender_value"], g["contract_id"], g["notice_value"]) == (
            "755161.6", "CONTR-2023-989191 LOTE 2", "755161.6")

    def test_contrato_del_resultado_solo_con_una_oferta_descrita(self):
        # La excepción exige una sola oferta descrita: con dos, no se sabe cuál ganó
        xml = _eforms([_resultado("RES-0001", "LOT-0005", ["TEN-0001"], ["CON-0001"]),
                       _resultado("RES-0002", "LOT-0008", ["TEN-0002", "TEN-0003"], ["CON-0002"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0005", 10), _oferta("TEN-0002", "TPA-0001", "LOT-0008", 20),
                       _oferta("TEN-0003", "TPA-0002", "LOT-0008", 30)],
                      [_contrato("CON-0001", ["TEN-0001"], referencia="LOTE 5"),
                       _contrato("CON-0002", ["TEN-0001"], referencia="LOTE 8")],
                      [_parte("TPA-0001", ["ORG-0002"]), _parte("TPA-0002", ["ORG-0003"])],
                      [_organizacion("ORG-0002", "MISMA SL", "B11111111"), _organizacion("ORG-0003", "OTRA SL", "B22222222")])
        filas = tm._parse_api_notice(_notice("25-2024"), xml)
        assert [(f["lot_id"], f["tender_id"], f["n_ofertas_descritas"]) for f in filas] == [
            ("LOT-0005", "TEN-0001", "1"), ("LOT-0008", "", "2")]

    def test_contrato_del_resultado_con_una_oferta_de_lote_desconocido(self):
        # Si la oferta que cita el contrato del resultado no dice su lote (o el aviso no la describe),
        # no se sabe si es de otro lote: no se aplica la excepción, aunque sea de la misma empresa
        xml = _eforms([_resultado("RES-0001", "LOT-0008", ["TEN-0002"], ["CON-0002"])],
                      [_oferta("TEN-0002", "TPA-0001", "LOT-0008", 20), _oferta("TEN-0009", "TPA-0001", "", 5)],
                      [_contrato("CON-0002", ["TEN-0009"], referencia="LOTE 8")],
                      [_parte("TPA-0001", ["ORG-0002"])], [_organizacion("ORG-0002", "MISMA SL", "B11111111")])
        filas = tm._parse_api_notice(_notice("27-2024"), xml)
        assert [(f["lot_id"], f["tender_id"]) for f in filas] == [("LOT-0008", "")]
        sin_describir = _eforms([_resultado("RES-0001", "LOT-0008", ["TEN-0002"], ["CON-0002"])],
                                [_oferta("TEN-0002", "TPA-0001", "LOT-0008", 20)],
                                [_contrato("CON-0002", ["TEN-0009"], referencia="LOTE 8")],
                                [_parte("TPA-0001", ["ORG-0002"])], [_organizacion("ORG-0002", "MISMA SL", "B11111111")])
        assert [(f["lot_id"], f["tender_id"]) for f in tm._parse_api_notice(_notice("27-2024"), sin_describir)] == [
            ("LOT-0008", "")]

    def test_id_interno_de_la_plataforma_no_es_el_nif(self):
        # 538782-2026: la UTE trae su número en la PLACSP (ID_UTE_TEMP_PLATAFORMA 329082) y, como OTROS,
        # los NIF de los socios. El número de la plataforma va aparte y nunca como NIF
        f, = tm._parse_api_notice(_notice("538782-2026"), _xml_real("538782-2026"))
        assert (f["win_nationalid"], f["win_platform_id"]) == ("B91251082 - B82387770", "329082")
        # Solo con el número de la plataforma: sin NIF; con ID_PLATAFORMA y NIF, el NIF
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"]), _resultado("RES-0002", "LOT-0002", ["TEN-0002"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 10), _oferta("TEN-0002", "TPA-0002", "LOT-0002", 20)],
                      [_contrato("CON-0001", ["TEN-0001", "TEN-0002"])],
                      [_parte("TPA-0001", ["ORG-0002"]), _parte("TPA-0002", ["ORG-0003"])],
                      [_organizacion("ORG-0002", "UTE SIN NIF", None, ids=[("ID_UTE_TEMP_PLATAFORMA", "333577")]),
                       _organizacion("ORG-0003", "EMPRESA", None, ids=[("ID_PLATAFORMA", "31210280164788"),
                                                                     ("NIF", "B27200104")])])
        filas = tm._parse_api_notice(_notice("26-2024"), xml)
        assert [(f["win_nationalid"], f["win_platform_id"]) for f in filas] == [
            ("", "333577"), ("B27200104", "31210280164788")]

    def test_ofertas_repetidas_en_el_resultado_una_fila(self):
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001", "TEN-0001"], ["CON-0001", "CON-0001"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 10)],
                      [_contrato("CON-0001", ["TEN-0001", "TEN-0001"], referencia="C1")], partes, orgs)
        filas = tm._parse_api_notice(_notice("22-2024"), xml)
        assert [(f["tender_id"], f["contract_id"], f["n_ofertas_descritas"]) for f in filas] == [("TEN-0001", "C1", "1")]

    def test_lote_sin_adjudicar_con_su_motivo(self):
        filas = tm._parse_api_notice(_api_real("515290-2026"), _xml_real("515290-2026"))
        assert len(filas) == 1
        f = filas[0]
        assert (f["lot_id"], f["winner_selection_status"], f["non_award_justification"]) == (
            "LOT-0002", "clos-nw", "no-signed")
        assert f["win_name"] == "" and f["tender_value"] == "" and f["tender_id"] == ""

    def test_motivo_de_no_adjudicacion_de_cada_lote(self):
        # Antes non_award_justification era el primer valor del aviso, repetido en todas sus filas
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"]),
                       _resultado("RES-0002", "LOT-0002", estado="clos-nw", motivo="no-rece")],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 500)], [_contrato("CON-0001", ["TEN-0001"])],
                      partes, orgs)
        filas = tm._parse_api_notice(_notice("7-2024"), xml)
        assert [(f["lot_id"], f["win_nationalid"], f["non_award_justification"]) for f in filas] == [
            ("LOT-0001", "B11111111", ""), ("LOT-0002", "", "no-rece")]

    def test_grupo_de_empresas_en_una_fila_con_el_lider_primero(self):
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 1000)], [_contrato("CON-0001", ["TEN-0001"])],
                      [_parte("TPA-0001", ["ORG-0002", "ORG-0003"], lider="ORG-0003")],
                      [_organizacion("ORG-0002", "SOCIA SL", "B22222222", "sme"),
                       _organizacion("ORG-0003", "LIDER SA", "A33333333", "large")])
        f, = tm._parse_api_notice(_notice("8-2024"), xml)
        assert (f["win_name"], f["win_nationalid"], f["win_size"]) == (
            "LIDER SA---SOCIA SL", "A33333333---B22222222", "large---sme")

    def test_oferta_con_el_contrato_de_su_lote(self):
        # Un contrato cita ofertas de varios lotes (335954-2026): la fecha es la del contrato de su
        # resultado de lote, no la de los otros
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"], ["CON-0001"]),
                       _resultado("RES-0002", "LOT-0002", ["TEN-0002"], ["CON-0002"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 10), _oferta("TEN-0002", "TPA-0001", "LOT-0002", 20)],
                      [_contrato("CON-0001", ["TEN-0001", "TEN-0002"], "2025-11-27+01:00", "C1"),
                       _contrato("CON-0002", ["TEN-0001", "TEN-0002"], "2025-11-24+01:00", "C2")],
                      partes, orgs)
        filas = tm._parse_api_notice(_notice("9-2024"), xml)
        assert [(f["contract_id"], f["dt_award"], f["contract_award_dates"]) for f in filas] == [
            ("C1", "2025-11-27+01:00", ""), ("C2", "2025-11-24+01:00", "")]

    def test_contratos_cruzados_se_toma_el_que_cita_la_oferta(self):
        # 539513-2026: el resultado del lote 1 cita CON-0001, que cita la oferta del lote 3, y CON-0003
        # cita la del lote 1: cada oferta va con el contrato que la cita (BT-3202), no con el del resultado
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"], ["CON-0001"]),
                       _resultado("RES-0003", "LOT-0003", ["TEN-0003"], ["CON-0003"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 25914.04),
                       _oferta("TEN-0003", "TPA-0001", "LOT-0003", 45641.07)],
                      [_contrato("CON-0001", ["TEN-0003"], "2024-06-01+02:00", "1"),
                       _contrato("CON-0003", ["TEN-0001"], "2024-06-03+02:00", "3")], partes, orgs)
        filas = tm._parse_api_notice(_notice("24-2024"), xml)
        assert [(f["lot_id"], f["contract_id"], f["dt_award"], f["ganadora_por"]) for f in filas] == [
            ("LOT-0001", "3", "2024-06-03+02:00", "contrato"), ("LOT-0003", "1", "2024-06-01+02:00", "contrato")]

    def test_varias_fechas_de_adjudicacion_de_una_oferta(self):
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 10)],
                      [_contrato("CON-0001", ["TEN-0001"], "2025-03-02+01:00", "A"),
                       _contrato("CON-0002", ["TEN-0001"], "2025-03-01+01:00", "B")],
                      partes, orgs)
        f, = tm._parse_api_notice(_notice("10-2024"), xml)
        assert (f["contract_id"], f["dt_award"]) == ("A---B", "2025-03-01+01:00")
        assert f["contract_award_dates"] == "2025-03-02+01:00---2025-03-01+01:00"

    def test_importes_por_separado_e_importe_ted(self):
        # value_euro mezclaba importe de la oferta, máximo del acuerdo marco y valor estimado del lote
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"]),
                       _resultado("RES-0002", "LOT-0002", ["TEN-0002"]), _resultado("RES-0003", "LOT-0003")],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001", 1000),
                       _oferta("TEN-0002", "TPA-0001", "LOT-0002", 900, "GBP")],
                      [_contrato("CON-0001", ["TEN-0001", "TEN-0002"])], partes, orgs,
                      [_lote("LOT-0001", estimado=1200), _lote("LOT-0002", estimado=990),
                       _lote("LOT-0003", estimado=50)], total=5000)
        df = tm._normalize_ted_data(pd.DataFrame(tm._parse_api_notice(_notice("11-2024"), xml)))
        assert df["tender_value"].tolist()[:2] == [1000.0, 900.0] and pd.isna(df.loc[2, "tender_value"])
        assert df["tender_value_cur"].tolist() == ["EUR", "GBP", ""]
        assert df["estimated_value_lot"].tolist() == [1200.0, 990.0, 50.0]
        assert df["estimated_value_lot_cur"].tolist() == ["EUR"] * 3
        assert df["notice_value"].tolist() == [5000.0] * 3
        # importe_ted: la oferta en euros; nunca el total del aviso en cada fila ni el estimado
        assert df.loc[0, "importe_ted"] == 1000.0
        assert df["importe_ted"].iloc[1:].isna().all()
        # Aviso de una sola fila sin importe de oferta: el valor del aviso, si está en euros
        uno = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001")], [_contrato("CON-0001", ["TEN-0001"])],
                      partes, orgs, total=777)
        df = tm._normalize_ted_data(pd.DataFrame(tm._parse_api_notice(_notice("12-2024"), uno)))
        assert df["importe_ted"].tolist() == [777.0]

    def test_valor_del_aviso_en_otra_moneda_no_va_a_importe_ted(self):
        partes, orgs = _org_simple()
        xml = _eforms([_resultado("RES-0001", "LOT-0001", ["TEN-0001"])],
                      [_oferta("TEN-0001", "TPA-0001", "LOT-0001")], [_contrato("CON-0001", ["TEN-0001"])],
                      partes, orgs, total=777).replace(b'<cbc:TotalAmount currencyID="EUR">',
                                                       b'<cbc:TotalAmount currencyID="GBP">')
        filas = tm._parse_api_notice(_notice("23-2024"), xml)
        assert (filas[0]["notice_value"], filas[0]["notice_value_cur"]) == ("777", "GBP")
        assert tm._normalize_ted_data(pd.DataFrame(filas))["importe_ted"].isna().all()

    def test_sin_xml_eforms_conserva_el_importe_de_la_api(self):
        # Avisos del esquema anterior (1.523 de 2024) o sin XML: su importe_ted sigue saliendo del
        # total-value de la API, como antes (974 de esos avisos tienen importe: 5.290,1 M€)
        api = dict(_api_real("20-2024"), **{"total-value": ["1234567.89"], "total-value-cur": ["EUR"]})
        for filas in (tm._parse_api_notice(api, _xml_real("20-2024")),
                      tm._filas_aviso(tm._aviso_api(api), None, "no_disponible"),
                      tm._filas_aviso(tm._aviso_api(api), None, "error", "HTTP 503")):
            df = tm._normalize_ted_data(pd.DataFrame(filas))
            assert df["_xml_eforms"].iloc[0] != "" and df["importe_ted"].tolist() == [1234567.89]
        assert tm._filas_aviso(tm._aviso_api(api), None, "error", "HTTP 503")[0]["_xml_eforms"] == "sin XML: HTTP 503"

    def test_bt758_y_version_del_aviso(self):
        n = _notice("536696-2026")
        n.update({"notice-identifier": "75c0a5b6-3d45-4bbf-9c5a-a2a72ef4d493", "notice-version": 1,
                  "change-notice-version-identifier": "335954-2026", "change-reason-code": "update-add",
                  "title-proc": {"spa": "SUMINISTRO DE PRODUCTOS"}, "description-proc": {"eng": "Supply"}})
        f = tm._parse_api_notice(n)[0]
        assert (f["changed_notice"], f["notice_identifier"], f["notice_version"], f["change_reason_code"]) == (
            "335954-2026", "75c0a5b6-3d45-4bbf-9c5a-a2a72ef4d493", "1", "update-add")
        assert (f["title_proc"], f["description_proc"]) == ("SUMINISTRO DE PRODUCTOS", "Supply")

    def test_xml_del_esquema_anterior_o_ilegible(self):
        filas = tm._parse_api_notice(_api_real("20-2024"), _xml_real("20-2024"))
        assert len(filas) == 1 and filas[0]["notice_type"] == "can-modif"
        assert filas[0]["_xml_eforms"].startswith("XML del esquema anterior a eForms")
        assert tm._parse_api_notice(_notice("13-2024"), b"<no")[0]["_xml_eforms"].startswith("XML ilegible")

    def test_xml_se_pide_una_vez_y_se_guarda_comprimido(self, ted_2024, ted_xml, tmp_path):
        tm.download_ted_spain(years=[2024])
        assert sorted(ted_xml.calls) == ["1-2024", "2-2024", "3-2024"]
        guardado = tmp_path / "xml" / "2024" / "1-2024.xml.gz"
        assert gzip.decompress(guardado.read_bytes()) == _xml_de_aviso(ted_2024.notices[0][1])
        ted_xml.calls.clear()
        # Caché y consolidado guardados con 2024 abierto: se vuelve a la API
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2024-06-01")
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")
        ted_2024.notices.append(_aviso(4))
        tm.download_ted_spain(years=[2024])
        assert ted_xml.calls == ["4-2024"]   # un aviso publicado no cambia: solo se pide el nuevo
        # --force los vuelve a pedir, pero un XML igual no crea versión
        ted_xml.calls.clear()
        tm.download_ted_spain(years=[2024], force_redownload=True)
        assert len(ted_xml.calls) == 4
        assert not (tmp_path / "xml" / "2024" / "_historico").exists()

    def test_respuesta_que_no_es_un_aviso_no_se_guarda(self, ted_2024, ted_xml, tmp_path):
        # Una página HTML servida con 200 (o un XML de error) no es el XML de un aviso: no se guarda
        # para siempre como si lo fuera; se reintenta y el aviso va sin XML
        ted_xml.fijos["2-2024"] = b"<html><body>Mantenimiento</body></html>"
        df = tm.download_ted_spain(years=[2024])
        assert ted_xml.calls.count("2-2024") == tm.TEDConfig.XML_REINTENTOS
        assert not (tmp_path / "xml" / "2024" / "2-2024.xml.gz").exists()
        dos = df[df["ted_notice_id"] == "2-2024"]
        assert dos["_xml_eforms"].tolist() == ["sin XML: HTTP 200 sin el XML de un aviso"]
        # El XML del esquema anterior sí es un aviso
        assert tm._es_xml_de_aviso(_xml_real("20-2024")) and not tm._es_xml_de_aviso(b"<html></html>")
        assert not tm._es_xml_de_aviso(_xml_real("646040-2026")[:500])   # cortado

    def test_xml_no_disponible_se_marca_y_el_anio_se_guarda(self, ted_2024, ted_xml, tmp_path):
        ted_xml.codigos["2-2024"] = 404
        df = tm.download_ted_spain(years=[2024])
        assert (tmp_path / "ted_can_2024_ES_api.parquet").exists() and not df.attrs.get("sin_guardar")
        dos = df[df["ted_notice_id"] == "2-2024"]
        assert dos["_xml_eforms"].tolist() == ["sin XML: TED responde 404"] and dos["win_name"].tolist() == [""]
        assert ted_xml.calls.count("2-2024") == 2

    def test_un_aviso_sin_xml_no_bloquea_el_anio_y_se_reintenta(self, ted_2024, ted_xml, tmp_path):
        # Un error persistente (distinto de 404) en un aviso: el año se guarda con ese aviso sin XML
        # (marcado) y la ejecución no sale con 1; la siguiente lo vuelve a pedir, también con el año
        # cerrado y su caché ya guardada
        ted_xml.codigos["3-2024"] = 503
        df = tm.download_ted_spain(years=[2024])
        assert not df.attrs.get("sin_guardar") and (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        assert ted_xml.calls.count("3-2024") == tm.TEDConfig.XML_REINTENTOS
        tres = df[df["ted_notice_id"] == "3-2024"]
        assert tres["_xml_eforms"].tolist() == ["sin XML: HTTP 503"] and tres["win_name"].tolist() == [""]
        del ted_xml.codigos["3-2024"]
        ted_xml.calls.clear()
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2025-01-10")   # la caché cerrada se reutiliza
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")   # sin el atajo del consolidado cerrado
        df = tm.download_ted_spain(years=[2024])
        assert ted_xml.calls == ["3-2024"]
        tres = df[(df["ted_notice_id"] == "3-2024") & df["_en_ultima_descarga"]]
        assert tres["_xml_eforms"].tolist() == [""] and tres["win_name"].tolist() == ["EMP 3"]
        assert "ted_can_2024_ES_api__20250110T000000Z.parquet" in _historico(tmp_path)
        # El que ya tiene XML no se vuelve a pedir
        ted_xml.calls.clear()
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")
        tm.download_ted_spain(years=[2024])
        assert ted_xml.calls == []

    def test_muchos_avisos_sin_xml_dejan_el_anio_sin_guardar(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # TED caído o una URL que ha cambiado: guardar todo sin XML sería peor que esperar
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_TOLERADOS", 0)
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_FRACCION", 0.0)
        ted_xml.codigos["3-2024"] = 503
        df = tm.download_ted_spain(years=[2024])
        assert df.attrs.get("sin_guardar") and not (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        # Los XML descargados quedan en disco: la siguiente ejecución solo pide el que falta
        del ted_xml.codigos["3-2024"]
        ted_xml.calls.clear()
        df = tm.download_ted_spain(years=[2024])
        assert ted_xml.calls == ["3-2024"] and not df.attrs.get("sin_guardar")

    def test_xml_danado_en_disco_se_vuelve_a_pedir(self, ted_2024, ted_xml, tmp_path):
        danado = tmp_path / "xml" / "2024" / "1-2024.xml.gz"
        danado.parent.mkdir(parents=True)
        danado.write_bytes(b"no es gzip")
        df = tm.download_ted_spain(years=[2024])
        assert ted_xml.calls.count("1-2024") == 1 and not df.attrs.get("sin_guardar")
        assert df.loc[df["ted_notice_id"] == "1-2024", "win_name"].tolist() == ["EMP 1"]
        assert [p.read_bytes() for p in (danado.parent / "_historico").iterdir()] == [b"no es gzip"]

    def test_xml_danado_que_no_se_puede_volver_a_pedir_cuenta_como_fallo(self, ted_2024, ted_xml, tmp_path,
                                                                         monkeypatch):
        danado = tmp_path / "xml" / "2024" / "1-2024.xml.gz"
        danado.parent.mkdir(parents=True)
        danado.write_bytes(b"no es gzip")
        ted_xml.codigos["1-2024"] = 503
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_TOLERADOS", 0)
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_FRACCION", 0.0)
        df = tm.download_ted_spain(years=[2024])
        assert df.attrs.get("sin_guardar") and not (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        assert danado.read_bytes() == b"no es gzip"   # no se ha tocado: solo se sustituye por un XML bueno

    def test_presupuesto_de_tiempo(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # Al agotarse el presupuesto se deja de pedir XML: el año no se guarda y lo descargado queda
        monkeypatch.setattr(tm.TEDConfig, "XML_WORKERS", 1)
        llamadas = []

        def agotado():
            llamadas.append(1)
            return len(llamadas) > 2   # el control del año y el primer XML, dentro del presupuesto
        monkeypatch.setattr(tm, "_presupuesto_agotado", agotado)
        df = tm.download_ted_spain(years=[2024])
        assert df.attrs.get("sin_guardar") and ted_xml.calls == ["1-2024"]
        assert not (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        monkeypatch.setattr(tm, "_presupuesto_agotado", lambda: False)
        ted_xml.calls.clear()
        df = tm.download_ted_spain(years=[2024])
        assert sorted(ted_xml.calls) == ["2-2024", "3-2024"] and not df.attrs.get("sin_guardar")
        # Con el presupuesto agotado antes de empezar, ni siquiera se lista el año en la API
        monkeypatch.setattr(tm, "_presupuesto_agotado", lambda: True)
        ted_2024.calls.clear()
        _fijar_fecha(tmp_path / "ted_can_2024_ES_api.parquet", "2024-06-01")
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")
        assert tm.download_ted_spain(years=[2024]) is None and not ted_2024.calls

    def test_presupuesto_con_el_reloj(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # El presupuesto se mide con el reloj desde que empieza download_ted_spain (reloj simulado: cada
        # XML tarda 0,6 s y el presupuesto es de 1 s: el tercero ya no se pide)
        reloj = [0.0]
        monkeypatch.setattr(tm.time, "monotonic", lambda: reloj[0])
        monkeypatch.setattr(tm.TEDConfig, "XML_WORKERS", 1)
        monkeypatch.setattr(tm.TEDConfig, "XML_PRESUPUESTO_S", 1.0)
        falso = tm._descargar_xml

        def lento(url):
            reloj[0] += 0.6
            return falso(url)
        monkeypatch.setattr(tm, "_descargar_xml", lento)
        df = tm.download_ted_spain(years=[2024])
        assert df.attrs.get("sin_guardar") and ted_xml.calls == ["1-2024", "2-2024"]
        assert not (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    def test_completar_la_cache_con_el_presupuesto_agotado_no_la_cambia(self, ted_2024, ted_xml, tmp_path,
                                                                        monkeypatch):
        # Año cerrado guardado con dos avisos sin XML; al reintentarlos se agota el presupuesto tras el
        # primero: la caché no cambia (ni a medias) y la siguiente ejecución los vuelve a pedir
        ted_xml.codigos.update({"2-2024": 503, "3-2024": 503})
        tm.download_ted_spain(years=[2024])
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        _fijar_fecha(cache, "2025-01-10")
        _fijar_fecha(tmp_path / "ted_es_can.parquet", "2024-06-01")
        antes = _sha(cache)
        ted_xml.codigos.clear()
        ted_xml.calls.clear()
        monkeypatch.setattr(tm.TEDConfig, "XML_WORKERS", 1)
        llamadas = []
        monkeypatch.setattr(tm, "_presupuesto_agotado", lambda: llamadas.append(1) or len(llamadas) > 2)
        tm.download_ted_spain(years=[2024])
        assert ted_xml.calls == ["2-2024"] and _sha(cache) == antes
        assert not (tmp_path / "_historico" / "ted_can_2024_ES_api__20250110T000000Z.parquet").exists()

    def test_un_404_no_cuenta_para_el_umbral(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # Sin tolerancia para errores, un 404 (el aviso no tiene XML en TED) no impide guardar el año
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_TOLERADOS", 0)
        monkeypatch.setattr(tm.TEDConfig, "XML_FALLOS_FRACCION", 0.0)
        ted_xml.codigos["2-2024"] = 404
        df = tm.download_ted_spain(years=[2024])
        assert not df.attrs.get("sin_guardar") and (tmp_path / "ted_can_2024_ES_api.parquet").exists()
        assert df.loc[df["ted_notice_id"] == "2-2024", "_xml_eforms"].tolist() == ["sin XML: TED responde 404"]

    def test_404_en_casi_todos_los_pedidos_deja_el_anio_sin_guardar(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # Casi todos los pedidos con 404 no son avisos sin XML sino una URL que ha cambiado
        monkeypatch.setattr(tm.TEDConfig, "XML_404_MASIVO_MIN", 2)
        ted_xml.codigos.update({"1-2024": 404, "2-2024": 404})
        df = tm.download_ted_spain(years=[2024])
        assert df.attrs.get("sin_guardar") and not (tmp_path / "ted_can_2024_ES_api.parquet").exists()

    def test_retry_after_con_tope_y_sin_pasar_del_presupuesto(self, tmp_path, monkeypatch):
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        reloj, esperas = [0.0], []
        monkeypatch.setattr(tm.time, "monotonic", lambda: reloj[0])

        def dormir(s):
            esperas.append(s)
            reloj[0] += s
        monkeypatch.setattr(tm.time, "sleep", dormir)
        monkeypatch.setattr(tm, "_INICIO", [0.0])
        monkeypatch.setattr(tm, "_descargar_xml", lambda url: (429, b"", {"Retry-After": "100000"}))
        monkeypatch.setattr(tm.TEDConfig, "XML_PRESUPUESTO_S", 10_000)
        assert tm._obtener_xml("1-2024", tm._Ritmo(1000))[0] == "error"
        assert max(esperas) == tm.TEDConfig.XML_ESPERA_MAX_S and len(esperas) >= tm.TEDConfig.XML_REINTENTOS - 1
        # Con poco presupuesto no espera más allá: queda pendiente para la siguiente ejecución
        esperas.clear()
        reloj[0] = 0.0
        monkeypatch.setattr(tm.TEDConfig, "XML_PRESUPUESTO_S", 150)
        assert tm._obtener_xml("1-2024", tm._Ritmo(1000)) == ("pendiente", "presupuesto de tiempo agotado")
        assert sum(esperas) <= 150

    def test_ritmo_maximo_de_peticiones(self, ted_2024, ted_xml, tmp_path, monkeypatch):
        # Como mucho XML_MAX_POR_SEGUNDO peticiones por segundo (reloj simulado: solo avanza al esperar)
        reloj = [1000.0]
        monkeypatch.setattr(tm.time, "monotonic", lambda: reloj[0])
        monkeypatch.setattr(tm.time, "sleep", lambda s: reloj.__setitem__(0, reloj[0] + s))
        monkeypatch.setattr(tm.TEDConfig, "XML_WORKERS", 1)
        numeros = [f"{i}-2024" for i in range(1, 11)]
        ted_2024.notices = [_aviso(i) for i in range(1, 11)]
        FakeTedApi.ultima = ted_2024
        estados, completo = tm._xml_avisos(numeros)
        assert completo and {e for e, _ in estados.values()} == {"ok"}
        assert reloj[0] - 1000.0 == pytest.approx(9 / tm.TEDConfig.XML_MAX_POR_SEGUNDO)

    def test_sesion_de_requests_por_hilo(self, monkeypatch):
        vistas = []

        class Respuesta:
            status_code, content, headers = 200, b"<x/>", {}

        def get(self, url, **kw):
            vistas.append(self)
            return Respuesta()
        tm._SESIONES.__dict__.pop("sesion", None)
        monkeypatch.setattr(tm.requests.Session, "get", get)
        monkeypatch.setattr(tm, "_descargar_xml", ORIG_DESCARGAR_XML)
        assert tm._descargar_xml("u1")[0] == 200 and tm._descargar_xml("u2")[0] == 200
        assert len(vistas) == 2 and vistas[0] is vistas[1] and isinstance(vistas[0], requests.Session)
        otro = []
        hilo = threading.Thread(target=lambda: otro.append(tm._sesion()))
        hilo.start()
        hilo.join()
        assert otro[0] is not vistas[0]

    def test_formato_anterior_de_la_cache_se_arrastra_sin_duplicar(self, ted_2024, tmp_path):
        # Caché del parser de sept. 2026 (filas por posición, con notice_subtype): el aviso 1 con
        # 4 filas copia y el 9, que TED ya no sirve
        vieja = pd.DataFrame({"ted_notice_id": ["1-2024"] * 4 + ["9-2024"], "year": ["2024"] * 5,
                              "lot_index": [0, 1, 2, 3, 0], "notice_subtype": ["29"] * 5,
                              "win_name": ["EMP 1"] * 4 + ["EMP 9"], "value_euro": ["100000"] * 5,
                              "source": ["api_v3"] * 5})
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        vieja.to_parquet(cache, index=False)
        _fijar_fecha(cache, "2025-01-10")
        df = tm.download_ted_spain(years=[2024])
        assert _estado(df) == {"1-2024": [True], "2-2024": [True], "3-2024": [True], "9-2024": [False]}
        nueve = df[df["ted_notice_id"] == "9-2024"].iloc[0]
        assert nueve["_origen"] == "caché de 2024 del parser anterior" and nueve["win_name"] == "EMP 9"
        assert nueve["_primera_descarga"] == "2025-01-10T00:00:00+00:00"
        # Otra ejecución no lo duplica (viene de la caché vieja y del consolidado anterior)
        otra = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert _estado(otra) == _estado(df)

    def test_varios_parsers_anteriores_entra_el_mas_reciente(self, ted_2024, tmp_path):
        # 9-2024, retirado, está en una caché del parser de v2026.02 (formato 1) y en otra del de
        # sept. 2026 (formato 2): entra una vez, con las filas del más reciente
        (tmp_path / "_historico").mkdir()
        pd.DataFrame({"ted_notice_id": ["9-2024"], "year": ["2024"], "lot_index": [0], "win_name": ["F1"],
                      "cae_town": ["['Sevilla']"], "source": ["api_v3"]}).to_parquet(
            tmp_path / "_historico" / "ted_can_2024_ES_api__20250101T000000Z.parquet", index=False)
        cache = tmp_path / "ted_can_2024_ES_api.parquet"
        pd.DataFrame({"ted_notice_id": ["9-2024", "9-2024"], "year": ["2024"] * 2, "lot_index": [0, 1],
                      "notice_subtype": ["29"] * 2, "win_name": ["F2", "F2"], "source": ["api_v3"] * 2}
                     ).to_parquet(cache, index=False)
        _fijar_fecha(cache, "2025-06-01")
        df = tm.download_ted_spain(years=[2024])
        nueve = df[df["ted_notice_id"] == "9-2024"]
        assert nueve["win_name"].tolist() == ["F2", "F2"] and not nueve["_en_ultima_descarga"].any()
        assert _estado(df)["1-2024"] == [True]

    def test_avisos_eforms_de_2023_que_el_csv_no_trae(self, monkeypatch, tmp_path, no_sleep, ted_xml):
        # El CSV de 2023 trae los avisos del esquema anterior; los eForms (2.609 CAN de España) solo
        # los da la API. Un aviso eForms que el CSV sí trae se queda con sus filas del CSV
        url = tm.TEDConfig.CSV_HUB_URL.format(year=2023)
        csv_2023 = ("ID_NOTICE_CAN,YEAR,ISO_COUNTRY_CODE,CAE_NAME,WIN_NAME,WIN_NATIONALID,"
                    "AWARD_VALUE_EURO_FIN_1,CANCELLED\n"
                    "2023000011,2023,ES,Ayuntamiento de Sevilla,EMPRESA SL,ESB11111111,380000,0\n"
                    "2023000012,2023,ES,Diputación de Huelva,OTRA SL,ESB22222222,500000,0\n")
        monkeypatch.setattr(tm, "_descargar", _csv_hub_fake({url: csv_2023}))
        api = FakeTedApi([
            ("20231110", _notice("639630-2023", winners=["B55555555"], win_names=["EFORMS SL"], values=["70000"])),
            ("20231201", _notice("12-2023", winners=["B99999999"], win_names=["OTRA"], values=["1"]))])
        monkeypatch.setattr(requests, "post", api)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm.download_ted_spain(years=[2023])
        assert api.calls and all("AND notice-subtype IN (25, 26, 27" in c["query"] for c in api.calls)
        assert sorted(df["ted_notice_id"]) == ["2023000011", "2023000012", "639630-2023"]
        nuevo = df[df["ted_notice_id"] == "639630-2023"]
        assert nuevo["win_nationalid"].tolist() == ["B55555555"] and nuevo["importe_ted"].tolist() == [70000.0]
        assert nuevo["source"].tolist() == ["api_v3"] and nuevo["year"].tolist() == [2023]
        assert (tmp_path / "ted_can_2023_ES_eforms.parquet").exists()
        # Las filas del CSV son las mismas que sin los avisos eForms
        csv = df[df["source"] == "csv_bulk"].reset_index(drop=True)
        solo_csv = tm._normalize_ted_data(tm._renombrar_csv(tm._download_csv_year(2023)))
        pd.testing.assert_frame_equal(csv[list(solo_csv.columns)], solo_csv, check_dtype=False)

    def test_eforms_con_el_anio_sacado_de_la_api(self, monkeypatch, tmp_path, no_sleep):
        # Si el CSV de 2023 falla, el año sale entero de la API. De una caché eForms de antes, el aviso que
        # la API trae no se repite y el que ya no trae se conserva, pero no como vigente
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        uno = pd.DataFrame(_filas(_notice("639630-2023", winners=["B55555555"], win_names=["EFORMS SL"],
                                          values=["70000"])), columns=tm._COLUMNAS_API)
        otro = pd.DataFrame(_filas(_notice("639631-2023", winners=["B66666666"], win_names=["RETIRADO SL"],
                                           values=["5"])), columns=tm._COLUMNAS_API)
        uno.to_parquet(tmp_path / "ted_can_2023_ES_api.parquet", index=False)
        pd.concat([uno, otro]).to_parquet(tmp_path / "ted_can_2023_ES_eforms.parquet", index=False)
        df = tm._consolidar({2023: "api"}, tmp_path / "ted_es_can.parquet")
        assert _estado(df) == {"639630-2023": [True], "639631-2023": [False]}
