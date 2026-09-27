"""
Tests offline de los scripts TED (ted/*.py).

Sin red: las respuestas de la TED Search API v3 y del CSV bulk de data.europa.eu
se simulan. Además de funciones sueltas se ejecutan las rutas "main" documentadas
(descarga, cross-validation, diagnóstico y análisis sector salud) sobre una copia
de los scripts en un árbol temporal con la misma estructura que el repo
(<tmp>/ted, <tmp>/nacional), lanzados desde un cwd distinto a la raíz.
"""

import importlib.util
import io
import logging
import re
import runpy
import shutil
import sys
import urllib.error
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

    def __call__(self, url, json=None, timeout=None, headers=None, **kw):
        assert url == "https://api.ted.europa.eu/v3/notices/search"
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


def _fake_read_csv(csv_by_year):
    def fake(filepath_or_buffer, *args, **kwargs):
        if isinstance(filepath_or_buffer, str) and filepath_or_buffer.startswith("http"):
            m = re.search(r"notices%20(\d{4})\.csv$", filepath_or_buffer)
            if m and m.group(1) in csv_by_year:
                return _ORIG_READ_CSV(io.StringIO(csv_by_year[m.group(1)]), dtype=str,
                                      chunksize=kwargs.get("chunksize"))
            raise urllib.error.HTTPError(filepath_or_buffer, 404, "Not Found", None, None)
        return _ORIG_READ_CSV(filepath_or_buffer, *args, **kwargs)
    return fake


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr(tm.time, "sleep", lambda s: None)


@pytest.fixture
def ted_http(monkeypatch, no_sleep):
    """Red simulada: CSV bulk 2019 + API v3 2024."""
    api = FakeTedApi(_api_notices_2024())
    monkeypatch.setattr(requests, "post", api)
    monkeypatch.setattr(pd, "read_csv", _fake_read_csv({"2019": TED_CSV_2019}))
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
        recs = tm._parse_api_notice(_api_notices_2024()[3][1])
        assert len(recs) == 2
        assert [r["win_nationalid"] for r in recs] == ["A10000001", "A10000002"]
        assert [r["value_euro"] for r in recs] == ["250000", "260000"]
        assert [r["lot_index"] for r in recs] == [0, 1]
        assert all(r["year"] == "2024" and r["ted_notice_id"] == "100004-2024" for r in recs)
        assert recs[0]["cae_nationalid"] == "Q2800001B"
        assert recs[0]["cpv"] == "33600000"

    def test_buyer_city_list_is_not_stringified(self):
        rec = tm._parse_api_notice(_api_notices_2024()[0][1])[0]
        assert rec["cae_town"] == "Sevilla"

    def test_number_offers_uses_tenders_statistic(self):
        # BT-760 se repite por tipo (t-sme, tenders...): nº de ofertas = 'tenders'
        rec = tm._parse_api_notice(_api_notices_2024()[0][1])[0]
        assert rec["number_offers"] == "4"
        assert rec["internal_id_proc"] == "SEV-2024-001"

    def test_number_offers_without_codes_keeps_previous_behaviour(self):
        n = _notice("1-2024", winners=["B1"], values=["1"], offers_vals=["7"])
        n.pop("received-submissions-type-code")
        assert tm._parse_api_notice(n)[0]["number_offers"] == "7"


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
        assert "tender-value" in body["fields"] and "winner-identifier" in body["fields"]

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
        monkeypatch.setattr(pd, "read_csv", _fake_read_csv({}))
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        with caplog.at_level(logging.WARNING, logger="ted_module"):
            df = tm.download_ted_spain(years=[2024], force_redownload=True)
        assert df is not None and len(df) == 100
        assert not (tmp_path / "ted_es_can.parquet").exists()
        assert "2024" in caplog.text


# ═══════════════════════════════════════════════════════════════════════════
#  ted_module.py — normalización
# ═══════════════════════════════════════════════════════════════════════════

class TestNormalize:
    def test_dates_with_mixed_utc_offsets_are_parsed(self):
        recs = []
        for _, n in _api_notices_2024():
            recs.extend(tm._parse_api_notice(n))
        df = tm._normalize_ted_data(pd.DataFrame(recs))
        # +01:00 (invierno) y +02:00 (verano) mezclados: no deben quedar todo NaT
        assert df["dt_award"].notna().sum() == 5
        assert pd.api.types.is_datetime64_any_dtype(df["dt_award"])
        assert df.loc[df["ted_notice_id"] == "100001-2024", "dt_award"].iloc[0] == pd.Timestamp("2024-03-15")

    def test_numeric_and_importe(self):
        recs = []
        for _, n in _api_notices_2024():
            recs.extend(tm._parse_api_notice(n))
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
        # CSV: 4 filas -> 1 FR filtrada, 1 cancelada eliminada; API: 4 avisos -> 5 registros (lotes)
        assert len(df) == 2 + 5
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
        assert len(df) == 7
        assert "Cache ilegible" in caplog.text
        assert len(pd.read_parquet(tmp_path / "ted_es_can.parquet")) == 7

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
    """pd.read_csv simulado: solo sirve las URL dadas (cualquier otra, 404)."""
    llamadas = []

    def fake(filepath_or_buffer, *args, **kwargs):
        if isinstance(filepath_or_buffer, str) and filepath_or_buffer.startswith("http"):
            llamadas.append((filepath_or_buffer, kwargs.get("compression")))
            if filepath_or_buffer in csv_by_url:
                return _ORIG_READ_CSV(io.StringIO(csv_by_url[filepath_or_buffer]), dtype=str,
                                      chunksize=kwargs.get("chunksize"))
            raise urllib.error.HTTPError(filepath_or_buffer, 404, "Not Found", None, None)
        return _ORIG_READ_CSV(filepath_or_buffer, *args, **kwargs)
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
        monkeypatch.setattr(pd, "read_csv", fake)
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
        monkeypatch.setattr(pd, "read_csv", fake)
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
                            lambda years, force_redownload: capturado.setdefault("years", years))
        monkeypatch.setattr(sys, "argv", ["ted_module.py", "download"])
        tm.main()
        # Antes: '2010-2025' fijo
        assert capturado["years"] == list(range(2006, 2032))

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
        monkeypatch.setattr(pd, "read_csv", _csv_hub_fake({url: csv_txt}))
        monkeypatch.setattr(requests, "post", _api_prohibida)
        monkeypatch.setattr(tm.TEDConfig, "DATA_DIR", tmp_path)
        df = tm.download_ted_spain(years=[2021], force_redownload=True)
        fila = df.iloc[0]
        assert (fila["TITLE"], fila["CONTRACT_NUMBER"], fila["B_CONTRACTOR_SME"]) == (
            "Servicio de limpieza", "CT-7", "Y")
        assert "TED_NOTICE_URL" in pd.read_parquet(tmp_path / "ted_can_2021_ES.parquet").columns


def test_nullable_strings_from_rebuilt_parquet():
    assert rtc.classify_buyer(pd.NA) == (False, False)
    assert rtc.normalize_name(pd.NA) == ""
    assert rtc.clean_nif(pd.NA) == ""
