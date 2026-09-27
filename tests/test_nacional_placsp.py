"""Tests del pipeline PLACSP nacional: códigos, lotes, versiones y normalización.

Cubren los errores que distorsionaban las cifras publicadas (issue #6 y
relacionados): importes con semántica equivocada, varias versiones de la misma
licitación contadas como contratos distintos, etiquetas de procedimiento/tipo
de contrato desplazadas y CPV sin el cero inicial.
"""

import io
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from nacional import licitaciones as lic
from nacional.licitaciones import (
    deduplicar_versiones,
    leer_placsp,
    normalizar_placsp,
    parsear_entry,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _entry(nombre):
    return parsear_entry(ET.parse(FIXTURES / nombre).getroot())


def _atom(entries_xml):
    return (
        '<feed xmlns="http://www.w3.org/2005/Atom" '
        'xmlns:cbc="urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2" '
        'xmlns:cac="urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2" '
        'xmlns:cbc-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2" '
        'xmlns:cac-place-ext="urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2">'
        + "".join(entries_xml) + "</feed>"
    )


def _entry_xml(id_, updated, estado, importe_adj=None, proc="1"):
    resultado = ""
    if importe_adj is not None:
        resultado = f"""
        <cac:TenderResult>
          <cbc:AwardDate>2024-02-01</cbc:AwardDate>
          <cac:WinningParty><cac:PartyName><cbc:Name>ADJ SL</cbc:Name></cac:PartyName></cac:WinningParty>
          <cac:AwardedTenderedProject>
            <cbc:ProcurementProjectLotID>1</cbc:ProcurementProjectLotID>
            <cac:LegalMonetaryTotal>
              <cbc:TaxExclusiveAmount currencyID="EUR">{importe_adj}</cbc:TaxExclusiveAmount>
            </cac:LegalMonetaryTotal>
          </cac:AwardedTenderedProject>
        </cac:TenderResult>"""
    return f"""
    <entry>
      <id>{id_}</id>
      <updated>{updated}</updated>
      <cac-place-ext:ContractFolderStatus>
        <cbc:ContractFolderID>EXP-{id_[-3:]}</cbc:ContractFolderID>
        <cbc-place-ext:ContractFolderStatusCode>{estado}</cbc-place-ext:ContractFolderStatusCode>
        <cac-place-ext:LocatedContractingParty>
          <cac:Party><cac:PartyName><cbc:Name>Órgano</cbc:Name></cac:PartyName></cac:Party>
        </cac-place-ext:LocatedContractingParty>
        <cac:ProcurementProject><cbc:Name>Obj</cbc:Name><cbc:TypeCode>2</cbc:TypeCode></cac:ProcurementProject>
        {resultado}
        <cac:TenderingProcess><cbc:ProcedureCode>{proc}</cbc:ProcedureCode></cac:TenderingProcess>
        <cac-place-ext:ValidNoticeInfo><cac-place-ext:AdditionalPublicationStatus>
          <cac-place-ext:AdditionalPublicationDocumentReference><cbc:IssueDate>2024-01-10</cbc:IssueDate>
          </cac-place-ext:AdditionalPublicationDocumentReference>
        </cac-place-ext:AdditionalPublicationStatus></cac-place-ext:ValidNoticeInfo>
      </cac-place-ext:ContractFolderStatus>
    </entry>"""


class TestCodigos:
    def test_etiquetas_contrastadas_con_los_datos(self):
        assert lic.PROCEDIMIENTOS["3"] == "Negociado sin publicidad"
        assert lic.PROCEDIMIENTOS["4"] == "Negociado con publicidad"
        assert lic.PROCEDIMIENTOS["6"] == "Contrato menor"
        assert lic.PROCEDIMIENTOS["9"] == "Abierto simplificado"
        assert lic.PROCEDIMIENTOS["100"] == "Normas internas"
        assert lic.TIPOS_CONTRATO["22"] == "Concesión Servicios"
        assert lic.TIPOS_CONTRATO["40"] == "Colaboración Público-Privada"
        assert lic.ESTADOS["PRE"] == "Anuncio previo"

    def test_entry_usa_las_etiquetas(self):
        r = _entry("entry_lotes.xml")
        assert r["tipo_contrato_code"] == "22"
        assert r["tipo_contrato"] == "Concesión Servicios"
        assert r["procedimiento"] == "Negociado sin publicidad"
        assert r["estado"] == "Resuelta"
        assert r["cpv_principal"] == "09134100"
        assert r["dependencia"] == "Consejería de Pruebas"
        assert r["nif_organo"] == "S2800000A"


def _status_con_anuncios(anuncios):
    avisos = "".join(
        f"""<cac-place-ext:ValidNoticeInfo>
              {f'<cbc-place-ext:NoticeTypeCode>{tipo}</cbc-place-ext:NoticeTypeCode>' if tipo else ''}
              <cac-place-ext:AdditionalPublicationStatus>
                {''.join(f'<cac-place-ext:AdditionalPublicationDocumentReference><cbc:IssueDate>{f}</cbc:IssueDate></cac-place-ext:AdditionalPublicationDocumentReference>' for f in fechas)}
              </cac-place-ext:AdditionalPublicationStatus>
            </cac-place-ext:ValidNoticeInfo>"""
        for tipo, fechas in anuncios
    )
    xml = _atom([f"<entry><cac-place-ext:ContractFolderStatus>{avisos}</cac-place-ext:ContractFolderStatus></entry>"])
    return ET.fromstring(xml).find("atom:entry/cac-place-ext:ContractFolderStatus", lic.NS)


class TestFechaPublicacion:
    def test_usa_el_anuncio_de_licitacion_aunque_no_sea_el_primero(self):
        # El anuncio de formalización aparece primero: antes se tomaba su fecha
        status = _status_con_anuncios([
            ("DOC_FORM", ["2025-06-01"]),
            ("DOC_CN", ["2025-03-03", "2025-03-01+01:00"]),
            ("DOC_PIN", ["2025-01-15"]),
        ])
        assert lic.fecha_publicacion_licitacion(status) == "2025-03-01"

    def test_sin_anuncio_de_licitacion_usa_el_primero_publicado(self):
        status = _status_con_anuncios([("DOC_CAN_ADJ", ["2025-05-10"]), (None, ["2025-04-02"])])
        assert lic.fecha_publicacion_licitacion(status) == "2025-04-02"

    def test_sin_anuncios(self):
        assert lic.fecha_publicacion_licitacion(_status_con_anuncios([])) is None


class TestLotes:
    def test_columnas_principales_son_el_primer_resultado(self):
        r = _entry("entry_lotes.xml")
        assert r["adjudicatario"] == "EMPRESA UNO SL"
        assert r["nif_adjudicatario"] == "B11111111"
        assert r["importe_adjudicacion"] == 40000.0
        assert r["importe_adj_con_iva"] == 48400.0
        assert r["num_ofertas"] == 1
        assert r["es_pyme"] is True
        assert r["fecha_adjudicacion"] == "2025-05-20"

    def test_todos_los_lotes_quedan_en_resultados(self):
        r = _entry("entry_lotes.xml")
        assert r["n_lotes"] == 2
        assert r["n_resultados"] == 2
        res = r["_resultados"]
        assert [x["lote"] for x in res] == ["1", "2"]
        assert [x["nif_adjudicatario"] for x in res] == ["B11111111", "A22222222"]
        assert sum(x["importe_adjudicacion"] for x in res) == 95000.0
        assert res[1]["es_pyme"] is False
        assert res[1]["resultado_code"] == "9"

    def test_importes_de_presupuesto(self):
        r = _entry("entry_lotes.xml")
        assert r["valor_estimado_contrato"] == 300000.0
        assert r["importe_sin_iva"] == 100000.0
        assert r["importe_con_iva"] == 121000.0

    def test_sin_resultados(self):
        r = _entry("entry_budget.xml")
        assert r["n_resultados"] == 0
        assert r["_resultados"] == []
        assert r["importe_adjudicacion"] is None


class TestExportacion:
    @pytest.fixture
    def salida(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        return tmp_path

    def _procesar(self, tmp_path, entries):
        atom = tmp_path / "feed.atom"
        atom.write_text(_atom(entries), encoding="utf-8")
        return lic.procesar_archivo_atom(atom)

    def test_conserva_la_version_mas_reciente_aunque_se_lea_antes(self, salida):
        # La versión adjudicada (más reciente) aparece ANTES que la publicada:
        # el antiguo keep='last' por orden de lectura se quedaba con la vieja.
        lics = self._procesar(salida, [
            _entry_xml("urn:1001", "2024-03-01T10:00:00.123+01:00", "RES", importe_adj="5000.00"),
            _entry_xml("urn:1001", "2024-01-15T10:00:00+01:00", "PUB"),
            _entry_xml("urn:1002", "2024-02-01T10:00:00.5+01:00", "PUB"),
        ])
        for x in lics:
            x["conjunto"] = "licitaciones"
        df = lic.exportar_datos(lics, "prueba")
        assert len(df) == 2
        fila = df.set_index("id").loc["urn:1001"]
        assert fila["estado_code"] == "RES"
        assert fila["importe_adjudicacion"] == 5000.0
        # Formatos mixtos de atom:updated: ninguno se pierde como NaT
        assert df["fecha_updated"].notna().all()

        res = pd.read_parquet(salida / "prueba_resultados.parquet")
        assert list(res["id"]) == ["urn:1001"]
        assert res["importe_adjudicacion"].tolist() == [5000.0]
        assert (salida / "prueba.csv").exists()
        assert "_n" not in pd.read_parquet(salida / "prueba.parquet").columns

    def test_resultados_solo_de_la_version_conservada(self, salida):
        lics = self._procesar(salida, [
            _entry_xml("urn:2001", "2024-01-01T00:00:00.000+01:00", "ADJ", importe_adj="100.00"),
            _entry_xml("urn:2001", "2024-05-01T00:00:00.000+02:00", "RES", importe_adj="120.00"),
        ])
        lic.exportar_datos(lics, "prueba")
        res = pd.read_parquet(salida / "prueba_resultados.parquet")
        assert res["importe_adjudicacion"].tolist() == [120.0]

    def test_fecha_publicacion_con_zona_horaria(self):
        s = pd.Series(["2024-01-15", "2024-01-16+01:00", None])
        out = lic.parsear_fechas(s)
        assert out.iloc[0] == pd.Timestamp("2024-01-15")
        assert out.iloc[1] == pd.Timestamp("2024-01-16")
        assert pd.isna(out.iloc[2])


class TestDeduplicar:
    def test_ultima_version_y_filas_sin_id(self):
        df = pd.DataFrame({
            "id": ["a", "a", None, "b", "a", None],
            "fecha_updated": ["2024-01-02T00:00:00+00:00", "2024-03-01T00:00:00.1+00:00", None,
                              "2024-01-01T00:00:00+00:00", None, None],
            "v": [1, 2, 3, 4, 5, 6],
        })
        out = deduplicar_versiones(df)
        # 'a' -> la de marzo (la fila sin fecha no gana a una fechada); sin id se conservan
        assert out["v"].tolist() == [2, 3, 4, 6]

    def test_empate_gana_la_ultima_leida(self):
        df = pd.DataFrame({"id": ["x", "x"], "fecha_updated": ["2024-01-01T00:00:00+00:00"] * 2, "v": [1, 2]})
        assert deduplicar_versiones(df)["v"].tolist() == [2]


def _parquet_publicado():
    """Mini réplica del esquema de licitaciones_espana.parquet (release v2026.02)."""
    return pd.DataFrame({
        "id": ["u1", "u1", "u2", "u3", "c1"],
        "conjunto": ["licitaciones", "licitaciones", "menores", "agregacion", "consultas"],
        "tipo_contrato_code": [22.0, 22.0, 2.0, 40.0, np.nan],
        "tipo_contrato": ["22", "22", "Servicios", "Concesión Servicios", None],
        "procedimiento_code": [3.0, 3.0, 6.0, 7.0, 9.0],
        "procedimiento": ["Negociado con publicidad", "Negociado con publicidad",
                          "Asociación innovación", "Contrato menor", "Consulta preliminar"],
        "estado_code": pd.Categorical(["ADJ", "RES", "RES", "PRE", "PUB"]),
        "estado": ["Adjudicada", "Resuelta", "Resuelta", "PRE", "Publicada"],
        "importe_sin_iva": [300000.0, 300000.0, 1000.0, 5e6, np.nan],
        "importe_con_iva": [121000.0, 121000.0, 1210.0, np.nan, np.nan],
        "importe_adjudicacion": [np.nan, 95000.0, 1000.0, np.nan, np.nan],
        "cpv_principal": [9134100.0, 9134100.0, 45000000.0, np.nan, 72000000.0],
        "fecha_updated": pd.to_datetime(["2024-01-01", "2024-06-01", "2024-02-01", "2024-03-01", "2024-04-01"], utc=True),
    })


class TestNormalizar:
    def test_esquema_antiguo_y_etiquetas(self):
        out = normalizar_placsp(_parquet_publicado())
        assert out["id"].tolist() == ["u1", "u2", "u3", "c1"]
        u1 = out.iloc[0]
        # importe_sin_iva antiguo = valor estimado -> se renombra y queda vacío
        assert u1["valor_estimado_contrato"] == 300000.0
        assert pd.isna(u1["importe_sin_iva"])
        assert u1["estado_code"] == "RES" and u1["estado"] == "Resuelta"
        assert u1["importe_adjudicacion"] == 95000.0
        assert u1["tipo_contrato_code"] == "22" and u1["tipo_contrato"] == "Concesión Servicios"
        assert u1["procedimiento_code"] == "3" and u1["procedimiento"] == "Negociado sin publicidad"
        assert u1["cpv_principal"] == "09134100"
        assert out.iloc[1]["procedimiento"] == "Contrato menor"
        assert out.iloc[2]["tipo_contrato"] == "Colaboración Público-Privada"
        assert out.iloc[2]["procedimiento"] == "Derivado de acuerdo marco"
        assert out.iloc[2]["estado"] == "Anuncio previo"
        # Las consultas preliminares conservan su etiqueta propia
        assert out.iloc[3]["procedimiento"] == "Consulta preliminar"
        assert pd.isna(out.iloc[3]["tipo_contrato_code"])
        cols = list(out.columns)
        assert cols.index("importe_sin_iva") == cols.index("valor_estimado_contrato") + 1

    def test_esquema_nuevo_no_toca_importes(self):
        df = _parquet_publicado().rename(columns={"importe_sin_iva": "valor_estimado_contrato"})
        df["importe_sin_iva"] = [100000.0, 100000.0, 1000.0, np.nan, np.nan]
        out = normalizar_placsp(df)
        assert out.iloc[0]["importe_sin_iva"] == 100000.0
        assert out.iloc[0]["valor_estimado_contrato"] == 300000.0

    def test_no_modifica_el_dataframe_original(self):
        df = _parquet_publicado()
        normalizar_placsp(df, deduplicar=False)
        assert "valor_estimado_contrato" not in df.columns
        assert df["procedimiento"].iloc[0] == "Negociado con publicidad"

    def test_leer_placsp_por_row_groups(self, tmp_path):
        path = tmp_path / "publicado.parquet"
        pq.write_table(pa.Table.from_pandas(_parquet_publicado(), preserve_index=False), path, row_group_size=2)
        assert pq.ParquetFile(path).num_row_groups == 3
        out = leer_placsp(path)
        assert out["id"].tolist() == ["u1", "u2", "u3", "c1"]
        assert out.iloc[0]["estado_code"] == "RES"
        assert "valor_estimado_contrato" in out.columns

    def test_cli_normalizar_placsp(self, tmp_path, monkeypatch, capsys):
        entrada = tmp_path / "publicado.parquet"
        salida = tmp_path / "normalizado.parquet"
        pq.write_table(pa.Table.from_pandas(_parquet_publicado(), preserve_index=False), entrada, row_group_size=2)
        from nacional import normalizar_placsp as cli
        monkeypatch.setattr(sys, "argv", ["normalizar_placsp.py", "-i", str(entrada), "-o", str(salida)])
        cli.main()
        out = pd.read_parquet(salida)
        assert out["id"].tolist() == ["u1", "u2", "u3", "c1"]
        assert out["procedimiento"].tolist()[:3] == ["Negociado sin publicidad", "Contrato menor",
                                                     "Derivado de acuerdo marco"]
        assert out["cpv_principal"].iloc[0] == "09134100"
        assert pq.ParquetFile(salida).schema_arrow.field("importe_sin_iva").type == pa.float64()
        assert "1 versiones anteriores descartadas" in capsys.readouterr().out


class _Resp:
    def __init__(self, chunks, status=200, falla_en=None):
        self.chunks, self.status_code, self.falla_en = chunks, status, falla_en

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

    def iter_content(self, chunk_size):
        for i, c in enumerate(self.chunks):
            if self.falla_en == i:
                raise requests.exceptions.ChunkedEncodingError("corte")
            yield c


class _Session:
    def __init__(self, respuestas):
        self.respuestas = list(respuestas)
        self.llamadas = 0

    def get(self, url, timeout, stream):
        self.llamadas += 1
        return self.respuestas.pop(0)


def _zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.atom", "x" * 5000)
    return buf.getvalue()


class TestDescarga:
    @pytest.fixture(autouse=True)
    def sin_esperas(self, monkeypatch):
        monkeypatch.setattr(lic.time, "sleep", lambda s: None)

    def test_corte_a_mitad_no_deja_zip_truncado(self, tmp_path):
        destino = tmp_path / "f.zip"
        datos = _zip_bytes()
        sesion = _Session([_Resp([datos[:2000], datos[2000:]], falla_en=1)] * 3)
        assert lic.descargar_archivo(sesion, "http://x", destino) is None
        assert not destino.exists()
        assert not (tmp_path / "f.zip.part").exists()

    def test_zip_local_corrupto_se_vuelve_a_descargar(self, tmp_path):
        destino = tmp_path / "f.zip"
        destino.write_bytes(b"PK" + b"0" * 5000)  # truncado de una ejecución antigua
        datos = _zip_bytes()
        sesion = _Session([_Resp([datos])])
        assert lic.descargar_archivo(sesion, "http://x", destino) == destino
        assert sesion.llamadas == 1
        assert zipfile.is_zipfile(destino)

    def test_zip_integro_no_se_descarga_otra_vez(self, tmp_path):
        destino = tmp_path / "f.zip"
        destino.write_bytes(_zip_bytes())
        sesion = _Session([])
        assert lic.descargar_archivo(sesion, "http://x", destino) == destino
        assert sesion.llamadas == 0

    def test_mes_en_curso_se_refresca(self):
        hoy = datetime(2026, 3, 15)
        assert lic.es_periodo_reciente(2026, 3, hoy)
        assert lic.es_periodo_reciente(2026, 2, hoy)
        assert not lic.es_periodo_reciente(2026, 1, hoy)
        assert lic.es_periodo_reciente(2025, 12, datetime(2026, 1, 5))
        assert not lic.es_periodo_reciente(2024, None, hoy)
