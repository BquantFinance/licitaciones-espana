"""Tests del pipeline PLACSP nacional: códigos, lotes, versiones y normalización.

Cubren los errores que distorsionaban las cifras publicadas (issue #6 y
relacionados): importes con semántica equivocada, etiquetas de
procedimiento/tipo de contrato desplazadas y CPV sin el cero inicial. Las
versiones de una misma licitación (una entrada del ATOM por actualización) se
sirven todas: se marcan con n_versiones / es_ultima_version, nunca se borran;
las entradas publicadas más de una vez, con entrada_repetida.

También: consultas preliminares de mercado (CPM), entradas borradas, tablas de
detalle del CODICE (adjudicatarios de UTE, lotes, criterios, modificaciones),
informes de descarga y procesado, y descargas sin perder versiones anteriores
de los ZIP (comun/historico.py).
"""

import io
import json
import os
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from comun import historico
from nacional import licitaciones as lic
from nacional.licitaciones import (
    leer_placsp,
    marcar_versiones,
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

    def test_sirve_todas_las_entradas_y_marca_la_mas_reciente(self, salida):
        # La versión adjudicada (más reciente) aparece ANTES que la publicada:
        # el orden de lectura no es cronológico; la marca va por atom:updated
        lics = self._procesar(salida, [
            _entry_xml("urn:1001", "2024-03-01T10:00:00.123+01:00", "RES", importe_adj="5000.00"),
            _entry_xml("urn:1001", "2024-01-15T10:00:00+01:00", "PUB"),
            _entry_xml("urn:1002", "2024-02-01T10:00:00.5+01:00", "PUB"),
        ])
        for x in lics:
            x["conjunto"] = "licitaciones"
        df = lic.exportar_datos(lics, "prueba")
        # No se elimina ninguna entrada
        assert len(df) == 3
        assert df["n_versiones"].tolist() == [2, 2, 1]
        assert df["es_ultima_version"].tolist() == [True, False, True]
        ultima = df[df["es_ultima_version"]].set_index("id").loc["urn:1001"]
        assert ultima["estado_code"] == "RES"
        assert ultima["importe_adjudicacion"] == 5000.0
        # Formatos mixtos de atom:updated: ninguno se pierde como NaT
        assert df["fecha_updated"].notna().all()

        guardado = pd.read_parquet(salida / "prueba.parquet")
        assert len(guardado) == 3 and "_n" not in guardado.columns
        assert (salida / "prueba.csv").exists()
        res = pd.read_parquet(salida / "prueba_resultados.parquet")
        assert list(res["id"]) == ["urn:1001"]
        assert res["importe_adjudicacion"].tolist() == [5000.0]
        assert res["es_ultima_version"].tolist() == [True]

    def test_resultados_de_todas_las_versiones(self, salida):
        lics = self._procesar(salida, [
            _entry_xml("urn:2001", "2024-01-01T00:00:00.000+01:00", "ADJ", importe_adj="100.00"),
            _entry_xml("urn:2001", "2024-05-01T00:00:00.000+02:00", "RES", importe_adj="120.00"),
        ])
        lic.exportar_datos(lics, "prueba")
        res = pd.read_parquet(salida / "prueba_resultados.parquet")
        assert res["importe_adjudicacion"].tolist() == [100.0, 120.0]
        assert res["es_ultima_version"].tolist() == [False, True]
        assert res["fecha_updated"].notna().all()

    def test_fecha_publicacion_con_zona_horaria(self):
        s = pd.Series(["2024-01-15", "2024-01-16+01:00", None])
        out = lic.parsear_fechas(s)
        assert out.iloc[0] == pd.Timestamp("2024-01-15")
        assert out.iloc[1] == pd.Timestamp("2024-01-16")
        assert pd.isna(out.iloc[2])


class TestVersiones:
    def test_marca_la_ultima_version_sin_eliminar_filas(self):
        df = pd.DataFrame({
            "id": ["a", "a", None, "b", "a", None],
            "fecha_updated": ["2024-01-02T00:00:00+00:00", "2024-03-01T00:00:00.1+00:00", None,
                              "2024-01-01T00:00:00+00:00", None, None],
            "v": [1, 2, 3, 4, 5, 6],
        })
        out = marcar_versiones(df)
        assert out["v"].tolist() == [1, 2, 3, 4, 5, 6]
        # 'a' -> la de marzo (una entrada sin fecha no gana a una fechada); sin id: una versión
        assert out["es_ultima_version"].tolist() == [False, True, True, True, False, True]
        assert out["n_versiones"].tolist() == [3, 3, 1, 1, 3, 1]

    def test_empate_gana_la_primera_leida(self):
        # Mismo id y atom:updated = la misma entrada publicada dos veces: la
        # marca va a la primera aparición, que no es entrada_repetida
        df = pd.DataFrame({"id": ["x", "x"], "fecha_updated": ["2024-01-01T00:00:00+00:00"] * 2, "v": [1, 2]})
        out = marcar_versiones(df)
        assert out["es_ultima_version"].tolist() == [True, False]
        assert out["entrada_repetida"].tolist() == [False, True]
        assert out["n_versiones"].tolist() == [1, 1]

    def test_sin_fechas_gana_la_ultima_leida(self):
        df = pd.DataFrame({"id": ["x", "x"], "v": [1, 2]})
        out = marcar_versiones(df)
        assert out["es_ultima_version"].tolist() == [False, True]
        assert out["entrada_repetida"].tolist() == [False, False]
        assert out["n_versiones"].tolist() == [2, 2]


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
        todas = normalizar_placsp(_parquet_publicado())
        # No se elimina ninguna fila: la versión antigua de u1 sigue ahí, marcada
        assert todas["id"].tolist() == ["u1", "u1", "u2", "u3", "c1"]
        assert todas["es_ultima_version"].tolist() == [False, True, True, True, True]
        assert todas["n_versiones"].tolist() == [2, 2, 1, 1, 1]
        out = todas[todas["es_ultima_version"]].reset_index(drop=True)
        assert normalizar_placsp(_parquet_publicado(), solo_ultima_version=True)["id"].tolist() == \
            ["u1", "u2", "u3", "c1"]
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
        out = normalizar_placsp(df, solo_ultima_version=True)
        assert out.iloc[0]["importe_sin_iva"] == 100000.0
        assert out.iloc[0]["valor_estimado_contrato"] == 300000.0

    def test_no_modifica_el_dataframe_original(self):
        df = _parquet_publicado()
        normalizar_placsp(df)
        assert "valor_estimado_contrato" not in df.columns
        assert df["procedimiento"].iloc[0] == "Negociado con publicidad"

    def test_leer_placsp_por_row_groups(self, tmp_path):
        path = tmp_path / "publicado.parquet"
        pq.write_table(pa.Table.from_pandas(_parquet_publicado(), preserve_index=False), path, row_group_size=2)
        assert pq.ParquetFile(path).num_row_groups == 3
        todas = leer_placsp(path)
        assert todas["id"].tolist() == ["u1", "u1", "u2", "u3", "c1"]
        assert todas["es_ultima_version"].tolist() == [False, True, True, True, True]
        out = leer_placsp(path, solo_ultima_version=True)
        assert out["id"].tolist() == ["u1", "u2", "u3", "c1"]
        assert out.iloc[0]["estado_code"] == "RES"
        assert out["n_versiones"].tolist() == [2, 1, 1, 1]
        assert "valor_estimado_contrato" in out.columns

    def test_cli_normalizar_placsp(self, tmp_path, monkeypatch, capsys):
        entrada = tmp_path / "publicado.parquet"
        salida = tmp_path / "normalizado.parquet"
        pq.write_table(pa.Table.from_pandas(_parquet_publicado(), preserve_index=False), entrada, row_group_size=2)
        from nacional import normalizar_placsp as cli
        monkeypatch.setattr(sys, "argv", ["normalizar_placsp.py", "-i", str(entrada), "-o", str(salida)])
        cli.main()
        out = pd.read_parquet(salida)
        # Todas las filas se conservan, con sus marcas de versión
        assert out["id"].tolist() == ["u1", "u1", "u2", "u3", "c1"]
        assert out["es_ultima_version"].tolist() == [False, True, True, True, True]
        assert out["n_versiones"].tolist() == [2, 2, 1, 1, 1]
        assert out["procedimiento"].tolist()[:4] == ["Negociado sin publicidad", "Negociado sin publicidad",
                                                     "Contrato menor", "Derivado de acuerdo marco"]
        assert out["cpv_principal"].iloc[0] == "09134100"
        assert pq.ParquetFile(salida).schema_arrow.field("importe_sin_iva").type == pa.float64()
        salida_txt = capsys.readouterr().out
        assert "Filas (todas se conservan): 5" in salida_txt
        assert "Licitaciones distintas (es_ultima_version): 4" in salida_txt


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


def _zip_bytes(contenido="x" * 5000):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.atom", contenido)
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

    def test_generar_urls_anual_y_mensuales(self):
        hoy = datetime(2026, 3, 10)
        anos = lic.generar_urls_conjunto("licitaciones", 2024, 2030, hoy)
        assert [a["ano"] for a in anos] == [2024, 2025, 2026]  # no pide años futuros
        assert anos[-1]["anual"]["nombre"] == "licitacionesPerfilesContratanteCompleto3_2026.zip"
        assert anos[-1]["anual"]["url"].endswith("/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2026.zip")
        assert [m["nombre"][-10:] for m in anos[-1]["mensuales"]] == ["202601.zip", "202602.zip", "202603.zip"]
        assert len(anos[0]["mensuales"]) == 12
        assert lic.generar_urls_conjunto("encargos", 2012, 2023, hoy)[0]["ano"] == 2022

    def test_refresco_de_anuales_y_mensuales_por_mtime(self, tmp_path):
        hoy = datetime(2026, 9, 27)
        copia = tmp_path / "EMP_SectorPublico_2025.zip"
        copia.write_bytes(b"x")
        _mtime(copia, datetime(2025, 12, 15))
        assert lic.hay_que_refrescar(2025, None, copia, hoy)       # bajado antes de cerrarse 2025
        _mtime(copia, datetime(2026, 1, 2))
        assert not lic.hay_que_refrescar(2025, None, copia, hoy)
        assert lic.hay_que_refrescar(2026, None, copia, hoy)       # año en curso: siempre
        assert not lic.hay_que_refrescar(2024, None, tmp_path / "no_existe.zip", hoy)
        # Mensuales: abiertos hasta el fin del mes siguiente (como es_periodo_reciente)
        assert lic.cierre_periodo(2026, 3) == datetime(2026, 5, 1)
        assert lic.cierre_periodo(2025, 12) == datetime(2026, 2, 1)
        for mes in range(1, 13):
            assert (hoy < lic.cierre_periodo(2026, mes)) == lic.es_periodo_reciente(2026, mes, hoy)
        _mtime(copia, datetime(2026, 1, 20))
        assert lic.hay_que_refrescar(2026, 1, copia, hoy)
        _mtime(copia, datetime(2026, 3, 5))
        assert not lic.hay_que_refrescar(2026, 1, copia, hoy)

    def test_refresco_no_machaca_la_version_anterior(self, tmp_path):
        destino = tmp_path / "encargos" / "EMP_SectorPublico_2026.zip"
        destino.parent.mkdir()
        v1, v2 = _zip_bytes("v1" * 3000), _zip_bytes("v2" * 3000)
        destino.write_bytes(v1)
        os.utime(destino, (1_700_000_000, 1_700_000_000))

        ruta, estado = lic._descargar(_Session([_Resp([v1])]), "http://x", destino, forzar=True)
        assert (ruta, estado) == (destino, "sin_cambios")
        assert not (destino.parent / historico.HISTORICO).exists()
        assert destino.stat().st_mtime > 1_700_000_000  # comprobado ahora: ya no se vuelve a pedir

        ruta, estado = lic._descargar(_Session([_Resp([v2])]), "http://x", destino, forzar=True)
        assert estado == "actualizado"
        assert [p.read_bytes() for p in historico.versiones(destino)] == [v1, v2]
        assert not (destino.parent / "EMP_SectorPublico_2026.zip.part").exists()

    def test_anual_404_pasa_a_mensuales_e_informe(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(lic, "DATA_DIR", tmp_path)
        disponibles = {"CPM_SectorPublico_2022.zip", "CPM_SectorPublico_202302.zip"}

        class Sesion:
            def __init__(self):
                self.pedidos = []

            def get(self, url, timeout, stream):
                self.pedidos.append(url.rsplit("/", 1)[-1])
                return _Resp([_zip_bytes()]) if self.pedidos[-1] in disponibles else _Resp([], status=404)

        sesion, informe = Sesion(), []
        descargados = lic.descargar_conjunto(sesion, "consultas", 2020, 2024, informe)
        # 2022: el anual existe y no se piden sus mensuales; 2023 y 2024: anual 404 → mensuales
        assert sesion.pedidos[:2] == ["CPM_SectorPublico_2022.zip", "CPM_SectorPublico_2023.zip"]
        assert "CPM_SectorPublico_202201.zip" not in sesion.pedidos
        assert len(sesion.pedidos) == 1 + 13 + 13
        assert sorted(d["nombre"] for d in descargados) == sorted(disponibles)
        assert (tmp_path / "consultas" / "CPM_SectorPublico_202302.zip").exists()
        assert [r["ano"] for r in informe] == [2022, 2023, 2024]
        assert informe[1]["estados"]["2023 (anual)"] == "404"
        assert informe[1]["estados"]["202302"] == "nuevo"

        capsys.readouterr()
        lic.imprimir_informe_descarga(informe)
        salida = capsys.readouterr().out
        assert "consultas 2024: NINGÚN FICHERO DISPONIBLE" in salida
        assert "AÑOS SIN NINGÚN FICHERO: consultas 2024" in salida
        assert "consultas 2023: 1 ZIP disponibles; 404: 2023 (anual), 202301, 202303" in salida
        assert "consultas 2022" not in salida


def _mtime(ruta, momento):
    os.utime(ruta, (momento.timestamp(), momento.timestamp()))


def _texto_fixture(nombre):
    """Entrada de una fixture como texto, para meterla en un feed."""
    texto = (FIXTURES / nombre).read_text(encoding="utf-8")
    return texto[texto.index("<entry"):]


def _borrado_xml(ref, when, motivo="ANULADA"):
    return (f'<at:deleted-entry xmlns:at="http://purl.org/atompub/tombstones/1.0" ref="{ref}" when="{when}">'
            f'<at:comment type="{motivo}"/></at:deleted-entry>')


def _escribir_zip(ruta, atoms):
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(_zip_contenido(atoms))
    return ruta


def _zip_contenido(atoms):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for nombre, contenido in atoms.items():
            zf.writestr(nombre, contenido)
    return buf.getvalue()


T1 = "2025-01-10T10:00:00.000+01:00"
T2 = "2025-03-10T10:00:00+01:00"


class TestConsultasPreliminares:
    def test_parsea_la_consulta(self):
        r = _entry("entry_cpm.xml")
        assert r["tipo_registro"] == "CPM"
        assert r["id"].endswith("TEST-CPM-001")
        assert r["id_consulta"] == "CPM/2024/001"
        assert r["nombre_consulta"] == "Consulta sobre plataformas de telemedicina"
        assert r["condiciones"] == "Participación abierta a cualquier operador | Respuestas por correo electrónico"
        assert r["tipo_condicion"] == "1 | 2"
        assert r["fecha_planificada"] == "2024-09-01"
        assert r["fecha_limite_respuestas"] == "2024-06-15+02:00"
        assert r["estado_code"] == "PUB"
        assert r["organo_contratante"] == "Servicio de Salud de Pruebas"
        assert r["nif_organo"] == "Q2800000J"
        assert r["dependencia"] == "Consejería de Sanidad"
        assert r["objeto"] == "Plataforma de telemedicina"
        assert r["valor_estimado_contrato"] == 1500000.0
        assert r["cpv_principal"] == "48180000"
        assert r["fecha_publicacion"] == "2024-05-10"
        assert r["fecha_updated"] == "2024-05-10T12:00:00.000+02:00"
        assert r["url"].endswith("idEvl=CPM001")

    def test_campos_anidados_se_encuentran(self):
        # Estructura sin verificar con datos reales: los campos pueden ir dentro
        # de un agregado (aquí uno ficticio) y se buscan a cualquier profundidad
        xml = _atom(["""<entry><id>urn:cpm2</id><updated>2024-01-01T00:00:00Z</updated>
          <cac-place-ext:PreliminaryMarketConsultationStatus>
            <cac-place-ext:Agregado>
              <cbc:PreliminaryMarketConsultationID>CPM-2</cbc:PreliminaryMarketConsultationID>
              <cbc-place-ext:LimitDate>2024-02-01</cbc-place-ext:LimitDate>
              <cbc-place-ext:PreliminaryMarketConsultationStatusCode>CERR</cbc-place-ext:PreliminaryMarketConsultationStatusCode>
              <cac:ProcurementProject><cbc:Name>Objeto anidado</cbc:Name></cac:ProcurementProject>
            </cac-place-ext:Agregado>
          </cac-place-ext:PreliminaryMarketConsultationStatus></entry>"""])
        r = parsear_entry(ET.fromstring(xml).find("atom:entry", lic.NS))
        assert r["id_consulta"] == "CPM-2"
        assert r["fecha_limite_respuestas"] == "2024-02-01"
        assert r["estado_code"] == "CERR" and r["estado"] == "CERR"
        assert r["objeto"] == "Objeto anidado"

    def test_una_licitacion_no_lleva_campos_cpm(self):
        r = _entry("entry_lotes.xml")
        assert r["tipo_registro"] == "LICITACION"
        assert all(r[c] is None for c in lic.COLUMNAS_CPM)

    def test_el_conjunto_consultas_ya_no_sale_vacio(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        z = _escribir_zip(tmp_path / "CPM_SectorPublico_2024.zip",
                          {"cpm.atom": _atom([_texto_fixture("entry_cpm.xml")])})
        lics = lic.procesar_zip(z, "consultas")
        assert len(lics) == 1
        assert lics[0]["conjunto"] == "consultas" and lics[0]["tipo_registro"] == "CPM"
        df = lic.exportar_datos(lics, "prueba")
        fila = df.iloc[0]
        assert fila["fecha_limite_respuestas"] == pd.Timestamp("2024-06-15")
        assert fila["fecha_planificada"] == pd.Timestamp("2024-09-01")
        assert fila["ano"] == 2024
        assert pd.read_parquet(tmp_path / "prueba.parquet")["id_consulta"].tolist() == ["CPM/2024/001"]


class TestBorrados:
    def test_se_guardan_aparte_sin_tocar_la_tabla_principal(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        z = _escribir_zip(tmp_path / "licitacionesPerfilesContratanteCompleto3_2024.zip", {"a.atom": _atom([
            _entry_xml("urn:3001", "2024-01-01T00:00:00+01:00", "PUB"),
            _borrado_xml("urn:2999", "2024-02-01T10:00:00.000+01:00"),
            _borrado_xml("urn:2998", "2024-02-02T10:00:00+01:00", motivo="OTRO"),
        ])})
        borrados, informes = [], []
        lics = lic.procesar_zip(z, "licitaciones", borrados, informes)
        assert [x["id"] for x in lics] == ["urn:3001"]
        assert [(b["id"], b["motivo"]) for b in borrados] == [("urn:2999", "ANULADA"), ("urn:2998", "OTRO")]
        assert borrados[0]["archivo_origen"] == z.name and borrados[0]["conjunto"] == "licitaciones"
        assert informes[0]["entradas"] == 1 and informes[0]["borrados"] == 2

        df = lic.exportar_datos(lics, "prueba", borrados)
        assert df["id"].tolist() == ["urn:3001"]
        tabla = pd.read_parquet(tmp_path / "prueba_borrados.parquet")
        assert tabla["id"].tolist() == ["urn:2999", "urn:2998"]
        assert tabla["fecha_borrado"].iloc[0] == pd.Timestamp("2024-02-01T09:00:00", tz="UTC")
        assert tabla["fecha_borrado"].notna().all()
        assert tabla["entrada_repetida"].tolist() == [False, False]
        assert (tmp_path / "prueba_borrados.csv").exists()

    def test_se_vacian_al_leerlas(self):
        feed = ET.fromstring(_atom([_borrado_xml("urn:1", "2024-01-01T00:00:00Z")]))
        elem = list(feed)[0]
        borrados = []
        lic._leer_elementos([elem], [], borrados, lic.nuevo_informe())
        assert borrados == [{"id": "urn:1", "fecha_borrado": "2024-01-01T00:00:00Z",
                             "motivo": "ANULADA", "comentario": None}]
        assert len(elem) == 0 and not elem.attrib


class TestInformeProcesado:
    def test_cuenta_entradas_filas_y_descartes_por_motivo(self, tmp_path, monkeypatch, capsys):
        original = lic._parsear_status

        def falla(entry, status, es_cpm):
            if lic.safe_text(entry, "atom:id") == "urn:mala":
                raise ValueError("importe raro")
            return original(entry, status, es_cpm)

        monkeypatch.setattr(lic, "_parsear_status", falla)
        truncado = _atom([_entry_xml("urn:4002", T1, "PUB")]).replace("</feed>", "")
        z = _escribir_zip(tmp_path / "PlataformasAgregadasSinMenores_2023.zip", {
            "a.atom": _atom([_entry_xml("urn:4001", T1, "PUB"), _entry_xml("urn:mala", T1, "PUB"),
                             "<entry><id>urn:rara</id><cac-place-ext:OtraCosa/></entry>"]),
            "b.atom": truncado,
        })
        informes = []
        lics = lic.procesar_zip(z, "agregacion", informes=informes)
        assert sorted(x["id"] for x in lics) == ["urn:4001", "urn:4002"]  # lo leído antes del corte se conserva
        inf = informes[0]
        assert (inf["atom"], inf["entradas"], inf["filas"], inf["ano"]) == (2, 4, 2, 2023)
        assert inf["descartadas"] == {
            "error al parsear: ValueError: importe raro": 1,
            "sin ContractFolderStatus ni PreliminaryMarketConsultationStatus (contiene OtraCosa)": 1,
        }
        assert len(inf["errores"]) == 1 and "lectura interrumpida" in inf["errores"][0]

        vacio = _escribir_zip(tmp_path / "PlataformasAgregadasSinMenores_2024.zip", {"a.atom": _atom([])})
        lic.procesar_zip(vacio, "agregacion", informes=informes)
        capsys.readouterr()
        lic.imprimir_informe_procesado(informes, ["agregacion: ningún ZIP de 2022"])
        salida = capsys.readouterr().out
        assert "agregacion: 2 ZIP · 4 entradas → 2 filas · 0 borradas · 2 descartadas · 1 errores" in salida
        assert "1 entradas descartadas — error al parsear: ValueError: importe raro" in salida
        assert "(contiene OtraCosa)" in salida
        assert "lectura interrumpida" in salida
        assert "agregacion 2024: sus ZIP no dieron ninguna fila" in salida
        assert "ningún ZIP de 2022" in salida


# Columnas que ya generaba el scraper (v2026.02 y siguientes): no se mueven
COLUMNAS_ANTERIORES = [
    'id', 'expediente', 'objeto', 'organo_contratante', 'nif_organo', 'dir3_organo', 'id_plataforma',
    'ciudad_organo', 'dependencia', 'tipo_contrato_code', 'tipo_contrato', 'subtipo_code',
    'procedimiento_code', 'procedimiento', 'estado_code', 'estado', 'valor_estimado_contrato',
    'importe_sin_iva', 'importe_con_iva', 'importe_adjudicacion', 'importe_adj_con_iva', 'adjudicatario',
    'nif_adjudicatario', 'num_ofertas', 'es_pyme', 'n_lotes', 'n_resultados', 'cpv_principal', 'cpvs',
    'ubicacion', 'nuts', 'duracion', 'duracion_unidad', 'financiacion_ue', 'urgencia', 'fecha_limite',
    'hora_limite', 'fecha_adjudicacion', 'fecha_publicacion', 'fecha_updated', 'url', 'conjunto',
    'archivo_origen', 'tipo_registro', 'ano', 'n_versiones', 'es_ultima_version',
]
COLUMNAS_RESULTADOS_ANTERIORES = [
    'id', 'expediente', 'conjunto', 'lote', 'resultado_code', 'adjudicatario', 'nif_adjudicatario',
    'importe_adjudicacion', 'importe_adj_con_iva', 'fecha_adjudicacion', 'num_ofertas', 'es_pyme',
    'fecha_updated', 'es_ultima_version',
]


class TestDetalleCodice:
    def test_resultado_con_ute_y_contadores_de_ofertas(self):
        r = _entry("entry_detalle.xml")
        res = r["_resultados"][0]
        # Las columnas existentes siguen siendo el primer adjudicatario
        assert res["adjudicatario"] == "LIMPIEZAS UNO SL" and res["nif_adjudicatario"] == "B11111111"
        assert res["n_adjudicatarios"] == 2
        assert res["adjudicatarios_todos"] == "LIMPIEZAS UNO SL | SERVICIOS DOS SA"
        assert res["nifs_adjudicatarios_todos"] == "B11111111 | A22222222"
        assert res["descripcion_resultado"] == "Oferta con mejor relación calidad-precio"
        assert (res["oferta_mas_baja"], res["oferta_mas_alta"]) == (95000.0, 119000.0)
        assert res["num_ofertas"] == 5 and res["num_ofertas_pyme"] == 3
        assert json.loads(res["contadores_ofertas"]) == {"ReceivedTenderQuantity": "5",
                                                         "SMEsReceivedTenderQuantity": "3"}
        assert res["ofertas_anormalmente_bajas"] is True
        assert res["num_contrato"] == "CT-2025-001"
        assert res["fecha_formalizacion"] == "2025-06-20"
        assert res["fecha_inicio_contrato"] == "2025-07-01"
        assert res["orden_resultado"] == 1 and "_adjudicatarios" not in res
        desierto = r["_resultados"][1]
        assert desierto["n_adjudicatarios"] == 0 and desierto["adjudicatarios_todos"] is None
        assert [(a["lote"], a["orden_resultado"], a["orden_adjudicatario"], a["nif_adjudicatario"],
                 a["tipo_id_adjudicatario"]) for a in r["_adjudicatarios"]] == [
            ("1", 1, 1, "B11111111", "NIF"), ("1", 1, 2, "A22222222", "NIF")]

    def test_lotes_y_criterios(self):
        r = _entry("entry_detalle.xml")
        lote1, lote2 = r["_lotes"]
        assert (lote1["lote"], lote1["objeto_lote"]) == ("1", "Lote 1: edificios")
        assert (lote1["importe_sin_iva"], lote1["importe_con_iva"]) == (120000.0, 145200.0)
        assert lote1["valor_estimado_contrato"] is None
        assert (lote1["cpv_principal"], lote1["cpvs"]) == ("90911200", "90911200;90919200")
        assert (lote1["ubicacion"], lote1["nuts"], lote1["programas_financiacion"]) == ("Madrid", "ES300", "EU")
        assert lote2["importe_sin_iva"] == 80000.0 and lote2["cpvs"] is None
        criterios = [(c["lote"], c["tipo_criterio_code"], c["descripcion"], c["peso"]) for c in r["_criterios"]]
        assert criterios == [(None, "OBJ", "Precio", 60.0), (None, "SUBJ", "Memoria técnica", 40.0),
                             ("1", "OBJ", "Precio", 70.0)]
        assert r["_criterios"][0]["subtipo_criterio_code"] == "1"

    def test_proceso_terminos_y_documentos(self):
        r = _entry("entry_detalle.xml")
        assert r["sara"] is True
        assert (r["sistema_contratacion_code"], r["forma_presentacion_code"]) == ("0", "1")
        assert r["fecha_inicio_presentacion"] == "2025-03-01"
        assert (r["fecha_limite"], r["hora_limite"]) == ("2025-03-31", "23:59:00")
        assert (r["fecha_limite_solicitudes"], r["hora_limite_solicitudes"]) == ("2025-03-15", "12:00:00")
        assert (r["fecha_limite_pliegos"], r["hora_limite_pliegos"]) == ("2025-03-20", "14:00:00")
        # financiacion_ue sigue siendo el primero; programas_financiacion, todos
        assert r["financiacion_ue"] == "EU" and r["programas_financiacion"] == "EU;PRTR"
        assert r["pliego_administrativo"] == "PCAP.pdf"
        assert r["pliego_administrativo_url"] == "https://contrataciondelestado.es/doc/PCAP-001"
        assert r["pliego_tecnico_url"] == "https://contrataciondelestado.es/doc/PPT-001"
        # Cada URL en la posición de su documento (el Anexo I no tiene)
        assert r["otros_documentos"] == "Anexo I.pdf | Anexo II.pdf"
        assert r["otros_documentos_url"] == " | https://contrataciondelestado.es/doc/ANEXO2-001"

    def test_modificaciones(self):
        r = _entry("entry_detalle.xml")
        assert r["n_modificaciones"] == 1
        mod = r["_modificaciones"][0]
        assert (mod["id_contrato"], mod["nota"]) == ("CT-2025-001", "Ampliación del servicio")
        assert (mod["importe_modificacion_sin_iva"], mod["importe_final_sin_iva"]) == (10000.0, 110000.0)
        assert (mod["duracion_modificacion"], mod["duracion_modificacion_unidad"]) == ("3", "MON")
        assert (mod["duracion_final"], mod["duracion_final_unidad"]) == ("27", "MON")
        detalle = json.loads(mod["detalle"])
        assert detalle["FinalLegalMonetaryTotal/TaxExclusiveAmount"] == "110000"
        assert detalle["FinalLegalMonetaryTotal/TaxExclusiveAmount/@currencyID"] == "EUR"

    def test_exporta_tablas_nuevas_sin_mover_columnas(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        z = _escribir_zip(tmp_path / "licitacionesPerfilesContratanteCompleto3_2025.zip", {
            "a.atom": _atom([_texto_fixture("entry_detalle.xml"), _texto_fixture("entry_lotes.xml")])})
        lic.exportar_datos(lic.procesar_zip(z, "licitaciones"), "prueba")

        principal = pd.read_parquet(tmp_path / "prueba.parquet")
        assert list(principal.columns) == COLUMNAS_ANTERIORES + lic.COLUMNAS_NUEVAS
        assert principal["fecha_limite_pliegos"].iloc[0] == pd.Timestamp("2025-03-20")
        res = pd.read_parquet(tmp_path / "prueba_resultados.parquet")
        assert list(res.columns) == COLUMNAS_RESULTADOS_ANTERIORES + lic.COLUMNAS_NUEVAS_RESULTADOS
        assert res["fecha_formalizacion"].iloc[0] == pd.Timestamp("2025-06-20")
        assert res["n_adjudicatarios"].tolist() == [2, 0, 1, 1]
        for tabla, filas in [("adjudicatarios", 4), ("lotes", 4), ("criterios", 3), ("modificaciones", 1)]:
            t = pd.read_parquet(tmp_path / f"prueba_{tabla}.parquet")
            assert len(t) == filas, tabla
            assert list(t.columns[:3]) == ["id", "expediente", "conjunto"]
            assert list(t.columns[-3:]) == ["fecha_updated", "es_ultima_version", "entrada_repetida"]
            assert t["fecha_updated"].notna().all()
            assert (tmp_path / f"prueba_{tabla}.csv").exists()
        adj = pd.read_parquet(tmp_path / "prueba_adjudicatarios.parquet")
        assert adj["nif_adjudicatario"].tolist() == ["B11111111", "A22222222", "B11111111", "A22222222"]
        assert adj["expediente"].tolist()[:2] == ["TEST/2025/DETALLE"] * 2


class TestEntradasRepetidas:
    def test_marca_las_copias_y_cuenta_versiones_distintas(self):
        df = pd.DataFrame({
            "id": ["a", "a", "a", "b", "a", None, None],
            "fecha_updated": ["2024-01-01T10:00:00+01:00", "2024-01-01T09:00:00Z",   # el mismo instante
                              "2024-02-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00",
                              "2024-02-01T00:00:00.000+00:00",
                              "2024-01-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"],
        })
        out = marcar_versiones(df)
        assert len(out) == 7
        assert out["entrada_repetida"].tolist() == [False, True, False, False, True, False, False]
        assert out["n_versiones"].tolist() == [2, 2, 2, 1, 2, 1, 1]
        # Una fila por id y nunca una copia
        assert out["es_ultima_version"].tolist() == [False, False, True, True, False, True, True]
        ultima, n, repetida = lic.marcas_version(df["id"], df["fecha_updated"])
        assert lic.info_versiones(df["id"], df["fecha_updated"])[1].tolist() == n.tolist()

    def test_misma_entrada_en_dos_zip(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        entrada = _entry_xml("urn:7001", T1, "ADJ", importe_adj="100.00")
        lics = []
        for nombre in ["contratosMenoresPerfilesContratantes_202501.zip",
                       "contratosMenoresPerfilesContratantes_202502.zip"]:
            z = _escribir_zip(tmp_path / nombre, {"a.atom": _atom([entrada])})
            lics += lic.procesar_zip(z, "menores")
        df = lic.exportar_datos(lics, "prueba")
        assert len(df) == 2  # se conservan las dos
        assert df["entrada_repetida"].tolist() == [False, True]
        assert df["n_versiones"].tolist() == [1, 1]
        assert df["es_ultima_version"].tolist() == [True, False]
        res = pd.read_parquet(tmp_path / "prueba_resultados.parquet")
        assert res["entrada_repetida"].tolist() == [False, True]
        # Sumar la última versión no cuenta dos veces la misma adjudicación
        assert res.loc[res["es_ultima_version"], "importe_adjudicacion"].sum() == 100.0

    def test_leer_y_normalizar_marcan_repetidas(self, tmp_path, monkeypatch, capsys):
        # u1 (versión de junio) publicada dos veces
        df = pd.concat([_parquet_publicado(), _parquet_publicado().iloc[[1]]], ignore_index=True)
        esperado = dict(
            entrada_repetida=[False, False, False, False, False, True],
            n_versiones=[2, 2, 1, 1, 1, 2],
            es_ultima_version=[False, True, True, True, True, False],
        )
        out = normalizar_placsp(df)
        for col, valores in esperado.items():
            assert out[col].tolist() == valores, col

        entrada = tmp_path / "rep.parquet"
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), entrada, row_group_size=2)
        todas = leer_placsp(entrada)
        for col, valores in esperado.items():
            assert todas[col].tolist() == valores, col
        ultimas = leer_placsp(entrada, solo_ultima_version=True)
        assert ultimas["id"].tolist() == ["u1", "u2", "u3", "c1"]
        assert not ultimas["entrada_repetida"].any()
        assert ultimas["n_versiones"].tolist() == [2, 1, 1, 1]

        from nacional import normalizar_placsp as cli
        salida = tmp_path / "normalizado.parquet"
        monkeypatch.setattr(sys, "argv", ["normalizar_placsp.py", "-i", str(entrada), "-o", str(salida)])
        cli.main()
        normalizado = pd.read_parquet(salida)
        for col, valores in esperado.items():
            assert normalizado[col].tolist() == valores, col
        texto = capsys.readouterr().out
        assert "1 entradas repetidas" in texto
        assert "Filas (todas se conservan): 6" in texto
        assert "Licitaciones distintas (es_ultima_version): 4" in texto


class TestVersionesDeZip:
    """Sesgo del superviviente: se leen todas las versiones guardadas de cada ZIP."""

    def test_ninguna_entrada_publicada_se_pierde(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "DATA_DIR", tmp_path)
        monkeypatch.setattr(lic, "OUTPUT_DIR", tmp_path)
        actual = tmp_path / "encargos" / "EMP_SectorPublico_2025.zip"
        # v1 trae 5002, que la PLACSP ya no sirve en v2
        v1 = {"a.atom": _atom([_entry_xml("urn:5001", T1, "PUB"), _entry_xml("urn:5002", T1, "PUB")])}
        v2 = {"a.atom": _atom([_entry_xml("urn:5001", T1, "PUB"),
                               _entry_xml("urn:5001", T2, "ADJ", importe_adj="10.00")])}
        historico.guardar_version(actual, _zip_contenido(v1))
        _mtime(actual, datetime(2025, 6, 1, 10, tzinfo=timezone.utc))  # el sello de _historico/ va en UTC
        assert historico.guardar_version(actual, _zip_contenido(v2)) == "actualizado"
        # Una copia truncada en _historico/ no rompe el procesado
        (actual.parent / historico.HISTORICO / "EMP_SectorPublico_2025__20200101T000000Z.zip").write_bytes(b"PK")

        informes, avisos = [], []
        lics = lic.procesar_conjunto("encargos", 2025, 2025, informes=informes, avisos=avisos)
        df = lic.exportar_datos(lics, "prueba")
        assert df["id"].tolist() == ["urn:5001", "urn:5001", "urn:5001", "urn:5002"]
        # Primero la copia actual: sus filas son las canónicas
        assert df["zip_historico"].isna().tolist()[:2] == [True, True]  # nulo (None en pandas 2, NaN en pandas 3)
        assert df["zip_historico"].tolist()[2:] == ["EMP_SectorPublico_2025__20250601T100000Z.zip"] * 2
        assert df["archivo_origen"].unique().tolist() == ["EMP_SectorPublico_2025.zip"]
        assert df["entrada_repetida"].tolist() == [False, False, True, False]
        assert df["n_versiones"].tolist() == [2, 2, 2, 1]
        assert df["es_ultima_version"].tolist() == [False, True, False, True]
        assert [i["zip_historico"] for i in informes] == [None, "EMP_SectorPublico_2025__20250601T100000Z.zip"]
        assert any("20200101T000000Z.zip (en _historico/) no es un ZIP válido" in a for a in avisos)

    def test_anual_y_mensuales_del_mismo_ano_se_leen_todos(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic, "DATA_DIR", tmp_path)
        d = tmp_path / "licitaciones"
        p = "licitacionesPerfilesContratanteCompleto3_"
        _escribir_zip(d / f"{p}202501.zip", {"a.atom": _atom([_entry_xml("urn:6001", T1, "PUB"),
                                                               _entry_xml("urn:6002", T1, "PUB")])})
        _escribir_zip(d / f"{p}2025.zip", {"a.atom": _atom([_entry_xml("urn:6001", T1, "PUB")])})
        _escribir_zip(d / f"{p}2024.zip", {"a.atom": _atom([_entry_xml("urn:6000", T1, "PUB")])})
        _escribir_zip(d / f"{p}2023.zip", {"a.atom": _atom([])})
        assert [z.name for z in lic.seleccionar_zips(sorted(d.glob("*.zip")), 2024, 2025)] == [
            f"{p}2024.zip", f"{p}2025.zip", f"{p}202501.zip"]

        avisos = []
        df = pd.DataFrame(lic.procesar_conjunto("licitaciones", 2024, 2025, avisos=avisos))
        marcar_versiones(df)
        # El anual se lee antes: lo que está en ambos es copia en el mensual y
        # lo que solo trae el mensual no se pierde
        assert df["id"].tolist() == ["urn:6000", "urn:6001", "urn:6001", "urn:6002"]
        assert df["entrada_repetida"].tolist() == [False, False, True, False]
        assert df["archivo_origen"].tolist() == [f"{p}2024.zip", f"{p}2025.zip", f"{p}202501.zip", f"{p}202501.zip"]
        assert avisos == []

    def test_main_anos_por_defecto_y_aviso_de_anos_sin_zip(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setattr(lic, "DATA_DIR", lic.DATA_DIR)      # main los cambia: se restauran al final
        monkeypatch.setattr(lic, "OUTPUT_DIR", lic.OUTPUT_DIR)
        _escribir_zip(tmp_path / "zips" / "consultas" / "CPM_SectorPublico_2023.zip",
                      {"cpm.atom": _atom([_texto_fixture("entry_cpm.xml")])})
        monkeypatch.setattr(sys, "argv", ["licitaciones.py", "--solo-procesar", "--conjunto", "consultas",
                                          "--data-dir", str(tmp_path / "zips"), "--output-dir", str(tmp_path / "out")])
        lic.main()
        salida = capsys.readouterr().out
        ano = datetime.now().year
        assert f"Años: 2012 - {ano}" in salida
        faltan = ", ".join(str(a) for a in [2022] + list(range(2024, ano + 1)))
        assert f"consultas: ningún ZIP de {faltan}" in salida
        out = pd.read_parquet(tmp_path / "out" / f"licitaciones_completo_2012_{ano}.parquet")
        assert out["tipo_registro"].tolist() == ["CPM"] and out["conjunto"].tolist() == ["consultas"]


class TestRevisionAdversarial:
    def test_mismo_id_con_fecha_nula_no_es_entrada_repetida(self):
        ultima, n_versiones, repetida = lic.marcas_version(
            ["a", "a", "a", "b", "b"], [None, None, T1, T1, T1])
        assert repetida.tolist() == [False, False, False, False, True]
        assert n_versiones.tolist() == [3, 3, 3, 1, 1]
        assert ultima.tolist() == [False, False, True, True, False]

    def test_tipo_registro_del_conjunto_consultas_sigue_siendo_cpm(self, tmp_path):
        z = tmp_path / "CPM_SectorPublico_2025.zip"
        _escribir_zip(z, {"a.atom": _atom([_entry_xml("urn:7001", T1, "PUB"),
                                           _texto_fixture("entry_cpm.xml")])})
        lics = lic.procesar_zip(z, "consultas")
        assert [l["tipo_registro"] for l in lics] == ["CPM", "CPM"]
        z2 = tmp_path / "licitacionesPerfilesContratanteCompleto3_2025.zip"
        _escribir_zip(z2, {"a.atom": _atom([_entry_xml("urn:7002", T1, "PUB"),
                                            _texto_fixture("entry_cpm.xml")])})
        assert [l["tipo_registro"] for l in lic.procesar_zip(z2, "licitaciones")] == ["LICITACION", "CPM"]

    def test_leer_placsp_parquet_vacio(self, tmp_path):
        ruta = tmp_path / "vacio.parquet"
        pq.write_table(pa.table({"id": pa.array([], pa.string()),
                                 "fecha_updated": pa.array([], pa.string())}), ruta)
        assert len(lic.leer_placsp(ruta, solo_ultima_version=True)) == 0
        assert len(lic.leer_placsp(ruta)) == 0

    def test_respuesta_que_no_es_zip_no_sustituye_la_copia_actual(self, tmp_path, monkeypatch):
        monkeypatch.setattr(lic.time, "sleep", lambda s: None)
        destino = tmp_path / "licitacionesPerfilesContratanteCompleto3_2025.zip"
        bueno = _zip_contenido({"a.atom": _atom([_entry_xml("urn:8001", T1, "PUB")])})
        destino.write_bytes(bueno)

        class Resp:
            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                yield b"<html>mantenimiento</html>"

        class Sesion:
            def get(self, *a, **k):
                return Resp()

        ruta, estado = lic._descargar(Sesion(), "http://x", destino, max_reintentos=2, forzar=True)
        assert (ruta, estado) == (None, "error")
        assert destino.read_bytes() == bueno
        assert not (tmp_path / historico.HISTORICO).exists()
        assert not destino.with_name(destino.name + ".part").exists()
