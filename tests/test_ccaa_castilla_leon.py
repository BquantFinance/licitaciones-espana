"""Tests offline de scripts/ccaa_castilla_leon.py.

El portal OpenDataSoft de la Junta (catálogo y exports/csv) y el histórico del
perfil de contratante se simulan con un ``requests.get`` falso.
"""

import importlib.util
import json
import re
import runpy
import sys
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_castilla_leon.py"
SLEEP_REAL = time.sleep


def _cargar():
    spec = importlib.util.spec_from_file_location("ccaa_castilla_leon", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


C = _cargar()
API = "https://analisis.datosabiertos.jcyl.es/api/explore/v2.1"
PATRON_EXPORT = re.compile(re.escape(API) + r"/catalog/datasets/([^/]+)/exports/csv")


# ---------------------------------------------------------------------------
# Portal simulado
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, json_data=None, body=b"", headers=None):
        self.status_code = status
        self._json = json_data
        self._trozos = body if isinstance(body, list) else [body]
        self.headers = headers or {}

    def json(self):
        if self._json is None:
            raise ValueError("no es JSON")
        return self._json

    def iter_content(self, chunk_size=8192):
        for trozo in self._trozos:
            if isinstance(trozo, BaseException):
                raise trozo
            yield trozo

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakePortal:
    """catalogo: lista de fichas (o código HTTP); exports: id -> bytes | código |
    lista de respuestas sucesivas (bytes o códigos)."""

    def __init__(self):
        self.catalogo = []
        self.exports = {}
        self.historico = b"EXPEDIENTE;OBJETO\n0001/2015;Obra \xe2\x82\xac\n"
        self.llamadas = []

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        self.llamadas.append({"url": url, "params": dict(params or {}), "stream": stream})
        if url == C.URL_CATALOGO:
            if isinstance(self.catalogo, int):
                return FakeResponse(status=self.catalogo)
            ini, n = int(params["offset"]), int(params["limit"])
            return FakeResponse(json_data={"total_count": len(self.catalogo), "results": self.catalogo[ini:ini + n]})
        m = PATRON_EXPORT.fullmatch(url)
        if m:
            return self._respuesta(self.exports, m.group(1))
        if url == C.URL_HISTORICO:
            return self._respuesta({"h": self.historico}, "h")
        return FakeResponse(status=404, body=b"<html>404</html>")

    @staticmethod
    def _respuesta(tabla, clave):
        cuerpo = tabla.get(clave, 404)
        if isinstance(cuerpo, list):
            cuerpo = cuerpo.pop(0) if len(cuerpo) > 1 else cuerpo[0]
        if isinstance(cuerpo, int):
            return FakeResponse(status=cuerpo, body=b"<html>error</html>")
        return FakeResponse(body=cuerpo)

    def exportados(self):
        return [PATRON_EXPORT.fullmatch(c["url"]).group(1) for c in self.llamadas if PATRON_EXPORT.fullmatch(c["url"])]


def _ficha(ds, titulo="", modificado="2026-01-01T00:00:00+00:00", keyword=None):
    return {"dataset_id": ds, "metas": {"default": {"title": titulo, "keyword": keyword,
                                                    "modified": modificado, "data_processed": modificado}}}


@pytest.fixture
def portal(monkeypatch):
    fake = FakePortal()
    esperas = []
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(time, "sleep", esperas.append)
    monkeypatch.setattr(C, "DATASETS_CONOCIDOS", {})
    fake.esperas = esperas
    return fake


def _ejecutar(salida, *args):
    return C.main(["--salida", str(salida), *args])


def _parquet(salida, nombre):
    return pd.read_parquet(salida / nombre)


def _v(serie):
    """Valores de una columna con los nulos como None (pandas 3 los lee como NaN)."""
    return [None if pd.isna(v) else v for v in serie]


def _estado(df, columna):
    return {v: bool(e) for v, e in zip(df[columna], df["_en_ultima_descarga"])}


CSV_V1 = "﻿Expediente;Adjudicatario NIF;Importe;Observaciones\n00123;B0012345;1.234,56;NA\nA-2;;10;\nC-3;X;30;N/A\n"


# ---------------------------------------------------------------------------
# Catálogo
# ---------------------------------------------------------------------------

def test_catalogo_se_recorre_entero_y_se_eligen_los_de_contratacion(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(C, "LIMITE_PAGINA", 2)
    monkeypatch.setattr(C, "DATASETS_CONOCIDOS", {"contratos-ordinarios": "Contratos ordinarios"})
    portal.catalogo = [
        _ficha("calidad-del-aire", "Calidad del aire"),
        _ficha("contratos-menores", "Contratos menores"),
        _ficha("contratos-ordinarios", "Contratos ordinarios"),
        _ficha("perfil-anuncios", "Anuncios", keyword=["Licitaciones"]),
        _ficha("adjudicaciones-sacyl", "Adjudicaciones de SACYL"),
    ]
    portal.exports = {ds: "a;b\n1;2\n".encode() for ds in
                      ("contratos-menores", "contratos-ordinarios", "perfil-anuncios", "adjudicaciones-sacyl")}

    assert _ejecutar(tmp_path) == 0

    paginas = [c["params"] for c in portal.llamadas if c["url"] == C.URL_CATALOGO]
    assert [p["offset"] for p in paginas] == [0, 2, 4]
    assert all(p["order_by"] == "dataset_id" and p["limit"] == 2 for p in paginas)
    assert sorted(portal.exportados()) == ["adjudicaciones-sacyl", "contratos-menores", "contratos-ordinarios",
                                           "perfil-anuncios"]
    assert all(c["params"] == C.PARAMS_EXPORT and c["stream"] for c in portal.llamadas
               if PATRON_EXPORT.fullmatch(c["url"]))
    assert sorted(p.name for p in tmp_path.glob("*.parquet")) == [
        "adjudicaciones-sacyl.parquet", "contratos-menores.parquet", "contratos-ordinarios.parquet",
        "licitaciones_perfil_historico.parquet", "perfil-anuncios.parquet"]
    catalogo = json.loads((tmp_path / "raw" / "catalogo.json").read_text(encoding="utf-8"))
    assert len(catalogo) == 5
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "descubiertos en el catálogo" in log and "perfil-anuncios" in log


def test_catalogo_caido_usa_la_lista_conocida_y_sale_con_error(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(C, "DATASETS_CONOCIDOS", {"contratos-menores": "", "contratos-inventado": ""})
    portal.catalogo = 503
    portal.exports = {"contratos-menores": b"a;b\n1;2\n"}

    assert _ejecutar(tmp_path) == 1

    assert sorted(portal.exportados()) == ["contratos-inventado", "contratos-menores"]
    assert (tmp_path / "contratos-menores.parquet").exists()
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "catálogo" in log and "HTTP 503 (tras 5 intentos)" in log
    assert "ids conocidos que no existen en el portal: contratos-inventado" in log


# ---------------------------------------------------------------------------
# Descarga tal cual y Parquet como texto
# ---------------------------------------------------------------------------

def test_original_tal_cual_y_parquet_con_todas_las_columnas_como_texto(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": CSV_V1.encode("utf-8")}

    assert _ejecutar(tmp_path) == 0

    raw = tmp_path / "raw" / "datasets" / "contratos-menores.csv"
    assert raw.read_bytes() == CSV_V1.encode("utf-8")          # sin tocar (BOM incluido)
    tabla = pq.read_table(tmp_path / "contratos-menores.parquet")
    assert all(pa.types.is_string(f.type) for f in tabla.schema if f.name != "_en_ultima_descarga")
    df = tabla.to_pandas()
    assert list(df.columns[:4]) == ["Expediente", "Adjudicatario NIF", "Importe", "Observaciones"]
    assert df["Expediente"].tolist() == ["00123", "A-2", "C-3"]           # ceros a la izquierda
    assert _v(df["Adjudicatario NIF"])[:2] == ["B0012345", None]
    assert df["Importe"].tolist() == ["1.234,56", "10", "30"]              # sin convertir
    assert _v(df["Observaciones"]) == ["NA", None, "N/A"]             # 'NA' no es nulo
    assert set(df["_dataset"]) == {"contratos-menores"}
    assert set(df["_archivo_origen"]) == {"datasets/contratos-menores.csv"}
    assert df["_fuente"].iloc[0].startswith(API + "/catalog/datasets/contratos-menores/exports/csv?")
    assert "use_labels=true" in df["_fuente"].iloc[0]
    assert df["_en_ultima_descarga"].tolist() == [True, True, True]
    assert (df["_primera_descarga"] == df["_fecha_descarga"]).all()
    assert json.loads((tmp_path / "raw" / "datasets" / "contratos-menores.metadatos.json")
                      .read_text(encoding="utf-8"))["dataset_id"] == "contratos-menores"


def test_filas_con_campos_de_mas_no_se_pierden(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_bytes("id;objeto\n1;uno\n2;dos;sobra\n3;tres\n".encode("utf-8"))
    df, avisos = C.leer_tabla(csv)
    assert df["id"].tolist() == ["1", "2", "3"]
    assert _v(df["_columna_extra_1"]) == [None, "sobra", None]
    assert any("1 filas con más campos" in a for a in avisos)


def test_comillas_desparejadas_no_se_tragan_registros(tmp_path):
    """Una comilla que no se cierra nunca es literal: no se traga el resto del
    fichero en un campo (antes quedaba una fila y un aviso de revisar)."""
    csv = tmp_path / "x.csv"
    csv.write_bytes(b'id;objeto\n1;"Obra sin cerrar\n2;dos\n3;tres\n')
    df, avisos = C.leer_tabla(csv)
    assert df.values.tolist() == [["1", '"Obra sin cerrar'], ["2", "dos"], ["3", "tres"]]
    assert any("comillas literales" in a for a in avisos)


def test_comilla_literal_que_se_tragaria_registros_reales(tmp_path):
    """Caso real de contratos-menores.csv (2026): un título que empieza por
    comilla se tragaba las líneas siguientes, que además acaban en ';'."""
    cabecera = ";".join(f"c{i}" for i in range(14))
    lineas = [cabecera,
              'B2021/015285;"ACONDICIONAMIENTO DE CAMINO (ZAMORA);Consejería;Contrato Menor;Obras;45.572,23;3 ;'
              '45.572,23;20/10/2021;OBRAS SL;B1;2 ;0 ;https://a;',
              'B2021/015287;SUSTITUCIÓN CENTRAL;Delegación;Contrato Menor;Obras;1.804,00;0 ;1.804,00;22/10/2021;'
              'VIGILANTES SL;B2;1 ;0 ;https://b;',
              'B2021/015356;LA SEMILLA DE LA LOCURA""-ANA RONCERO";Delegación;Contrato Menor;Servicios;1149.5;0;'
              '1149.5;2021-10-04;ANA;111;1;0;https://c']
    csv = tmp_path / "menores.csv"
    csv.write_bytes(("\n".join(lineas) + "\n").encode("utf-8"))
    df, avisos = C.leer_tabla(csv)
    assert df["c0"].tolist() == ["B2021/015285", "B2021/015287", "B2021/015356"]
    assert df["c1"].tolist() == ['"ACONDICIONAMIENTO DE CAMINO (ZAMORA)', "SUSTITUCIÓN CENTRAL",
                                 'LA SEMILLA DE LA LOCURA""-ANA RONCERO"']
    assert df["c13"].tolist() == ["https://a", "https://b", "https://c"]
    assert any("se tragaban 2 registros" in a for a in avisos)


def test_cp1252_y_separador_coma(tmp_path):
    csv = tmp_path / "h.csv"
    csv.write_bytes("EXPEDIENTE,OBJETO\n0001/2015,\"Obra – 5.000 €, fase 2\"\n".encode("cp1252"))
    df, _ = C.leer_tabla(csv)
    assert df.iloc[0].tolist() == ["0001/2015", "Obra – 5.000 €, fase 2"]


def test_historico_del_perfil(portal, tmp_path):
    portal.historico = "EXPEDIENTE;OBJETO;IMPORTE\n0001/2015;Obra “Puente” – 5.000 €;05000\n".encode("cp1252")
    assert _ejecutar(tmp_path) == 0
    df = _parquet(tmp_path, C.PARQUET_HISTORICO)
    assert df["OBJETO"].tolist() == ["Obra “Puente” – 5.000 €"]
    assert df["IMPORTE"].tolist() == ["05000"]
    assert set(df["_dataset"]) == {"historico-perfil-contratante"}
    assert (tmp_path / "raw" / "historico" / "1284165771488.csv").exists()

    # Fichero estático: no se vuelve a pedir salvo con --comprobar-todo
    antes = len(portal.llamadas)
    _ejecutar(tmp_path)
    assert not any(c["url"] == C.URL_HISTORICO for c in portal.llamadas[antes:])
    _ejecutar(tmp_path, "--comprobar-todo")
    assert any(c["url"] == C.URL_HISTORICO for c in portal.llamadas[antes:])


# ---------------------------------------------------------------------------
# Reintentos y fallos
# ---------------------------------------------------------------------------

def test_reintentos_con_backoff_y_404_sin_reintentar(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": [503, 502, b"a;b\n1;2\n"]}

    assert _ejecutar(tmp_path) == 0
    assert portal.exportados() == ["contratos-menores"] * 3
    assert [s for s in portal.esperas if s >= 1] == [2.0, 4.0]

    estado, detalle = C.descargar(API + "/catalog/datasets/no-existe/exports/csv", tmp_path / "n.csv", tipo="csv")
    assert (estado, detalle) == ("no_existe", "HTTP 404")
    assert portal.exportados().count("no-existe") == 1


def test_corte_a_mitad_se_reintenta_y_no_deja_parcial(portal, tmp_path, monkeypatch):
    destino = tmp_path / "d.csv"
    respuestas = [FakeResponse(body=[b"a;b\n1;", requests.exceptions.ChunkedEncodingError("corte")]),
                  FakeResponse(body=[b"a;b\n1;", b"2\n"])]
    monkeypatch.setattr(requests, "get", lambda *a, **k: respuestas.pop(0))
    assert C.descargar("http://x/d.csv", destino, tipo="csv") == ("nuevo", "")
    assert destino.read_bytes() == b"a;b\n1;2\n"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["d.csv"]


def test_pagina_html_con_200_se_rechaza(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": b"<!DOCTYPE html><html><body>Mantenimiento</body></html>"}
    assert _ejecutar(tmp_path) == 1
    assert not (tmp_path / "raw" / "datasets" / "contratos-menores.csv").exists()
    assert "la respuesta es HTML" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Sesgo del superviviente: nada de lo visto se pierde
# ---------------------------------------------------------------------------

def _dos_ejecuciones(portal, salida, csv1, csv2, modificado2="2099-01-01T00:00:00+00:00"):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": csv1}
    r1 = _ejecutar(salida)
    SLEEP_REAL(1.1)          # la versión nueva tiene que llevar otra fecha (resolución: 1 s)
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores", modificado=modificado2)]
    portal.exports = {"contratos-menores": csv2}
    r2 = _ejecutar(salida)
    return r1, r2


def test_registro_retirado_y_modificado_se_conservan(portal, tmp_path):
    v1 = b"id;importe\nA;10\nB;20\nC;30\n"
    v2 = b"id;importe\nA;10\nC;35\n"                  # retira B y cambia C
    assert _dos_ejecuciones(portal, tmp_path, v1, v2) == (0, 0)

    df = _parquet(tmp_path, "contratos-menores.parquet")
    assert df[["id", "importe", "_en_ultima_descarga"]].values.tolist() == [
        ["A", "10", True], ["B", "20", False], ["C", "30", False], ["C", "35", True]]
    assert df.loc[df["id"] == "A", "_primera_descarga"].iloc[0] < df.loc[df["id"] == "A", "_ultima_descarga"].iloc[0]
    # La capa cruda conserva las dos versiones
    raw = tmp_path / "raw" / "datasets" / "contratos-menores.csv"
    assert [p.read_bytes() for p in C.versiones(raw)] == [v1, v2]
    # El Parquet anterior también se conserva
    assert len(C.versiones(tmp_path / "contratos-menores.parquet")) == 2

    # Una tercera ejecución sin cambios no duplica nada y actualiza _ultima_descarga
    SLEEP_REAL(1.1)
    assert _ejecutar(tmp_path, "--comprobar-todo") == 0
    df3 = _parquet(tmp_path, "contratos-menores.parquet")
    assert len(df3) == 4 and df3["_en_ultima_descarga"].tolist() == [True, False, False, True]
    assert df3.loc[0, "_ultima_descarga"] > df.loc[0, "_ultima_descarga"]


def test_parquet_se_reconstruye_desde_todas_las_versiones_crudas(portal, tmp_path):
    v1 = b"id;importe\nA;10\nB;20\n"
    v2 = b"id;importe\nA;10\n"
    _dos_ejecuciones(portal, tmp_path, v1, v2)
    parquet = tmp_path / "contratos-menores.parquet"
    esperado = _parquet(tmp_path, "contratos-menores.parquet")
    for p in C.versiones(parquet):     # sin Parquet previo: solo con raw/
        p.unlink()
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    pd.testing.assert_frame_equal(_parquet(tmp_path, "contratos-menores.parquet"), esperado)


def test_descarga_fallida_no_pierde_nada(portal, tmp_path):
    v1 = b"id;importe\nA;10\nB;20\n"
    r1, r2 = _dos_ejecuciones(portal, tmp_path, v1, 503)
    assert (r1, r2) == (0, 1)
    assert (tmp_path / "raw" / "datasets" / "contratos-menores.csv").read_bytes() == v1
    df = _parquet(tmp_path, "contratos-menores.parquet")
    assert _estado(df, "id") == {"A": True, "B": True}
    assert "contratos-menores: HTTP 503 (tras 5 intentos)" in (tmp_path / "raw" / "descarga_log.txt").read_text(
        encoding="utf-8")


def test_descarga_vacia_no_retira_nada(portal, tmp_path):
    v1 = b"id;importe\nA;10\nB;20\n"
    assert _dos_ejecuciones(portal, tmp_path, v1, b"id;importe\n") == (0, 0)
    df = _parquet(tmp_path, "contratos-menores.parquet")
    assert _estado(df, "id") == {"A": True, "B": True}
    raw = tmp_path / "raw" / "datasets" / "contratos-menores.csv"
    assert [p.read_bytes() for p in C.versiones(raw)] == [v1, b"id;importe\n"]
    assert "no tiene filas" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_duplicados_de_origen_se_conservan(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": b"id;importe\nA;10\nA;10\nB;5\n"}
    assert _ejecutar(tmp_path) == 0
    assert _parquet(tmp_path, "contratos-menores.parquet")["id"].tolist() == ["A", "A", "B"]


def test_sin_cambios_en_el_catalogo_no_se_vuelve_a_pedir(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores", modificado="2020-01-01T00:00:00Z")]
    portal.exports = {"contratos-menores": b"a;b\n1;2\n"}
    _ejecutar(tmp_path)
    _ejecutar(tmp_path)
    assert portal.exportados() == ["contratos-menores"]
    _ejecutar(tmp_path, "--comprobar-todo")
    assert portal.exportados() == ["contratos-menores"] * 2
    assert not (tmp_path / "raw" / "datasets" / "_historico").exists()   # contenido idéntico


def test_dataset_que_desaparece_del_portal_se_marca_retirado(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-emergencia-covid", "Contratos de emergencia COVID")]
    portal.exports = {"contratos-emergencia-covid": b"id;importe\nA;10\n"}
    assert _ejecutar(tmp_path) == 0
    SLEEP_REAL(1.1)
    portal.catalogo, portal.exports = [], {}
    assert _ejecutar(tmp_path) == 0
    assert portal.exportados() == ["contratos-emergencia-covid"] * 2
    df = _parquet(tmp_path, "contratos-emergencia-covid.parquet")
    assert df["id"].tolist() == ["A"] and df["_en_ultima_descarga"].tolist() == [False]
    assert (tmp_path / "raw" / "datasets" / "contratos-emergencia-covid.csv").exists()
    assert "RETIRADOS" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli(portal, tmp_path, monkeypatch):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": b"a;b\n1;2\n"}
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--salida", str(tmp_path)])
    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert salida.value.code == 0
    assert (tmp_path / "contratos-menores.parquet").exists()


def test_solo_descarga_no_genera_parquet(portal, tmp_path):
    portal.catalogo = [_ficha("contratos-menores", "Contratos menores")]
    portal.exports = {"contratos-menores": b"a;b\n1;2\n"}
    assert _ejecutar(tmp_path, "--solo-descarga") == 0
    assert not list(tmp_path.glob("*.parquet"))
    assert _ejecutar(tmp_path, "--solo-parquet") == 0
    assert (tmp_path / "contratos-menores.parquet").exists()


def test_salida_por_defecto_en_el_repo():
    assert C.SALIDA == REPO_ROOT / "ccaa_castilla_leon"


def test_historico_sin_datos_no_crea_una_tabla_falsa(portal, tmp_path):
    """El portal sirve un aviso en vez del CSV cuando no hay datos (verificado
    en vivo en 2026): no es una tabla de 1 fila ni un fallo."""
    portal.historico = ("Fichero actualizado a fecha: 2026-09-27 18:05:09\n\n"
                        "No existen datos asociados a este dataset\n").encode("utf-8")
    assert _ejecutar(tmp_path) == 0
    assert not (tmp_path / C.PARQUET_HISTORICO).exists()
    assert not (tmp_path / "raw" / "historico" / "1284165771488.csv").exists()
