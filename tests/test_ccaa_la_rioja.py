"""Tests offline de scripts/ccaa_la_rioja.py.

El servidor de descargas de datos abiertos de La Rioja
(ias1.larioja.org/opendata/download?r=base64("cd=N|cf=03")) se simula con un
``requests.get`` y un ``requests.head`` falsos; los CSV (ISO-8859-15, ';' y
CRLF, como los reales) se generan en el propio test.
"""

import base64
import importlib.util
import json
import runpy
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_la_rioja.py"
SLEEP_REAL = time.sleep


def _cargar():
    spec = importlib.util.spec_from_file_location("ccaa_la_rioja", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
REALES = dict(M.CODIGOS_CONOCIDOS)
ANIO = M.ahora().year          # el script decide qué refrescar con el año real
# Códigos de prueba: dos años cerrados, el anterior y el año en curso
CONOCIDOS = {ANIO - 3: 367, ANIO - 2: 406, ANIO - 1: 979, ANIO: 1151}
CABECERA = "COD_CONTRATO;DEPARTAMENTO;TIPO_EXPEDIENTE;TERC_CIF;TERC_NOMBRE;CONCEPTO;FECHA;IMPORTE_EJERCICIO"


def nombre(anio):
    return f"contratos_CAR_{anio}.csv"


def csv_rioja(anio, filas=None, cabecera=CABECERA):
    """CSV como los del portal: ISO-8859-15, separador ';' y CRLF."""
    if filas is None:
        filas = [f"{anio}.GG.15.0000001.0000001;SERVICIO RIOJANO DE SALUD;CONTRATO DE SUMINISTRO;B26435461;"
                 f"SUMINISTROS RIOJA, S.L.;Material sanitario;{anio}/03/01 00:00:00.000;1234,56"]
    return ("\r\n".join([cabecera] + filas) + "\r\n").encode("iso-8859-15")


# ---------------------------------------------------------------------------
# Servidor de descargas simulado
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size=8192):
        yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakePortal:
    """codigos: cd -> (nombre publicado, bytes) | código HTTP (lo que no está
    da 404). fallos_head: cd -> código HTTP que da solo el HEAD."""

    def __init__(self):
        self.codigos = {}
        self.fallos_head = {}
        self.llamadas = []

    @staticmethod
    def cd(url):
        texto = base64.b64decode(parse_qs(urlparse(url).query)["r"][0]).decode("ascii")
        campos = dict(parte.split("=") for parte in texto.split("|"))
        assert campos["cf"] == "03"
        return int(campos["cd"])

    def _responder(self, metodo, url, params):
        assert not params
        cd = self.cd(url)
        self.llamadas.append((metodo, cd))
        valor = self.fallos_head.get(cd) if metodo == "HEAD" else None
        valor = valor or self.codigos.get(cd, 404)
        if isinstance(valor, int):
            return FakeResponse(status=valor, body=b"<html><body>Error</body></html>")
        publicado, cuerpo = valor
        cabeceras = {"Content-Disposition": f'attachment; filename="{publicado}"', "Content-Type": "text/csv"}
        return FakeResponse(body=b"" if metodo == "HEAD" else cuerpo, headers=cabeceras)

    def get(self, url, params=None, headers=None, timeout=None, stream=False):
        return self._responder("GET", url, params)

    def head(self, url, params=None, headers=None, timeout=None, allow_redirects=True):
        return self._responder("HEAD", url, params)

    def pedidas(self, metodo, cd):
        return self.llamadas.count((metodo, cd))


@pytest.fixture
def portal(monkeypatch):
    fake = FakePortal()
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(requests, "head", fake.head)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    monkeypatch.setattr(M, "CODIGOS_CONOCIDOS", dict(CONOCIDOS))
    monkeypatch.setattr(M, "SONDEO_SEGUIDOS", 3)
    monkeypatch.setattr(M, "SONDEO_PASO", 5)
    monkeypatch.setattr(M, "SONDEO_ALCANCE", 40)
    for anio, cd in CONOCIDOS.items():
        fake.codigos[cd] = (nombre(anio), csv_rioja(anio))
    return fake


def _ejecutar(salida, *args):
    return M.main(["--salida", str(salida), *args])


def _log(salida):
    return (salida / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def _parquet(salida):
    return pd.read_parquet(salida / "contratos_menores.parquet")


def _raw(salida, cd, anio):
    return salida / "raw" / "contratos_menores" / f"cd{cd}" / nombre(anio)


def _manifiesto(salida):
    return json.loads((salida / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# URL y cabeceras del servidor
# ---------------------------------------------------------------------------

def test_url_de_descarga_como_la_del_catalogo():
    # Las que enlazan el catálogo y datos.gob.es: 2024 en CSV y 2021 en XLS
    assert M.url_codigo(979) == "https://ias1.larioja.org/opendata/download?r=Y2Q9OTc5fGNmPTAz"
    assert M.url_codigo(866, "01") == "https://ias1.larioja.org/opendata/download?r=Y2Q9ODY2fGNmPTAx"
    # Con el relleno '==' tal cual (verificado en vivo con 2026)
    assert M.url_codigo(1175) == "https://ias1.larioja.org/opendata/download?r=Y2Q9MTE3NXxjZj0wMw=="


@pytest.mark.parametrize("cabeceras, esperado", [
    ({"Content-Disposition": 'attachment; filename="contratos_CAR_2024.csv"'}, "contratos_CAR_2024.csv"),
    ({"content-disposition": "attachment; filename=contratos_CAR_2024.csv"}, "contratos_CAR_2024.csv"),
    ({"Content-Disposition": "attachment; filename*=UTF-8''contratos%20CAR.csv"}, "contratos CAR.csv"),
    ({"Content-Disposition": "attachment"}, None),
    ({}, None),
])
def test_nombre_publicado_de_content_disposition(cabeceras, esperado):
    assert M.nombre_publicado(cabeceras) == esperado


# ---------------------------------------------------------------------------
# Descarga y Parquet
# ---------------------------------------------------------------------------

def test_descarga_los_anios_y_genera_el_parquet_como_texto(portal, tmp_path):
    # Filas reales de 2024: NIF enmascarado, ';' y comillas dentro de campos
    # entrecomillados, importes con coma y negativos
    filas = ['2024.GG.07.0000497.0000001;CULTURA, TURISMO, DEPORTE Y JUVENTUD;CONTRATO DE SERVICIOS;***6651**;'
             'GONZALEZ GONZALEZ, ROI;"CM-CULT-Actuacion Roi Borrallas;""Solo""- Festival Teatrea";'
             '2024/04/08 00:00:00.000;2783',
             '2021.GG.17.0000017.0000002;INSTITUTO DE ESTUDIOS RIOJANOS;CONTRATO DE SUMINISTRO;B88454624;'
             'SOLFIX ENGINEERING SL;COVID-19_Lote 4. MASCARILLAS FFP2;2021/03/30 00:00:00.000;-266,2',
             '2024.GG.06.0000536.0000001;SALUD Y POLÍTICAS SOCIALES;CONTRATO DE SUMINISTRO;S2633001I;'
             '"CENTRO INFANTIL ""LA COMETA""";Gastos material escolar menor La Cometa.;2024/02/23 00:00:00.000;25']
    portal.codigos[979] = (nombre(ANIO - 1), csv_rioja(ANIO - 1, filas))

    assert _ejecutar(tmp_path) == 0

    tabla = pq.read_table(tmp_path / "contratos_menores.parquet")
    assert all(pa.types.is_string(f.type) for f in tabla.schema if f.name != "_en_ultima_descarga")
    df = tabla.to_pandas()
    assert list(df.columns[:8]) == list(M.COLUMNAS)
    assert df["_anio_fichero"].tolist() == [str(a) for a in (ANIO - 3, ANIO - 2, ANIO - 1, ANIO - 1, ANIO - 1, ANIO)]
    anterior = df[df["_anio_fichero"] == str(ANIO - 1)]
    assert anterior["TERC_CIF"].tolist() == ["***6651**", "B88454624", "S2633001I"]
    assert anterior["CONCEPTO"].iloc[0] == 'CM-CULT-Actuacion Roi Borrallas;"Solo"- Festival Teatrea'
    assert anterior["TERC_NOMBRE"].iloc[2] == 'CENTRO INFANTIL "LA COMETA"'
    assert anterior["DEPARTAMENTO"].iloc[2] == "SALUD Y POLÍTICAS SOCIALES"
    assert anterior["IMPORTE_EJERCICIO"].tolist() == ["2783", "-266,2", "25"]
    assert anterior["FECHA"].iloc[0] == "2024/04/08 00:00:00.000"
    assert set(anterior["_fuente"]) == {M.url_codigo(979)}
    assert set(anterior["_recurso"]) == {"opd-979"}
    assert set(anterior["_archivo_origen"]) == {f"contratos_menores/cd979/{nombre(ANIO - 1)}"}
    assert df["_en_ultima_descarga"].all()
    # Originales tal cual y su procedencia
    assert _raw(tmp_path, 979, ANIO - 1).read_bytes() == csv_rioja(ANIO - 1, filas)
    entrada = _manifiesto(tmp_path)[f"contratos_menores/cd979/{nombre(ANIO - 1)}"]
    assert (entrada["cd"], entrada["anio"], entrada["url"]) == (979, ANIO - 1, M.url_codigo(979))


def test_csv_en_iso_8859_15(portal, tmp_path):
    # '€' (0xA4) y 'Ž' (0xB4) solo son eso en latin-9: cp1252 y latin-1 darían '¤' y '´'
    filas = [f"{ANIO}.GG.05.0000001.0000001;AGRICULTURA, GANADERÍA;CONTRATO DE SERVICIOS;A26019992;"
             f"PEÑA Ž, S.A.;Canon de 100 € de L?AUDE;{ANIO}/01/02 00:00:00.000;100"]
    portal.codigos[1151] = (nombre(ANIO), csv_rioja(ANIO, filas))
    assert b"\xa4" in portal.codigos[1151][1] and b"\xb4" in portal.codigos[1151][1]

    assert _ejecutar(tmp_path, "--sin-sondeo") == 0

    df = _parquet(tmp_path)
    fila = df[df["_anio_fichero"] == str(ANIO)].iloc[0]
    assert fila["TERC_NOMBRE"] == "PEÑA Ž, S.A."
    assert fila["CONCEPTO"] == "Canon de 100 € de L?AUDE"
    assert fila["DEPARTAMENTO"] == "AGRICULTURA, GANADERÍA"


def test_comilla_literal_no_se_traga_registros(tmp_path):
    """Una comilla al principio de un campo que no se cierra hasta varias
    líneas después (registros enteros) es literal: se conservan todas las
    filas y la comilla queda en el texto (comun.lectura_csv)."""
    ruta = tmp_path / nombre(2024)
    ruta.write_bytes(csv_rioja(2024, ['1;"Obra A;org;B1;x;c;f;10', '2;Obra B;org;B2;y;c;f;20',
                                      '3;Obra "C";org;B3;z;c;f;30']))
    df, avisos = M.leer_tabla(ruta)
    assert df["COD_CONTRATO"].tolist() == ["1", "2", "3"]
    assert df["DEPARTAMENTO"].tolist() == ['"Obra A', "Obra B", 'Obra "C"']
    assert any("comillas literales" in a for a in avisos)


def test_columnas_distintas_de_las_conocidas_se_avisan_y_se_conservan(portal, tmp_path):
    portal.codigos[1151] = (nombre(ANIO), csv_rioja(ANIO, [f"C1;D;T;B1;N;X;{ANIO}/01/02 00:00:00.000;1;LR"],
                                                    cabecera=CABECERA + ";MUNICIPIO"))
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0
    assert "columnas distintas de las conocidas" in _log(tmp_path)
    assert _parquet(tmp_path).dropna(subset=["MUNICIPIO"])["MUNICIPIO"].tolist() == ["LR"]


def test_solo_se_vuelven_a_pedir_el_anio_actual_y_el_anterior(portal, tmp_path):
    _ejecutar(tmp_path, "--sin-sondeo")
    _ejecutar(tmp_path, "--sin-sondeo")
    assert [portal.pedidas("GET", cd) for cd in (367, 406, 979, 1151)] == [1, 1, 2, 2]
    assert "ya descargado; --comprobar-todo" in _log(tmp_path)
    _ejecutar(tmp_path, "--sin-sondeo", "--comprobar-todo")
    assert portal.pedidas("GET", 367) == 2
    assert not (_raw(tmp_path, 367, ANIO - 3).parent / "_historico").exists()   # nada cambió


def test_codigo_conocido_que_no_existe_es_un_error(portal, tmp_path):
    del portal.codigos[406]
    assert _ejecutar(tmp_path, "--sin-sondeo") == 1
    assert f"contratos_menores {ANIO - 2} (cd=406): publicado según las fuentes" in _log(tmp_path)
    assert not _raw(tmp_path, 406, ANIO - 2).parent.exists()


@pytest.mark.parametrize("publicado", [f"movimientos_CAR_{ANIO - 1}.csv", nombre(ANIO - 2)])
def test_codigo_que_sirve_otro_fichero_no_se_descarga(portal, tmp_path, publicado):
    portal.codigos[979] = (publicado, csv_rioja(ANIO - 1))
    assert _ejecutar(tmp_path, "--sin-sondeo") == 1
    assert portal.pedidas("GET", 979) == 0
    assert f"el servidor lo sirve como '{publicado}'" in _log(tmp_path)
    assert str(ANIO - 1) not in set(_parquet(tmp_path)["_anio_fichero"])


# ---------------------------------------------------------------------------
# Sondeo de códigos nuevos
# ---------------------------------------------------------------------------

def _anio_nuevo_tras_un_salto(portal, monkeypatch):
    """Año en curso sin código conocido: el portal lo crea, con los demás datos
    abiertos del año, después de un salto de códigos sin nada (404)."""
    monkeypatch.setattr(M, "CODIGOS_CONOCIDOS", {a: cd for a, cd in CONOCIDOS.items() if a < ANIO})
    del portal.codigos[1151]
    portal.codigos[980] = (f"movimientos_CAR_{ANIO - 1}.csv", b"x;y\r\n1;2\r\n")
    portal.codigos[981] = 403
    lote = [f"movimientos_ADER_{ANIO}.csv", f"movimientos_CAR_{ANIO}.csv", f"detalles_CAR_{ANIO}.csv",
            nombre(ANIO), f"aplicaciones_CAR_{ANIO}.csv", f"capturaspesca_{ANIO - 1}.csv", f"licencias_{ANIO}.csv"]
    for cd, publicado in enumerate(lote, start=1010):
        portal.codigos[cd] = (publicado, csv_rioja(ANIO) if publicado == nombre(ANIO) else b"x;y\r\n1;2\r\n")
    return 1013


def test_sondeo_encuentra_el_anio_nuevo_tras_un_salto_de_codigos(portal, tmp_path, monkeypatch):
    cd = _anio_nuevo_tras_un_salto(portal, monkeypatch)

    assert _ejecutar(tmp_path) == 0

    df = _parquet(tmp_path)
    nuevo = df[df["_anio_fichero"] == str(ANIO)]
    assert nuevo["_recurso"].tolist() == [f"opd-{cd}"] and nuevo["_fuente"].tolist() == [M.url_codigo(cd)]
    assert _raw(tmp_path, cd, ANIO).read_bytes() == csv_rioja(ANIO)
    sondeo = json.loads((tmp_path / "raw" / "sondeo_codigos.json").read_text(encoding="utf-8"))
    assert sondeo["existentes"][str(cd)] == nombre(ANIO) and sondeo["existentes"]["981"] == "HTTP 403"
    # Los demás ficheros del año no se descargan; el salto (982-1009) no se pide entero
    assert all(portal.pedidas("GET", c) == 0 for c in range(980, 1017) if c != cd)
    assert sum(portal.pedidas("HEAD", c) for c in range(990, 1009)) <= 4
    assert f"contratos_menores: {ANIO}" not in _log(tmp_path)


def test_sondeo_retoma_lo_encontrado_y_revisa_los_no_publicos(portal, tmp_path, monkeypatch):
    monkeypatch.setattr(M, "CODIGOS_CONOCIDOS", {a: cd for a, cd in CONOCIDOS.items() if a < ANIO})
    del portal.codigos[1151]
    portal.codigos[980] = 403                      # el del año, aún no público
    portal.codigos[982] = (f"movimientos_CAR_{ANIO}.csv", b"x;y\r\n1;2\r\n")
    assert _ejecutar(tmp_path) == 0
    assert f"contratos_menores: {ANIO}" in _log(tmp_path)          # no publicado todavía

    portal.codigos[980] = (nombre(ANIO), csv_rioja(ANIO))
    assert _ejecutar(tmp_path) == 0

    assert str(ANIO) in set(_parquet(tmp_path)["_anio_fichero"])
    assert portal.pedidas("HEAD", 981) == 1        # un hueco ya sondeado no se vuelve a pedir
    assert portal.pedidas("HEAD", 980) == 3        # sondeo, nuevo sondeo y comprobación antes de descargar


def test_sondeo_anota_otros_ficheros_de_contratacion_una_vez(portal, tmp_path):
    portal.codigos[1152] = (f"contratos_ADER_{ANIO}.csv", b"x;y\r\n1;2\r\n")
    assert _ejecutar(tmp_path) == 0
    assert f"cd=1152: contratos_ADER_{ANIO}.csv" in _log(tmp_path)
    assert portal.pedidas("GET", 1152) == 0
    antes = len(_log(tmp_path))
    assert _ejecutar(tmp_path) == 0
    assert "contratos_ADER" not in _log(tmp_path)[antes:]


def test_mismo_anio_con_dos_codigos_se_descargan_los_dos(portal, tmp_path):
    portal.codigos[1152] = (nombre(ANIO - 1), csv_rioja(ANIO - 1, [f"OTRO;D;T;B1;N;X;{ANIO - 1}/05/05 00:00:00.000;7"]))
    assert _ejecutar(tmp_path) == 0
    df = _parquet(tmp_path)
    assert sorted(df.loc[df["_anio_fichero"] == str(ANIO - 1), "_recurso"]) == ["opd-1152", "opd-979"]
    assert f"contratos_menores {ANIO - 1}: publicado con más de un código (979, 1152)" in _log(tmp_path)


def test_sondeo_caido_es_un_error_pero_se_descargan_los_conocidos(portal, tmp_path):
    portal.codigos[1152] = 503
    assert _ejecutar(tmp_path) == 1
    assert "sondeo de códigos nuevos" in _log(tmp_path)
    assert sorted(set(_parquet(tmp_path)["_anio_fichero"])) == [str(a) for a in sorted(CONOCIDOS)]


# ---------------------------------------------------------------------------
# Sesgo del superviviente
# ---------------------------------------------------------------------------

def test_registro_retirado_y_modificado_se_conservan(portal, tmp_path):
    fila = f"{ANIO}/01/02 00:00:00.000"
    portal.codigos[1151] = (nombre(ANIO), csv_rioja(ANIO, [f"A;D;T;B1;N;X;{fila};10", f"B;D;T;B2;N;X;{fila};20",
                                                           f"C;D;T;B3;N;X;{fila};30"]))
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0
    SLEEP_REAL(1.1)          # la versión nueva tiene que llevar otra fecha (resolución: 1 s)
    portal.codigos[1151] = (nombre(ANIO), csv_rioja(ANIO, [f"A;D;T;B1;N;X;{fila};10", f"C;D;T;B3;N;X;{fila};35"]))
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0

    df = _parquet(tmp_path)
    actual = df[df["_anio_fichero"] == str(ANIO)]
    assert actual[["COD_CONTRATO", "IMPORTE_EJERCICIO", "_en_ultima_descarga"]].values.tolist() == [
        ["A", "10", True], ["B", "20", False], ["C", "30", False], ["C", "35", True]]
    assert df.loc[df["_anio_fichero"] != str(ANIO), "_en_ultima_descarga"].all()
    assert len(M.versiones(_raw(tmp_path, 1151, ANIO))) == 2


def test_anio_retirado_por_el_portal_conserva_sus_filas(portal, tmp_path):
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0
    SLEEP_REAL(1.1)
    portal.codigos[1151] = 404
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0

    df = _parquet(tmp_path)
    assert df.loc[df["_anio_fichero"] == str(ANIO), "_en_ultima_descarga"].tolist() == [False]
    assert df.loc[df["_anio_fichero"] != str(ANIO), "_en_ultima_descarga"].all()
    assert _raw(tmp_path, 1151, ANIO).exists()
    assert _manifiesto(tmp_path)[f"contratos_menores/cd1151/{nombre(ANIO)}"]["publicado"] is False
    assert f"contratos_menores {ANIO} (cd=1151): el portal ya no lo sirve" in _log(tmp_path)


@pytest.mark.parametrize("fallo", [("GET", 503), ("GET", b"<html><body>Mantenimiento</body></html>"),
                                   ("HEAD", 503)])
def test_descarga_fallida_no_pierde_nada(portal, tmp_path, fallo):
    assert _ejecutar(tmp_path, "--sin-sondeo") == 0
    antes = _parquet(tmp_path)
    SLEEP_REAL(1.1)
    metodo, valor = fallo
    if metodo == "HEAD":
        portal.fallos_head[1151] = valor
    else:
        portal.codigos[1151] = valor if isinstance(valor, int) else (nombre(ANIO), valor)
    assert _ejecutar(tmp_path, "--sin-sondeo") == 1       # no se sabe si se ha retirado: error, nada cambia

    despues = _parquet(tmp_path)
    assert despues["_en_ultima_descarga"].all()
    assert despues["COD_CONTRATO"].tolist() == antes["COD_CONTRATO"].tolist()
    raw = _raw(tmp_path, 1151, ANIO)
    assert raw.read_bytes() == csv_rioja(ANIO) and len(M.versiones(raw)) == 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def test_cli(portal, tmp_path, monkeypatch):
    # runpy carga el script de nuevo: usa los códigos reales, no los del test
    for anio, cd in REALES.items():
        portal.codigos[cd] = (nombre(anio), csv_rioja(anio))
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--salida", str(tmp_path), "--sin-sondeo"])
    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert salida.value.code == 0
    assert (tmp_path / "contratos_menores.parquet").exists()


def test_salida_por_defecto_en_el_repo():
    assert M.SALIDA == REPO_ROOT / "ccaa_la_rioja"
