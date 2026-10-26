"""Tests offline de scripts/ccaa_cantabria.py.

La API de la base de datos de contratos del Portal de Transparencia de
Cantabria (POST /filter y POST /export) se simula con un ``requests.post``
falso. Las exportaciones se generan con el formato real: línea 'sep=;',
cabecera con nombres repetidos, todos los campos entre comillas, CRLF,
Windows-1252 (el euro en 0x80), fechas DD-MM-AAAA y campos con ';' y saltos de
línea. Los NIF son inventados.
"""

import importlib.util
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "ccaa_cantabria.py"


def _cargar():
    spec = importlib.util.spec_from_file_location("ccaa_cantabria", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
HOY = M.ahora().date()
ANIO = HOY.year
ANTIGUO = ANIO - 3          # año cerrado: solo se vuelve a pedir con --comprobar-todo

# Cabecera recortada de la exportación real (mismo orden relativo y nombres repetidos)
CABECERA = ["Consejería", "Órgano directivo", "Objeto del contrato", "Tipo de tramitación", "Tipo de contrato",
            "Procedimiento de contratación", "Contrato menor", "Fecha formalización", "Sin lotes",
            "Sin lotes - CIF/NIF adjudicatario", "Sin lotes - Razón social adjudicatario",
            "Sin lotes - Importe sin IVA", "Sin lotes - Importe con IVA", "Ofertas - Nº ofertas",
            "Ofertas (detalle)", "Contrato general", "Sin lotes", "Sin lotes - CIF/NIF adjudicatario",
            "Observaciones públicas"]


def contrato(fecha, objeto="Suministro de material de oficina", importe="7.825,00 €", nif="B00000001",
             ofertas="2", detalle="1- cif/nif: B00000001 razon: EMPRESA UNO, SL sin_iva: 0,00 €; "
                                  "2- cif/nif: B00000002 razon: EMPRESA DOS, SA sin_iva: 0,00 €"):
    """Un contrato menor tal como lo exporta el portal (las fechas, DD-MM-AAAA)."""
    return {"fecha": fecha, "valores": [
        "Consejería de Cultura, Turismo y Deporte", "Dirección General de Cultura", objeto, "ORDINARIA",
        "Suministro", "Menor", "X", fecha.strftime("%d-%m-%Y") if fecha else "", "X", nif,
        "EMPRESA UNO, SL", importe, "9.468,25 €", ofertas, detalle, "", "", "", ""]}


def exportacion(contratos, sep_linea=True):
    """Bytes de una exportación: 'sep=;', cabecera, todos los campos entre comillas, CRLF, cp1252."""
    def linea(valores):
        return ";".join('"' + str(v).replace('"', '""') + '"' for v in valores)
    lineas = (["sep=;"] if sep_linea else []) + [linea(CABECERA)] + [linea(c["valores"]) for c in contratos]
    return ("\r\n".join(lineas) + "\r\n").encode("cp1252")


class FakeResponse:
    def __init__(self, status=200, content=b"", headers=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}


class FakeAPI:
    """contratos: lista de dicts de contrato(); fallos: {('filter'|'export', AAAA-MM-DD desde): código HTTP
    o 'red'}; tope: registros como mucho en una exportación (el real, 1.000)."""

    def __init__(self):
        self.contratos = []
        self.fallos = {}
        self.tope = 1000
        self.recortar = {}         # desde -> nº de registros que da /export (exportación incompleta)
        self.html = set()          # desde de las exportaciones que responden con una página HTML
        self.llamadas = []

    def en_ventana(self, datos):
        if "fechaFormalizacionFrom" not in datos:
            return list(self.contratos)
        desde = date.fromisoformat(datos["fechaFormalizacionFrom"])
        hasta = date.fromisoformat(datos["fechaFormalizacionTo"])
        return [c for c in self.contratos if c["fecha"] and desde <= c["fecha"] <= hasta]

    def post(self, url, json=None, headers=None, timeout=None):
        assert headers["User-Agent"].startswith("licitaciones-espana")
        tipo = url.rsplit("/", 1)[1]
        desde = json.get("fechaFormalizacionFrom")
        self.llamadas.append((tipo, desde, json.get("fechaFormalizacionTo")))
        fallo = self.fallos.get((tipo, desde))
        if fallo == "red":
            raise requests.exceptions.ConnectionError("conexión rechazada")
        if fallo:
            return FakeResponse(status=fallo, content=b"<html>Error</html>")
        lista = self.en_ventana(json)
        if tipo == "filter":
            assert headers["Accept"] == "application/json"
            cuerpo = {"pageInfo": {"size": 1, "totalElements": len(lista), "totalPages": len(lista), "page": 0},
                      "content": [{"id": 1}] if lista else []}
            return FakeResponse(content=__import__("json").dumps(cuerpo).encode("utf-8"))
        assert tipo == "export" and headers["Accept"] == "text/csv"
        if desde in self.html:
            return FakeResponse(content=b"<!DOCTYPE html><html><body>Mantenimiento</body></html>")
        lista = lista[:min(self.tope, self.recortar.get(desde, self.tope))]
        return FakeResponse(content=exportacion(lista),
                            headers={"Content-Type": "text/csv;charset=ISO-8859-1"})

    def pedidas(self, tipo, desde=None):
        return [c for c in self.llamadas if c[0] == tipo and (desde is None or c[1] == desde)]


@pytest.fixture
def api(monkeypatch):
    fake = FakeAPI()
    monkeypatch.setattr(requests, "post", fake.post)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    return fake


def ejecutar(salida, *args):
    return M.main(["--salida", str(salida), *args])


def parquet(salida):
    return pd.read_parquet(Path(salida) / "contratos.parquet")


def manifiesto(salida):
    return json.loads((Path(salida) / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))


def mes(anio, m):
    return f"{anio}-{m:02d}"


# ---------------------------------------------------------------------------
# Descarga y Parquet
# ---------------------------------------------------------------------------

def test_primera_descarga_un_fichero_por_mes_y_parquet_como_texto(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5)), contrato(date(ANIO, 1, 20), objeto='Obra "urgente"; fase 1\nlote A'),
                     contrato(date(ANIO, 3, 2), importe="300,10 €")]
    rc = ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 3))
    assert rc == 0
    raw = tmp_path / "raw" / "contratos" / str(ANIO)
    assert sorted(p.name for p in raw.iterdir() if p.is_file()) == [f"{ANIO}-01.csv", f"{ANIO}-03.csv"]
    assert (raw / f"{ANIO}-01.csv").read_bytes() == exportacion(api.contratos[:2])
    datos = manifiesto(tmp_path)[f"contratos/{ANIO}/{ANIO}-01.csv"]
    assert datos["contratos_api"] == 2 and datos["publicado"] is True
    assert datos["ventana_desde"] == f"{ANIO}-01-01" and datos["ventana_hasta"] == f"{ANIO}-01-31"
    df = parquet(tmp_path)
    assert len(df) == 3
    # Nombres repetidos como los pone pandas y valores tal cual (euro de cp1252, comillas, ';' y saltos)
    assert "Sin lotes.1" in df.columns and "Sin lotes - CIF/NIF adjudicatario.1" in df.columns
    assert set(df["Sin lotes - Importe sin IVA"]) == {"7.825,00 €", "300,10 €"}
    assert 'Obra "urgente"; fase 1\nlote A' in set(df["Objeto del contrato"])
    assert df["Contrato general"].isna().all()           # vacío = nulo
    assert df["_en_ultima_descarga"].all()
    assert set(df["_ventana_desde"]) == {f"{ANIO}-01-01", f"{ANIO}-03-01"}
    assert set(df["_anio"]) == {str(ANIO)}
    assert list(df.columns[-9:]) == list(M.ORDEN_METADATOS)
    # Febrero no tiene contratos: no hay fichero y se anota
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert f"MESES SIN CONTRATOS EN LA API (1):\n   - {ANIO}-02" in log


def test_segunda_ejecucion_sin_cambios(api, tmp_path, monkeypatch):
    """Sin cambios en la API, otra ejecución no guarda versión nueva del crudo ni cambia los registros. Las fechas de versión
    van al segundo: si la comprobación cae en el mismo segundo que la descarga, el Parquet sale idéntico byte a byte; si cae
    después (la ejecución de la semana siguiente), solo cambia `_ultima_descarga` de los registros reconfirmados
    (comun.salida.acumular_fichero) y la versión anterior del Parquet va a _historico/. Se fija el reloj (M.ahora) en las
    dos: antes el resultado dependía de en qué segundo acababa la primera ejecución, y el test fallaba a veces."""
    api.contratos = [contrato(date(ANIO, 1, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    crudo = tmp_path / "raw" / "contratos" / str(ANIO) / f"{ANIO}-01.csv"
    descarga = datetime.fromtimestamp(crudo.stat().st_mtime, timezone.utc)
    antes_bytes = (tmp_path / "contratos.parquet").read_bytes()
    antes = parquet(tmp_path)

    def sin_cambios_en_la_ultima():
        ultima = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8").rsplit("Inicio:", 1)[1]
        assert "DESCARGADOS" not in ultima
        assert f"SIN CAMBIOS (1):\n   - contratos {ANIO}-01 {ANIO}-01.csv (1 contratos)" in ultima
        assert not (tmp_path / "raw" / "contratos" / str(ANIO) / "_historico").exists()

    # En el mismo segundo que la descarga: nada cambia
    monkeypatch.setattr(M, "ahora", lambda: descarga)
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    assert (tmp_path / "contratos.parquet").read_bytes() == antes_bytes
    assert not (tmp_path / "_historico").exists()
    sin_cambios_en_la_ultima()

    # Una semana después: los mismos registros; solo _ultima_descarga pasa a la fecha de la comprobación
    semana = descarga + timedelta(days=7)
    monkeypatch.setattr(M, "ahora", lambda: semana)
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    sin_cambios_en_la_ultima()
    despues = parquet(tmp_path)
    assert manifiesto(tmp_path)[f"contratos/{ANIO}/{ANIO}-01.csv"]["comprobado"] == M.iso(semana)
    assert despues["_ultima_descarga"].tolist() == [M.iso(semana)] != antes["_ultima_descarga"].tolist()
    assert despues["_en_ultima_descarga"].all()
    iguales = [c for c in antes.columns if c != "_ultima_descarga"]
    pd.testing.assert_frame_equal(despues[iguales], antes[iguales])
    assert len(list((tmp_path / "_historico").glob("contratos__*.parquet"))) == 1


def test_meses_antiguos_solo_con_comprobar_todo(api, tmp_path):
    api.contratos = [contrato(date(ANTIGUO, 6, 1))]
    assert ejecutar(tmp_path, "--desde", mes(ANTIGUO, 6), "--hasta", mes(ANTIGUO, 6)) == 0
    api.llamadas.clear()
    assert ejecutar(tmp_path, "--desde", mes(ANTIGUO, 6), "--hasta", mes(ANTIGUO, 6)) == 0
    assert not api.pedidas("export")
    assert ejecutar(tmp_path, "--desde", mes(ANTIGUO, 6), "--hasta", mes(ANTIGUO, 6), "--comprobar-todo") == 0
    assert len(api.pedidas("export", f"{ANTIGUO}-06-01")) == 1


def test_contrato_retirado_y_contrato_cambiado(api, tmp_path):
    uno, dos = contrato(date(ANIO, 1, 5), objeto="Uno"), contrato(date(ANIO, 1, 6), objeto="Dos")
    api.contratos = [uno, dos]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    api.contratos = [contrato(date(ANIO, 1, 6), objeto="Dos (corregido)")]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    df = parquet(tmp_path)
    vigente = dict(zip(df["Objeto del contrato"], df["_en_ultima_descarga"]))
    assert vigente == {"Uno": False, "Dos": False, "Dos (corregido)": True}
    historico = tmp_path / "raw" / "contratos" / str(ANIO) / "_historico"
    assert len(list(historico.iterdir())) == 1          # la versión anterior del crudo se conserva


def test_mes_que_se_queda_sin_contratos_se_retira(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5)), contrato(date(ANIO, 2, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 2)) == 0
    api.contratos = [contrato(date(ANIO, 1, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 2)) == 0
    entrada = manifiesto(tmp_path)[f"contratos/{ANIO}/{ANIO}-02.csv"]
    assert entrada["publicado"] is False and "retirado_desde" in entrada
    df = parquet(tmp_path)
    assert len(df) == 2
    assert df.loc[df["_ventana_desde"] == f"{ANIO}-02-01", "_en_ultima_descarga"].tolist() == [False]
    assert (tmp_path / "raw" / "contratos" / str(ANIO) / f"{ANIO}-02.csv").exists()   # el crudo no se borra


def test_retirada_masiva_es_un_error_y_no_retira_nada(api, tmp_path):
    api.contratos = [contrato(date(ANIO, m, 3)) for m in range(1, 6)]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 5)) == 0
    api.contratos = [contrato(date(ANIO, 7, 1))]            # la API sigue dando algo, pero no estos meses
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 5)) == 1
    assert all(e["publicado"] for e in manifiesto(tmp_path).values())
    assert parquet(tmp_path)["_en_ultima_descarga"].all()


def test_mes_con_mas_contratos_que_el_tope_se_parte_en_tramos(api, tmp_path, monkeypatch):
    monkeypatch.setattr(M, "TOPE_EXPORTACION", 2)
    api.tope = 2
    api.contratos = [contrato(date(ANIO, 1, d), objeto=f"C{d}") for d in (2, 3, 10, 20, 28)]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    carpeta = tmp_path / "raw" / "contratos" / str(ANIO)
    nombres = sorted(p.name for p in carpeta.iterdir() if p.is_file())
    assert f"{ANIO}-01.csv" not in nombres and all("_" in n for n in nombres)
    assert sum(manifiesto(tmp_path)[f"contratos/{ANIO}/{n}"]["contratos_api"] for n in nombres) == 5
    df = parquet(tmp_path)
    assert sorted(df["Objeto del contrato"]) == ["C10", "C2", "C20", "C28", "C3"]
    # Si el mes vuelve a caber en una exportación, los tramos quedan retirados y el mes vigente
    monkeypatch.setattr(M, "TOPE_EXPORTACION", 1000)
    api.tope = 1000
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    datos = manifiesto(tmp_path)
    assert datos[f"contratos/{ANIO}/{ANIO}-01.csv"]["publicado"] is True
    assert not any(datos[f"contratos/{ANIO}/{n}"]["publicado"] for n in nombres)
    df = parquet(tmp_path)
    assert sorted(df.loc[df["_en_ultima_descarga"], "Objeto del contrato"]) == ["C10", "C2", "C20", "C28", "C3"]


def test_un_dia_con_mas_contratos_que_el_tope_es_un_error(api, tmp_path, monkeypatch):
    monkeypatch.setattr(M, "TOPE_EXPORTACION", 2)
    api.contratos = [contrato(date(ANIO, 1, 9), objeto=f"C{i}") for i in range(3)]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert not (tmp_path / "raw" / "contratos").exists()
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert f"el {ANIO}-01-09 tiene 3 contratos" in log


def test_exportacion_incompleta_no_se_guarda_y_conserva_la_anterior(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5), objeto="Uno")]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    fichero = tmp_path / "raw" / "contratos" / str(ANIO) / f"{ANIO}-01.csv"
    antes = fichero.read_bytes()
    api.contratos.append(contrato(date(ANIO, 1, 6), objeto="Dos"))
    api.recortar[f"{ANIO}-01-01"] = 1
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert fichero.read_bytes() == antes
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "la exportación trae 1 registros y la API cuenta 2" in log
    assert parquet(tmp_path)["_en_ultima_descarga"].all()


def test_una_pagina_html_no_es_una_exportacion(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5))]
    api.html.add(f"{ANIO}-01-01")
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert not (tmp_path / "raw" / "contratos" / str(ANIO) / f"{ANIO}-01.csv").exists()
    assert "no es un CSV" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_fallo_de_red_no_retira_nada(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    antes = (tmp_path / "contratos.parquet").read_bytes()
    api.fallos[("filter", f"{ANIO}-01-01")] = "red"
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert manifiesto(tmp_path)[f"contratos/{ANIO}/{ANIO}-01.csv"]["publicado"] is True
    assert (tmp_path / "contratos.parquet").read_bytes() == antes
    assert len(api.pedidas("filter", f"{ANIO}-01-01")) == 1 + M.INTENTOS


def test_un_4xx_no_se_reintenta(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5))]
    api.fallos[("export", f"{ANIO}-01-01")] = 403
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert len(api.pedidas("export", f"{ANIO}-01-01")) == 1
    assert "HTTP 403" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_la_api_sin_ningun_contrato_es_un_error(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    api.contratos = []
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 1
    assert manifiesto(tmp_path)[f"contratos/{ANIO}/{ANIO}-01.csv"]["publicado"] is True
    assert not api.pedidas("export", f"{ANIO}-01-01")[1:]


def test_cero_resultados_desde_el_principio(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 6, 5))]             # nada en la ventana pedida
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 2)) == 0
    assert not (tmp_path / "contratos.parquet").exists()
    assert not api.pedidas("export")


def test_aviso_de_contratos_sin_fecha_de_formalizacion(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5)), contrato(None, objeto="Sin fecha")]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1)) == 0
    log = (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "la API cuenta 2 contratos y las ventanas descargadas, 1 (+1)" in log


def test_solo_parquet_no_pide_nada(api, tmp_path):
    api.contratos = [contrato(date(ANIO, 1, 5))]
    assert ejecutar(tmp_path, "--desde", mes(ANIO, 1), "--hasta", mes(ANIO, 1), "--solo-descarga") == 0
    assert not (tmp_path / "contratos.parquet").exists()
    api.llamadas.clear()
    assert ejecutar(tmp_path, "--solo-parquet") == 0
    assert not api.llamadas and len(parquet(tmp_path)) == 1


# ---------------------------------------------------------------------------
# Lectura
# ---------------------------------------------------------------------------

def test_lectura_sin_linea_sep_y_con_bytes_que_no_son_cp1252(tmp_path):
    ruta = tmp_path / "x.csv"
    ruta.write_bytes(exportacion([contrato(date(2024, 1, 5))], sep_linea=False))
    df, avisos = M.leer_tabla(ruta)
    assert len(df) == 1 and not avisos and df.loc[0, "Fecha formalización"] == "05-01-2024"
    ruta.write_bytes(exportacion([contrato(date(2024, 1, 5))]).replace(b"EMPRESA UNO, SL\"", b"EMPRESA\x81\"", 1))
    df, avisos = M.leer_tabla(ruta)
    assert len(df) == 1 and avisos == ["x.csv: bytes que no son cp1252: leído como latin-1"]


def test_problema_exportacion():
    bien = exportacion([contrato(date(2024, 1, 5))])
    assert M.problema_exportacion(bien, 1) is None
    assert M.problema_exportacion(bien, 2) == "la exportación trae 1 registros y la API cuenta 2"
    assert M.problema_exportacion(b"", 0) == "respuesta vacía"
    assert M.problema_exportacion(b'{"error": 1}', 1).startswith("la respuesta no es un CSV")
    otra = 'sep=;\r\n"Columna"\r\n"valor"\r\n'.encode("cp1252")
    assert M.problema_exportacion(otra, 1).startswith("la cabecera no trae Consejería")


def test_nombres_de_ventana():
    assert M.nombre_ventana(date(2024, 2, 1), date(2024, 2, 29)) == "2024-02.csv"
    assert M.nombre_ventana(date(2024, 2, 1), date(2024, 2, 15)) == "2024-02-01_2024-02-15.csv"
    assert M.ventana_de_nombre("2024-02.csv") == (date(2024, 2, 1), date(2024, 2, 29))
    assert M.ventana_de_nombre("2024-02-16_2024-02-29.csv") == (date(2024, 2, 16), date(2024, 2, 29))
    assert M.ventana_de_nombre("_manifiesto.json") is None
    assert M.meses((2023, 11), (2024, 2)) == [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
    with pytest.raises(ValueError):
        M.leer_mes("2024-13")
