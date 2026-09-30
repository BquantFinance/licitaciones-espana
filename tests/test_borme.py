"""Tests offline de los scripts BORME (borme/scripts).

Sin red: HTTP simulado (requests.Session.get), pdfplumber sustituido por un
doble que devuelve texto con el formato real que extrae pdfplumber de los
BORME-A (nombres de personas ficticios) y parquets pequeños.
"""

import concurrent.futures
import csv
import datetime as dt
import importlib.util
import json
import runpy
import shutil
import sys
import threading
import time
import types
from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import requests

SCRIPTS = Path(__file__).resolve().parents[1] / "borme" / "scripts"


# ─────────────────────────────────────────────
#  Carga de módulos (pdfplumber sustituido)
# ─────────────────────────────────────────────
class _FakePDF:
    def __init__(self, pages):
        self.pages = [types.SimpleNamespace(extract_text=lambda t=t: t) for t in pages]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakePdfplumber(types.ModuleType):
    """pdfplumber.open(ruta) -> páginas registradas por nombre de fichero."""

    def __init__(self):
        super().__init__("pdfplumber")
        self.textos = {}

    def open(self, path):
        name = Path(path).name
        if name not in self.textos:
            raise ValueError(f"PDF ilegible: {name}")
        return _FakePDF(self.textos[name])


FAKE_PDFPLUMBER = _FakePdfplumber()


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    prev = sys.modules.get("pdfplumber")
    sys.modules["pdfplumber"] = FAKE_PDFPLUMBER
    try:
        spec.loader.exec_module(module)
    finally:
        if prev is None:
            sys.modules.pop("pdfplumber", None)
        else:
            sys.modules["pdfplumber"] = prev
    return module


scraper = _load("borme_scraper")
bparser = _load("borme_batch_parser")
anon = _load("borme_anonymize")
match = _load("borme_placsp_match")
validate = _load("borme_validate")


def _run_cli(monkeypatch, script, *args):
    monkeypatch.setattr(sys, "argv", [script, *map(str, args)])
    monkeypatch.setitem(sys.modules, "pdfplumber", FAKE_PDFPLUMBER)
    # Procesos -> hilos: el doble de pdfplumber vive en este proceso
    monkeypatch.setattr(concurrent.futures, "ProcessPoolExecutor", ThreadPoolExecutor)
    runpy.run_path(str(SCRIPTS / script), run_name="__main__")


@pytest.fixture
def fake_pdf(monkeypatch):
    FAKE_PDFPLUMBER.textos = {}
    monkeypatch.setattr(bparser, "ProcessPoolExecutor", ThreadPoolExecutor)
    yield FAKE_PDFPLUMBER.textos
    FAKE_PDFPLUMBER.textos = {}


# ─────────────────────────────────────────────
#  Fixtures de texto (formato pdfplumber de BORME-A)
# ─────────────────────────────────────────────
PAG1 = """BOLETÍN OFICIAL DEL REGISTRO MERCANTIL
Núm. 3 Jueves 4 de enero de 2024 Pág. 101
82-3-4202-A-EMROB
:evc
SECCIÓN PRIMERA
Empresarios
Actos inscritos
MADRID
1001 - ALFA SOLUCIONES SL.
Constitución. Comienzo de operaciones: 1.12.23. Objeto social: Consultoría informática. Desarrollo de programas. Domicilio: C/
MAYOR 5 2º B (MADRID). Capital: 3.000,00 Euros. Nombramientos. Adm. Unico: FULANO MENGANO JUAN. Datos registrales. T 45123
, F 1, S 8, H M 793456, I/A 1 ( 2.01.24).
1002 - BETA INVERSIONES SA.
Ceses/Dimisiones. Adm. Solid.: ZUTANO PERENGANO ANA;PRUEBA EJEMPLO LUIS. Nombramientos. Adm. Unico: ZUTANO PERENGANO
ANA. Ampliación de capital. Capital: 5.000,00 Euros. Resultante Suscrito: 1.205.000,00 Euros. Datos registrales. T 1234 , F 56, S 8, H M
12345, I/A 20 (22.12.23).
1003 - GAMMA LOGISTICA SL.
Sociedad unipersonal. Cambio de identidad del socio único: DELTA HOLDING SL. Datos registrales. T 999 , F 10, S 8, H M 5555, I/A 7
(28.12.23)."""

PAG2 = """BOLETÍN OFICIAL DEL REGISTRO MERCANTIL
Núm. 3 Jueves 4 de enero de 2024 Pág. 102
82-3-4202-A-EMROB
:evc
1004 - EPSILON OBRAS SL.
Cambio de domicilio social. C/ NUEVA 7 (ALCALA DE HENARES). Cambio de objeto social. Construcción de edificios. Datos registrales. T
2000 , F 20, S 8, H M 6666, I/A 3 (27.12.23).
1005 - ZETA SERVICIOS SL.
Reducción de capital. Importe reducción: 1.000,00 Euros. Resultante Suscrito: 3.000,00 Euros. Revocaciones. Apoderado: PRUEBA
EJEMPLO LUIS. Reelecciones. Auditor: AUDITORES EJEMPLO SL. Datos registrales. T 3000 , F 30, S 8, H M 7777, I/A 9 (29.12.23).
1006 - ETA COMERCIAL SL.
Disolución. Voluntaria. Extinción. Datos registrales. T 4000 , F 40, S 8, H M 8888, I/A 11 (29.12.23).
https://www.boe.es BOLETÍN OFICIAL DEL REGISTRO MERCANTIL D.L.: M-5188/1990 - ISSN: 1989-3079"""

# Primer BORME del año: la numeración se reinicia (anuncios de 1-3 cifras)
PAG_ENERO = """BOLETÍN OFICIAL DEL REGISTRO MERCANTIL
Núm. 1 Martes 2 de enero de 2024 Pág. 1
10-1-4202-A-EMROB
:evc
SECCIÓN PRIMERA
Empresarios
Actos inscritos
ARABA/ÁLAVA
1 - IOTA TALLERES SL.
Nombramientos. Apoderado: FULANO MENGANO JUAN. Datos registrales. T 100 , F 1, S 8, H VI 100, I/A 5 (20.12.23).
2 - KAPPA DISEÑO SL.
Constitución. Comienzo de operaciones: 1.12.23. Objeto social: Diseño gráfico. Domicilio: C/ PORTAL DE CASTILLA 40
7 - 1º A (VITORIA-GASTEIZ). Capital: 3.000,00 Euros. Datos registrales. T 101 , F 2, S 8, H VI 101, I/A 1 (21.12.23).
3 - LAMBDA OBRAS SL.
Extinción. Datos registrales. T 102 , F 3, S 8, H VI 102, I/A 9 (21.12.23).
http://www.boe.es BOLETÍN OFICIAL DEL REGISTRO MERCANTIL D.L.: M-5188/1990 - ISSN: 1989-3079"""

PDF_MADRID = "BORME-A-2024-3-28.pdf"
PDF_ENERO = "BORME-A-2024-1-01.pdf"


def _pdf_file(root, fecha, name, contenido=b"%PDF-1.4 fake"):
    p = Path(root) / f"{fecha:%Y}" / f"{fecha:%m}" / f"{fecha:%d}" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(contenido)
    return p


# ═════════════════════════════════════════════
#  borme_batch_parser.py
# ═════════════════════════════════════════════
class TestParser:
    @pytest.fixture(autouse=True)
    def _pdf(self, fake_pdf, tmp_path):
        fake_pdf[PDF_MADRID] = [PAG1, PAG2]
        self.path = _pdf_file(tmp_path / "borme_pdfs", dt.date(2024, 1, 4), PDF_MADRID)
        e, c = bparser.parse_single_pdf(str(self.path))
        self.emp = pd.DataFrame(e).set_index("num_entrada")
        self.car = pd.DataFrame(c)

    def test_una_fila_por_anuncio_incluido_el_ultimo(self):
        assert list(self.emp.index) == ["1001", "1002", "1003", "1004", "1005", "1006"]
        assert set(self.emp["fecha_borme"]) == {"2024-01-04"}
        assert set(self.emp["provincia"]) == {"MADRID"}
        assert set(self.emp["cod_provincia"]) == {"28"}
        assert set(self.emp["num_borme"]) == {3}

    def test_nombres_y_actos(self):
        assert self.emp.loc["1001", "empresa"] == "ALFA SOLUCIONES SL"
        assert self.emp.loc["1001", "empresa_norm"] == "ALFA SOLUCIONES"
        assert self.emp.loc["1001", "actos"] == "Constitución|Nombramientos"
        assert "Ampliación de capital" in self.emp.loc["1002", "actos"].split("|")
        assert self.emp.loc["1006", "actos"].split("|") == ["Disolución", "Extinción"]

    def test_sociedad_unipersonal_es_acto_no_parte_del_nombre(self):
        # Antes: empresa = "GAMMA LOGISTICA SL. Sociedad unipersonal" y el acto se perdía
        assert self.emp.loc["1003", "empresa"] == "GAMMA LOGISTICA SL"
        assert self.emp.loc["1003", "empresa_norm"] == "GAMMA LOGISTICA"
        assert self.emp.loc["1003", "actos"] == "Sociedad unipersonal"

    def test_datos_registrales_formato_sin_libro(self):
        # "T 1234 , F 56, S 8, H M 12345, I/A 20 (22.12.23)": antes no casaba nunca
        assert self.emp["hoja_registral"].notna().all()
        assert self.emp.loc["1002", ["tomo", "hoja_registral", "inscripcion", "fecha_inscripcion"]].tolist() == [
            "1234", "M 12345", "20", "22.12.23"]
        # Día de una cifra: "( 2.01.24)"
        assert self.emp.loc["1001", "fecha_inscripcion"] == "2.01.24"

    def test_capital_es_el_resultante(self):
        assert self.emp.loc["1001", "capital_euros"] == 3000.0  # constitución
        assert self.emp.loc["1002", "capital_euros"] == 1205000.0  # ampliación: no 5.000
        assert self.emp.loc["1005", "capital_euros"] == 3000.0  # reducción
        assert pd.isna(self.emp.loc["1006", "capital_euros"])

    def test_constitucion(self):
        row = self.emp.loc["1001"]
        assert row["fecha_constitucion"] == "1.12.23"  # sin el "." final
        assert row["objeto_social"] == "Consultoría informática. Desarrollo de programas"
        assert row["domicilio"] == "C/ MAYOR 5 2º B (MADRID)"

    def test_cambio_de_domicilio_no_arrastra_otros_actos(self):
        assert self.emp.loc["1004", "domicilio"] == "C/ NUEVA 7 (ALCALA DE HENARES)"

    def test_cargos(self):
        got = set(map(tuple, self.car[["num_entrada", "tipo_acto", "cargo", "persona"]].values))
        assert got == {
            ("1001", "nombramiento", "Adm. Unico", "FULANO MENGANO JUAN"),
            ("1002", "cese", "Adm. Solid.", "ZUTANO PERENGANO ANA"),
            ("1002", "cese", "Adm. Solid.", "PRUEBA EJEMPLO LUIS"),
            ("1002", "nombramiento", "Adm. Unico", "ZUTANO PERENGANO ANA"),
            ("1005", "revocacion", "Apoderado", "PRUEBA EJEMPLO LUIS"),
            ("1005", "reeleccion", "Auditor", "AUDITORES EJEMPLO SL"),
        }
        assert set(self.car["hoja_registral"]) == {"M 793456", "M 12345", "M 7777"}


@pytest.mark.parametrize("texto, esperado", [
    ("Datos registrales. T 856, L 683, F 96, S 8, H CC 11959, I/A 2 (29.01.15).",
     ("856", "CC 11959", "2", "29.01.15")),
    ("Datos registrales. T 16030 , F 160, S 8, H M 271304, I/A 6 ( 2.02.15).",
     ("16030", "M 271304", "6", "2.02.15")),
    ("Datos registrales. T 194 , F 46, S 8, H NA004126, I/A00037 (12.03.10).",
     ("194", "NA004126", "00037", "12.03.10")),
    ("Datos registrales. T 13 , F 145, S 8, H 605, I/A 1 (3.11.09).",
     ("13", "605", "1", "3.11.09")),
])
def test_datos_registrales_variantes_reales(texto, esperado):
    m = bparser.DATOS_REG_RE.search(texto)
    assert m is not None
    assert (m.group(1), m.group(5).strip(), m.group(6), m.group(7)) == esperado


def test_primer_borme_del_ano_anuncios_de_1_a_3_cifras(fake_pdf, tmp_path):
    fake_pdf[PDF_ENERO] = [PAG_ENERO]
    path = _pdf_file(tmp_path, dt.date(2024, 1, 2), PDF_ENERO)
    e, c = bparser.parse_single_pdf(str(path))
    emp = pd.DataFrame(e)
    # Antes (\d{4,7}) este PDF no producía ninguna fila
    assert list(emp["num_entrada"]) == ["1", "2", "3"]
    assert list(emp["empresa"]) == ["IOTA TALLERES SL", "KAPPA DISEÑO SL", "LAMBDA OBRAS SL"]
    # "7 - 1º A" a principio de línea dentro del domicilio no abre un anuncio nuevo
    assert emp.loc[1, "domicilio"] == "C/ PORTAL DE CASTILLA 40 7 - 1º A (VITORIA-GASTEIZ)"
    assert set(emp["provincia"]) == {"ARABA/ÁLAVA"}
    assert len(c) == 1
    # El validador usa la misma lógica
    assert [m.group(1) for m in validate.entry_starts(validate.clean(PAG_ENERO))] == ["1", "2", "3"]


def test_numeros_cortos_fuera_de_secuencia_no_cortan_anuncios():
    texto = "57315 - ALFA SL.\nDomicilio: C/ X\n12 - 2º B (MADRID).\n57316 - BETA SL.\nExtinción."
    assert [m.group(1) for m in bparser._entry_starts(texto)] == ["57315", "57316"]
    # Línea de CVE invertida sin limpiar antes del primer anuncio: no es un anuncio
    texto = "82-1-4202-A-EMROB :evc\nMADRID\n1 - ALFA SL.\nExtinción.\n2 - BETA SL.\nExtinción."
    assert [m.group(1) for m in bparser._entry_starts(texto)] == ["1", "2"]
    assert [m.group(1) for m in validate.entry_starts(texto)] == ["1", "2"]


@pytest.mark.parametrize("ruta, fecha", [
    # Directorio base con un componente tipo año: antes daba "2024-borme_pdfs-2015"
    ("/datos/2024/borme_pdfs/2015/02/10/BORME-A-2015-27-10.pdf", "2015-02-10"),
    ("/datos/borme_pdfs/2015/2/9/BORME-A-2015-27-10.pdf", "2015-02-09"),
    # Antes el límite 2030 dejaba todas las fechas en 1 de enero
    ("/datos/borme_pdfs/2031/03/05/BORME-A-2031-45-28.pdf", "2031-03-05"),
    ("/datos/sin_estructura/BORME-A-2015-27-10.pdf", "2015-01-01"),
])
def test_fecha_borme_desde_la_ruta(fake_pdf, ruta, fecha):
    fake_pdf[Path(ruta).name] = [PAG1]
    e, _ = bparser.parse_single_pdf(ruta)
    assert e[0]["fecha_borme"] == fecha


def test_pdf_ilegible_es_error_no_pdf_vacio(fake_pdf, tmp_path):
    path = _pdf_file(tmp_path, dt.date(2024, 1, 4), PDF_MADRID)  # no registrado: ilegible
    with pytest.raises(ValueError):
        bparser.parse_single_pdf(str(path))
    assert bparser._process_one(str(path)) == ([], [], str(path), False)


def test_validate_find_empresa_sociedad_unipersonal():
    empresa, body = validate.find_empresa(
        "GAMMA LOGISTICA SL. Sociedad unipersonal. Cambio de identidad del socio único: DELTA SL.")
    assert empresa == "GAMMA LOGISTICA SL"
    assert body.startswith(". Sociedad unipersonal.")
    # Forma jurídica en minúsculas tras el nombre: sigue formando parte del nombre
    empresa, _ = validate.find_empresa("OMEGA. Sociedad Anónima. Nombramientos. Adm. Unico: X Y.")
    assert empresa == "OMEGA. Sociedad Anónima"


class TestRunBatch:
    def _emp(self, out):
        return pd.read_parquet(out / "borme_empresas.parquet")

    def test_resume_conserva_filas_anteriores(self, fake_pdf, tmp_path):
        base = tmp_path / "borme_pdfs"
        nuevo = "BORME-A-2024-4-28.pdf"
        fake_pdf[PDF_MADRID] = [PAG1, PAG2]
        fake_pdf[nuevo] = [PAG1]
        _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
        bparser.run_batch(base, base, workers=2)
        assert set(self._emp(base)["pdf_filename"]) == {PDF_MADRID}

        # Llega un PDF nuevo: --resume solo lo procesa a él, pero la salida
        # debe seguir conteniendo lo anterior (antes se sobrescribía)
        _pdf_file(base, dt.date(2024, 1, 5), nuevo)
        bparser.run_batch(base, base, workers=2, resume=True)
        emp = self._emp(base)
        assert emp.groupby("pdf_filename").size().to_dict() == {PDF_MADRID: 6, nuevo: 3}
        car = pd.read_parquet(base / "borme_cargos.parquet")
        assert set(car["pdf_filename"]) == {PDF_MADRID, nuevo}
        assert not (base / "borme_parse_parts").exists()
        done = json.loads((base / "borme_parse_progress.json").read_text())["done"]
        assert len(done) == 2

    def test_resume_tras_interrupcion(self, fake_pdf, tmp_path, monkeypatch):
        base = tmp_path / "borme_pdfs"
        fake_pdf[PDF_MADRID] = [PAG1, PAG2]
        _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)

        # Corte (Ctrl-C) después de guardar el progreso del batch y antes de
        # escribir los parquets finales
        real = pd.to_datetime

        def corte(*a, **k):
            raise KeyboardInterrupt

        monkeypatch.setattr(pd, "to_datetime", corte)
        with pytest.raises(KeyboardInterrupt):
            bparser.run_batch(base, base, workers=2)
        monkeypatch.setattr(pd, "to_datetime", real)
        assert not (base / "borme_empresas.parquet").exists()

        # Antes: "Nada que procesar" y las filas se perdían para siempre
        bparser.run_batch(base, base, workers=2, resume=True)
        assert len(self._emp(base)) == 6

    def test_progreso_sin_filas_guardadas_se_reprocesa(self, fake_pdf, tmp_path):
        # Progreso de la versión anterior: PDFs "hechos" cuyas filas nunca se guardaron
        base = tmp_path / "borme_pdfs"
        fake_pdf[PDF_MADRID] = [PAG1, PAG2]
        pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
        (base / "borme_parse_progress.json").write_text(json.dumps({"done": [str(pdf)], "errors": []}))
        bparser.run_batch(base, base, workers=2, resume=True)
        assert len(self._emp(base)) == 6

    def test_sin_capital_no_rompe_el_resumen(self, fake_pdf, tmp_path):
        base = tmp_path / "borme_pdfs"
        fake_pdf[PDF_MADRID] = [PAG1.split("1002 - ")[0].replace("Capital: 3.000,00 Euros. ", "")]
        _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
        bparser.run_batch(base, base, workers=1)  # antes: KeyError 'capital_euros'
        assert "capital_euros" not in self._emp(base).columns

    def test_pdf_ilegible_no_se_marca_hecho(self, fake_pdf, tmp_path):
        base = tmp_path / "borme_pdfs"
        _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)  # ilegible
        fake_pdf[PDF_ENERO] = [PAG_ENERO]
        _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
        bparser.run_batch(base, base, workers=2)
        prog = json.loads((base / "borme_parse_progress.json").read_text())
        assert [Path(p).name for p in prog["done"]] == [PDF_ENERO]
        assert [Path(p).name for p in prog["errors"]] == [PDF_MADRID]

        fake_pdf[PDF_MADRID] = [PAG1, PAG2]  # ya legible: --resume lo reintenta
        bparser.run_batch(base, base, workers=2, resume=True)
        assert set(self._emp(base)["pdf_filename"]) == {PDF_MADRID, PDF_ENERO}


# ─────────────────────────────────────────────
#  Sesgo del superviviente: parse incremental, acumular() y --semilla
# ─────────────────────────────────────────────
META = list(bparser.COLUMNAS_META)
PDF_NUEVO = "BORME-A-2024-4-28.pdf"
PDF_OTRO = "BORME-A-2023-100-28.pdf"
# Otra lectura del mismo boletín (PDF corregido o parser cambiado): otro nombre en
# el anuncio 1001 y el 1006 ya no sale
PAG1_V2 = PAG1.replace("1001 - ALFA SOLUCIONES SL.", "1001 - ALFA SOLUCIONES NUEVAS SL.")
PAG2_V2 = PAG2.split("1006 - ")[0]
PAG_OTRO = """BOLETÍN OFICIAL DEL REGISTRO MERCANTIL
Núm. 100 Lunes 29 de mayo de 2023 Pág. 5
MADRID
5000 - CAJA DE AHORROS NUEVA SA.
Nombramientos. Apoderado: FULANO MENGANO JUAN. Datos registrales. T 1 , F 1, S 8, H M 1, I/A 1 (1.05.23)."""


@pytest.fixture
def fechas(monkeypatch):
    """Cada ejecución del parser con su fecha (en la realidad, días distintos)."""
    usadas = []

    def ahora():
        usadas.append(f"2026-10-{len(usadas) + 1:02d}T00:00:00+00:00")
        return usadas[-1]

    monkeypatch.setattr(bparser, "_ahora", ahora)
    return usadas


def _tablas(out):
    return (pd.read_parquet(out / "borme_empresas.parquet"),
            pd.read_parquet(out / "borme_cargos.parquet"))


def _foto(out, patron="*"):
    """Contenido y fecha de modificación de los ficheros (para ver si se reescriben)."""
    return {p.relative_to(out).as_posix(): (p.read_bytes(), p.stat().st_mtime_ns)
            for p in sorted(out.rglob(patron)) if p.is_file()}


def _salida_antigua(base):
    """Lo que escribía run_batch antes de acumular (con un worker): el parse de
    cada PDF en orden, los mismos tipos y el mismo dedup."""
    emp, car = [], []
    for p in sorted(Path(base).rglob("BORME-A-*.pdf")):
        e, c = bparser.parse_single_pdf(str(p))
        emp += e
        car += c
    emp, car = pd.DataFrame(emp), pd.DataFrame(car)
    emp["fecha_borme"] = pd.to_datetime(emp["fecha_borme"], errors="coerce")
    emp["capital_euros"] = pd.to_numeric(emp["capital_euros"], errors="coerce")
    emp = emp.drop_duplicates(subset=["fecha_borme", "num_entrada", "empresa_norm"], keep="first")
    car["fecha_borme"] = pd.to_datetime(car["fecha_borme"], errors="coerce")
    car = car.drop_duplicates(subset=["fecha_borme", "num_entrada", "cargo", "persona", "tipo_acto"],
                              keep="first")
    return emp.reset_index(drop=True), car.reset_index(drop=True)


def _releer(df, ruta):
    df.to_parquet(ruta, index=False)
    return pd.read_parquet(ruta)


def test_una_ejecucion_da_la_salida_de_antes_mas_las_columnas_de_control(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_ENERO: [PAG_ENERO]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    # El mismo PDF en otra carpeta: sus anuncios se descartan como repetidos, como siempre
    _pdf_file(base / "copia", dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, out, workers=2)

    emp, car = _tablas(out)
    emp_antes, car_antes = _salida_antigua(base)
    assert len(emp_antes) == 9 and len(car_antes) == 7
    pd.testing.assert_frame_equal(emp.drop(columns=META), _releer(emp_antes, tmp_path / "e.parquet"))
    pd.testing.assert_frame_equal(car.drop(columns=META), _releer(car_antes, tmp_path / "c.parquet"))
    for df in (emp, car):
        assert list(df.columns[-3:]) == META
        assert set(df["_primera_descarga"]) == set(df["_ultima_descarga"]) == {fechas[0]}
        assert df["_en_ultima_descarga"].all()


def test_reparse_sin_algunos_pdf_conserva_sus_filas(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_ENERO: [PAG_ENERO], PDF_NUEVO: [PAG1]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    bparser.run_batch(base, out, workers=2)
    emp1, _ = _tablas(out)

    # Carpeta borrada (u otra máquina sin el archivo completo) y un PDF nuevo:
    # antes la salida se reconstruía solo con los PDF en disco y enero desaparecía
    shutil.rmtree(base / "2024" / "01" / "02")
    _pdf_file(base, dt.date(2024, 1, 5), PDF_NUEVO)
    bparser.run_batch(base, out, workers=2)
    emp, car = _tablas(out)
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_ENERO: 3, PDF_MADRID: 6, PDF_NUEVO: 3}
    assert emp["_en_ultima_descarga"].all() and car["_en_ultima_descarga"].all()
    pd.testing.assert_frame_equal(emp.iloc[:len(emp1)], emp1)  # lo anterior, tal cual
    assert set(emp.loc[emp["pdf_filename"] == PDF_NUEVO, "_primera_descarga"]) == {fechas[1]}

    # Parse completo con el código actual: enero sigue sin estar en disco y se conserva
    bparser.run_batch(base, out, workers=2, reprocesar=True)
    emp, car = _tablas(out)
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_ENERO: 3, PDF_MADRID: 6, PDF_NUEVO: 3}
    assert emp["_en_ultima_descarga"].all()
    enero = emp["pdf_filename"] == PDF_ENERO
    assert set(emp.loc[enero, "_ultima_descarga"]) == {fechas[0]}
    assert set(emp.loc[~enero, "_ultima_descarga"]) == {fechas[2]}
    assert set(car["pdf_filename"]) == {PDF_ENERO, PDF_MADRID, PDF_NUEVO}


@pytest.mark.parametrize("como", ["cambia el PDF", "cambia el parser"])
def test_parse_distinto_conserva_las_filas_anteriores_como_no_vigentes(fake_pdf, fechas, tmp_path, como):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, out, workers=2)

    fake_pdf[PDF_MADRID] = [PAG1_V2, PAG2_V2]
    if como == "cambia el PDF":
        pdf.write_bytes(b"%PDF-1.4 fake corregido")  # la ejecución incremental lo detecta
        bparser.run_batch(base, out, workers=2)
    else:
        bparser.run_batch(base, out, workers=2)  # incremental: el PDF no ha cambiado
        assert _tablas(out)[0]["_en_ultima_descarga"].all()
        bparser.run_batch(base, out, workers=2, reprocesar=True)

    emp, car = _tablas(out)
    vigentes, antiguas = emp[emp["_en_ultima_descarga"]], emp[~emp["_en_ultima_descarga"]]
    assert sorted(vigentes["num_entrada"]) == ["1001", "1002", "1003", "1004", "1005"]
    assert vigentes.set_index("num_entrada").loc["1001", "empresa"] == "ALFA SOLUCIONES NUEVAS SL"
    # La lectura anterior se conserva: el 1001 con el nombre de antes y el 1006
    assert sorted(zip(antiguas["num_entrada"], antiguas["empresa"])) == [
        ("1001", "ALFA SOLUCIONES SL"), ("1006", "ETA COMERCIAL SL")]
    assert set(antiguas["_ultima_descarga"]) == {fechas[0]}
    # Lo que no cambia es la misma fila, confirmada por el parse nuevo
    iguales = vigentes[vigentes["num_entrada"] != "1001"]
    assert set(iguales["_primera_descarga"]) == {fechas[0]}
    assert set(iguales["_ultima_descarga"]) == {fechas[-1]}
    # Cargos: el del 1001 (cambia la empresa) queda en sus dos versiones
    c1001 = car[car["num_entrada"] == "1001"].sort_values("_en_ultima_descarga")
    assert c1001[["empresa", "_en_ultima_descarga"]].values.tolist() == [
        ["ALFA SOLUCIONES SL", False], ["ALFA SOLUCIONES NUEVAS SL", True]]
    assert car.loc[car["num_entrada"] != "1001", "_en_ultima_descarga"].all()


def test_pdf_que_ya_no_da_cargos_retira_los_anteriores(fake_pdf, fechas, tmp_path):
    # Ámbito = PDF parseado con alguna fila, no "PDF con cargos": un PDF que ahora
    # da empresas pero ningún cargo deja sus cargos anteriores como no vigentes
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf[PDF_ENERO] = [PAG_ENERO]
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    bparser.run_batch(base, out, workers=1)
    fake_pdf[PDF_ENERO] = [PAG_ENERO.replace("Nombramientos. Apoderado: FULANO MENGANO JUAN. ", "")]
    bparser.run_batch(base, out, workers=1, reprocesar=True)
    emp, car = _tablas(out)
    assert len(car) == 1 and not car["_en_ultima_descarga"].any()
    assert emp.groupby("_en_ultima_descarga").size().to_dict() == {False: 1, True: 3}


def test_parse_fallido_o_vacio_no_retira_nada(fake_pdf, fechas, tmp_path, monkeypatch):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, out, workers=2)
    tablas = _foto(out, "*.parquet")
    parseados = []
    real = bparser.parse_single_pdf
    monkeypatch.setattr(bparser, "parse_single_pdf", lambda p: parseados.append(Path(p).name) or real(p))

    # Cambia en disco y no se puede leer: error, nada retirado ni reescrito, y la
    # siguiente ejecución lo reintenta
    pdf.write_bytes(b"%PDF-1.4 truncado")
    del fake_pdf[PDF_MADRID]
    for intento in (1, 2):
        bparser.run_batch(base, out, workers=2)
        assert parseados == [PDF_MADRID] * intento
        prog = json.loads((out / "borme_parse_progress.json").read_text())
        assert [Path(p).name for p in prog["errors"]] == [PDF_MADRID]
        assert _foto(out, "*.parquet") == tablas

    # Se lee pero sin ningún anuncio: tampoco retira nada, y ya no se reintenta
    fake_pdf[PDF_MADRID] = [PAG1.split("1001 - ")[0]]
    bparser.run_batch(base, out, workers=2)
    assert _foto(out, "*.parquet") == tablas
    prog = json.loads((out / "borme_parse_progress.json").read_text())
    assert prog["errors"] == [] and prog["done"] == ["2024/01/04/" + PDF_MADRID]
    progreso = _foto(out)
    bparser.run_batch(base, out, workers=2)
    assert _foto(out) == progreso


def test_reejecucion_sin_cambios_no_crea_version_nueva(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_ENERO: [PAG_ENERO]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    bparser.run_batch(base, out, workers=2)
    antes = _foto(out)
    bparser.run_batch(base, out, workers=2)
    bparser.run_batch(base, out, workers=2, resume=True)
    assert _foto(out) == antes
    assert not (out / "_historico").exists() and len(fechas) == 1


def test_salida_borrada_o_restaurada_de_una_copia_anterior_se_completa(fake_pdf, fechas, tmp_path):
    # Solo cuenta como parseado lo que tiene filas en la salida (como antes con --resume)
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_NUEVO: [PAG1], PDF_ENERO: [PAG_ENERO]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, out, workers=2)
    copia = {n: (out / n).read_bytes() for n in ("borme_empresas.parquet", "borme_cargos.parquet")}
    _pdf_file(base, dt.date(2024, 1, 5), PDF_NUEVO)
    bparser.run_batch(base, out, workers=2)

    for n, contenido in copia.items():  # se restaura la copia anterior (sin el PDF nuevo)
        (out / n).write_bytes(contenido)
    bparser.run_batch(base, out, workers=2)
    emp, car = _tablas(out)
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_MADRID: 6, PDF_NUEVO: 3}
    assert set(car["pdf_filename"]) == {PDF_MADRID, PDF_NUEVO}

    (out / "borme_cargos.parquet").unlink()  # tabla borrada
    bparser.run_batch(base, out, workers=2)
    emp, car = _tablas(out)
    assert set(car["pdf_filename"]) == {PDF_MADRID, PDF_NUEVO} and car["_en_ultima_descarga"].all()
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_MADRID: 6, PDF_NUEVO: 3}

    # Un PDF con parse vacío no necesita filas en la salida: no se reparsea cada vez
    fake_pdf[PDF_ENERO] = [PAG_ENERO.split("1 - ")[0]]
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    bparser.run_batch(base, out, workers=2)
    antes = _foto(out)
    bparser.run_batch(base, out, workers=2)
    assert _foto(out) == antes


def test_salida_guardada_con_guardar_registros(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_NUEVO: [PAG1]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, out, workers=2)
    antes = (out / "borme_empresas.parquet").read_bytes()
    _pdf_file(base, dt.date(2024, 1, 5), PDF_NUEVO)
    bparser.run_batch(base, out, workers=2)
    copias = sorted((out / "_historico").glob("borme_empresas__*.parquet"))
    assert len(copias) == 1 and copias[0].read_bytes() == antes
    assert len(list((out / "_historico").glob("borme_cargos__*.parquet"))) == 1


def test_versiones_de_historico_se_acumulan_en_orden(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID, b"%PDF-1.4 v1")
    # Como borme_scraper.py --comprobar cuando boe.es sirve otro contenido
    assert scraper.guardar_version(pdf, b"%PDF-1.4 v2") == "actualizado"
    copia, = (pdf.parent / "_historico").iterdir()
    fake_pdf.update({copia.name: [PAG1, PAG2], PDF_MADRID: [PAG1_V2, PAG2_V2]})
    bparser.run_batch(base, out, workers=2)

    emp, _ = _tablas(out)
    assert set(emp["pdf_filename"]) == {PDF_MADRID}  # la copia cuenta como el mismo PDF
    assert set(emp["fecha_borme"].dt.strftime("%Y-%m-%d")) == {"2024-01-04"}
    antiguas = emp[~emp["_en_ultima_descarga"]]
    assert sorted(zip(antiguas["num_entrada"], antiguas["empresa"])) == [
        ("1001", "ALFA SOLUCIONES SL"), ("1006", "ETA COMERCIAL SL")]
    assert emp["_en_ultima_descarga"].sum() == 5


def test_version_antigua_ilegible_retiene_las_posteriores(fake_pdf, fechas, tmp_path):
    # Acumular la actual antes que una antigua dejaría vigente la antigua: se espera
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID, b"%PDF-1.4 v1")
    scraper.guardar_version(pdf, b"%PDF-1.4 v2")
    copia, = (pdf.parent / "_historico").iterdir()
    fake_pdf[PDF_MADRID] = [PAG1_V2, PAG2_V2]  # la copia aún no se puede leer
    bparser.run_batch(base, out, workers=2)
    assert not (out / "borme_empresas.parquet").exists()
    prog = json.loads((out / "borme_parse_progress.json").read_text())
    assert [Path(p).name for p in prog["errors"]] == [copia.name] and prog["done"] == []

    fake_pdf[copia.name] = [PAG1, PAG2]
    bparser.run_batch(base, out, workers=2)
    emp, _ = _tablas(out)
    assert sorted(emp.loc[~emp["_en_ultima_descarga"], "num_entrada"]) == ["1001", "1006"]
    assert emp["_en_ultima_descarga"].sum() == 5


def test_pdf_cambiado_tras_parsearlo_solo_se_parsea_la_version_nueva(fake_pdf, fechas, tmp_path, monkeypatch):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID, b"%PDF-1.4 v1")
    bparser.run_batch(base, out, workers=2)
    assert scraper.guardar_version(pdf, b"%PDF-1.4 v2") == "actualizado"
    fake_pdf[PDF_MADRID] = [PAG1_V2, PAG2_V2]
    parseados = []
    real = bparser.parse_single_pdf
    monkeypatch.setattr(bparser, "parse_single_pdf", lambda p: parseados.append(Path(p).name) or real(p))

    bparser.run_batch(base, out, workers=2)
    assert parseados == [PDF_MADRID]  # la copia de _historico/ ya se parseó cuando era la actual
    emp, _ = _tablas(out)
    assert sorted(emp.loc[~emp["_en_ultima_descarga"], "num_entrada"]) == ["1001", "1006"]
    parseados.clear()
    bparser.run_batch(base, out, workers=2)
    assert parseados == []


def test_resume_no_vuelve_a_parsear_lo_de_la_ejecucion_interrumpida(fake_pdf, fechas, tmp_path, monkeypatch):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_NUEVO: [PAG1]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    monkeypatch.setattr(bparser, "BATCH_SIZE", 1)
    real = bparser._preparar
    monkeypatch.setattr(bparser, "_preparar", lambda *a: (_ for _ in ()).throw(KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        bparser.run_batch(base, out, workers=1)
    monkeypatch.setattr(bparser, "_preparar", real)

    _pdf_file(base, dt.date(2024, 1, 5), PDF_NUEVO)
    parseados = []
    real_parse = bparser.parse_single_pdf
    monkeypatch.setattr(bparser, "parse_single_pdf", lambda p: parseados.append(Path(p).name) or real_parse(p))
    bparser.run_batch(base, out, workers=1, resume=True)
    assert parseados == [PDF_NUEVO]
    emp, _ = _tablas(out)
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_MADRID: 6, PDF_NUEVO: 3}
    assert emp["_en_ultima_descarga"].all() and not (out / "borme_parse_parts").exists()


def test_salida_del_parser_anterior_se_acumula(fake_pdf, fechas, tmp_path):
    # Salida y progreso de la versión anterior del parser (sin columnas de control
    # ni registro de versiones), con un PDF que ya no está en disco
    base = tmp_path / "borme_pdfs"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_ENERO: [PAG_ENERO]})
    _pdf_file(tmp_path / "otra_maquina", dt.date(2024, 1, 2), PDF_ENERO)
    enero_emp, enero_car = _salida_antigua(tmp_path / "otra_maquina")
    pdf = _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    emp_viejo, car_viejo = _salida_antigua(base)
    # El parser antiguo dejaba "Sociedad unipersonal" en el nombre del 1003
    emp_viejo.loc[emp_viejo["num_entrada"] == "1003", "empresa"] = "GAMMA LOGISTICA SL. Sociedad unipersonal"
    pd.concat([emp_viejo, enero_emp]).to_parquet(base / "borme_empresas.parquet", index=False)
    pd.concat([car_viejo, enero_car]).to_parquet(base / "borme_cargos.parquet", index=False)
    (base / "borme_parse_progress.json").write_text(json.dumps({"done": [str(pdf)], "errors": []}))

    bparser.run_batch(base, base, workers=2)
    emp, car = _tablas(base)
    assert emp.groupby("pdf_filename").size().to_dict() == {PDF_ENERO: 3, PDF_MADRID: 7}
    assert emp.loc[~emp["_en_ultima_descarga"], ["num_entrada", "empresa"]].values.tolist() == [
        ["1003", "GAMMA LOGISTICA SL. Sociedad unipersonal"]]
    enero = emp[emp["pdf_filename"] == PDF_ENERO]
    assert enero["_en_ultima_descarga"].all() and enero["_primera_descarga"].isna().all()
    assert car["_en_ultima_descarga"].all() and len(car) == 7
    assert len(list((base / "_historico").glob("borme_empresas__*.parquet"))) == 1


def _publicado(df):
    """Como en el release: sin columnas de control ni objeto_social."""
    return df.drop(columns=[c for c in META + ["objeto_social", "_origen"] if c in df.columns])


def test_semilla_solo_anade_los_actos_que_faltan(fake_pdf, fechas, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    bparser.run_batch(base, tmp_path / "ref", workers=1)
    # Publicado: los mismos actos leídos por el parser antiguo (otro nombre en el
    # 1002: no cuenta, la clave es el acto), uno que el parser actual no da (1007)
    # y un PDF que no está en disco con un anuncio partido en dos filas
    pub = _publicado(_tablas(tmp_path / "ref")[0])
    pub.loc[pub["num_entrada"] == "1002", "empresa"] = "BETA INVERSIONES SA. Sociedad unipersonal"
    extra = pub.iloc[[0, 0, 0]].copy()
    extra["num_entrada"] = ["1007", "5000", "5000"]
    extra["pdf_filename"] = [PDF_MADRID, PDF_OTRO, PDF_OTRO]
    extra["empresa"] = ["IOTA SL", "CAJA DE AHORROS DE SALAMANCA Y SORIA,", "AGREDA"]
    semilla = tmp_path / "borme_empresas_pub.parquet"
    pd.concat([pub, extra], ignore_index=True).to_parquet(semilla, index=False)

    bparser.run_batch(base, out, workers=2, semillas=[semilla])
    emp, _ = _tablas(out)
    sembradas, parse = emp[emp["_origen"].notna()], emp[emp["_origen"].isna()]
    assert sorted(zip(sembradas["pdf_filename"], sembradas["num_entrada"], sembradas["empresa"])) == [
        (PDF_OTRO, "5000", "AGREDA"), (PDF_OTRO, "5000", "CAJA DE AHORROS DE SALAMANCA Y SORIA,"),
        (PDF_MADRID, "1007", "IOTA SL")]
    assert set(sembradas["_origen"]) == {"release v2026.02"} and not sembradas["_en_ultima_descarga"].any()
    assert sembradas["objeto_social"].isna().all()
    # Las filas del parse, intactas
    assert len(parse) == 6 and parse["_en_ultima_descarga"].all()
    assert parse.set_index("num_entrada").loc["1002", "empresa"] == "BETA INVERSIONES SA"

    # Sembrar otra vez no añade nada ni crea otra versión de la salida
    tablas = _foto(out, "*.parquet")
    bparser.run_batch(base, out, workers=2, semillas=[semilla])
    assert _foto(out, "*.parquet") == tablas and not (out / "_historico").exists()

    # Llega el PDF que solo conocía la semilla: su parse entra como vigente, lo
    # sembrado se conserva y el detector usa solo lo vigente de cada PDF
    fake_pdf[PDF_OTRO] = [PAG_OTRO]
    _pdf_file(base, dt.date(2023, 5, 29), PDF_OTRO)
    bparser.run_batch(base, out, workers=2, semillas=[semilla])
    emp, car = _tablas(out)
    otro = emp[emp["pdf_filename"] == PDF_OTRO]
    assert sorted(zip(otro["empresa"], otro["_en_ultima_descarga"])) == [
        ("AGREDA", False), ("CAJA DE AHORROS DE SALAMANCA Y SORIA,", False), ("CAJA DE AHORROS NUEVA SA", True)]
    vig_emp, vig_car = match.filas_vigentes(emp, car)
    assert len(vig_emp) == 7 and vig_emp["_en_ultima_descarga"].all()


def test_semilla_de_cargos_y_anonimizar_conservan_el_hash_publicado(fake_pdf, tmp_path, monkeypatch):
    base, data = tmp_path / "borme_pdfs", tmp_path / "data"
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    semilla = tmp_path / "borme_cargos_pub.parquet"
    pd.DataFrame({
        "fecha_borme": pd.to_datetime(["2024-01-04", "2024-01-04", "2023-05-29"]),
        "num_entrada": ["1001", "1003", "5000"],
        "empresa": ["ALFA SOLUCIONES SL", "GAMMA LOGISTICA SL", "CAJA DE AHORROS NUEVA SA"],
        "empresa_norm": ["ALFA SOLUCIONES", "GAMMA LOGISTICA", "CAJA DE AHORROS NUEVA"],
        "provincia": ["MADRID"] * 3,
        "hoja_registral": ["M 793456", "M 5555", "M 1"],
        "tipo_acto": ["nombramiento"] * 3,
        "cargo": ["Adm. Unico", "Socio único", "Apoderado"],
        "persona_hash": ["hash_1001", "hash_1003", "hash_5000"],
        "pdf_filename": [PDF_MADRID, PDF_MADRID, PDF_OTRO],
    }).to_parquet(semilla, index=False)

    _run_cli(monkeypatch, "borme_batch_parser.py", "--input", base, "--workers", 1, "--semilla", semilla)
    car = pd.read_parquet(base / "borme_cargos.parquet")
    sembradas = car[car["_origen"].notna()]
    # El 1001 ya tiene cargos en el parse: no entra; el 1003 no tiene ninguno y el 5000 no está en disco
    assert sorted(sembradas["num_entrada"]) == ["1003", "5000"]
    assert sembradas["persona"].isna().all() and not sembradas["_en_ultima_descarga"].any()
    assert "_origen" not in pd.read_parquet(base / "borme_empresas.parquet").columns

    _run_cli(monkeypatch, "borme_anonymize.py", "--input", base, "--output", data)
    pub = pd.read_parquet(data / "borme_cargos_pub.parquet")
    assert "persona" not in pub.columns and len(pub) == len(car)
    assert list(pub.columns[-4:]) == ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga", "_origen"]
    assert set(pub.loc[pub["_origen"].notna(), "persona_hash"]) == {"hash_1003", "hash_5000"}
    assert set(pub.loc[pub["_origen"].isna(), "persona_hash"]) == {anon.hash_persona(n) for n in (
        "FULANO MENGANO JUAN", "ZUTANO PERENGANO ANA", "PRUEBA EJEMPLO LUIS", "AUDITORES EJEMPLO SL")}
    emp_pub = pd.read_parquet(data / "borme_empresas_pub.parquet")
    assert list(emp_pub.columns[-3:]) == META and "objeto_social" not in emp_pub.columns


def test_semilla_sin_ningun_pdf_en_disco(fake_pdf, tmp_path):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    base.mkdir()
    semilla = tmp_path / "borme_empresas_pub.parquet"
    _publicado(_empresas_privadas()).to_parquet(semilla, index=False)
    bparser.run_batch(base, out, workers=1, semillas=[semilla])
    emp = pd.read_parquet(out / "borme_empresas.parquet")
    assert len(emp) == 3 and set(emp["_origen"]) == {"release v2026.02"}
    assert not emp["_en_ultima_descarga"].any()
    assert not (out / "borme_cargos.parquet").exists()


# ═════════════════════════════════════════════
#  borme_anonymize.py
# ═════════════════════════════════════════════
def _empresas_privadas():
    return pd.DataFrame({
        "fecha_borme": pd.to_datetime(["2024-01-04", "2024-01-04", "2024-02-01"]),
        "num_borme": [3, 3, 20],
        "num_entrada": ["1001", "1002", "9000"],
        "empresa": ["ALFA SOLUCIONES SL", "FULANO MENGANO, JUAN", "FULANO MENGANO, JUAN"],
        "empresa_norm": ["ALFA SOLUCIONES", "FULANO MENGANO, JUAN", "FULANO MENGANO, JUAN"],
        "provincia": ["MADRID"] * 3,
        "cod_provincia": ["28"] * 3,
        "tipo_borme": ["A"] * 3,
        "actos": ["Constitución|Nombramientos", "Empresario Individual", "Nombramientos"],
        "domicilio": ["C/ MAYOR 5 2º B (MADRID)",
                      "C/ LUNA 3 MADRID. Estado Civil : Soltero . Datos registrales. T 1 , F 1",
                      None],
        "capital_euros": [3000.0, np.nan, np.nan],
        "objeto_social": ["Consultoría", None, None],
        "hoja_registral": ["M 1", "M 2", "M 2"],
        "pdf_filename": [PDF_MADRID, PDF_MADRID, "BORME-A-2024-20-28.pdf"],
    })


def _cargos_privados():
    return pd.DataFrame({
        "fecha_borme": pd.to_datetime(["2024-01-04", "2024-02-01"]),
        "num_entrada": ["1001", "9000"],
        "empresa": ["ALFA SOLUCIONES SL", "FULANO MENGANO, JUAN"],
        "empresa_norm": ["ALFA SOLUCIONES", "FULANO MENGANO, JUAN"],
        "provincia": ["MADRID", "MADRID"],
        "hoja_registral": ["M 1", "M 2"],
        "tipo_acto": ["nombramiento", "nombramiento"],
        "cargo": ["Adm. Unico", "Apoderado"],
        "persona": ["ZUTANO PERENGANO ANA", "PRUEBA EJEMPLO LUIS"],
        "pdf_filename": [PDF_MADRID, "BORME-A-2024-20-28.pdf"],
    })


def _textos(df):
    return {v for col in df.columns for v in df[col].dropna().astype(str)}


def test_hash_persona():
    h = anon.hash_persona("GARCIA LOPEZ JUAN")
    assert len(h) == 16 and int(h, 16) >= 0
    assert anon.hash_persona("  garcia lopez juan ") == h  # determinista y normalizado
    assert anon.hash_persona("GARCIA LOPEZ JUAN", salt="otra") != h
    assert anon.hash_persona(None) == "" and anon.hash_persona(float("nan")) == ""


def test_anonymize_cli_solo_hashea_personas(monkeypatch, tmp_path):
    src, out = tmp_path / "borme_pdfs", tmp_path / "data"
    src.mkdir()
    emp_priv = _empresas_privadas()
    emp_priv.to_parquet(src / "borme_empresas.parquet", index=False)
    _cargos_privados().to_parquet(src / "borme_cargos.parquet", index=False)

    _run_cli(monkeypatch, "borme_anonymize.py", "--input", src, "--output", out)

    emp = pd.read_parquet(out / "borme_empresas_pub.parquet")
    car = pd.read_parquet(out / "borme_cargos_pub.parquet")
    assert "objeto_social" not in emp.columns
    assert "persona" not in car.columns and "persona_hash" in car.columns
    textos = _textos(emp) | _textos(car)
    for nombre in ["ZUTANO", "PRUEBA"]:
        assert not any(nombre in t for t in textos), nombre
    assert car["persona_hash"].tolist() == [anon.hash_persona("ZUTANO PERENGANO ANA"),
                                            anon.hash_persona("PRUEBA EJEMPLO LUIS")]
    # Lo demás se publica tal como aparece en el BORME: domicilio completo y
    # nombre de la empresa (también el de los empresarios individuales)
    assert emp["domicilio"].tolist()[:2] == emp_priv["domicilio"].tolist()[:2]
    assert emp["empresa_norm"].tolist() == emp_priv["empresa_norm"].tolist()
    assert car["empresa_norm"].tolist() == ["ALFA SOLUCIONES", "FULANO MENGANO, JUAN"]


def test_build_admin_graph():
    car = pd.DataFrame({
        "persona_hash": ["p1", "p1", "p1", "p2", "p2", "p3"],
        "empresa_norm": ["A", "B", "C", "A", "B", "A"],
        "tipo_acto": ["nombramiento", "nombramiento", "cese", "reeleccion", "nombramiento", "nombramiento"],
    })
    g = anon.build_admin_graph(car)
    assert g[["empresa_a", "empresa_b", "n_admins_compartidos", "admin_hashes"]].values.tolist() == [
        ["A", "B", 2, "p1|p2"]]
    assert anon.build_admin_graph(car.iloc[[5]]).empty


# ═════════════════════════════════════════════
#  borme_placsp_match.py
# ═════════════════════════════════════════════
@pytest.mark.parametrize("borme, placsp", [
    ("ACME SL", "ACME, S.L."),
    ("ACME SL", "Acme S.L.U."),
    ("ACME SL", "acme sl"),
    ("ACME SA", "ACME, S.A.U."),
    ("ACME SL", "ACME SL (EN LIQUIDACION)"),
    ("CONSTRUCCIONES PEÑA SL", "Construcciones Peña, S.L."),
    ("CENTRO DE FORMACION MARITIMA SL", "CENTRO DE FORMACIÓN MARÍTIMA, S.L.U."),
    ("GAMMA LOGISTICA SL. Sociedad unipersonal", "GAMMA LOGISTICA S.L."),
    # Variantes que antes no casaban
    ("ACME SOCIEDAD DE RESPONSABILIDAD LIMITADA", "ACME S.L."),
    ("ACME SOCIEDAD DE RESPONSABILIDAD LIMITADA LABORAL", "ACME, S.L.L."),
    ("ACME SL", "ACME SOCIEDAD LIMITADA UNIPERSONAL"),
    ("ACME SA", "ACME, S.A. UNIPERSONAL"),
    ("ACME SOCIEDAD ANONIMA LABORAL", "ACME, S.A.L."),
    ("ACME SAL", "ACME SOCIEDAD ANÓNIMA LABORAL"),
])
def test_normalize_empresa_mismo_nombre(borme, placsp):
    assert match.normalize_empresa(borme) == match.normalize_empresa(placsp) != ""


@pytest.mark.parametrize("a, b", [
    ("ACME SL", "ACMEX SL"),
    ("CONSTRUCCIONES PEÑA SL", "CONSTRUCCIONES PENA SL"),
])
def test_normalize_empresa_nombres_distintos(a, b):
    assert match.normalize_empresa(a) != match.normalize_empresa(b)


def _borme_match(dir_):
    emp = pd.DataFrame({
        "fecha_borme": pd.to_datetime(["2023-03-01", "2020-01-10", "2023-09-01", "2022-01-10",
                                       "2015-05-05", "2019-01-01", "2019-01-01", "2023-01-15"]),
        "empresa": ["NUEVA SL", "PEQUEÑA SA", "CERRADA SL", "CONCURSO SL",
                    "GRANDE SOCIEDAD DE RESPONSABILIDAD LIMITADA", "NO ADJUDICATARIA UNO SL",
                    "NO ADJUDICATARIA DOS SL", ""],
        "actos": ["Constitución|Nombramientos", "Ampliación de capital", "Disolución|Extinción",
                  "Situación concursal", "Nombramientos", "Nombramientos", "Nombramientos",
                  "Constitución"],
        "capital_euros": [3000.0, 3000.0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
    })
    emp["empresa_norm"] = emp["empresa"]
    emp.to_parquet(dir_ / "borme_empresas.parquet", index=False)
    car = pd.DataFrame({
        "fecha_borme": pd.to_datetime(["2023-03-01"] * 4),
        "empresa": ["NUEVA SL", "PEQUEÑA SA", "NO ADJUDICATARIA UNO SL", "NO ADJUDICATARIA DOS SL"],
        "tipo_acto": ["nombramiento"] * 4,
        "cargo": ["Adm. Unico"] * 4,
        "persona": ["ADMIN COMUN", "ADMIN COMUN", "OTRO ADMIN", "OTRO ADMIN"],
    })
    car["empresa_norm"] = car["empresa"]
    car.to_parquet(dir_ / "borme_cargos.parquet", index=False)


def _placsp(path):
    adj = ["Nueva, S.L.", "PEQUEÑA, S.A.", "Cerrada S.L.", "CONCURSO, S.L.", "GRANDE, S.L.", "-", None]
    n = len(adj)
    fechas = [dt.date(2023, 6, 1), dt.date(2023, 2, 1), dt.date(2023, 5, 1), dt.date(2023, 2, 1),
              dt.date(2023, 2, 1), dt.date(2023, 2, 1), None]
    df = pd.DataFrame({
        "id": [f"id{i}" for i in range(n)],
        "expediente": [f"EXP{i}" for i in range(n)],
        "objeto": ["Obra"] * n,
        "organo_contratante": ["Ayuntamiento"] * n,
        "tipo_contrato": ["Obras"] * n,
        "procedimiento": ["Abierto"] * n,
        "estado": ["Resuelta"] * n,
        # Tipos como en licitaciones_espana.parquet: importes Float64 y fechas date32
        "importe_sin_iva": pd.array([1e5] * n, dtype="Float64"),
        "importe_con_iva": pd.array([1.21e5] * n, dtype="Float64"),
        "importe_adjudicacion": pd.array([50000, 250000, 60000, 70000, 80000, 90000, None], dtype="Float64"),
        "importe_adj_con_iva": pd.array([None] * n, dtype="Float64"),
        "adjudicatario": adj,
        "nif_adjudicatario": ["B00000000"] * n,
        "num_ofertas": [1] * n,
        "fecha_adjudicacion": fechas,
        "fecha_publicacion": fechas,
        "nuts": ["ES300"] * n,
        "urgencia": [np.nan] * n,
    })
    df.to_parquet(path, index=False)


def test_placsp_match_cli(monkeypatch, tmp_path):
    borme = tmp_path / "borme_pdfs"
    borme.mkdir()
    _borme_match(borme)
    _placsp(tmp_path / "licitaciones_espana.parquet")
    out = tmp_path / "anomalias"

    _run_cli(monkeypatch, "borme_placsp_match.py", "--borme", borme,
             "--placsp", tmp_path / "licitaciones_espana.parquet", "--output", out)

    f1 = pd.read_parquet(out / "flag1_recien_creada.parquet")
    assert f1[["adj_norm", "dias_desde_constitucion"]].values.tolist() == [["NUEVA", 92]]
    f2 = pd.read_parquet(out / "flag2_capital_ridiculo.parquet")
    assert f2[["adj_norm", "capital_euros", "ratio_importe_capital"]].values.tolist() == [
        ["PEQUEÑA", 3000.0, 250000 / 3000]]
    f3 = pd.read_parquet(out / "flag3_multi_admin.parquet")
    # Solo administradores comunes a empresas ADJUDICATARIAS (antes: todo el BORME)
    assert f3.values.tolist() == [["ADMIN COMUN", 2]]
    f4 = pd.read_parquet(out / "flag4_disolucion.parquet")
    assert f4[["adj_norm", "dias_hasta_disolucion"]].values.tolist() == [["CERRADA", 123]]
    f5 = pd.read_parquet(out / "flag5_concursal.parquet")
    assert f5["adj_norm"].tolist() == ["CONCURSO"]
    for f in (f1, f2, f4, f5):
        assert {"id", "adjudicatario", "importe_adjudicacion", "organo_contratante", "objeto"} <= set(f.columns)


def test_run_matching_no_casa_nombres_vacios(tmp_path):
    borme = tmp_path / "b"
    borme.mkdir()
    _borme_match(borme)
    _placsp(tmp_path / "p.parquet")
    df_placsp = match.load_placsp(tmp_path / "p.parquet")
    assert (df_placsp["adj_norm"] == "").sum() == 1  # "-"
    match.run_matching(borme, tmp_path / "p.parquet", tmp_path / "out")
    for name in ["flag1_recien_creada", "flag2_capital_ridiculo", "flag4_disolucion", "flag5_concursal"]:
        assert "" not in set(pd.read_parquet(tmp_path / "out" / f"{name}.parquet")["adj_norm"])


def test_load_placsp_una_fila_por_licitacion(tmp_path):
    # licitaciones_espana.parquet guarda cada actualización del ATOM como otra fila
    borme = tmp_path / "b"
    borme.mkdir()
    _borme_match(borme)
    _placsp(tmp_path / "p.parquet")
    df = pd.read_parquet(tmp_path / "p.parquet")
    df["fecha_updated"] = pd.Timestamp("2023-07-01", tz="UTC")
    vieja = df.iloc[[0]].assign(importe_adjudicacion=pd.array([1.0], dtype="Float64"),
                                fecha_updated=pd.Timestamp("2023-06-02", tz="UTC"))
    pd.concat([df, vieja, vieja], ignore_index=True).to_parquet(tmp_path / "p.parquet", index=False)

    placsp = match.load_placsp(tmp_path / "p.parquet")
    assert placsp["id"].is_unique
    match.run_matching(borme, tmp_path / "p.parquet", tmp_path / "out")
    f1 = pd.read_parquet(tmp_path / "out" / "flag1_recien_creada.parquet")
    assert f1[["id", "importe_adjudicacion"]].values.tolist() == [["id0", 50000.0]]


def test_run_matching_sin_adjudicaciones_no_rompe(tmp_path):
    borme = tmp_path / "b"
    borme.mkdir()
    _borme_match(borme)
    _placsp(tmp_path / "p.parquet")
    df = pd.read_parquet(tmp_path / "p.parquet")
    df["adjudicatario"] = None  # p.ej. un parquet filtrado sin adjudicaciones
    df.to_parquet(tmp_path / "p.parquet", index=False)
    match.run_matching(borme, tmp_path / "p.parquet", tmp_path / "out")  # antes: ZeroDivisionError
    assert pd.read_parquet(tmp_path / "out" / "flag1_recien_creada.parquet").empty


def test_filas_vigentes_una_version_de_cada_acto():
    emp = pd.DataFrame({
        "pdf_filename": ["P1", "P1", "P1", "P2", "P2"],
        "num_entrada": ["1", "1", "2", "9", "9"],
        "_en_ultima_descarga": [True, False, False, False, False],
        "_origen": [None, None, "release v2026.02", "release v2026.02", "release v2026.02"],
    })
    car = pd.DataFrame({"pdf_filename": ["P1", "P1", "P2"], "_en_ultima_descarga": [False, True, False],
                        "cargo": ["A", "B", "C"]})
    e, c = match.filas_vigentes(emp, car)
    # P1: solo su último parse; P2 solo lo conoce la semilla: lo de la semilla
    assert e[["pdf_filename", "num_entrada"]].values.tolist() == [["P1", "1"], ["P2", "9"], ["P2", "9"]]
    assert c["cargo"].tolist() == ["B", "C"]
    # Tablas de la versión anterior del parser (sin columnas de control): todo
    e, c = match.filas_vigentes(emp.drop(columns="_en_ultima_descarga"), car)
    assert len(e) == 5 and len(c) == 3


def test_placsp_match_usa_la_version_vigente_de_cada_acto(tmp_path):
    borme = tmp_path / "b"
    borme.mkdir()
    _borme_match(borme)
    emp = pd.read_parquet(borme / "borme_empresas.parquet")
    emp["pdf_filename"] = [f"BORME-A-2023-{i}-28.pdf" for i in range(len(emp))]
    emp["_en_ultima_descarga"] = True
    # El parse actual lee otro capital para PEQUEÑA SA; la lectura anterior (3.000 €)
    # se conserva como no vigente y no debe contar para el flag 2
    pequena = emp["empresa"] == "PEQUEÑA SA"
    anterior = emp[pequena].assign(_en_ultima_descarga=False)
    emp.loc[pequena, "capital_euros"] = 300000.0
    pd.concat([emp, anterior], ignore_index=True).to_parquet(borme / "borme_empresas.parquet", index=False)
    _placsp(tmp_path / "p.parquet")
    match.run_matching(borme, tmp_path / "p.parquet", tmp_path / "out")
    assert pd.read_parquet(tmp_path / "out" / "flag2_capital_ridiculo.parquet").empty
    f1 = pd.read_parquet(tmp_path / "out" / "flag1_recien_creada.parquet")
    assert f1["adj_norm"].tolist() == ["NUEVA"]  # lo demás, como siempre


def test_flag3_solo_empresas_indicadas():
    car = pd.DataFrame({
        "persona": ["A", "A", "B", "B"],
        "empresa_norm": ["X", "Y", "Z", "W"],
        "tipo_acto": ["nombramiento"] * 4,
    })
    assert match.flag_mismos_administradores(car).to_dict() == {"A": 2, "B": 2}
    assert match.flag_mismos_administradores(car, {"X", "Y"}).to_dict() == {"A": 2}


# ═════════════════════════════════════════════
#  borme_scraper.py
# ═════════════════════════════════════════════
def _resp(status=200, text="", content=None):
    r = requests.Response()
    r.status_code = status
    r._content = content if content is not None else text.encode("utf-8")
    r.encoding = "utf-8"
    return r


def _index(d, files):
    base = f"/borme/dias/{d:%Y/%m/%d}/pdfs/"
    links = "".join(f'<li><a href="{base}{f}" title="Descargar PDF">{f}</a></li>\n' for f in files)
    return f"<html><body><h3>SECCIÓN PRIMERA</h3><ul>\n{links}</ul></body></html>"


def _index_url(d):
    return f"https://www.boe.es/borme/dias/{d:%Y/%m/%d}/index.php"


def _pdf_url(d, f):
    return f"https://www.boe.es/borme/dias/{d:%Y/%m/%d}/pdfs/{f}"


class FakeBOE:
    """Servidor simulado: url -> respuesta, excepción o callable."""

    def __init__(self, monkeypatch):
        self.routes = {}
        self.calls = []
        boe = self
        monkeypatch.setattr(requests.Session, "get", lambda self_, url, **kw: boe.get(url))

    def get(self, url):
        self.calls.append(url)
        r = self.routes.get(url, _resp(404, "Error 404"))
        if isinstance(r, Exception):
            raise r
        return r() if callable(r) else r

    def publish(self, d, files):
        self.routes[_index_url(d)] = _resp(200, _index(d, files))
        for f in files:
            self.routes[_pdf_url(d, f)] = _resp(200, content=b"%PDF-1.4 " + f.encode())


def _state(out):
    return json.loads((out / "scraper_state.json").read_text())


JUE, VIE, LUN = dt.date(2024, 1, 4), dt.date(2024, 1, 5), dt.date(2024, 1, 8)


def test_scraper_cli_estructura_manifest_y_estado(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf", "BORME-S-2024-3.pdf", "BORME-A-2024-3-28.pdf"])
    boe.routes[_index_url(VIE)] = _resp(200, "<html>Hoy no se ha publicado BORME</html>")
    boe.publish(LUN, ["BORME-A-2024-4-08.pdf"])
    out = tmp_path / "borme_pdfs"

    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", LUN,
             "--output", out, "--delay", 0)

    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").read_bytes().startswith(b"%PDF-")
    assert (out / "2024/01/04/BORME-S-2024-3.pdf").exists()
    assert (out / "2024/01/08/BORME-A-2024-4-08.pdf").exists()
    # Fin de semana: ni se pide
    assert not any("/2024/01/06/" in u or "/2024/01/07/" in u for u in boe.calls)
    with open(out / "manifest.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert list(rows[0].keys()) == ["date", "pdf_filename", "tipo", "url", "size_bytes", "sha256"]
    assert [(r["date"], r["pdf_filename"], r["tipo"]) for r in rows] == [
        ("2024-01-04", "BORME-A-2024-3-28.pdf", "A"),
        ("2024-01-04", "BORME-S-2024-3.pdf", "S"),
        ("2024-01-08", "BORME-A-2024-4-08.pdf", "A")]
    st = _state(out)
    assert st["last_completed_date"] == "2024-01-08"
    assert st["total_pdfs"] == 3 and st["errors"] == []


def test_scraper_dia_fallido_se_reintenta_con_resume(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.routes[_index_url(JUE)] = _resp(503, "Service Unavailable")
    boe.publish(VIE, ["BORME-A-2024-4-28.pdf"])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", VIE, "--output", out, "--delay", 0]

    _run_cli(monkeypatch, "borme_scraper.py", *args)
    st = _state(out)
    # Antes: el 503 se trataba como "sin BORME" y last_completed_date = 2024-01-05
    assert st["last_completed_date"] == "2024-01-03"
    assert [e["date"] for e in st["errors"]] == ["2024-01-04"]

    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--resume")
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").exists()
    assert _state(out)["last_completed_date"] == "2024-01-05"
    # El PDF del viernes ya estaba: no se vuelve a descargar
    assert boe.calls.count(_pdf_url(VIE, "BORME-A-2024-4-28.pdf")) == 1


def test_scraper_pdf_fallido_deja_el_dia_pendiente(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf", "BORME-A-2024-3-08.pdf"])
    boe.routes[_pdf_url(JUE, "BORME-A-2024-3-08.pdf")] = requests.ConnectionError("reset")
    boe.publish(VIE, ["BORME-A-2024-4-28.pdf"])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", VIE, "--output", out, "--delay", 0]

    _run_cli(monkeypatch, "borme_scraper.py", *args)
    st = _state(out)
    assert st["last_completed_date"] == "2024-01-03"
    assert st["total_pdfs"] == 2  # lo descargado cuenta aunque el día quede pendiente

    boe.publish(JUE, ["BORME-A-2024-3-28.pdf", "BORME-A-2024-3-08.pdf"])
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--resume")
    assert (out / "2024/01/04/BORME-A-2024-3-08.pdf").exists()
    assert boe.calls.count(_pdf_url(JUE, "BORME-A-2024-3-28.pdf")) == 1
    assert _state(out)["last_completed_date"] == "2024-01-05"


def test_scraper_escritura_cortada_no_deja_pdf_truncado(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    out = tmp_path / "borme_pdfs"
    out.mkdir()
    manifest = scraper.Manifest(out)
    manifest.open()

    class _DiscoLleno:
        def __init__(self, f):
            self.f = f

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.f.close()
            return False

        def write(self, data):
            self.f.write(data[: len(data) // 2])
            raise OSError(28, "No space left on device")

    real_open = open
    monkeypatch.setattr(scraper, "open", lambda p, m="r", *a, **k: (
        _DiscoLleno(real_open(p, m, *a, **k)) if m == "wb" else real_open(p, m, *a, **k)), raising=False)
    with pytest.raises(OSError):
        scraper.scrape_day(requests.Session(), JUE, out, manifest, set(), 0)
    monkeypatch.delattr(scraper, "open")

    n, _ = scraper.scrape_day(requests.Session(), JUE, out, manifest, set(), 0)
    manifest.close()
    assert n == 1
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").read_bytes() == b"%PDF-1.4 BORME-A-2024-3-28.pdf"


def _args(out, start, end, workers):
    return Namespace(output=str(out), start=start.isoformat(), end=end.isoformat(),
                     resume=False, delay=0, workers=workers)


def test_scraper_paralelo_marca_de_agua_no_salta_dias(monkeypatch, tmp_path):
    dias = [dt.date(2024, 1, d) for d in (8, 9, 10, 11, 12)]
    terminados = set()
    lock = threading.Lock()

    def fake_scrape_day(session, d, *a, **k):
        time.sleep(0.4 if d == dias[0] else 0.02)  # el primer día termina el último
        if d == dias[2]:
            raise RuntimeError("HTTP 503 en índice")
        with lock:
            terminados.add(d)
        return 1, 10

    marcas = []
    orig = scraper.ScraperState.mark_completed

    def spy(self, d, n_pdfs, n_bytes):
        with lock:
            # Invariante de --resume: todo día laborable <= marca está terminado
            marcas.append((d, all(x in terminados for x in dias if x <= d)))
        return orig(self, d, n_pdfs, n_bytes)

    monkeypatch.setattr(scraper, "scrape_day", fake_scrape_day)
    monkeypatch.setattr(scraper.ScraperState, "mark_completed", spy)
    scraper.run(_args(tmp_path, dias[0], dias[-1], workers=3))

    assert marcas and all(ok for _, ok in marcas)
    # El día 10 falló: no se puede retomar más allá del 9
    assert _state(tmp_path)["last_completed_date"] == "2024-01-09"


def test_scraper_paralelo_ctrl_c_cancela_dias_pendientes(monkeypatch, tmp_path):
    llamadas = []

    def fake_scrape_day(session, d, *a, **k):
        llamadas.append(d)
        if len(llamadas) == 1:
            raise KeyboardInterrupt
        time.sleep(0.05)
        return 0, 0

    monkeypatch.setattr(scraper, "scrape_day", fake_scrape_day)
    scraper.run(_args(tmp_path, dt.date(2024, 1, 1), dt.date(2024, 3, 29), workers=2))
    # Antes el with esperaba a los ~65 días encolados
    assert len(llamadas) < 10


def test_extract_pdf_links_tipos_y_duplicados():
    html = _index(JUE, ["BORME-A-2024-3-28.pdf", "BORME-B-2024-3-28.pdf", "BORME-C-2024-123.pdf",
                        "BORME-S-2024-3.pdf", "BORME-A-2024-3-28.pdf"])
    links = scraper.extract_pdf_links(html)
    assert [(link["pdf_filename"], link["tipo"]) for link in links] == [
        ("BORME-A-2024-3-28.pdf", "A"), ("BORME-B-2024-3-28.pdf", "B"),
        ("BORME-C-2024-123.pdf", "C"), ("BORME-S-2024-3.pdf", "S")]
    assert links[0]["url"] == "/borme/dias/2024/01/04/pdfs/BORME-A-2024-3-28.pdf"


# ═════════════════════════════════════════════
#  Pipeline del README (pasos 1-4) + borme_validate.py
# ═════════════════════════════════════════════
def test_pipeline_readme_completo(monkeypatch, tmp_path, fake_pdf, capsys):
    boe = FakeBOE(monkeypatch)
    boe.publish(dt.date(2024, 1, 2), [PDF_ENERO, "BORME-S-2024-1.pdf"])
    boe.publish(JUE, [PDF_MADRID])
    fake_pdf[PDF_ENERO] = [PAG_ENERO]
    fake_pdf[PDF_MADRID] = [PAG1, PAG2]
    pdfs, data, anomalias = tmp_path / "borme_pdfs", tmp_path / "borme_data", tmp_path / "anomalias"

    # 1. Descargar PDFs
    _run_cli(monkeypatch, "borme_scraper.py", "--start", "2024-01-01", "--end", JUE,
             "--output", pdfs, "--delay", 0)
    # 2. Parsear -> borme_empresas.parquet + borme_cargos.parquet (en --input)
    _run_cli(monkeypatch, "borme_batch_parser.py", "--input", pdfs, "--workers", 2)
    emp = pd.read_parquet(pdfs / "borme_empresas.parquet")
    car = pd.read_parquet(pdfs / "borme_cargos.parquet")
    assert len(emp) == 9 and len(car) == 7
    assert str(emp["fecha_borme"].dtype).startswith("datetime64")
    assert set(emp["fecha_borme"].dt.strftime("%Y-%m-%d")) == {"2024-01-02", "2024-01-04"}
    # 3. Anonimizar
    _run_cli(monkeypatch, "borme_anonymize.py", "--input", pdfs, "--output", data)
    emp_pub = pd.read_parquet(data / "borme_empresas_pub.parquet")
    car_pub = pd.read_parquet(data / "borme_cargos_pub.parquet")
    assert len(emp_pub) == 9 and len(car_pub) == 7
    textos = _textos(emp_pub) | _textos(car_pub)
    for nombre in ["FULANO", "ZUTANO", "PRUEBA"]:  # personas de los cargos
        assert not any(nombre in t for t in textos)
    assert "C/ MAYOR 5 2º B (MADRID)" in set(emp_pub["domicilio"])  # domicilio social tal cual
    # 4. Cruce con PLACSP
    _placsp(tmp_path / "licitaciones_espana.parquet")
    _run_cli(monkeypatch, "borme_placsp_match.py", "--borme", pdfs,
             "--placsp", tmp_path / "licitaciones_espana.parquet", "--output", anomalias)
    assert sorted(p.name for p in anomalias.iterdir()) == [
        "flag1_recien_creada.parquet", "flag2_capital_ridiculo.parquet", "flag3_multi_admin.parquet",
        "flag4_disolucion.parquet", "flag5_concursal.parquet"]

    # Validación del parser
    capsys.readouterr()
    _run_cli(monkeypatch, "borme_validate.py", "--input", pdfs, "--sample", 5)
    salida = capsys.readouterr().out
    assert "Total BORME-A PDFs: 2" in salida
    assert "Entradas totales: 9" in salida
    assert "Errores: 0" in salida


# ═════════════════════════════════════════════
#  borme_scraper.py — sumario de la API de datos abiertos (secciones A, B y C)
# ═════════════════════════════════════════════
def _sumario_url(d):
    return f"https://www.boe.es/datosabiertos/api/borme/sumario/{d:%Y%m%d}"


def _sumario_xml(d, files):
    """Respuesta de /datosabiertos/api/borme/sumario/{AAAAMMDD} (formato XML documentado)."""
    items = "".join(
        f"<item><identificador>{f[:-4]}</identificador>"
        f'<url_pdf szBytes="1">https://www.boe.es/borme/dias/{d:%Y/%m/%d}/pdfs/{f}</url_pdf>'
        f"<url_xml>https://www.boe.es/diario_borme/xml.php?id={f[:-4]}</url_xml></item>"
        for f in files)
    return (f'<?xml version="1.0" encoding="UTF-8"?><response><status><code>200</code></status>'
            f'<data><sumario><diario numero="3"><seccion codigo="A">{items}</seccion>'
            f"</diario></sumario></data></response>")


def _publica_sumario(boe, d, files):
    boe.routes[_sumario_url(d)] = _resp(200, _sumario_xml(d, files))
    for f in files:
        boe.routes[_pdf_url(d, f)] = _resp(200, content=b"%PDF-1.4 " + f.encode())


def _manifest(out):
    with open(out / "manifest.csv", newline="", encoding="utf-8") as f:
        return [(r["date"], r["pdf_filename"], r["tipo"]) for r in csv.DictReader(f)]


def test_scraper_une_indice_html_y_sumario_api(monkeypatch, tmp_path):
    # El índice HTML solo enlaza la sección A; el sumario oficial trae también B y C
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    _publica_sumario(boe, JUE, ["BORME-A-2024-3-28.pdf", "BORME-B-2024-3-28.pdf",
                                "BORME-C-2024-123.pdf", "BORME-C-2024-124.pdf"])
    out = tmp_path / "borme_pdfs"
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", out, "--delay", 0)
    assert _manifest(out) == [
        ("2024-01-04", "BORME-A-2024-3-28.pdf", "A"), ("2024-01-04", "BORME-B-2024-3-28.pdf", "B"),
        ("2024-01-04", "BORME-C-2024-123.pdf", "C"), ("2024-01-04", "BORME-C-2024-124.pdf", "C")]
    assert (out / "2024/01/04/BORME-C-2024-124.pdf").exists()
    # El PDF que está en los dos listados se descarga una vez
    assert boe.calls.count(_pdf_url(JUE, "BORME-A-2024-3-28.pdf")) == 1
    assert _state(out)["last_completed_date"] == "2024-01-04"


def test_scraper_indice_404_pero_sumario_con_pdfs(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    _publica_sumario(boe, JUE, ["BORME-A-2024-3-28.pdf"])
    out = tmp_path / "borme_pdfs"
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", out, "--delay", 0)
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").exists()


def test_scraper_frase_no_se_publica_no_descarta_pdfs_enlazados(monkeypatch, tmp_path):
    # Antes detect_no_borme() se miraba antes que los enlaces: una página con PDFs
    # que contuviera "no se publica" en cualquier texto daba el día por vacío
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    html = boe.routes[_index_url(JUE)].text.replace(
        "</body>", "<p>El BORME no se publica sábados, domingos ni festivos.</p></body>")
    boe.routes[_index_url(JUE)] = _resp(200, html)
    out = tmp_path / "borme_pdfs"
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", out, "--delay", 0, "--sin-sumario-api")
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").exists()


def test_scraper_sumario_caido_deja_el_dia_pendiente(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    boe.routes[_sumario_url(JUE)] = _resp(503, "Service Unavailable")
    boe.publish(VIE, ["BORME-A-2024-4-28.pdf"])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", VIE, "--output", out, "--delay", 0]

    _run_cli(monkeypatch, "borme_scraper.py", *args)
    st = _state(out)
    # Lo del índice se descarga, pero sin sumario pueden faltar secciones
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").exists()
    assert st["last_completed_date"] == "2024-01-03"
    assert [e["date"] for e in st["errors"]] == ["2024-01-04"]

    _publica_sumario(boe, JUE, ["BORME-A-2024-3-28.pdf", "BORME-C-2024-99.pdf"])
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--resume")
    assert (out / "2024/01/04/BORME-C-2024-99.pdf").exists()
    assert _state(out)["last_completed_date"] == "2024-01-05"
    assert boe.calls.count(_pdf_url(JUE, "BORME-A-2024-3-28.pdf")) == 1


def test_scraper_sumario_caido_sin_indice_no_se_da_por_dia_vacio(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.routes[_sumario_url(JUE)] = requests.ConnectionError("reset")
    out = tmp_path / "borme_pdfs"
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", out, "--delay", 0)
    assert _state(out)["last_completed_date"] == "2024-01-03"


def test_scraper_sumario_4xx_usa_solo_el_indice(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    boe.routes[_sumario_url(JUE)] = _resp(400, "Bad Request")
    out = tmp_path / "borme_pdfs"
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", out, "--delay", 0)
    assert (out / "2024/01/04/BORME-A-2024-3-28.pdf").exists()
    assert _state(out)["last_completed_date"] == "2024-01-04"


def test_scraper_sin_sumario_api_no_la_consulta(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, ["BORME-A-2024-3-28.pdf"])
    _run_cli(monkeypatch, "borme_scraper.py", "--start", JUE, "--end", JUE,
             "--output", tmp_path / "o", "--delay", 0, "--sin-sumario-api")
    assert not any("datosabiertos" in u for u in boe.calls)


def test_extract_pdf_links_sumario_xml_y_json():
    xml = _sumario_xml(JUE, ["BORME-A-2024-3-28.pdf", "BORME-C-2024-5.pdf", "BORME-A-2024-3-28.pdf"])
    json_txt = json.dumps({"data": {"sumario": {"diario": [{"seccion": [{"item": [
        {"url_pdf": {"texto": "https://www.boe.es/borme/dias/2024/01/04/pdfs/BORME-B-2024-3-08.pdf"}},
    ]}]}]}}}).replace("/", "\\/")   # json_encode de PHP escapa las barras
    assert [(link["url"], link["tipo"]) for link in scraper.extract_pdf_links_sumario(xml)] == [
        ("/borme/dias/2024/01/04/pdfs/BORME-A-2024-3-28.pdf", "A"),
        ("/borme/dias/2024/01/04/pdfs/BORME-C-2024-5.pdf", "C")]
    assert [link["pdf_filename"] for link in scraper.extract_pdf_links_sumario(json_txt)] == [
        "BORME-B-2024-3-08.pdf"]


# ═════════════════════════════════════════════
#  borme_scraper.py — sin machacar PDFs (guardar_version)
# ═════════════════════════════════════════════
def test_scraper_comprobar_pdf_cambiado_pasa_a_historico(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, [PDF_MADRID])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", JUE, "--output", out, "--delay", 0, "--sin-sumario-api"]
    _run_cli(monkeypatch, "borme_scraper.py", *args)
    pdf = out / "2024/01/04" / PDF_MADRID
    original, mtime = pdf.read_bytes(), pdf.stat().st_mtime_ns

    # Ya en disco: sin --comprobar no se vuelve a pedir
    _run_cli(monkeypatch, "borme_scraper.py", *args)
    assert boe.calls.count(_pdf_url(JUE, PDF_MADRID)) == 1
    # Con --comprobar y el mismo contenido: no se toca
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--comprobar")
    assert boe.calls.count(_pdf_url(JUE, PDF_MADRID)) == 2
    assert pdf.stat().st_mtime_ns == mtime and not (pdf.parent / "_historico").exists()
    assert len(_manifest(out)) == 1

    # boe.es sirve otro contenido: el anterior pasa a _historico/ (antes no se
    # detectaba; y si se hubiera vuelto a descargar, se habría machacado)
    boe.routes[_pdf_url(JUE, PDF_MADRID)] = _resp(200, content=b"%PDF-1.4 corregido")
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--comprobar")
    assert pdf.read_bytes() == b"%PDF-1.4 corregido"
    copia, = (pdf.parent / "_historico").iterdir()
    assert copia.read_bytes() == original and copia.name.startswith("BORME-A-2024-3-28__")
    assert bparser._nombre_pdf(copia) == PDF_MADRID  # el parser la trata como versión del mismo PDF
    assert _manifest(out) == [("2024-01-04", PDF_MADRID, "A")] * 2  # una fila por versión
    assert _state(out)["errors"] == []


def test_scraper_pdf_que_falta_en_disco_se_vuelve_a_descargar(monkeypatch, tmp_path):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, [PDF_MADRID, "BORME-A-2024-3-08.pdf"])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", JUE, "--output", out, "--delay", 0, "--sin-sumario-api"]
    _run_cli(monkeypatch, "borme_scraper.py", *args)
    # Carpeta del día borrada, y un fichero vacío de una descarga cortada del código antiguo:
    # constan en el manifest, pero no están en disco (antes no se volvían a pedir nunca)
    shutil.rmtree(out / "2024")
    vacio = out / "2024/01/04" / "BORME-A-2024-3-08.pdf"
    vacio.parent.mkdir(parents=True)
    vacio.write_bytes(b"")
    _run_cli(monkeypatch, "borme_scraper.py", *args)
    assert (out / "2024/01/04" / PDF_MADRID).read_bytes() == b"%PDF-1.4 " + PDF_MADRID.encode()
    assert vacio.read_bytes() == b"%PDF-1.4 BORME-A-2024-3-08.pdf"
    assert not (vacio.parent / "_historico").exists()  # un fichero vacío no es una versión
    assert boe.calls.count(_pdf_url(JUE, PDF_MADRID)) == 2


@pytest.mark.parametrize("fallo", [requests.ConnectionError("reset"), _resp(200, "<html>Error</html>")])
def test_scraper_comprobar_con_descarga_fallida_no_toca_el_pdf(monkeypatch, tmp_path, fallo):
    boe = FakeBOE(monkeypatch)
    boe.publish(JUE, [PDF_MADRID])
    out = tmp_path / "borme_pdfs"
    args = ["--start", JUE, "--end", JUE, "--output", out, "--delay", 0, "--sin-sumario-api"]
    _run_cli(monkeypatch, "borme_scraper.py", *args)
    pdf = out / "2024/01/04" / PDF_MADRID
    antes = (pdf.read_bytes(), pdf.stat().st_mtime_ns)

    boe.routes[_pdf_url(JUE, PDF_MADRID)] = fallo
    _run_cli(monkeypatch, "borme_scraper.py", *args, "--comprobar")
    assert (pdf.read_bytes(), pdf.stat().st_mtime_ns) == antes
    assert not (pdf.parent / "_historico").exists()
    assert [e["date"] for e in _state(out)["errors"]] == ["2024-01-04"]  # el día queda pendiente
    assert len(_manifest(out)) == 1


# ═════════════════════════════════════════════
#  Memoria: primera descarga con las semillas del release
# ═════════════════════════════════════════════
# Con las semillas del release (9,25 M de empresas y 17,1 M de cargos), el parser llegaba a
# 12,4 GiB al sembrar y el anonimizador a 10,1 GiB con las dos tablas. Lo reescrito tiene
# que dar exactamente lo mismo que el código anterior, que se copia aquí tal cual (main en
# c889c2d) como referencia.
import pyarrow.parquet as pq  # noqa: E402

from comun.historico import acumular as _acumular, sembrar as _sembrar_historico  # noqa: E402


def _sembrar_anterior(salida, ruta, origen=bparser.ORIGEN_SEMILLA):
    clave, posicion = bparser.CLAVE_SEMILLA, bparser.COLUMNA_POSICION
    columnas = pq.read_schema(ruta).names
    claves = pd.read_parquet(ruta, columns=clave + [c for c in ("_origen",) if c in columnas])
    claves[posicion] = np.arange(len(claves))
    base = (salida[clave] if salida is not None and len(salida)
            else pd.DataFrame({c: pd.Series(dtype=object) for c in clave}))
    resultado, informe = _sembrar_historico(base, claves, clave, origen=origen, contenido=[])
    marcas = resultado.iloc[len(base):]
    if not len(marcas):
        return salida, informe
    nuevas = bparser._leer_filas(ruta, marcas[posicion].to_numpy(dtype="int64"))
    nuevas["_origen"] = marcas["_origen"].to_numpy()
    nuevas["_en_ultima_descarga"] = False
    if salida is None or len(salida) == 0:
        return nuevas, informe
    if "_origen" not in salida.columns:
        salida = salida.assign(_origen=pd.Series([None] * len(salida), index=salida.index, dtype=object))
    orden = list(salida.columns) + [c for c in nuevas.columns if c not in salida.columns]
    out = pd.concat([salida, nuevas], ignore_index=True, sort=False)[orden]
    out["_en_ultima_descarga"] = out["_en_ultima_descarga"].astype(bool)
    return out, informe


def _acumular_pdfs_anterior(anterior, nuevos, fecha, pdfs):
    if anterior is None or len(anterior) == 0:
        return _acumular(None, nuevos, fecha) if len(nuevos) else anterior
    anterior = bparser._con_meta(anterior)
    dentro = anterior["pdf_filename"].isin(pdfs).to_numpy()
    acumuladas = _acumular(anterior[dentro], nuevos, fecha, permitir_vacio=True)
    return pd.concat([anterior[~dentro], acumuladas], ignore_index=True, sort=False)


def _anonimizar_anterior(src, out):
    """main() de borme_anonymize.py anterior: las dos tablas a la vez y copias completas."""
    df_emp = pd.read_parquet(src / "borme_empresas.parquet")
    df_car = pd.read_parquet(src / "borme_cargos.parquet")
    keep_cols = [
        "fecha_borme", "num_borme", "num_entrada",
        "empresa", "empresa_norm", "provincia", "cod_provincia",
        "tipo_borme", "actos", "domicilio", "capital_euros",
        "fecha_constitucion", "hoja_registral", "tomo", "inscripcion",
        "fecha_inscripcion", "pdf_filename",
    ]
    cols = [c for c in keep_cols + anon.COLUMNAS_CONTROL if c in df_emp.columns]
    df_emp_pub = df_emp[cols].copy()
    df = df_car.copy()
    nombres = df["persona"] if "persona" in df.columns else pd.Series(None, index=df.index, dtype=object)
    hashes = nombres.apply(anon.hash_persona)
    if "persona_hash" in df.columns:
        hashes = hashes.where(nombres.notna(), df["persona_hash"])
    df["persona_hash"] = hashes
    df = df.drop(columns=["persona"], errors="ignore")
    col_order = [
        "fecha_borme", "num_entrada", "empresa", "empresa_norm",
        "provincia", "hoja_registral", "tipo_acto", "cargo",
        "persona_hash", "pdf_filename",
    ] + anon.COLUMNAS_CONTROL
    df_car_pub = df[[c for c in col_order if c in df.columns]]
    out.mkdir(parents=True, exist_ok=True)
    df_emp_pub.to_parquet(out / "borme_empresas_pub.parquet", index=False, engine="pyarrow")
    df_car_pub.to_parquet(out / "borme_cargos_pub.parquet", index=False, engine="pyarrow")


PDFS_MEMORIA = [f"BORME-A-2024-{n}-28.pdf" for n in range(1, 9)]


def _claves_aleatorias(rng, n, nulos=True):
    """pdf_filename y num_entrada como los del BORME, con repeticiones, cadenas vacías y
    (con nulos=True) nulos de los dos tipos."""
    pdf = rng.choice(np.array(PDFS_MEMORIA + [""], dtype=object), n)
    num = rng.choice(np.array(["1", "2", "3", "10", "0", "00", ""], dtype=object), n)
    if nulos:
        pdf[rng.random(n) < 0.05] = None
        num[rng.random(n) < 0.05] = np.nan
    return pdf, num


def _tabla_aleatoria(rng, n, nulos=True, origen=False):
    pdf, num = _claves_aleatorias(rng, n, nulos)
    df = pd.DataFrame({
        "fecha_borme": pd.to_datetime(rng.choice(["2024-01-04", "2023-05-29"], n)),
        "num_entrada": num,
        "empresa": rng.choice(np.array(["ALFA SL", "BETA SA", None], dtype=object), n),
        "capital_euros": rng.choice([3000.0, np.nan], n),
        "pdf_filename": pdf,
    })
    if origen:
        df["_origen"] = rng.choice(np.array(["release anterior", None], dtype=object), n)
    return df


@pytest.mark.parametrize("semilla", range(8))
def test_sembrar_da_lo_mismo_que_antes(tmp_path, semilla):
    rng = np.random.default_rng(semilla)
    ruta = tmp_path / "borme_empresas_pub.parquet"
    _tabla_aleatoria(rng, 300, origen=semilla % 2 == 1).to_parquet(ruta, index=False, row_group_size=64)
    parse = _releer(_tabla_aleatoria(rng, 120).assign(_primera_descarga="2026-10-01", _ultima_descarga="2026-10-01",
                                                      _en_ultima_descarga=True), tmp_path / "s.parquet")
    for caso, salida in (("sin salida", None), ("salida vacía", parse.iloc[:0]), ("con salida", parse),
                         ("con _origen", parse.assign(_origen=None))):
        nuevo, informe = bparser._sembrar(salida, ruta)
        anterior, informe_anterior = _sembrar_anterior(salida, ruta)
        assert informe == informe_anterior, caso
        if anterior is None:
            assert nuevo is None, caso
        else:
            pd.testing.assert_frame_equal(nuevo, anterior, obj=caso)


def test_sembrar_con_claves_que_no_son_texto_usa_el_camino_general(tmp_path):
    rng = np.random.default_rng(1)
    ruta = tmp_path / "pub.parquet"
    _tabla_aleatoria(rng, 200).to_parquet(ruta, index=False)
    salida = _tabla_aleatoria(rng, 80, nulos=False)
    salida["num_entrada"] = salida["num_entrada"].map(lambda v: int(v) if v not in ("", None) else 7)
    assert bparser._motivos_por_clave(salida[bparser.CLAVE_SEMILLA], ruta) is None
    nuevo, informe = bparser._sembrar(salida, ruta)
    anterior, informe_anterior = _sembrar_anterior(salida, ruta)
    assert informe == informe_anterior
    pd.testing.assert_frame_equal(nuevo, anterior)


def test_sembrar_no_pasa_las_claves_a_objetos_de_python(tmp_path, monkeypatch):
    # seleccionar_semilla hacía str(v) de cada valor de la clave: 35 M de objetos con las
    # tablas del release. Con claves de texto no se llama a sembrar() ni a texto_canonico
    rng = np.random.default_rng(2)
    ruta = tmp_path / "pub.parquet"
    _tabla_aleatoria(rng, 200).to_parquet(ruta, index=False)
    salida = _tabla_aleatoria(rng, 50)
    esperado, informe = _sembrar_anterior(salida, ruta)
    monkeypatch.setattr(bparser, "sembrar", lambda *a, **k: pytest.fail("sembrar() con claves de texto"))
    nuevo, informe_nuevo = bparser._sembrar(salida, ruta)
    pd.testing.assert_frame_equal(nuevo, esperado)
    assert informe_nuevo == informe and informe["anadidas"] > 0 and informe["descartadas_clave"] > 0


def _anterior_con_bloques(rng, n=400):
    """Tabla acumulada: las filas de cada PDF seguidas (como las deja el parser)."""
    tabla = _tabla_aleatoria(rng, n, nulos=False)
    tabla["pdf_filename"] = np.sort(rng.choice(np.array(PDFS_MEMORIA, dtype=object), n))
    return _acumular(None, tabla, "2026-10-01")


@pytest.mark.parametrize("pdfs", [set(), {PDFS_MEMORIA[0]}, {PDFS_MEMORIA[3]}, {PDFS_MEMORIA[-1]},
                                  set(PDFS_MEMORIA[1::2]), set(PDFS_MEMORIA), {"BORME-A-2025-1-01.pdf"}])
def test_acumular_pdfs_da_lo_mismo_que_antes(tmp_path, pdfs):
    rng = np.random.default_rng(len(pdfs))
    anterior = _releer(_anterior_con_bloques(rng), tmp_path / "a.parquet")
    nuevos = _tabla_aleatoria(rng, 60, nulos=False)
    nuevos["pdf_filename"] = rng.choice(np.array(sorted(pdfs) or ["BORME-A-2025-1-01.pdf"], dtype=object), 60)
    esperado = _acumular_pdfs_anterior(anterior, nuevos, "2026-10-02", pdfs)
    pd.testing.assert_frame_equal(bparser._acumular_pdfs(anterior, nuevos, "2026-10-02", pdfs), esperado)
    # Con muchos tramos, se copian las filas como antes
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(bparser, "MAX_TRAMOS", 1)
        pd.testing.assert_frame_equal(bparser._acumular_pdfs(anterior, nuevos, "2026-10-02", pdfs), esperado)


def test_acumular_pdfs_no_copia_las_filas_de_los_otros_pdf(tmp_path, monkeypatch):
    rng = np.random.default_rng(5)
    anterior = _releer(_anterior_con_bloques(rng), tmp_path / "a.parquet")
    nuevos = _tabla_aleatoria(rng, 30, nulos=False).assign(pdf_filename=PDFS_MEMORIA[2])
    esperado = _acumular_pdfs_anterior(anterior, nuevos, "2026-10-02", {PDFS_MEMORIA[2]})
    getitem = pd.DataFrame.__getitem__

    def sin_filtrar_la_tabla(self, key):
        if (len(self) == len(anterior) and isinstance(key, np.ndarray) and key.dtype == bool
                and key.sum() > len(self) // 2):
            raise AssertionError("anterior[~dentro] copia casi toda la tabla")
        return getitem(self, key)

    monkeypatch.setattr(pd.DataFrame, "__getitem__", sin_filtrar_la_tabla)
    resultado = bparser._acumular_pdfs(anterior, nuevos, "2026-10-02", {PDFS_MEMORIA[2]})
    monkeypatch.undo()
    pd.testing.assert_frame_equal(resultado, esperado)


def _resumen_anterior(df_empresas, df_cargos):
    """Líneas del resumen final de run_batch anterior, desde "(filas)" hasta los tipos de acto."""
    lineas = []
    for nombre, df in (("Empresas", df_empresas), ("Cargos", df_cargos)):
        if df is None or len(df) == 0:
            continue
        lineas.append(f"   {nombre} (filas): {len(df):,}")
        if "_en_ultima_descarga" in df.columns:
            lineas.append(f"      del último parse de su PDF: {int(df['_en_ultima_descarga'].sum()):,}")
        if "_origen" in df.columns:
            lineas.append(f"      de la semilla: {int(df['_origen'].notna().sum()):,}")
    if df_empresas is not None and len(df_empresas) > 0:
        lineas.append(f"   Empresas unicas: {df_empresas['empresa_norm'].nunique():,}")
        lineas.append(f"   Provincias: {df_empresas['provincia'].nunique()}")
        lineas.append(f"   Rango fechas: {df_empresas['fecha_borme'].min()} -> {df_empresas['fecha_borme'].max()}")
        constit = df_empresas[df_empresas["actos"].str.contains("Constitución", na=False)]
        lineas.append(f"   Constituciones: {len(constit):,}")
        if "capital_euros" in df_empresas.columns:
            lineas.append(f"   Con capital: {df_empresas['capital_euros'].notna().sum():,}")
    if df_cargos is not None and len(df_cargos) > 0:
        lineas.append(f"   Cargos unicos (tipos): {df_cargos['cargo'].nunique()}")
        if "persona" in df_cargos.columns:
            lineas.append(f"   Personas unicas: {df_cargos['persona'].nunique():,}")
        for tipo, n in df_cargos['tipo_acto'].value_counts().items():
            lineas.append(f"      {tipo}: {n:,}")
    return lineas


def test_run_batch_una_tabla_entera_y_despues_la_otra_con_el_resumen_de_antes(fake_pdf, fechas, tmp_path,
                                                                                 monkeypatch, caplog):
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    fake_pdf.update({PDF_MADRID: [PAG1, PAG2], PDF_ENERO: [PAG_ENERO]})
    _pdf_file(base, dt.date(2024, 1, 4), PDF_MADRID)
    _pdf_file(base, dt.date(2024, 1, 2), PDF_ENERO)
    bparser.run_batch(base, tmp_path / "ref", workers=1)
    semilla_emp, semilla_car = tmp_path / "borme_empresas_pub.parquet", tmp_path / "borme_cargos_pub.parquet"
    emp_ref, car_ref = _tablas(tmp_path / "ref")
    extra = _publicado(emp_ref).iloc[[0, 1]].assign(pdf_filename=PDF_OTRO)
    pd.concat([_publicado(emp_ref), extra], ignore_index=True).to_parquet(semilla_emp, index=False)
    car_pub = car_ref.drop(columns=META + ["persona"]).assign(persona_hash="h")
    pd.concat([car_pub, car_pub.iloc[[0]].assign(pdf_filename=PDF_OTRO)]).to_parquet(semilla_car, index=False)

    llamadas = []
    leer, guardar = bparser.leer_registros, bparser.guardar_registros
    monkeypatch.setattr(bparser, "leer_registros", lambda r: llamadas.append(("leer", Path(r).name)) or leer(r))
    monkeypatch.setattr(bparser, "guardar_registros",
                        lambda df, r: llamadas.append(("guardar", Path(r).name)) or guardar(df, r))
    caplog.set_level("INFO", logger=bparser.log.name)
    bparser.run_batch(base, out, workers=2, semillas=[semilla_car, semilla_emp])
    assert llamadas == [("leer", "borme_empresas.parquet"), ("guardar", "borme_empresas.parquet"),
                        ("leer", "borme_cargos.parquet"), ("guardar", "borme_cargos.parquet")]
    emp, car = _tablas(out)
    sembradas_emp = set(emp.loc[emp["_origen"].notna(), "pdf_filename"])
    assert sembradas_emp == set(car.loc[car["_origen"].notna(), "pdf_filename"]) == {PDF_OTRO}
    mensajes = [r.getMessage() for r in caplog.records]
    inicio = next(i for i, m in enumerate(mensajes) if m.startswith("   Empresas (filas)"))
    esperado = _resumen_anterior(emp, car)
    assert mensajes[inicio:inicio + len(esperado)] == esperado
    assert mensajes[inicio + len(esperado)] == "=" * 60


def test_resumen_con_una_cifra_que_falla_lanza_el_error_al_final(tmp_path, fechas):
    # Una semilla de empresas sin empresa_norm: el resumen falla como antes, después de
    # escribir la tabla y el progreso
    base, out = tmp_path / "borme_pdfs", tmp_path / "salida"
    base.mkdir()
    semilla = tmp_path / "borme_empresas_pub.parquet"
    _publicado(_empresas_privadas()).drop(columns="empresa_norm").to_parquet(semilla, index=False)
    with pytest.raises(KeyError, match="empresa_norm"):
        bparser.run_batch(base, out, workers=1, semillas=[semilla])
    assert len(pd.read_parquet(out / "borme_empresas.parquet")) == 3
    assert (out / "borme_parse_progress.json").exists()


def _tablas_privadas_con_semilla():
    emp = _empresas_privadas().assign(_primera_descarga="2026-10-01", _ultima_descarga="2026-10-01",
                                      _en_ultima_descarga=[True, True, False])
    sembradas = emp.iloc[[0]].assign(num_entrada="77", _primera_descarga=None, _ultima_descarga=None,
                                     _en_ultima_descarga=False, _origen="release v2026.02")
    emp = pd.concat([emp.assign(_origen=None), sembradas], ignore_index=True)
    car = _cargos_privados().assign(_primera_descarga="2026-10-01", _ultima_descarga="2026-10-01",
                                    _en_ultima_descarga=True, _origen=None, persona_hash=None)
    car_sembrados = car.iloc[[0, 1]].assign(persona=None, persona_hash=["hash_a", None], _origen="release v2026.02",
                                            _en_ultima_descarga=False)
    return emp, pd.concat([car, car_sembrados], ignore_index=True)


def test_anonimizar_da_lo_mismo_que_antes(monkeypatch, tmp_path):
    src = tmp_path / "parse"
    src.mkdir()
    emp, car = _tablas_privadas_con_semilla()
    emp.to_parquet(src / "borme_empresas.parquet", index=False)
    car.to_parquet(src / "borme_cargos.parquet", index=False)
    _anonimizar_anterior(src, tmp_path / "antes")
    _run_cli(monkeypatch, "borme_anonymize.py", "--input", src, "--output", tmp_path / "despues")
    for nombre in ("borme_empresas_pub.parquet", "borme_cargos_pub.parquet"):
        antes, despues = tmp_path / "antes" / nombre, tmp_path / "despues" / nombre
        pd.testing.assert_frame_equal(pd.read_parquet(despues), pd.read_parquet(antes))
        assert despues.read_bytes() == antes.read_bytes(), nombre


def test_anonimizar_no_modifica_las_tablas_de_entrada():
    emp, car = _tablas_privadas_con_semilla()
    copia_emp, copia_car = emp.copy(), car.copy()
    anon.anonymize_empresas(emp)
    pub = anon.anonymize_cargos(car)
    pd.testing.assert_frame_equal(emp, copia_emp)
    pd.testing.assert_frame_equal(car, copia_car)
    assert pub["persona_hash"].iloc[-2] == "hash_a" and pub["persona_hash"].iloc[0] == anon.hash_persona(
        "ZUTANO PERENGANO ANA")


def test_anonimizar_escribe_una_tabla_antes_de_leer_la_otra(monkeypatch, tmp_path):
    src, out = tmp_path / "parse", tmp_path / "pub"
    src.mkdir()
    emp, car = _tablas_privadas_con_semilla()
    emp.to_parquet(src / "borme_empresas.parquet", index=False)
    car.to_parquet(src / "borme_cargos.parquet", index=False)
    leer = pd.read_parquet

    def espia(ruta, *args, **kwargs):
        if Path(ruta).name == "borme_cargos.parquet":
            assert (out / "borme_empresas_pub.parquet").exists(), "las dos tablas a la vez en memoria"
        return leer(ruta, *args, **kwargs)

    monkeypatch.setattr(pd, "read_parquet", espia)
    _run_cli(monkeypatch, "borme_anonymize.py", "--input", src, "--output", out)
    assert (out / "borme_cargos_pub.parquet").exists()


def test_anonimizar_sin_tabla_de_cargos_no_escribe_nada(monkeypatch, tmp_path):
    src, out = tmp_path / "parse", tmp_path / "pub"
    src.mkdir()
    _tablas_privadas_con_semilla()[0].to_parquet(src / "borme_empresas.parquet", index=False)
    with pytest.raises(FileNotFoundError):
        _run_cli(monkeypatch, "borme_anonymize.py", "--input", src, "--output", out)
    assert not any(out.iterdir())


def test_donde_es_where_con_cualquier_tipo(tmp_path):
    rng = np.random.default_rng(3)
    tabla = pd.DataFrame({"hash": rng.choice(np.array(["a", "b", ""], dtype=object), 60),
                          "publicado": rng.choice(np.array(["x", None, "y"], dtype=object), 60)})
    condicion = pd.Series(rng.random(60) < 0.5)
    leida = _releer(tabla, tmp_path / "t.parquet")   # str en pandas 3, object en 2.2
    casos = {"object": (tabla["hash"], tabla["publicado"]),
             "parquet": (leida["hash"], leida["publicado"]),
             "string[pyarrow]": (tabla["hash"].astype("string[pyarrow]"), tabla["publicado"].astype("string[pyarrow]")),
             "tipos distintos": (leida["hash"], tabla["publicado"])}
    for caso, (valores, otros) in casos.items():
        pd.testing.assert_series_equal(anon._donde(condicion, valores, otros), valores.where(condicion, otros),
                                       obj=caso)
