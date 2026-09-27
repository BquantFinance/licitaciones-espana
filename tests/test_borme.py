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


def _pdf_file(root, fecha, name):
    p = Path(root) / f"{fecha:%Y}" / f"{fecha:%m}" / f"{fecha:%d}" / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"%PDF-1.4 fake")
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


# ═════════════════════════════════════════════
#  borme_anonymize.py
# ═════════════════════════════════════════════
@pytest.mark.parametrize("domicilio, esperado", [
    ("C/ MAYOR 5 2º B (MADRID)", "MADRID"),
    ("AVDA VIRGEN DE LA MONTAÑA 1 - LOCAL EXTERIOR (CACERES)", "CACERES"),
    # Domicilio desbordado (empresario individual, sin paréntesis de municipio)
    ("C/ X 5 OURENSE. Estado Civil : Soltero . Datos registrales. T 785 , F 203, S 8, H OR 11977, I/A 1 ( 2.02.15)", None),
    ("PS DE GRACIA Num.46 P.2", None),
])
def test_domicilio_solo_municipio(domicilio, esperado):
    assert anon._municipio(domicilio) == esperado


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


def test_anonymize_cli_sin_datos_personales(monkeypatch, tmp_path):
    src, out = tmp_path / "borme_pdfs", tmp_path / "data"
    src.mkdir()
    _empresas_privadas().to_parquet(src / "borme_empresas.parquet", index=False)
    _cargos_privados().to_parquet(src / "borme_cargos.parquet", index=False)

    _run_cli(monkeypatch, "borme_anonymize.py", "--input", src, "--output", out)

    emp = pd.read_parquet(out / "borme_empresas_pub.parquet")
    car = pd.read_parquet(out / "borme_cargos_pub.parquet")
    assert "objeto_social" not in emp.columns
    assert "persona" not in car.columns and "persona_hash" in car.columns
    textos = _textos(emp) | _textos(car)
    for dato_personal in ["FULANO", "ZUTANO", "PRUEBA", "Soltero", "C/ MAYOR", "C/ LUNA"]:
        assert not any(dato_personal in t for t in textos), dato_personal
    # El domicilio conserva el municipio; las empresas normales, su nombre
    assert emp["domicilio"].tolist()[0] == "MADRID"
    assert emp["empresa"].tolist()[0] == "ALFA SOLUCIONES SL"
    # El empresario individual se hashea igual en todas sus filas y en cargos
    h = anon.hash_persona("FULANO MENGANO, JUAN")
    assert emp["empresa_norm"].tolist()[1:] == [h, h]
    assert car["empresa_norm"].tolist() == ["ALFA SOLUCIONES", h]
    assert car["persona_hash"].tolist() == [anon.hash_persona("ZUTANO PERENGANO ANA"),
                                            anon.hash_persona("PRUEBA EJEMPLO LUIS")]


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
    for nombre in ["FULANO", "ZUTANO", "PRUEBA", "C/ MAYOR"]:
        assert not any(nombre in t for t in textos)
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
