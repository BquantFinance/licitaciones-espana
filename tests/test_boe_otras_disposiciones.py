"""Tests offline de boe/boe_otras_disposiciones.py.

La API de sumarios del BOE se simula con un ``requests.get`` falso. Los
sumarios tienen la estructura real (diario > seccion > departamento >
epigrafe > item, con un elemento suelto donde la API no pone lista); los
títulos, identificadores y nombres son inventados.
"""

import importlib.util
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "boe" / "boe_otras_disposiciones.py"


def _cargar():
    spec = importlib.util.spec_from_file_location("boe_otras_disposiciones", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
LUNES = date(2025, 10, 13)      # lunes; el 19 es domingo


def item(n, titulo):
    return {"identificador": f"BOE-A-2025-{n}", "control": f"2025/{n}", "titulo": titulo,
            "url_pdf": {"szBytes": "1000", "szKBytes": "1", "pagina_inicial": "1", "pagina_final": "2",
                        "texto": f"https://www.boe.es/boe/dias/2025/10/13/pdfs/BOE-A-2025-{n}.pdf"},
            "url_html": f"https://www.boe.es/diario_boe/txt.php?id=BOE-A-2025-{n}",
            "url_xml": f"https://www.boe.es/diario_boe/xml.php?id=BOE-A-2025-{n}"}


def sumario(dia, extra=None):
    """Sumario del día con la forma de la API: lista o elemento suelto según el caso."""
    sec3 = {"codigo": "3", "nombre": "III. Otras disposiciones", "departamento": [
        {"codigo": "9562", "nombre": "MINISTERIO DE CIENCIA, INNOVACIÓN Y UNIVERSIDADES", "epigrafe": [
            {"nombre": "Convenios", "item": [
                item(f"{dia}1", "Resolución de 1 de octubre de 2025, de la Secretaría General Técnica, por la que se "
                                "publica el Convenio entre el Consejo Superior de Investigaciones Científicas y "
                                "Empresa Inventada, SL, para la investigación."),
                item(f"{dia}2", "Resolución de 2 de octubre de 2025, por la que se publica la Adenda al Convenio "
                                "con la Fundación Doña Jimena."),
            ]},
            {"nombre": "Premios", "item": item(f"{dia}3", "Orden de 3 de octubre de 2025 por la que se concede el "
                                                          "Premio Nacional a don Juan Pérez García y a doña Ana de la "
                                                          "Fuente Ruiz.")},
        ]},
        {"codigo": "7723", "nombre": "MINISTERIO DE HACIENDA",
         "item": item(f"{dia}4", "Resolución por la que se publica el Protocolo general de actuación con la "
                                 "Diputación Provincial de Ejemplo.")},
    ]}
    if extra:
        sec3["departamento"].append(extra)
    sec2 = {"codigo": "2A", "nombre": "II. Autoridades y personal. - A.", "departamento": {
        "codigo": "1", "nombre": "JEFATURA DEL ESTADO",
        "item": item(f"{dia}9", "Real Decreto por el que se nombra a don Nombre Inventado Apellido.")}}
    return {"status": {"code": "200", "text": "ok"}, "data": {"sumario": {
        "metadatos": {"publicacion": "BOE", "fecha_publicacion": dia},
        "diario": [{"numero": "247", "sumario_diario": {"identificador": f"BOE-S-{dia}"},
                    "seccion": [sec2, sec3]}]}}}


class Respuesta:
    def __init__(self, codigo, cuerpo=b""):
        self.status_code = codigo
        self.content = cuerpo
        self.headers = {}


class Api:
    def __init__(self):
        self.peticiones = []
        self.codigos = {}          # dia -> código forzado
        self.extra = {}            # dia -> departamento añadido
        self.cuerpo = {}           # dia -> bytes forzados

    def get(self, url, headers=None, timeout=None, params=None):
        assert headers["Accept"] == "application/json"
        assert "licitaciones-espana" in headers["User-Agent"]
        dia = url.rsplit("/", 1)[1]
        self.peticiones.append(dia)
        if dia in self.codigos:
            return Respuesta(self.codigos[dia], b"{}")
        if dia in self.cuerpo:
            return Respuesta(200, self.cuerpo[dia])
        return Respuesta(200, json.dumps(sumario(dia, self.extra.get(dia))).encode("utf-8"))


@pytest.fixture
def api(monkeypatch):
    a = Api()
    monkeypatch.setattr(requests, "get", a.get)
    monkeypatch.setattr("time.sleep", lambda s: None)
    return a


def correr(tmp_path, *args):
    return M.main(["--salida", str(tmp_path), *args])


def leer(tmp_path):
    return pd.read_parquet(tmp_path / "disposiciones.parquet")


def test_seccion_iii_un_dia(api, tmp_path):
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-13") == 0
    df = leer(tmp_path)
    assert len(df) == 4                                    # la sección II no entra
    assert list(df["identificador"]) == [f"BOE-A-2025-20251013{i}" for i in (1, 2, 3, 4)]
    assert [None if pd.isna(v) else v for v in df["_tipo"]] == ["convenio", "adenda", None, "protocolo"]
    assert set(df["departamento"]) == {"MINISTERIO DE CIENCIA, INNOVACIÓN Y UNIVERSIDADES", "MINISTERIO DE HACIENDA"}
    assert df.loc[df["identificador"].str.endswith("4"), "epigrafe"].isna().all()   # item suelto del departamento
    assert df["url_pdf"].iloc[0].endswith("BOE-A-2025-202510131.pdf")
    assert set(df["fecha"]) == {"20251013"} and set(df["diario"]) == {"247"}
    premio = df[df["identificador"].str.endswith("3")].iloc[0]
    assert premio["titulo"] == ("Orden de 3 de octubre de 2025 por la que se concede el Premio Nacional a "
                                "[persona física] y a [persona física].")
    assert premio["_personas_quitadas"] == "2"
    assert "Fundación Doña Jimena" in df["titulo"].iloc[1]      # una persona jurídica no se toca
    assert df["_en_ultima_descarga"].all()


def test_crudo_sin_personas_y_manifiesto(api, tmp_path):
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-13") == 0
    crudo = (tmp_path / "raw" / "2025" / "20251013.json").read_text(encoding="utf-8")
    assert "Juan Pérez" not in crudo and "Ana de la Fuente" not in crudo and "Nombre Inventado" not in crudo
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert man["20251013"]["estado"] == "ok" and man["20251013"]["disposiciones"] == 4
    assert len(man["20251013"]["sumario_sha256"]) == 64


def test_domingo_no_se_pide_y_404_es_dia_sin_boe(api, tmp_path):
    api.codigos["20251015"] = 404
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-20") == 0
    assert "20251019" not in api.peticiones
    assert api.peticiones == ["20251013", "20251014", "20251015", "20251016", "20251017", "20251018", "20251020"]
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert man["20251015"]["estado"] == "sin_boe"
    api.peticiones.clear()
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-20", "--comprobar-todo") == 0
    assert "20251015" not in api.peticiones                 # un día sin BOE no se vuelve a pedir


def test_incremental_repasa_los_ultimos_dias(api, tmp_path):
    assert correr(tmp_path, "--desde", "2025-09-01", "--hasta", "2025-10-20") == 0
    api.peticiones.clear()
    assert correr(tmp_path, "--hasta", "2025-10-22") == 0
    assert api.peticiones[-2:] == ["20251021", "20251022"]
    assert len(api.peticiones) == M.DIAS_REPASO + 2
    assert "sin_cambios" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_cambio_de_un_dia_conserva_lo_retirado(api, tmp_path):
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-13") == 0
    api.cuerpo["20251013"] = json.dumps(sumario("20251013"), ensure_ascii=False).replace(
        "Protocolo general de actuación", "Convenio").encode("utf-8")
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-13") == 0
    df = leer(tmp_path)
    viejo = df[df["titulo"].str.contains("Protocolo general")]
    assert len(viejo) == 1 and not viejo["_en_ultima_descarga"].iloc[0]
    assert len(df[df["_en_ultima_descarga"]]) == 4
    assert len(list((tmp_path / "raw" / "2025" / "_historico").glob("20251013__*.json"))) == 1


def test_error_5xx_sale_con_1_y_sigue_con_los_demas(api, tmp_path):
    api.codigos["20251014"] = 503
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-15") == 1
    assert api.peticiones.count("20251014") == M.INTENTOS
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text(encoding="utf-8"))
    assert man["20251013"]["estado"] == "ok" and man["20251015"]["estado"] == "ok"
    assert "estado" not in man["20251014"]
    api.codigos.clear()
    api.peticiones.clear()
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-15") == 0
    assert "20251014" in api.peticiones


def test_403_para_la_ejecucion(api, tmp_path):
    api.codigos["20251014"] = 403
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-16") == 1
    assert api.peticiones == ["20251013", "20251014"]
    assert "parada" in (tmp_path / "raw" / "descarga_log.txt").read_text(encoding="utf-8")


def test_429_persistente_para(api, tmp_path):
    api.codigos["20251013"] = 429
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-14") == 1
    assert api.peticiones == ["20251013"] * M.INTENTOS


def test_respuesta_que_no_es_json(api, tmp_path):
    api.cuerpo["20251013"] = b"<?xml version='1.0'?><response/>"
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-14") == 1
    assert len(leer(tmp_path)) == 4                          # el 14 sí


def test_cero_disposiciones_no_toca_el_parquet(api, tmp_path):
    api.codigos["20251013"] = 404
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-13") == 1
    assert not (tmp_path / "disposiciones.parquet").exists()


def test_solo_parquet_sin_red(api, tmp_path, monkeypatch):
    assert correr(tmp_path, "--desde", "2025-10-13", "--hasta", "2025-10-14") == 0
    (tmp_path / "disposiciones.parquet").unlink()
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("no debe pedir nada"))
    assert correr(tmp_path, "--solo-parquet") == 0
    assert len(leer(tmp_path)) == 8


@pytest.mark.parametrize("titulo, esperado, n", [
    ("Convenio con don José Luis Martínez de la Rosa.", "Convenio con [persona física].", 1),
    ("Premio a D. Pedro Gómez y Dña. Lucía Sanz López", "Premio a [persona física] y [persona física]", 2),
    ("Medalla a doña María J. Pérez.", "Medalla a [persona física].", 1),
    ("Convenio con la Fundación Doña Jimena", "Convenio con la Fundación Doña Jimena", 0),
    ("Convenio con el Ayuntamiento de Don Benito", "Convenio con el Ayuntamiento de Don Benito", 0),
    ("Legado de don José Ortega y Gasset", "Legado de [persona física]", 1),
    ("", "", 0)])
def test_sin_personas(titulo, esperado, n):
    assert M.sin_personas(titulo) == (esperado, n)


@pytest.mark.parametrize("titulo, esperado", [
    ("Resolución ... por la que se publica la Adenda de prórroga al Convenio", "adenda"),
    ("ENCOMIENDA de gestión", "encomienda"), ("Acuerdo marco de colaboración", "acuerdo"),
    ("Orden por la que se convocan ayudas", None), ("Convenios con", None),
    ("Resolución por la que se registra y publica el Convenio colectivo de Empresa Inventada, SA", "convenio_colectivo"),
    ("Ortega y Gasset", None)])
def test_tipo(titulo, esperado):
    assert M.tipo(titulo) == esperado
