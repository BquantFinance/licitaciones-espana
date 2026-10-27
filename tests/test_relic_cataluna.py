"""Tests offline de relic/relic_cataluna.py.

El portal de datos abiertos de la Generalitat (Socrata, conjunto t3wj-j4pu) se
simula con un ``requests.get`` falso. El CSV tiene el formato real (coma,
UTF-8, las 17 columnas en catalán, una fila por empresa y clasificación); los
nombres y los números registrales son inventados.
"""

import csv
import gzip
import importlib.util
import io
import json
from pathlib import Path

import pandas as pd
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "relic" / "relic_cataluna.py"


def _cargar():
    spec = importlib.util.spec_from_file_location("relic_cataluna", SCRIPT)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


M = _cargar()
CAB = list(M.COLUMNAS)
VACIO = [""] * 8


def fila(nombre, registro, prohib=False, ambito="", causa="", inicio="", fin="", clasif=""):
    if prohib:
        return [nombre, registro, "01/10/2026", "true", ambito, "15/09/2026", inicio, causa, fin, "false", *VACIO[:7]]
    return [nombre, registro, "01/10/2026", "false", "", "", "", "", "", "true" if clasif else "false",
            "false" if clasif else "", "Obres" if clasif else "", clasif, "1" if clasif else "", "3" if clasif else "",
            "01/01/2025" if clasif else "", "OB" if clasif else ""]


EMPRESA_A = fila("CONSTRUCCIONS EXEMPLE, SL", "NB0000001", True, "Tot el sector públic", "71.1.b", "01/10/2026",
                 "01/10/2028")
EMPRESA_B = fila("OBRES INVENTADES S A", "NB0000002", clasif="C")
COOP = fila("FAIZAN INVENTADA S COOP", "", True, "Ajuntament de X", "71.1.e", "02/02/2026", "02/02/2027")
PERSONA = fila("JOAN NOMINVENTAT COGNOMPRIMER", "NG0000003", True, "Tot el sector públic", "71.1.a", "03/03/2026",
               "03/03/2029")
PERSONA_2 = fila("Maria Inventada Segona", "NT0000004", clasif="C")
COMUNIDAD = fila("PERE INVENTAT I GERMANS CB", "NL0000005")


def csv_relic(filas, cabecera=CAB):
    t = io.StringIO()
    w = csv.writer(t, lineterminator="\n")
    w.writerow(cabecera)
    w.writerows(filas)
    return t.getvalue().encode("utf-8")


class Respuesta:
    def __init__(self, codigo=200, cuerpo=b""):
        self.status_code = codigo
        self._cuerpo = cuerpo
        self.headers = {}

    def iter_content(self, chunk_size=1):
        for i in range(0, len(self._cuerpo), chunk_size):
            yield self._cuerpo[i:i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def portal(monkeypatch):
    estado = {"cuerpo": csv_relic([EMPRESA_A, EMPRESA_B, COOP, PERSONA, PERSONA_2, COMUNIDAD]), "codigo": 200,
              "pedidas": []}

    def get(url, params=None, headers=None, timeout=None, stream=False):
        estado["pedidas"].append(url)
        assert url == M.URL
        assert "User-Agent" in headers and "@" not in headers["User-Agent"]
        return Respuesta(estado["codigo"], estado["cuerpo"])
    monkeypatch.setattr(requests, "get", get)
    monkeypatch.setattr("time.sleep", lambda s: None)
    return estado


def todo_el_texto(salida):
    """Todo lo que el script deja en disco, descomprimido, como texto (para buscar nombres de personas)."""
    trozos = []
    for p in salida.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix == ".parquet":
            trozos.append(pd.read_parquet(p).to_csv())
        elif p.name.endswith(".gz"):
            trozos.append(gzip.decompress(p.read_bytes()).decode("utf-8"))
        else:
            trozos.append(p.read_bytes().decode("utf-8", "replace"))
    return "\n".join(trozos)


@pytest.mark.parametrize("nombre, esperado", [
    ("CONSTRUCCIONS EXEMPLE, SL", True), ("OBRES INVENTADES S A", True), ("XYZ, S.L.U.", True),
    ("ALFA BETA S. L.", True), ("FAIZAN INVENTADA S COOP", True), ("Fundació Privada Inventada", True),
    ("ASSOCIACIÓ CULTURAL INVENTADA", True), ("THE INVENTED GROUP LLC", True), ("ARRELS INVENTATS SCCLP", True),
    ("MANCOMUNIDAD DE SERVICIOS INVENTADA", True), ("CLUB ESPORTIU INVENTAT", True),
    ("JOAN NOMINVENTAT COGNOMPRIMER", False), ("Maria Inventada Segona", False), ("PERE INVENTAT I GERMANS CB", False),
    ("PERE INVENTAT I GERMANS, C.B.", False), ("COMUNITAT DE BÉNS INVENTADA", False), ("ECOGLOBAL INVENTADA", False),
    ("", False)])
def test_es_persona_juridica(nombre, esperado):
    assert M.es_persona_juridica(nombre) is esperado


def test_descarga_sin_personas_fisicas(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    assert portal["pedidas"] == [M.URL]
    raw = pd.read_csv(tmp_path / "raw" / "relic_pj.csv.gz", dtype=str, keep_default_na=False)
    assert list(raw.columns) == CAB
    assert set(raw["Nom empresa"]) == {"CONSTRUCCIONS EXEMPLE, SL", "OBRES INVENTADES S A", "FAIZAN INVENTADA S COOP"}
    texto = todo_el_texto(tmp_path)
    for persona in ("NOMINVENTAT", "Inventada Segona", "GERMANS", "NG0000003", "NT0000004", "NL0000005"):
        assert persona not in texto
    man = json.loads((tmp_path / "raw" / "_manifiesto.json").read_text())
    assert (man["filas_leidas"], man["filas_conservadas"], man["filas_quitadas_persona_fisica"]) == (6, 3, 3)
    assert len(man["sha256"]) == 64 and man["url"] == M.URL
    assert not (tmp_path / ".descargas").exists()   # el original, que trae las personas, no se guarda
    pro = pd.read_parquet(tmp_path / "prohibiciones.parquet")
    assert sorted(pro["Nom empresa"]) == ["CONSTRUCCIONS EXEMPLE, SL", "FAIZAN INVENTADA S COOP"]
    assert pro["_en_ultima_descarga"].all()
    assert set(pro["Causa prohibició"]) == {"71.1.b", "71.1.e"}
    ins = pd.read_parquet(tmp_path / "inscripciones.parquet")
    assert len(ins) == 3 and ins["Nom empresa"].map(type).eq(str).all()


def test_prohibicion_retirada_se_conserva(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    # la semana siguiente la prohibición de la cooperativa ha caducado: el portal ya no la sirve
    portal["cuerpo"] = csv_relic([EMPRESA_A, EMPRESA_B, PERSONA])
    assert M.main(["--salida", str(tmp_path)]) == 0
    pro = pd.read_parquet(tmp_path / "prohibiciones.parquet")
    vigente = dict(zip(pro["Nom empresa"], pro["_en_ultima_descarga"]))
    assert vigente == {"CONSTRUCCIONS EXEMPLE, SL": True, "FAIZAN INVENTADA S COOP": False}
    assert len(list((tmp_path / "raw" / "_historico").glob("relic_pj.csv__*.gz"))) == 1
    # una tercera ejecución sin cambios no crea versiones nuevas de la capa cruda
    assert M.main(["--salida", str(tmp_path)]) == 0
    assert len(list((tmp_path / "raw" / "_historico").glob("relic_pj.csv__*.gz"))) == 1


def test_cabecera_cambiada_no_toca_nada(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = (tmp_path / "raw" / "relic_pj.csv.gz").read_bytes()
    portal["cuerpo"] = csv_relic([EMPRESA_A], cabecera=["Nom", *CAB[1:]])
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert (tmp_path / "raw" / "relic_pj.csv.gz").read_bytes() == antes
    assert "faltan columnas: Nom empresa" in (tmp_path / "raw" / "descarga_log.txt").read_text()


def test_sin_personas_juridicas_no_toca_nada(portal, tmp_path):
    """Caso de cero resultados: una descarga que solo trae personas físicas (o vacía) no sustituye a la anterior."""
    assert M.main(["--salida", str(tmp_path)]) == 0
    antes = (tmp_path / "prohibiciones.parquet").read_bytes()
    for cuerpo in (csv_relic([PERSONA, PERSONA_2]), csv_relic([])):
        portal["cuerpo"] = cuerpo
        assert M.main(["--salida", str(tmp_path)]) == 1
        assert (tmp_path / "prohibiciones.parquet").read_bytes() == antes
        assert not (tmp_path / ".descargas").exists()


@pytest.mark.parametrize("codigo", [403, 404])
def test_error_del_portal(portal, tmp_path, codigo):
    portal["codigo"] = codigo
    assert M.main(["--salida", str(tmp_path)]) == 1
    assert f"HTTP {codigo}" in (tmp_path / "raw" / "descarga_log.txt").read_text()
    assert not (tmp_path / "raw" / "relic_pj.csv.gz").exists()


def test_solo_parquet_rehace_igual(portal, tmp_path):
    assert M.main(["--salida", str(tmp_path)]) == 0
    a = pd.read_parquet(tmp_path / "inscripciones.parquet")
    assert M.main(["--salida", str(tmp_path), "--solo-parquet"]) == 0
    assert portal["pedidas"] == [M.URL]
    b = pd.read_parquet(tmp_path / "inscripciones.parquet")
    pd.testing.assert_frame_equal(a, b)
