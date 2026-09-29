"""Consolidaciones de Open Data Barcelona (scripts/ccaa_cataluna_parquet.py, consolidar_bcn):

- cada recurso se construye con todas sus versiones (comun.historico.acumular): una segunda descarga
  que cambia o retira filas conserva las anteriores con _en_ultima_descarga=False; una versión vacía o
  ilegible no retira nada;
- los CSV que no son UTF-8 se leen en CP1252 (antes latin-1: el '€' llegaba como chr(128));
- --semilla añade las publicaciones del perfil de contratante que el release trae y la descarga ya no.
"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ccaa_cataluna_parquet_bcn", REPO_ROOT / "scripts" / "ccaa_cataluna_parquet.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)

META = ["_primera_descarga", "_ultima_descarga", "_en_ultima_descarga"]
URL = "https://contractaciopublica.cat/ca/detall-publicacio/{}/{}"
UUID_A, UUID_B, UUID_C = ("992d779c-6a90-5a7f-047e-a86a9ed3d999", "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9",
                          "11111111-2222-3333-4444-555555555555")


@pytest.fixture(autouse=True)
def sin_semilla(monkeypatch):
    monkeypatch.setattr(cp, "SEMILLA", None, raising=False)   # raising=False: el test corre también con el código anterior


def _recurso(entrada, carpeta, nombre, contenido, encoding="utf-8"):
    ruta = entrada / "02_barcelona" / carpeta / nombre
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(contenido.encode(encoding) if isinstance(contenido, str) else contenido)
    return ruta


def _version_anterior(ruta, sello, contenido, encoding="utf-8"):
    """Una versión anterior del recurso en _historico/, como la deja guardar_version."""
    anterior = ruta.parent / "_historico" / f"{ruta.stem}__{sello}{ruta.suffix}"
    anterior.parent.mkdir(exist_ok=True)
    anterior.write_bytes(contenido.encode(encoding))
    return anterior


def _menores(entrada, salida):
    cp.consolidate_barcelona_menores(entrada, salida)
    return pd.read_parquet(salida / "contratacion" / "contratos_menores_bcn.parquet")


# --- Codificación -------------------------------------------------------------------------------

def test_csv_cp1252_el_euro_y_las_comillas_no_llegan_como_controles_c1(tmp_path):
    """El 2015 de contratos menores está en CP1252: leído como latin-1, ' 1.830,20 € ' llegaba como
    ' 1.830,20 \\x80 ' y 'D’INFORMATICA' como 'D\\x92INFORMATICA' (37.721 celdas en el VPS)."""
    texto = ('Proveïdor,"Objecte del contracte"," Import adjudicat "\r\n'          # como el CSV real
             '"DISTRIBUIDORA D’INFORMATICA SL","“Ordinadors” – 3 unitats…"," 1.830,20 € "\r\n'
             '"Òmnium","Neteja • Sants"," 99,00 € "\r\n')
    ruta = _recurso(tmp_path, "contratos_menores", "2015_menors_.csv", texto, encoding="cp1252")
    df = cp.load_csv(ruta)
    assert df["Proveïdor"].tolist() == ["DISTRIBUIDORA D’INFORMATICA SL", "Òmnium"]
    assert df["Objecte del contracte"].tolist() == ["“Ordinadors” – 3 unitats…", "Neteja • Sants"]
    assert df[" Import adjudicat "].tolist() == [" 1.830,20 € ", " 99,00 € "]
    salida = _menores(tmp_path, tmp_path / "pq")
    assert salida[" Import adjudicat "].tolist() == [" 1.830,20 € ", " 99,00 € "]
    assert not any(chr(c) in v for v in salida[" Import adjudicat "] for c in range(0x80, 0xA0))


def test_utf8_no_cambia_y_los_bytes_sin_asignar_de_cp1252_no_se_pierden(tmp_path):
    # UTF-8: como siempre (va primero)
    utf8 = _recurso(tmp_path, "contratos_menores", "u.csv", "Nom,Import\nL’Hospitalet,5 €\n")
    assert cp.load_csv(utf8)["Nom"].tolist() == ["L’Hospitalet"]
    # Los 5 bytes que CP1252 no asigna (0x81, 0x8D, 0x8F, 0x90, 0x9D) no hacen fallar la lectura: dan el
    # control C1 del mismo valor (como latin-1) y se puede volver a los bytes originales
    crudo = b"Nom,Import\nA\x81B\x8dC\x8fD\x90E\x9d,\x80 5\n"
    ruta = _recurso(tmp_path, "contratos_menores", "raro.csv", crudo)
    df = cp.load_csv(ruta)
    assert df["Nom"].tolist() == ["A\x81B\x8dC\x8fD\x90E\x9d"] and df["Import"].tolist() == ["€ 5"]
    assert "".join(df.iloc[0]).encode(cp.CP1252) == b"A\x81B\x8dC\x8fD\x90E\x9d\x80 5"
    # Los 256 bytes: un carácter cada uno, ida y vuelta sin pérdida
    todos = bytes(range(256))
    assert len(todos.decode(cp.CP1252)) == 256 and todos.decode(cp.CP1252).encode(cp.CP1252) == todos


# --- Versiones -----------------------------------------------------------------------------------

def test_segunda_descarga_conserva_lo_retirado_y_la_version_anterior(tmp_path):
    """Antes el parquet se rehacía solo con la copia vigente: lo que el portal retiraba o cambiaba
    desaparecía (y no había _en_ultima_descarga)."""
    ruta = _recurso(tmp_path, "contratos_menores", "2019_menors.csv",
                    "Expedient,Proveïdor,Import\nE-2,EMPRESA B,20\nE-3,EMPRESA C,31\nE-4,EMPRESA D,40\n")
    _version_anterior(ruta, "20260101T000000Z",
                      "Expedient,Proveïdor,Import\nE-1,EMPRESA A,10\nE-2,EMPRESA B,20\nE-3,EMPRESA C,30\n")
    _recurso(tmp_path, "contratos_menores", "2018_menors.csv", "Expedient,Proveïdor,Import\nX-1,EMPRESA X,7\n")
    df = _menores(tmp_path, tmp_path / "pq")
    assert list(df.columns) == ["Expedient", "Proveïdor", "Import", "_año"] + META     # meta al final
    df19 = df[df["_año"] == 2019].set_index(["Expedient", "Import"])
    assert len(df19) == 5                                                  # 3 vigentes + retirada + versión vieja
    assert not df19.loc[("E-1", 10), "_en_ultima_descarga"]                # retirada: se conserva
    assert not df19.loc[("E-3", 30), "_en_ultima_descarga"]                # versión anterior de E-3
    assert df19.loc[("E-3", 31), "_en_ultima_descarga"] and df19.loc[("E-4", 40), "_en_ultima_descarga"]
    assert df19.loc[("E-1", 10), "_ultima_descarga"] == "2026-01-01T00:00:00Z"
    assert df19.loc[("E-2", 20), "_primera_descarga"] == "2026-01-01T00:00:00Z"
    assert df19.loc[("E-2", 20), "_en_ultima_descarga"]
    # Tipos como en una lectura suelta del CSV (el importe sigue siendo número)
    assert pd.api.types.is_integer_dtype(df["Import"])
    assert df[df["_año"] == 2018]["_en_ultima_descarga"].tolist() == [True]


def test_una_sola_version_es_la_salida_de_siempre_mas_las_columnas_meta(tmp_path):
    _recurso(tmp_path, "contratos_menores", "2019_menors.csv", "Expedient,Import\nE-1,10\nE-2,\n")
    df = _menores(tmp_path, tmp_path / "pq")
    assert df["Expedient"].tolist() == ["E-1", "E-2"] and df["Import"].tolist()[0] == 10
    assert list(df.columns) == ["Expedient", "Import", "_año"] + META and df["_en_ultima_descarga"].all()


@pytest.mark.parametrize("vigente", ["Expedient,Import\n",                        # vacía (solo cabecera)
                                     "<html><body>Error 503</body></html>\n"])    # ilegible
def test_version_vacia_o_ilegible_no_retira_nada(tmp_path, vigente):
    ruta = _recurso(tmp_path, "contratos_menores", "2019_menors.csv", vigente)
    _version_anterior(ruta, "20260101T000000Z", "Expedient,Import\nE-1,10\nE-2,20\n")
    df = _menores(tmp_path, tmp_path / "pq")
    assert df["Expedient"].tolist() == ["E-1", "E-2"] and df["_en_ultima_descarga"].all()


def test_recurso_sin_ninguna_version_legible_no_tumba_el_resto(tmp_path):
    ruta = _recurso(tmp_path, "contratos_menores", "2019_menors.csv", "solo\n1\n")
    _version_anterior(ruta, "20260101T000000Z", "otra\n2\n")
    _recurso(tmp_path, "contratos_menores", "2018_menors.csv", "Expedient,Import\nX-1,7\n")
    df = _menores(tmp_path, tmp_path / "pq")
    assert df["Expedient"].tolist() == ["X-1"]


def test_versiones_de_un_excel_tambien_se_acumulan(tmp_path):
    carpeta = tmp_path / "02_barcelona" / "contratos_menores"
    (carpeta / "_historico").mkdir(parents=True)
    pd.DataFrame({"Id": [1, 2], "Nom": ["a", "b"]}).to_excel(
        carpeta / "_historico" / "2020_menors__20260101T000000Z.xlsx", index=False)
    pd.DataFrame({"Id": [2, 3], "Nom": ["b", "c"]}).to_excel(carpeta / "2020_menors.xlsx", index=False)
    df = _menores(tmp_path, tmp_path / "pq").set_index("Id")
    assert sorted(df.index) == [1, 2, 3] and not df.loc[1, "_en_ultima_descarga"]
    assert df.loc[2, "_en_ultima_descarga"] and df.loc[3, "_en_ultima_descarga"]


# --- Semilla del perfil de contratante --------------------------------------------------------------

def _perfil(entrada):
    _recurso(entrada, "perfil_contratante", "Licitacions_Publicades_PSCP.csv",
             "CODI_EXPEDIENT,ENLLAC_PUBLICACIO,NUMERO_LOT\n"
             f"EXP-A,{URL.format(UUID_A, '300')},\n")
    _recurso(entrada, "perfil_contratante", "Licitacions_fins_20160601.csv", "EXPEDIENT,URL\nOLD-1,http://x\n")


def _release(tmp_path):
    carpeta = tmp_path / "release" / "catalunya"
    (carpeta / "contratacion").mkdir(parents=True)
    # Como el publicado: textos vacíos como '' y el lote como float. La fila del fichero anterior a 2016
    # no tiene uuid: comparada por contenido, '' no casa con el nulo de la descarga y se duplicaría
    pd.DataFrame({
        "CODI_EXPEDIENT": ["EXP-A", "EXP-B", "EXP-B", ""],
        "ENLLAC_PUBLICACIO": [URL.format(UUID_A.upper(), "12"), URL.format(UUID_B, "5"), URL.format(UUID_B, "5"), ""],
        "NUMERO_LOT": [np.nan, 1.0, 2.0, np.nan],
        "EXPEDIENT": ["", "", "", "OLD-1"],
        "URL": ["", "", "", "http://x"],
        "_archivo_origen": ["Licitacions_Publicades_PSCP.csv"] * 3 + ["Licitacions_fins_20160601.csv"],
    }).to_parquet(carpeta / "contratacion" / "perfil_contratante_bcn.parquet", index=False)
    # Los menores del release no se siembran (coinciden con la descarga; no están en SEMILLAS_BCN)
    pd.DataFrame({"Expedient": ["RETIRADO"], "Import": [1], "_año": [2019]}).to_parquet(
        carpeta / "contratacion" / "contratos_menores_bcn.parquet", index=False)
    return carpeta


def test_semilla_del_perfil_anade_las_publicaciones_que_ya_no_se_sirven(tmp_path, monkeypatch):
    """El CSV del perfil es una ventana que el portal va cerrando: el release trae 6.120 publicaciones
    que la primera descarga del VPS ya no. Se casan por el uuid del procedimiento (otra fase u otro
    idioma en la URL es la misma publicación); las filas sin uuid no se siembran."""
    _perfil(tmp_path)
    _recurso(tmp_path, "contratos_menores", "2019_menors.csv", "Expedient,Import\nE-1,10\n")
    monkeypatch.setattr(cp, "SEMILLA", str(_release(tmp_path)))
    cp.consolidate_barcelona_perfil(tmp_path, tmp_path / "pq")
    df = pd.read_parquet(tmp_path / "pq" / "contratacion" / "perfil_contratante_bcn.parquet")
    assert len(df) == 4                                       # 2 descargadas + las 2 filas de EXP-B
    sembradas = df[df["_origen"].notna()]
    assert sembradas["CODI_EXPEDIENT"].tolist() == ["EXP-B", "EXP-B"]
    assert set(sembradas["_origen"]) == {"release v2026.02"} and not sembradas["_en_ultima_descarga"].any()
    assert sembradas["_primera_descarga"].isna().all()        # nulos, no '' (columnas de control)
    descargadas = df[df["_origen"].isna()]
    assert descargadas["_en_ultima_descarga"].all() and descargadas["_primera_descarga"].notna().all()
    assert df["EXPEDIENT"].tolist().count("OLD-1") == 1       # la fila sin uuid no se duplica
    # Sembrar otra vez da lo mismo, y los menores no se siembran
    cp.consolidate_barcelona_perfil(tmp_path, tmp_path / "pq")
    assert len(pd.read_parquet(tmp_path / "pq" / "contratacion" / "perfil_contratante_bcn.parquet")) == 4
    assert _menores(tmp_path, tmp_path / "pq")["Expedient"].tolist() == ["E-1"]


def test_main_pasa_la_semilla_a_barcelona_y_sin_ella_no_se_siembra(tmp_path):
    _perfil(tmp_path / "crudo")
    semilla = _release(tmp_path)
    salida = tmp_path / "pq"
    perfil = salida / "contratacion" / "perfil_contratante_bcn.parquet"
    args = ["--entrada", str(tmp_path / "crudo"), "--salida", str(salida), "--categorias", "contratacion"]
    assert cp.main(args + ["--semilla", str(semilla)]) == 0
    assert len(pd.read_parquet(perfil)) == 4
    assert cp.main(args) == 0 and cp.SEMILLA is None          # una ejecución sin --semilla no arrastra la anterior
    df = pd.read_parquet(perfil)
    assert len(df) == 2 and "_origen" not in df.columns
