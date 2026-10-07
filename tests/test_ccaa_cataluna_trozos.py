"""Conversión de los CSV de Catalunya por trozos (scripts/ccaa_cataluna_parquet.py, convert_to_parquet).

El semanal murió por memoria al convertir publicaciones_pscp.csv: con tres versiones de 2,5 GB, el
código anterior lo tenía todo en memoria (pico de 15,2 GiB). Ahora las versiones se leen por trozos y
se guardan en disco, la acumulación se decide con las huellas de las filas, los tipos se infieren
columna a columna y el Parquet se escribe por grupos de filas. La salida tiene que ser exactamente la de
antes: mismo esquema (con los metadatos de pandas), mismas filas en el mismo orden. La referencia es el
convert_to_parquet anterior (main en 4d908a0), que se copia aquí y usa las funciones que no han
cambiado (load_csv, leer_texto, acumular_versiones, tipos_como_csv, sembrar_release).
"""
import csv
import importlib.util
import io
import os
import random
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ccaa_cataluna_parquet_trozos", REPO_ROOT / "scripts" / "ccaa_cataluna_parquet.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)
from comun.historico import _claves, acumular  # noqa: E402

UUIDS = ["992d779c-6a90-5a7f-047e-a86a9ed3d999", "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9",
         "11111111-2222-3333-4444-555555555555", "abcdefab-cdef-abcd-efab-cdefabcdefab",
         "00000000-0000-0000-0000-000000000001", "fedcba98-7654-3210-fedc-ba9876543210"]
URL = "https://contractaciopublica.cat/{}/detall-publicacio/{}/{}"
CLAVE_PSCP = (["enllac_publicacio"], "uuid_publicacio")


# =============================================================================
# Referencia: convert_to_parquet anterior, todo en memoria
# =============================================================================

def convertir_anterior(input_path, output_path, semilla=None):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    vers = cp.versiones_csv(input_path)
    if len(vers) == 1:
        df = acumular(None, cp.load_csv(input_path), vers[0][1], permitir_vacio=True)
    else:
        df = cp.tipos_como_csv(cp.acumular_versiones(vers, cp.leer_texto),
                               output_path.with_name(output_path.name + '.csv.tmp'))
    if semilla is not None:
        df = cp.sembrar_release(df, *semilla)
    for col in df.columns:
        if cp.es_texto(df[col]) and col not in cp.COLUMNAS_CONTROL:
            df[col] = cp.texto_sin_nulos(df[col])
    df.to_parquet(output_path, index=False, compression='snappy')
    return len(df)


def mismo_parquet(a, b):
    """Mismo esquema (tipos y metadatos de pandas, byte a byte) y mismas filas en el mismo orden."""
    ea, eb = pq.read_schema(a), pq.read_schema(b)
    assert ea.equals(eb, check_metadata=True), f"\n{ea}\n---\n{eb}"
    ta, tb = pq.read_table(a), pq.read_table(b)
    assert ta.num_rows == tb.num_rows
    assert ta.equals(tb), next((c, ta.column(c).to_pylist(), tb.column(c).to_pylist())
                               for c in ta.column_names if not ta.column(c).equals(tb.column(c)))
    pd.testing.assert_frame_equal(pd.read_parquet(a), pd.read_parquet(b))


def comparar(csv_path, tmp_path, semilla=None, filas_por_lote=3, monkeypatch=None, filas_por_trozo=2):
    """Convierte csv_path con el código anterior y con el nuevo (trozos de filas_por_lote filas) y
    comprueba que la salida es la misma. filas_por_trozo: el de la lectura de los ceros a la izquierda,
    que es la misma en los dos. Devuelve la tabla nueva."""
    monkeypatch.setattr(cp, "FILAS_POR_TROZO", filas_por_trozo)
    REVISAR = list(cp.REVISAR)
    antes = tmp_path / "antes" / "x.parquet"
    n_antes = convertir_anterior(csv_path, antes, semilla)
    revisar_antes = list(cp.REVISAR)
    cp.REVISAR[:] = REVISAR
    despues = tmp_path / "despues" / "x.parquet"
    monkeypatch.setattr(cp, "FILAS_POR_LOTE", filas_por_lote)
    n_despues, _ = cp.convert_to_parquet(csv_path, despues, "prueba", semilla=semilla)
    assert cp.REVISAR == revisar_antes
    assert n_despues == n_antes
    mismo_parquet(antes, despues)
    # Nada se queda en la carpeta de la salida salvo el Parquet
    assert sorted(p.name for p in despues.parent.iterdir()) == ["x.parquet"]
    return pq.read_table(despues)


# =============================================================================
# Datos: versiones de un CSV como las de la PSCP y el RPC
# =============================================================================

def _texto_aleatorio(rng):
    return rng.choice(["Obra", "Servei de neteja", "Subministrament, material", 'Lot "A"', "línia 1\nlínia 2",
                       "  espais  ", "L’Hospitalet", "€ 1.830,20", "NA", "null", "", "1e3", "-0", "0.50"])


def _fila(rng, i, columnas):
    u = UUIDS[i % len(UUIDS)] if rng.random() < 0.9 else None
    valores = {
        "codi_ambit": str(rng.choice([1500001, 1500002, 1500003])),
        "codi_ine10": rng.choice(["0801930008", "4300000001", "2512000000", ""]) if i % 7 else "",
        "id": str(i),
        "import": rng.choice(["12.5", "1000", "", "3", "-4.25"]),
        "flag": rng.choice(["True", "False", ""]) if "flag" in columnas else "",
        "flag_ple": rng.choice(["True", "False"]),
        "nom": _texto_aleatorio(rng),
        "mixto": rng.choice(["1", "2.5", "x", "", "True"]),
        "buida": "",
        "lot": rng.choice(["1", "2", ""]),
        "enllac_publicacio": URL.format(rng.choice(["ca", "es"]), u, rng.randint(1, 9)) if u else
        rng.choice(["", "https://altra/url"]),
        "nova": rng.choice(["n1", "n2", ""]),
    }
    return [valores[c] for c in columnas]


def _escribir(ruta, columnas, filas, encoding="utf-8"):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(columnas)
    w.writerows(filas)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_bytes(buf.getvalue().encode(encoding))
    return ruta


COLUMNAS = ["codi_ambit", "codi_ine10", "id", "import", "flag", "flag_ple", "nom", "mixto", "buida", "lot",
            "enllac_publicacio"]


def versiones(tmp_path, semilla, n_versiones=3, n=25, nueva_columna=False, reordenar=False):
    """CSV vigente y n_versiones - 1 anteriores en _historico/: filas que se retiran, que cambian, que
    vuelven, repetidas y nuevas; opcionalmente una columna que aparece en la 2ª versión y otro orden de
    columnas en la última."""
    rng = random.Random(semilla)
    carpeta = tmp_path / "crudo"
    ruta = carpeta / "pscp.csv"
    columnas = list(COLUMNAS)
    base = [_fila(rng, i, columnas) for i in range(n)]
    base += [base[0]] * 2                                       # filas repetidas tal cual
    filas = base
    for v in range(n_versiones):
        if v > 0:
            filas = [f for f in filas if rng.random() > 0.15]   # retiradas
            filas = [list(f) for f in filas]
            for f in filas:
                if rng.random() < 0.15:
                    f[columnas.index("import")] = rng.choice(["99", "", "7.75"])   # cambiadas
            filas += [_fila(rng, 1000 * v + i, columnas) for i in range(rng.randint(0, 6))]   # nuevas
            if base and rng.random() < 0.5:
                vuelve = list(base[rng.randrange(len(base))])   # una que vuelve
                filas.append(vuelve + [""] * (len(columnas) - len(vuelve)))
            if nueva_columna and v == 1:
                columnas = columnas + ["nova"]
                filas = [f + [rng.choice(["n1", "n2", ""])] for f in filas]
        escribir_cols, escribir_filas = columnas, filas
        if reordenar and v == n_versiones - 1:
            orden = list(reversed(range(len(columnas))))
            escribir_cols = [columnas[k] for k in orden]
            escribir_filas = [[f[k] for k in orden] for f in filas]
        if v < n_versiones - 1:
            destino = carpeta / "_historico" / f"pscp__2026010{v + 1}T000000Z.csv"
        else:
            destino = ruta
        _escribir(destino, escribir_cols, escribir_filas)
    os.utime(ruta, (1_780_000_000, 1_780_000_000))
    return ruta


def publicado(tmp_path, nulas=False, vacio=False, tipos_distintos=False):
    """Parquet publicado de la PSCP (la semilla): procedimientos que siguen y otros que ya no se
    descargan, columnas que la descarga no tiene y otras que le faltan."""
    filas = {
        "codi_ambit": [1500001, 1500002, 1500009, 1500003],
        "id": ["1", "2", "900", "901"] if tipos_distintos else [1, 2, 900, 901],
        "import": [12.5, None, 8.0, 1.0],
        "nom": ["Obra", None, "Retirada", "Altra"],
        "enllac_publicacio": [URL.format("ca", UUIDS[0], 1), URL.format("es", UUIDS[1].upper(), 2),
                              URL.format("ca", "77777777-0000-0000-0000-000000000000", 3),
                              (None if nulas else URL.format("ca", "88888888-0000-0000-0000-000000000000", 4))],
        "nomes_publicat": ["a", "b", "c", "d"],
    }
    df = pd.DataFrame(filas)
    if vacio:
        df = df.iloc[:2]
    ruta = tmp_path / "semilla" / "publicaciones_pscp.parquet"
    ruta.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ruta, index=False)
    return ruta


# =============================================================================
# La salida es la de antes
# =============================================================================

@pytest.mark.parametrize("filas_por_lote", [1, 2, 3, 7, 1000])
@pytest.mark.parametrize("semilla", range(6))
def test_varias_versiones_por_trozos_da_lo_mismo_que_de_una_vez(tmp_path, monkeypatch, semilla, filas_por_lote):
    ruta = versiones(tmp_path, semilla)
    tabla = comparar(ruta, tmp_path, filas_por_lote=filas_por_lote, monkeypatch=monkeypatch)
    # Las versiones se conservan: hay filas ya no servidas y fechas de las tres versiones
    df = tabla.to_pandas()
    assert not df["_en_ultima_descarga"].all()
    assert df["_primera_descarga"].nunique() >= 2


@pytest.mark.parametrize("filas_por_lote", [1, 3, 1000])
@pytest.mark.parametrize("semilla", range(4))
def test_una_version_por_trozos_da_lo_mismo_que_de_una_vez(tmp_path, monkeypatch, semilla, filas_por_lote):
    ruta = versiones(tmp_path, semilla, n_versiones=1, n=40)
    tabla = comparar(ruta, tmp_path, filas_por_lote=filas_por_lote, monkeypatch=monkeypatch)
    # Tipos de una lectura de una vez: el cero de codi_ine10 se conserva y lo numérico es numérico
    esquema = tabla.schema
    assert pa.types.is_string(esquema.field("codi_ine10").type) or \
        pa.types.is_large_string(esquema.field("codi_ine10").type)   # string con pandas 2
    assert esquema.field("codi_ambit").type == pa.int64()
    assert esquema.field("import").type == pa.float64()


@pytest.mark.parametrize("semilla", range(4))
def test_columna_nueva_y_columnas_en_otro_orden(tmp_path, monkeypatch, semilla):
    """Una columna que aparece en una versión (las filas anteriores que casan toman su valor) y una
    versión con las columnas en otro orden (la huella se calcula en el orden de la versión nueva)."""
    ruta = versiones(tmp_path, semilla, nueva_columna=True, reordenar=True)
    tabla = comparar(ruta, tmp_path, filas_por_lote=2, monkeypatch=monkeypatch)
    assert "nova" in tabla.column_names


@pytest.mark.parametrize("filas_por_lote", [1, 3, 1000])
@pytest.mark.parametrize("caso", ["normal", "nulas", "vacio", "tipos_distintos"])
@pytest.mark.parametrize("semilla", range(3))
def test_semilla_da_lo_mismo_que_de_una_vez(tmp_path, monkeypatch, semilla, caso, filas_por_lote):
    """La semilla de la PSCP (por el uuid del procedimiento): filas añadidas con _origen y
    _en_ultima_descarga=False, columnas que solo tiene el publicado, columnas de la descarga que el
    publicado no tiene (sus enteros pasan a float con los nulos), filas del publicado sin clave
    (comparadas por contenido) y tipos distintos en el publicado (_armonizar)."""
    ruta = versiones(tmp_path, semilla)
    sem = (publicado(tmp_path, **({caso: True} if caso != "normal" else {})), CLAVE_PSCP)
    tabla = comparar(ruta, tmp_path, semilla=sem, filas_por_lote=filas_por_lote, monkeypatch=monkeypatch)
    df = tabla.to_pandas()
    assert "_origen" in df.columns and "nomes_publicat" in df.columns
    sembradas = df[df["_origen"].notna()]
    assert set(sembradas["_origen"]) <= {"release v2026.02"} and not sembradas["_en_ultima_descarga"].any()


@pytest.mark.parametrize("semilla", range(3))
def test_semilla_con_clave_de_varias_columnas(tmp_path, monkeypatch, semilla):
    """Como el RPC: clave_texto sobre columnas que la descarga tiene como número y el publicado como texto."""
    ruta = versiones(tmp_path, semilla, n_versiones=2)
    sem = (publicado(tmp_path, tipos_distintos=True), ["id", "codi_ambit"])
    comparar(ruta, tmp_path, semilla=sem, filas_por_lote=2, monkeypatch=monkeypatch)


def test_semilla_que_no_existe_o_sin_columnas_de_la_clave(tmp_path, monkeypatch):
    ruta = versiones(tmp_path, 1)
    comparar(ruta, tmp_path / "a", semilla=(tmp_path / "no_existe.parquet", CLAVE_PSCP), monkeypatch=monkeypatch)
    comparar(ruta, tmp_path / "b", semilla=(publicado(tmp_path), ["no_esta"]), monkeypatch=monkeypatch)


# --- Cero filas ------------------------------------------------------------------------------------

def test_csv_solo_con_cabecera(tmp_path, monkeypatch):
    ruta = _escribir(tmp_path / "crudo" / "v.csv", COLUMNAS, [])
    tabla = comparar(ruta, tmp_path, monkeypatch=monkeypatch)
    assert tabla.num_rows == 0 and tabla.column_names[-3:] == list(cp.COLUMNAS_META)


def test_cero_filas_con_semilla(tmp_path, monkeypatch):
    ruta = _escribir(tmp_path / "crudo" / "v.csv", COLUMNAS, [])
    tabla = comparar(ruta, tmp_path, semilla=(publicado(tmp_path), CLAVE_PSCP), monkeypatch=monkeypatch)
    assert tabla.num_rows == 4   # todo el publicado


def test_versiones_vacias_no_retiran_nada(tmp_path, monkeypatch):
    """Una versión vacía (salvo la primera) se ignora; con todas vacías la salida no tiene filas."""
    carpeta = tmp_path / "crudo"
    _escribir(carpeta / "_historico" / "v__20260101T000000Z.csv", COLUMNAS, [])
    ruta = _escribir(carpeta / "v.csv", COLUMNAS, [])
    assert comparar(ruta, tmp_path / "a", monkeypatch=monkeypatch).num_rows == 0
    rng = random.Random(3)
    _escribir(carpeta / "_historico" / "v__20260102T000000Z.csv", COLUMNAS,
              [_fila(rng, i, COLUMNAS) for i in range(5)])
    tabla = comparar(ruta, tmp_path / "b", monkeypatch=monkeypatch)
    assert tabla.num_rows == 5 and all(tabla.column("_en_ultima_descarga").to_pylist())


def test_version_sin_una_columna_es_un_caso_a_revisar(tmp_path, monkeypatch):
    rng = random.Random(4)
    carpeta = tmp_path / "crudo"
    _escribir(carpeta / "_historico" / "v__20260101T000000Z.csv", COLUMNAS, [_fila(rng, i, COLUMNAS) for i in range(6)])
    sin = [c for c in COLUMNAS if c != "nom"]
    ruta = _escribir(carpeta / "v.csv", sin, [_fila(rng, i, sin) for i in range(6)])
    cp.REVISAR.clear()
    comparar(ruta, tmp_path, monkeypatch=monkeypatch)
    assert len(cp.REVISAR) == 1 and "faltan ['nom']" in cp.REVISAR[0]
    cp.REVISAR.clear()


# --- Lecturas que pandas no hace igual por trozos ------------------------------------------------------

@pytest.mark.parametrize("posicion", range(1, 9))
def test_linea_con_campos_de_mas_en_cualquier_frontera(tmp_path, monkeypatch, posicion, caplog):
    """pandas por trozos no comprueba la primera línea de cada trozo: una con campos de más se quedaría
    recortada en vez de descartarse. La conversión lo detecta y lee el CSV de una vez, como antes."""
    lineas = ["a,b,c"] + [f"{i},x{i},{i}.5" for i in range(10)]
    lineas.insert(posicion + 1, "99,y,1,extra")
    ruta = tmp_path / "crudo" / "malo.csv"
    ruta.parent.mkdir(parents=True)
    ruta.write_text("\n".join(lineas) + "\n", encoding="utf-8")
    with caplog.at_level("INFO"):
        tabla = comparar(ruta, tmp_path, filas_por_lote=3, monkeypatch=monkeypatch)
    assert tabla.column("a").to_pylist() == list(range(10))
    assert "1 líneas mal formadas descartadas" in caplog.text


def test_lineas_cortas_y_cp1252(tmp_path, monkeypatch):
    texto = "Nom;Import;Codi\nL’Hospitalet;1.830,20 €;08002\nGirona;5\n" + "".join(
        f"m{i};{i};0{i}\n" for i in range(9))
    ruta = tmp_path / "crudo" / "cp.csv"
    ruta.parent.mkdir(parents=True)
    ruta.write_bytes(texto.encode("cp1252"))
    comparar(ruta, tmp_path, filas_por_lote=2, monkeypatch=monkeypatch)


def test_indice_implicito(tmp_path, monkeypatch):
    """Más campos que cabeceras en la primera fila: pandas usa la 1ª columna como índice (y no se
    restauran ceros); la salida, la de siempre."""
    ruta = tmp_path / "crudo" / "imp.csv"
    ruta.parent.mkdir(parents=True)
    ruta.write_text("a,b\n" + "".join(f"0{i},{i},x{i}\n" for i in range(7)), encoding="utf-8")
    comparar(ruta, tmp_path, filas_por_lote=2, monkeypatch=monkeypatch)


# =============================================================================
# Huellas, memoria y ficheros
# =============================================================================

def test_huellas_iguales_a_las_de_acumular():
    """Las huellas con las que se casan las versiones son las de comun.historico._claves sobre la lectura
    de texto (leer_texto): texto, nulos, '\\x00', vacíos, Unicode y saltos de línea."""
    rng = np.random.default_rng(0)
    valores = np.array(["a", "", "\x00", "ñ€", "1", "1.0", " x ", "a\nb", None], dtype=object)
    for _ in range(20):
        n = int(rng.integers(0, 40))
        texto = {c: list(rng.choice(valores, n)) for c in ("x", "y", "z")}
        df = pd.DataFrame({c: pd.Series(v) for c, v in texto.items()})   # str en pandas 3, object en 2
        for columnas in (["x"], ["x", "y", "z"], ["z", "x"]):
            lote = pa.RecordBatch.from_pydict({c: texto[c] for c in columnas}, schema=cp._esquema_texto(columnas))
            np.testing.assert_array_equal(cp._huellas_lote(lote, columnas), _claves(df, columnas).to_numpy())


def test_no_lee_entero_un_csv_bien_formado(tmp_path, monkeypatch):
    """Las versiones (y el texto acumulado) solo se leen por trozos: leer de una vez una versión de la
    PSCP pasaba de 8 GiB."""
    ruta = versiones(tmp_path, 2)
    leer = pd.read_csv
    enteras = []

    def espia(path, *args, **kwargs):
        if kwargs.get("chunksize") is None and "columna_" not in str(path):
            enteras.append(str(path))
        return leer(path, *args, **kwargs)

    monkeypatch.setattr(cp, "FILAS_POR_LOTE", 4)
    monkeypatch.setattr(cp.pd, "read_csv", espia)
    cp.convert_to_parquet(ruta, tmp_path / "pq" / "x.parquet", "x", semilla=(publicado(tmp_path), CLAVE_PSCP))
    assert enteras == []


def test_si_falla_a_medias_el_parquet_anterior_sigue_entero(tmp_path, monkeypatch):
    ruta = versiones(tmp_path, 5)
    salida = tmp_path / "pq" / "x.parquet"
    cp.convert_to_parquet(ruta, salida, "x")
    antes = salida.read_bytes()
    escribir = pq.ParquetWriter.write_table
    llamadas = []

    def falla_al_segundo(self, *a, **k):
        llamadas.append(1)
        if len(llamadas) == 2:
            raise OSError("disco lleno")
        return escribir(self, *a, **k)

    monkeypatch.setattr(cp, "FILAS_POR_LOTE", 3)
    monkeypatch.setattr(pq.ParquetWriter, "write_table", falla_al_segundo)
    with pytest.raises(OSError):
        cp.convert_to_parquet(ruta, salida, "x")
    assert salida.read_bytes() == antes
    assert sorted(p.name for p in salida.parent.iterdir()) == ["x.parquet"]   # sin restos de la carpeta temporal


def test_restos_de_una_ejecucion_que_murio_se_borran(tmp_path, monkeypatch):
    ruta = versiones(tmp_path, 6, n_versiones=1)
    salida = tmp_path / "pq" / "x.parquet"
    restos = salida.with_name(cp.CARPETA_TROZOS.format(salida.name))
    restos.mkdir(parents=True)
    (restos / "version_0.arrow").write_bytes(b"a medias")
    cp.convert_to_parquet(ruta, salida, "x")
    assert not restos.exists() and salida.exists()


def test_main_convierte_por_trozos(tmp_path, monkeypatch):
    """main() con --semilla: la PSCP de las ejecuciones reales pasa por convert_to_parquet por trozos."""
    for nombre in ("menores", "contratistas", "perfil", "modificaciones", "resumen", "autorizacion"):
        monkeypatch.setattr(cp, f"consolidate_barcelona_{nombre}", lambda i, o: (0, 0))
    monkeypatch.setattr(cp, "FILAS_POR_LOTE", 4)
    entrada = tmp_path / "crudo"
    destino = entrada / "01_transparencia_catalunya" / "01_contratacion" / "publicaciones_pscp.csv"
    origen = versiones(tmp_path / "gen", 7)
    destino.parent.mkdir(parents=True)
    (destino.parent / "_historico").mkdir()
    for p in (origen.parent / "_historico").iterdir():
        (destino.parent / "_historico" / p.name.replace("pscp__", "publicaciones_pscp__")).write_bytes(p.read_bytes())
    destino.write_bytes(origen.read_bytes())
    semilla = tmp_path / "release" / "catalunya"
    (semilla / "contratacion").mkdir(parents=True)
    publicado(tmp_path).rename(semilla / "contratacion" / "publicaciones_pscp.parquet")
    assert cp.main(["--entrada", str(entrada), "--salida", str(tmp_path / "pq"), "--categorias", "contratacion",
                    "--semilla", str(semilla)]) == 0
    df = pd.read_parquet(tmp_path / "pq" / "contratacion" / "publicaciones_pscp.parquet")
    assert df["_origen"].notna().any() and not df["_en_ultima_descarga"].all()
