"""Tests offline de scripts/ccaa_valencia.py y scripts/ccaa_valencia_parquet.py.

La API CKAN de dadesobertes.gva.es y las descargas se simulan con un
``requests.get`` falso; los CSV se generan en el propio test.
"""

import contextlib
import importlib.util
import os
import runpy
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DESCARGA = REPO_ROOT / "scripts" / "ccaa_valencia.py"
SCRIPT_PARQUET = REPO_ROOT / "scripts" / "ccaa_valencia_parquet.py"


def _cargar(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


V = _cargar("ccaa_valencia", SCRIPT_DESCARGA)
P = _cargar("ccaa_valencia_parquet", SCRIPT_PARQUET)

API_URL = "https://dadesobertes.gva.es/api/3/action/package_show"


# ---------------------------------------------------------------------------
# Simulación de CKAN
# ---------------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status=200, json_data=None, body=b""):
        self.status_code = status
        self._json = json_data
        # body: bytes, o lista de trozos (bytes) y excepciones a lanzar en orden
        self._trozos = body if isinstance(body, list) else [body]
        self.cerrada = False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} Error", response=self)

    def json(self):
        return self._json

    def iter_content(self, chunk_size=8192):
        for trozo in self._trozos:
            if isinstance(trozo, BaseException):
                raise trozo
            yield trozo

    def close(self):
        self.cerrada = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class FakeCKAN:
    """paquetes: dataset_id -> lista de recursos (o código HTTP de error).
    cuerpos: url -> bytes | lista de trozos/excepciones."""

    def __init__(self, paquetes, cuerpos):
        self.paquetes = paquetes
        self.cuerpos = cuerpos
        self.llamadas = []

    def get(self, url, params=None, timeout=None, stream=False):
        self.llamadas.append({"url": url, "params": params, "timeout": timeout, "stream": stream})
        if url == API_URL:
            recursos = self.paquetes.get(params["id"], 404)
            if isinstance(recursos, int):
                return FakeResponse(status=recursos, json_data={"success": False})
            return FakeResponse(json_data={"success": True, "result": {"name": params["id"], "resources": recursos}})
        cuerpo = self.cuerpos[url]
        if isinstance(cuerpo, int):
            return FakeResponse(status=cuerpo)
        return FakeResponse(body=cuerpo)

    def descargas(self):
        return [c["url"] for c in self.llamadas if c["url"] != API_URL]


def _recurso(nombre, url, formato="CSV", rid=None):
    return {"id": rid or str(url).rsplit("/", 1)[-1], "name": nombre, "format": formato, "url": url}


@pytest.fixture
def ckan(monkeypatch):
    """Instala un CKAN falso (vacío) y anula las pausas entre descargas."""
    fake = FakeCKAN({}, {})
    monkeypatch.setattr(requests, "get", fake.get)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    return fake


# ---------------------------------------------------------------------------
# ccaa_valencia.py (descarga)
# ---------------------------------------------------------------------------

def test_catalogo_coincide_con_las_14_categorias_del_readme():
    assert list(V.DATASETS) == [
        "contratacion", "subvenciones", "presupuestos", "convenios", "lobbies",
        "empleo", "paro", "siniestralidad", "patrimonio", "entidades",
        "territorio", "turismo", "sanidad", "transporte",
    ]
    ids = [d for lista in V.DATASETS.values() for d in lista]
    assert len(ids) == len(set(ids)), "dataset repetido en el catálogo"
    assert V.API_URL == API_URL


def test_process_dataset_consulta_package_show_y_descarga_solo_csv(ckan, tmp_path):
    ckan.paquetes["eco-gvo-contratos-2024"] = [
        _recurso("Contratos 2024", "http://gva/c2024.csv"),
        _recurso("Contratos 2024 (json)", "http://gva/c2024.json", formato="JSON"),
        _recurso("Contratos 2024 csv minúsculas", "http://gva/c2024b.csv", formato="csv"),
        _recurso("Sin url", None),
    ]
    ckan.cuerpos.update({"http://gva/c2024.csv": b"a;b\n1;2\n", "http://gva/c2024b.csv": b"a;b\n3;4\n"})

    n, _ = V.process_dataset("eco-gvo-contratos-2024", tmp_path)

    assert n == 2
    api = [c for c in ckan.llamadas if c["url"] == API_URL]
    assert api == [{"url": API_URL, "params": {"id": "eco-gvo-contratos-2024"}, "timeout": 30, "stream": False}]
    assert ckan.descargas() == ["http://gva/c2024.csv", "http://gva/c2024b.csv"]
    assert all(c["stream"] for c in ckan.llamadas if c["url"] != API_URL)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["Contratos 2024 csv minúsculas.csv", "Contratos 2024.csv"]


def test_recursos_con_el_mismo_nombre_en_distintos_datasets_no_se_pierden(ckan, monkeypatch, tmp_path):
    # Caso real de paro: tra-reg-paro-2019/2018/2017 publican recursos sin año
    # con el mismo nombre; antes solo se guardaba el primero (2019) y los demás
    # se daban por "Ya existe" (el paro publicado solo tiene 2019 en esos archivos).
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(V, "DATASETS", {"paro": ["tra-reg-paro-2019", "tra-reg-paro-2018", "tra-reg-paro-2017"]})
    nombre = "Demandantes activos parados por género"
    for anyo in (2019, 2018, 2017):
        ckan.paquetes[f"tra-reg-paro-{anyo}"] = [_recurso(nombre, f"http://gva/paro{anyo}.csv")]
        ckan.cuerpos[f"http://gva/paro{anyo}.csv"] = f"ANYO;TOTAL\n{anyo};1\n".encode()

    V.main()

    carpeta = tmp_path / "valencia_datos" / "paro"
    assert {p.name: p.read_text() for p in carpeta.iterdir()} == {
        f"{nombre}.csv": "ANYO;TOTAL\n2019;1\n",
        f"{nombre}_tra-reg-paro-2018.csv": "ANYO;TOTAL\n2018;1\n",
        f"{nombre}_tra-reg-paro-2017.csv": "ANYO;TOTAL\n2017;1\n",
    }


def test_colisiones_dentro_del_mismo_dataset_y_sin_distinguir_mayusculas(ckan, tmp_path):
    ckan.paquetes["ds"] = [
        _recurso("Lista de Campings", "http://gva/1.csv"),
        _recurso("LISTA DE CAMPINGS", "http://gva/2.csv"),
        _recurso("Lista de Campings", "http://gva/3.csv"),
    ]
    ckan.cuerpos.update({f"http://gva/{i}.csv": f"id;n\n{i};x\n".encode() for i in (1, 2, 3)})

    assert V.process_dataset("ds", tmp_path)[0] == 3
    nombres = {p.name for p in tmp_path.iterdir()}
    # Distintos también en sistemas de archivos que no distinguen mayúsculas
    assert nombres == {"Lista de Campings.csv", "LISTA DE CAMPINGS_ds.csv", "Lista de Campings_ds_2.csv"}
    assert len({n.lower() for n in nombres}) == 3
    assert len({p.read_text() for p in tmp_path.iterdir()}) == 3


def test_reanudacion_asigna_los_mismos_nombres_y_no_redescarga(ckan, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    datasets = ["datos-de-paro-en-la-comunitat-valenciana", "datos-del-paro-en-la-comunidad-valenciana-2024"]
    monkeypatch.setattr(V, "DATASETS", {"paro": datasets})
    for ds in datasets:
        ckan.paquetes[ds] = [_recurso("Descarga recurso en formato CSV", f"http://gva/{ds}.csv")]
        ckan.cuerpos[f"http://gva/{ds}.csv"] = f"origen\n{ds}\n".encode()
    carpeta = tmp_path / "valencia_datos" / "paro"

    V.main()
    antes = {p.name: p.read_text() for p in carpeta.iterdir()}
    n_descargas = len(ckan.descargas())
    V.main()

    assert len(ckan.descargas()) == n_descargas == 2  # la 2ª ejecución no descarga nada
    assert {p.name: p.read_text() for p in carpeta.iterdir()} == antes
    assert antes == {
        "Descarga recurso en formato CSV.csv": f"origen\n{datasets[0]}\n",
        f"Descarga recurso en formato CSV_{datasets[1]}.csv": f"origen\n{datasets[1]}\n",
    }


def test_descarga_interrumpida_no_queda_como_completa(ckan, tmp_path):
    ckan.paquetes["ds"] = [_recurso("Contratos", "http://gva/c.csv")]
    ckan.cuerpos["http://gva/c.csv"] = [b"id;v\n1;AAA", KeyboardInterrupt()]

    with pytest.raises(KeyboardInterrupt):
        V.process_dataset("ds", tmp_path)
    assert list(tmp_path.iterdir()) == []  # ni el CSV truncado ni el .part

    ckan.cuerpos["http://gva/c.csv"] = [b"id;v\n1;AAA", b"\n2;BBB\n"]
    assert V.process_dataset("ds", tmp_path)[0] == 1
    assert (tmp_path / "Contratos.csv").read_bytes() == b"id;v\n1;AAA\n2;BBB\n"


def test_error_de_red_a_mitad_no_deja_parcial_y_se_anota(ckan, tmp_path):
    ckan.paquetes["ds"] = [_recurso("A", "http://gva/a.csv"), _recurso("B", "http://gva/b.csv")]
    ckan.cuerpos["http://gva/a.csv"] = [b"x;y\n1;", requests.exceptions.ChunkedEncodingError("corte")]
    ckan.cuerpos["http://gva/b.csv"] = 500

    assert V.process_dataset("ds", tmp_path) == (0, 0)
    assert list(tmp_path.iterdir()) == []

    fallidos = []
    V.process_dataset("ds", tmp_path, fallidos=fallidos)
    assert [f.split(" (")[0] for f in fallidos] == ["ds: A.csv", "ds: B.csv"]


def test_series_anuales_se_amplian_con_los_anios_que_existen_en_el_portal(ckan):
    # El catálogo fijo llegaba a 2025: 2026 (y años anteriores con el mismo patrón)
    # no se pedían nunca
    datasets = {
        "contratacion": ["eco-gvo-contratos-2024", "eco-gvo-contratos-2025", "eco-contratos-dana"],
        "paro": ["datos-de-paro-en-la-comunitat-valenciana"],
    }
    ckan.paquetes.update({ds: [] for ds in datasets["contratacion"]})
    ckan.paquetes["eco-gvo-contratos-2026"] = []
    ckan.paquetes["eco-gvo-contratos-2013"] = []
    ckan.paquetes["eco-gvo-contratos-2020"] = 500  # error del portal: no se puede saber
    fallidos = []

    ampliado = V.ampliar_series_anuales(datasets, anio_actual=2026, anio_minimo=2010, fallidos=fallidos)

    assert ampliado["contratacion"] == [
        "eco-gvo-contratos-2024", "eco-gvo-contratos-2025", "eco-contratos-dana",
        "eco-gvo-contratos-2013", "eco-gvo-contratos-2026",
    ]
    assert ampliado["paro"] == datasets["paro"]  # sin año: no se sondea
    assert fallidos == ["eco-gvo-contratos-2020: no se pudo comprobar si existe"]
    sondeados = [c["params"]["id"] for c in ckan.llamadas if c["url"] == API_URL]
    assert sorted(sondeados) == sorted(f"eco-gvo-contratos-{a}" for a in range(2010, 2027) if a not in (2024, 2025))
    assert datasets["contratacion"][-1] == "eco-contratos-dana"  # no se modifica el catálogo original


def test_main_descarga_el_anio_nuevo_y_lo_anota(ckan, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(V, "DATASETS", {"contratacion": ["eco-gvo-contratos-2025"]})
    anio = V.datetime.now().year
    if anio <= 2025:
        pytest.skip("el año en curso ya está en el catálogo")
    nuevo = f"eco-gvo-contratos-{anio}"
    ckan.paquetes["eco-gvo-contratos-2025"] = [_recurso("Contratos 2025", "http://gva/c2025.csv")]
    ckan.paquetes[nuevo] = [_recurso("Contratos nuevo", "http://gva/cnuevo.csv")]
    ckan.cuerpos.update({"http://gva/c2025.csv": b"a;b\n1;2\n", "http://gva/cnuevo.csv": b"a;b\n3;4\n"})

    assert V.main() == 0
    carpeta = tmp_path / "valencia_datos" / "contratacion"
    assert sorted(p.name for p in carpeta.iterdir()) == ["Contratos 2025.csv", "Contratos nuevo.csv"]
    log = (tmp_path / "valencia_datos" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "DATASETS NUEVOS" in log and nuevo in log


def test_recurso_actualizado_en_el_portal_se_vuelve_a_descargar(ckan, tmp_path):
    recurso = _recurso("Contratos 2025", "http://gva/c2025.csv")
    ckan.paquetes["eco-gvo-contratos-2025"] = [recurso]
    ckan.cuerpos["http://gva/c2025.csv"] = b"a;b\n1;2\n3;4\n"
    local = tmp_path / "Contratos 2025.csv"
    local.write_bytes(b"a;b\n1;2\n")
    os.utime(local, (1_000_000, 1_000_000))

    # Sin fecha de modificación: se conserva la copia (comportamiento anterior)
    V.process_dataset("eco-gvo-contratos-2025", tmp_path)
    assert local.read_bytes() == b"a;b\n1;2\n" and ckan.descargas() == []

    # Modificado antes de la copia local: tampoco se descarga
    recurso["last_modified"] = "1970-01-02T00:00:00"
    V.process_dataset("eco-gvo-contratos-2025", tmp_path)
    assert ckan.descargas() == []

    # Modificado después: se vuelve a descargar
    recurso["last_modified"] = "2026-01-15T10:00:00.000000"
    assert V.process_dataset("eco-gvo-contratos-2025", tmp_path)[0] == 1
    assert local.read_bytes() == b"a;b\n1;2\n3;4\n"

    # Si la actualización falla se conserva la copia anterior y se anota
    os.utime(local, (1_000_000, 1_000_000))
    ckan.cuerpos["http://gva/c2025.csv"] = 503
    fallidos = []
    V.process_dataset("eco-gvo-contratos-2025", tmp_path, fallidos=fallidos)
    assert local.read_bytes() == b"a;b\n1;2\n3;4\n"
    assert len(fallidos) == 1


def test_download_file_cierra_la_respuesta(monkeypatch, tmp_path):
    respuesta = FakeResponse(status=404)
    monkeypatch.setattr(requests, "get", lambda *a, **k: respuesta)
    ok, _ = V.download_file("http://gva/x.csv", tmp_path / "x.csv")
    assert not ok and respuesta.cerrada


def test_format_y_name_nulos_de_ckan_no_rompen(ckan, tmp_path):
    ckan.paquetes["ds"] = [
        {"name": "sin formato", "format": None, "url": "http://gva/1.csv"},
        {"name": None, "format": "CSV", "url": "http://gva/2.csv"},
        {"name": "   ", "format": "CSV ", "url": "http://gva/3.csv"},
    ]
    ckan.cuerpos.update({"http://gva/2.csv": b"a;b\n1;2\n", "http://gva/3.csv": b"a;b\n3;4\n"})

    assert V.process_dataset("ds", tmp_path)[0] == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["data.csv", "data_ds.csv"]


def test_dataset_inexistente_se_anota_como_fallido(ckan, tmp_path):
    fallidos = []
    assert V.process_dataset("no-existe", tmp_path, None, fallidos) == (0, 0)
    assert fallidos == ["no-existe: sin respuesta válida de la API"]


def test_main_end_to_end_y_codigo_de_salida(ckan, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(V, "DATASETS", {"contratacion": ["eco-gvo-contratos-2014"], "lobbies": ["sec-regia-grupos"]})
    ckan.paquetes["eco-gvo-contratos-2014"] = [_recurso("Contratos 2014", "http://gva/c14.csv")]
    ckan.paquetes["sec-regia-grupos"] = [_recurso("Grupos de interés", "http://gva/g.csv")]
    ckan.cuerpos.update({"http://gva/c14.csv": b"a;b\n1;2\n", "http://gva/g.csv": 503})

    assert V.main() == 1
    salida = capsys.readouterr().out
    assert "DESCARGA COMPLETADA CON ERRORES (1)" in salida
    log = (tmp_path / "valencia_datos" / "descarga_log.txt").read_text(encoding="utf-8")
    assert "ERRORES DE DESCARGA" in log and "sec-regia-grupos: Grupos de interés.csv" in log
    assert (tmp_path / "valencia_datos" / "contratacion" / "Contratos 2014.csv").exists()

    ckan.cuerpos["http://gva/g.csv"] = b"a;b\n1;2\n"  # el portal se recupera: se reanuda
    assert V.main() == 0
    assert "✅ DESCARGA COMPLETADA" in capsys.readouterr().out
    assert ckan.descargas().count("http://gva/c14.csv") == 1


def test_cli_catalogo_completo(ckan, monkeypatch, tmp_path):
    """python scripts/ccaa_valencia.py con todo el catálogo contra el CKAN falso."""
    for lista in V.DATASETS.values():
        for ds in lista:
            ckan.paquetes[ds] = [_recurso(f"Recurso {ds}", f"http://gva/{ds}.csv")]
            ckan.cuerpos[f"http://gva/{ds}.csv"] = f"id;ds\n1;{ds}\n".encode()
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT_DESCARGA), run_name="__main__")

    assert salida.value.code == 0
    base = tmp_path / "valencia_datos"
    assert sorted(p.name for p in base.iterdir() if p.is_dir()) == sorted(V.DATASETS)
    for categoria, lista in V.DATASETS.items():
        assert sorted(p.name for p in (base / categoria).iterdir()) == sorted(f"Recurso {ds}.csv" for ds in lista)


def test_cli_portal_caido_sale_con_error(ckan, monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)  # sin paquetes: todas las consultas dan 404
    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT_DESCARGA), run_name="__main__")
    assert salida.value.code == 1
    assert "DESCARGA COMPLETADA CON ERRORES" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ccaa_valencia_parquet.py (conversión)
# ---------------------------------------------------------------------------

def _modos_texto():
    """Por defecto y, si existe la opción, con texto como object (pandas 2.x)."""
    modos = ["defecto"]
    try:
        pd.get_option("future.infer_string")
        modos.append("object")
    except Exception:  # pandas < 2.1: el texto ya es object
        pass
    return modos


def _modo(modo):
    if modo == "object":
        return pd.option_context("future.infer_string", False)
    return contextlib.nullcontext()


def _es_texto(tipo):
    return pa.types.is_string(tipo) or pa.types.is_large_string(tipo)


def test_detecta_latin1_aunque_el_primer_acento_este_lejos(tmp_path):
    csv = tmp_path / "paro.csv"
    filas = ["id;municipio"] + [f"{i};Municipio{i}" for i in range(50000)] + ["50000;València Alacant ñ"]
    csv.write_bytes(("\n".join(filas) + "\n").encode("latin-1"))

    encoding, sep = P.detect_encoding_and_sep(csv)
    assert encoding != "utf-8" and sep == ";"

    salida = tmp_path / "paro.parquet"
    assert P.convert_to_parquet(csv, salida)
    df = pd.read_parquet(salida)
    assert len(df) == 50001
    assert df["municipio"].iloc[-1] == "València Alacant ñ"


def test_cp1252_conserva_euro_y_comillas(tmp_path):
    # Mismo patrón que convenios 2020 publicado: D’ECONOMÍA, 900,00€, “UMBRACLE”
    texto = "id;objeto\n1;CONSELLERÍA D’ECONOMÍA – 900,00€ “UMBRACLE”\n"
    csv = tmp_path / "convenios.csv"
    csv.write_bytes(texto.encode("cp1252"))

    salida = tmp_path / "convenios.parquet"
    assert P.convert_to_parquet(csv, salida)
    assert pd.read_parquet(salida)["objeto"].iloc[0] == "CONSELLERÍA D’ECONOMÍA – 900,00€ “UMBRACLE”"


def test_utf8_con_bom(tmp_path):
    csv = tmp_path / "bom.csv"
    csv.write_bytes("﻿id;nombre\n1;Elx\n".encode("utf-8"))
    salida = tmp_path / "bom.parquet"
    assert P.convert_to_parquet(csv, salida)
    assert list(pd.read_parquet(salida).columns) == ["id", "nombre", *P.COLUMNAS_META]


@pytest.mark.parametrize("modo", _modos_texto())
def test_vacios_se_guardan_como_nulos_y_no_como_texto_nan(tmp_path, modo):
    csv = tmp_path / "grupos.csv"
    csv.write_text("nombre;nif;ciudad\nAERTE;;Elx\nASCER;G12022687;\n", encoding="utf-8")
    salida = tmp_path / "grupos.parquet"

    with _modo(modo):
        assert P.convert_to_parquet(csv, salida)

    tabla = pq.read_table(salida)
    assert tabla.column("nif").to_pylist() == [None, "G12022687"]
    assert tabla.column("ciudad").to_pylist() == ["Elx", None]
    assert _es_texto(tabla.schema.field("nif").type)


@pytest.mark.parametrize("modo", _modos_texto())
def test_columna_vacia_se_guarda_como_texto_y_se_puede_leer_la_carpeta(tmp_path, modo):
    # Caso real: FONDO_EUROPEO vacío en 2014 (double) y con texto en 2020 hacía
    # fallar pd.read_parquet('valencia/contratacion/') con ArrowInvalid.
    carpeta = tmp_path / "contratacion"
    carpeta.mkdir()
    csv14, csv20 = tmp_path / "c2014.csv", tmp_path / "c2020.csv"
    csv14.write_text("EJERCICIO;FONDO_EUROPEO;IMPORTE\n2014;;10\n2014;;20\n", encoding="utf-8")
    csv20.write_text("EJERCICIO;FONDO_EUROPEO;IMPORTE\n2020;FEDER - Fondo Europeo de Desarrollo Regional;30\n", encoding="utf-8")

    with _modo(modo):
        assert P.convert_to_parquet(csv14, carpeta / "Contratos_2014.parquet")
        assert P.convert_to_parquet(csv20, carpeta / "Contratos_2020.parquet")

    assert _es_texto(pq.read_schema(carpeta / "Contratos_2014.parquet").field("FONDO_EUROPEO").type)
    df = pd.read_parquet(carpeta)
    assert len(df) == 3
    assert df["FONDO_EUROPEO"].isna().sum() == 2
    assert sorted(df["IMPORTE"].tolist()) == [10, 20, 30]


def test_escritura_interrumpida_no_deja_parquet_a_medias(monkeypatch, tmp_path):
    csv = tmp_path / "c.csv"
    csv.write_text("a;b\n1;2\n", encoding="utf-8")
    salida = tmp_path / "c.parquet"
    original = pq.write_table

    def escribe_a_medias(error):
        def _falla(table, where, *args, **kwargs):
            if isinstance(where, (str, os.PathLike)):
                Path(where).write_bytes(b"PAR1 a medias")
            else:
                where.write(b"PAR1 a medias")
            raise error
        return _falla

    monkeypatch.setattr(pq, "write_table", escribe_a_medias(OSError("disco lleno")))
    assert P.convert_to_parquet(csv, salida) is False
    assert sorted(p.name for p in tmp_path.iterdir()) == ["c.csv"]

    monkeypatch.setattr(pq, "write_table", escribe_a_medias(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        P.convert_to_parquet(csv, salida)
    assert sorted(p.name for p in tmp_path.iterdir()) == ["c.csv"]

    monkeypatch.setattr(pq, "write_table", original)
    assert P.convert_to_parquet(csv, salida)
    assert pd.read_parquet(salida)["a"].tolist() == [1]


def test_separador_correcto_aunque_haya_una_linea_mala_al_principio(tmp_path, capsys):
    # Antes: ',' fallaba por la línea mala, se usaba ';' por defecto y salía una
    # única columna con la línea entera.
    csv = tmp_path / "coma.csv"
    csv.write_text("id,nombre,importe\n1,Elx,10\n2,Alcoi, Alcoy,20\n3,Xàtiva,30\n", encoding="utf-8")

    assert P.detect_encoding_and_sep(csv) == ("utf-8", ",")
    salida = tmp_path / "coma.parquet"
    assert P.convert_to_parquet(csv, salida)
    df = pd.read_parquet(salida)
    assert list(df.columns) == ["id", "nombre", "importe", *P.COLUMNAS_META]
    assert df["id"].tolist() == [1, 3]
    capturado = capsys.readouterr()
    # pandas >= 2.1 emite ParserWarning (se cuenta); pandas 2.0 lo escribe él mismo en stderr
    assert "1 líneas mal formadas descartadas" in capturado.out or "Skipping line 3" in capturado.err


@pytest.mark.parametrize("modo", _modos_texto())
def test_codigos_con_ceros_a_la_izquierda_se_guardan_tal_cual(tmp_path, monkeypatch, modo):
    # En los parquet publicados codigo_postal empieza en 3001 y codigo_ine en 3001
    filas = ["codigo_postal;municipio;habitantes;cod_provincia"]
    filas += [f"4600{i};Municipi {i};{1000 + i};46" for i in range(5)]
    filas += ["03001;Alacant;337000;03", ";Sense CP;10;12"]
    csv = tmp_path / "centros.csv"
    csv.write_text("\n".join(filas) + "\n", encoding="utf-8")
    monkeypatch.setattr(P, "FILAS_POR_TROZO", 2)  # el 0 aparece en un trozo posterior

    salida = tmp_path / "centros.parquet"
    with _modo(modo):
        assert P.convert_to_parquet(csv, salida)
    tabla = pq.read_table(salida)
    assert tabla.column("codigo_postal").to_pylist()[-2:] == ["03001", None]
    assert tabla.column("cod_provincia").to_pylist()[-3:] == ["46", "03", "12"]
    assert _es_texto(tabla.schema.field("codigo_postal").type)
    # Sin ceros a la izquierda: sigue siendo numérica
    assert pa.types.is_integer(tabla.schema.field("habitantes").type)


def test_csv_actualizado_se_vuelve_a_convertir(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    carpeta = tmp_path / "valencia_datos" / "contratacion"
    carpeta.mkdir(parents=True)
    csv = carpeta / "Contratos 2025.csv"
    csv.write_text("a;b\n1;2\n", encoding="utf-8")
    assert P.main() == 0
    parquet = tmp_path / "valencia_parquet" / "contratacion" / "Contratos_2025.parquet"
    assert len(pd.read_parquet(parquet)) == 1

    # La descarga trae una versión nueva del CSV: antes se daba por "Ya existe"
    csv.write_text("a;b\n1;2\n3;4\n", encoding="utf-8")
    os.utime(parquet, (1_000_000, 1_000_000))
    assert P.main() == 0
    assert len(pd.read_parquet(parquet)) == 2


def test_archivo_de_una_columna_latin1(tmp_path):
    csv = tmp_path / "una.csv"
    csv.write_bytes("municipio\nValència\nAlacant\n".encode("latin-1"))
    salida = tmp_path / "una.parquet"
    assert P.convert_to_parquet(csv, salida)
    assert pd.read_parquet(salida)["municipio"].tolist() == ["València", "Alacant"]


def test_main_no_existe_entrada(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    assert P.main() == 1


def test_main_sin_csv_no_divide_por_cero(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "valencia_datos" / "paro").mkdir(parents=True)
    assert P.main() == 0


def test_main_end_to_end_nombres_reanudacion_y_errores(monkeypatch, tmp_path, capsys):
    monkeypatch.chdir(tmp_path)
    datos = tmp_path / "valencia_datos"
    (datos / "lobbies").mkdir(parents=True)
    (datos / "turismo").mkdir()
    (datos / "lobbies" / "Grupos de interés.csv").write_text("nombre;nif\nAERTE;G46659728\n", encoding="utf-8")
    (datos / "turismo" / "Lista de Hoteles (Histórico).csv").write_bytes("id;nombre\n1;Hotel Ñ\n".encode("cp1252"))
    (datos / "turismo" / "vacio.csv").write_bytes(b"")
    (datos / "descarga_log.txt").write_text("log", encoding="utf-8")

    with pytest.raises(SystemExit) as salida:
        runpy.run_path(str(SCRIPT_PARQUET), run_name="__main__")
    assert salida.value.code == 1  # vacio.csv no se puede convertir
    out = capsys.readouterr().out
    assert "ARCHIVOS NO CONVERTIDOS (1)" in out and "turismo/vacio.csv" in out

    parquet = tmp_path / "valencia_parquet"
    assert sorted(str(p.relative_to(parquet)) for p in parquet.rglob("*")) == [
        "lobbies", "lobbies/Grupos_de_interés.parquet",
        "turismo", "turismo/Lista_de_Hoteles_(Histórico).parquet",
    ]
    assert pd.read_parquet(parquet / "turismo" / "Lista_de_Hoteles_(Histórico).parquet")["nombre"].tolist() == ["Hotel Ñ"]

    (datos / "turismo" / "vacio.csv").unlink()
    assert P.main() == 0
    assert "Ya existe: Grupos_de_interés.parquet" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline completo: descarga (CKAN simulado) -> conversión
# ---------------------------------------------------------------------------

def test_pipeline_descarga_y_conversion(ckan, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(V, "DATASETS", {
        "contratacion": ["eco-gvo-contratos-2014", "eco-gvo-contratos-2020"],
        "paro": ["tra-reg-paro-2019", "tra-reg-paro-2018"],
    })
    ckan.paquetes.update({
        "eco-gvo-contratos-2014": [_recurso("Contratos inscritos 2014", "http://gva/c2014.csv")],
        "eco-gvo-contratos-2020": [_recurso("Contratos inscritos 2020", "http://gva/c2020.csv")],
        "tra-reg-paro-2019": [_recurso("Demandantes activos parados por género", "http://gva/p2019.csv")],
        "tra-reg-paro-2018": [_recurso("Demandantes activos parados por género", "http://gva/p2018.csv")],
    })
    ckan.cuerpos.update({
        "http://gva/c2014.csv": "EJERCICIO;OBJETO;FONDO_EUROPEO\n2014;Obras en l’Alcúdia – 5€;\n".encode("cp1252"),
        "http://gva/c2020.csv": "EJERCICIO;OBJETO;FONDO_EUROPEO\n2020;Suministro;FEDER\n2020;;FSE\n".encode("utf-8"),
        "http://gva/p2019.csv": b"ANYO;MES;TOTAL\n2019;1;10\n2019;2;11\n",
        "http://gva/p2018.csv": b"ANYO;MES;TOTAL\n2018;1;20\n",
    })

    codigos = (V.main(), P.main())

    base = tmp_path / "valencia_parquet"
    paro = pd.read_parquet(base / "paro")
    assert sorted(paro["ANYO"].unique().tolist()) == [2018, 2019] and len(paro) == 3
    assert sorted(p.name for p in (base / "paro").iterdir()) == [
        "Demandantes_activos_parados_por_género.parquet",
        "Demandantes_activos_parados_por_género_tra-reg-paro-2018.parquet",
    ]

    contratos = pd.read_parquet(base / "contratacion")
    assert len(contratos) == 3
    assert "Obras en l’Alcúdia – 5€" in contratos["OBJETO"].tolist()
    assert contratos["OBJETO"].isna().sum() == 1
    assert "nan" not in contratos["OBJETO"].dropna().tolist()
    assert codigos == (0, 0)


# ---------------------------------------------------------------------------
# Sesgo del superviviente: versiones del CSV (_historico/) y registros acumulados
# ---------------------------------------------------------------------------

CSV_V1 = b"id;cp;importe\n1;03001;10.5\n2;46001;20\n3;12001;30\n"
CSV_V2 = b"id;cp;importe\n1;03001;10.5\n3;12001;35\n4;03002;40\n"


def _portal_con_contratos(ckan, cuerpo):
    recurso = _recurso("Contratos 2025", "http://gva/c2025.csv")
    ckan.paquetes["eco-gvo-contratos-2025"] = [recurso]
    ckan.cuerpos["http://gva/c2025.csv"] = cuerpo
    return recurso


def test_redescarga_conserva_registro_retirado_y_modificado(ckan, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    carpeta = tmp_path / "valencia_datos" / "contratacion"
    carpeta.mkdir(parents=True)
    local = carpeta / "Contratos 2025.csv"
    recurso = _portal_con_contratos(ckan, CSV_V1)
    V.process_dataset("eco-gvo-contratos-2025", carpeta)
    os.utime(local, (1_000_000, 1_000_000))

    # El portal retira el 2, cambia el importe del 3 y añade el 4
    recurso["last_modified"] = "2026-01-15T10:00:00"
    ckan.cuerpos["http://gva/c2025.csv"] = CSV_V2
    assert V.process_dataset("eco-gvo-contratos-2025", carpeta)[0] == 1
    assert local.read_bytes() == CSV_V2
    historico = list((carpeta / "_historico").iterdir())
    assert [p.read_bytes() for p in historico] == [CSV_V1]
    assert historico[0].name == "Contratos 2025__19700112T134640Z.csv"

    # Mismo contenido otra vez: ninguna versión nueva y no se vuelve a pedir
    os.utime(local, (1_000_000, 1_000_000))
    V.process_dataset("eco-gvo-contratos-2025", carpeta)
    V.process_dataset("eco-gvo-contratos-2025", carpeta)
    assert len(ckan.descargas()) == 3
    assert len(list((carpeta / "_historico").iterdir())) == 1

    assert P.main() == 0
    df = pd.read_parquet(tmp_path / "valencia_parquet" / "contratacion" / "Contratos_2025.parquet")
    filas = sorted(zip(df["id"], df["cp"], df["importe"], df["_en_ultima_descarga"]))
    assert filas == [(1, "03001", 10.5, True), (2, "46001", 20.0, False), (3, "12001", 30.0, False),
                     (3, "12001", 35.0, True), (4, "03002", 40.0, True)]
    assert df["id"].dtype == "int64" and df["importe"].dtype == "float64"
    viejo = "1970-01-12T13:46:40Z"
    uno, dos, cuatro = (df[df["id"] == i].iloc[0] for i in (1, 2, 4))
    assert uno["_primera_descarga"] == viejo and uno["_ultima_descarga"] > viejo
    assert dos["_primera_descarga"] == dos["_ultima_descarga"] == viejo
    assert cuatro["_primera_descarga"] > viejo


@pytest.mark.parametrize("cuerpo", [b"", b"  \n", b"id;cp;importe\n", 503,
                                    [b"id;cp;importe\n1;", requests.exceptions.ChunkedEncodingError("corte")]])
def test_descarga_vacia_o_fallida_no_pierde_nada(ckan, monkeypatch, tmp_path, cuerpo):
    monkeypatch.chdir(tmp_path)
    carpeta = tmp_path / "valencia_datos" / "contratacion"
    carpeta.mkdir(parents=True)
    local = carpeta / "Contratos 2025.csv"
    local.write_bytes(CSV_V1)
    os.utime(local, (1_000_000, 1_000_000))
    recurso = _portal_con_contratos(ckan, cuerpo)
    recurso["last_modified"] = "2026-01-15T10:00:00"

    fallidos = []
    assert V.process_dataset("eco-gvo-contratos-2025", carpeta, fallidos=fallidos) == (0, 0)
    assert len(fallidos) == 1 and len(ckan.descargas()) == 1
    assert local.read_bytes() == CSV_V1 and os.path.getmtime(local) == 1_000_000
    assert sorted(p.name for p in carpeta.iterdir()) == ["Contratos 2025.csv"]

    assert P.main() == 0
    df = pd.read_parquet(tmp_path / "valencia_parquet" / "contratacion" / "Contratos_2025.parquet")
    assert df["id"].tolist() == [1, 2, 3] and df["_en_ultima_descarga"].all()


def test_version_vacia_en_el_historico_no_marca_nada_como_retirado(tmp_path):
    csv = tmp_path / "c.csv"
    (tmp_path / "_historico").mkdir()
    (tmp_path / "_historico" / "c__20250101T000000Z.csv").write_bytes(CSV_V1)
    (tmp_path / "_historico" / "c__20250201T000000Z.csv").write_bytes(b"id;cp;importe\n")
    (tmp_path / "_historico" / "c__otro__20250101T000000Z.csv").write_bytes(b"id;cp;importe\n9;1;1\n")
    csv.write_bytes(CSV_V1)
    assert P.convert_to_parquet(csv, tmp_path / "c.parquet")
    df = pd.read_parquet(tmp_path / "c.parquet")
    assert df["id"].tolist() == [1, 2, 3] and df["_en_ultima_descarga"].all()
    assert set(df["_primera_descarga"]) == {"2025-01-01T00:00:00Z"}
    assert df["cp"].tolist() == ["03001", "46001", "12001"]


def test_una_sola_version_da_la_salida_de_siempre_mas_meta(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    carpeta = tmp_path / "valencia_datos" / "turismo"
    carpeta.mkdir(parents=True)
    csv = carpeta / "Hoteles.csv"
    csv.write_bytes("id;cp;nombre;vacia;importe\n1;03001;l’Alcúdia €;;1.5\n2;46001;;;\n".encode("cp1252"))
    os.utime(csv, (1_000_000, 1_000_000))
    parquet = tmp_path / "valencia_parquet" / "turismo" / "Hoteles.parquet"
    parquet.parent.mkdir(parents=True)
    pd.DataFrame({"id": [1]}).to_parquet(parquet)  # parquet de antes, sin columnas meta

    assert P.main() == 0  # aunque sea posterior al CSV, se reconstruye con meta
    tabla = pq.read_table(parquet)
    assert tabla.column_names == ["id", "cp", "nombre", "vacia", "importe", *P.COLUMNAS_META]
    tipos = tabla.schema.types
    assert str(tipos[0]) == "int64" and str(tipos[4]) == "double"
    assert all(_es_texto(t) for t in tipos[1:4])
    df = tabla.to_pandas()
    assert df["cp"].tolist() == ["03001", "46001"]
    assert df["nombre"].tolist()[0] == "l’Alcúdia €" and pd.isna(df["nombre"].iloc[1])
    assert df["vacia"].isna().all() and df["importe"].iloc[0] == 1.5
    assert set(df["_primera_descarga"]) == set(df["_ultima_descarga"]) == {"1970-01-12T13:46:40Z"}
    assert df["_en_ultima_descarga"].dtype == bool and df["_en_ultima_descarga"].all()
    assert not (tmp_path / "valencia_datos" / "turismo" / "_historico").exists()
