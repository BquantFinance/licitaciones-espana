"""Tests offline de scripts/medir_consultas.py.

Se construye una release diminuta con la estructura REAL de v2026.10 (una carpeta por ZIP, como deja
`unzip X.zip -d X`, con las 6 partes de la PLACSP en licitaciones_completo/, carpetas v2026.02/ y _historico/
que no se deben leer, y los nombres y tipos de columna de los ficheros publicados) y con los casos que hacen
que una consulta ingenua cuente mal: versiones anteriores, entradas repetidas con otro contenido, el tope de un
acuerdo marco repetido en cada empresa, avisos del TED cancelados en una versión posterior, el mismo menor
publicado dos veces, fases anteriores de la PSCP, NIF enmascarados... Cada consulta tiene que dar el resultado
contado a mano, con DuckDB y con pandas. También el caso de cero resultados, las tablas o columnas que faltan
y que el JSON no lleve rutas.
"""
import datetime as dt
import importlib.util
import json
import sys
from pathlib import Path

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")
pytest.importorskip("pandas")

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("medir_consultas", REPO / "scripts" / "medir_consultas.py")
mc = importlib.util.module_from_spec(_spec)
sys.modules["medir_consultas"] = mc
_spec.loader.exec_module(mc)

UTC = dt.timezone.utc


def _ts(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=UTC)


def _escribir(ruta, columnas, filas):
    """Parquet con el esquema dado ({nombre: tipo pyarrow}) y las filas (lista de tuplas en ese orden)."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    nombres = list(columnas)
    datos = {n: [f[i] for f in filas] for i, n in enumerate(nombres)}
    pq.write_table(pa.table(datos, schema=pa.schema([(n, t) for n, t in columnas.items()])), ruta)


S, F, B = pa.large_string(), pa.float64(), pa.bool_()
TS_UTC = pa.timestamp("us", tz="UTC")

COL_LIC = {"id": S, "objeto": S, "nuts": S, "fecha_updated": TS_UTC, "ano": F, "n_versiones": pa.int64(),
           "es_ultima_version": B, "entrada_repetida": B}
# (id, objeto, nuts, fecha_updated, ano, n_versiones, es_ultima_version, entrada_repetida)
LIC = {
    "licitaciones_completo_sin_ano": [
        # L3: la misma entrada (id y fecha) publicada dos veces, con otro año. Gana la primera leída (la de 2026,
        # sin entrada_repetida), aunque esté en un fichero posterior: las partes no conservan el orden de lectura
        ("L3", "Suministro de papel", "ES300", _ts("2026-02-01"), None, 1, False, True)],
    "licitaciones_completo_hasta_2019": [("L4", "Limpieza", "ES618", _ts("2018-06-01"), 2018.0, 1, True, False)],
    "licitaciones_completo_2020_2021": [("L2", "Obras de un puente", "ES511", _ts("2020-05-01"), 2020.0, 1, True, False)],
    "licitaciones_completo_2022_2023": [("L5", "Servicio de ambulancias (desierto)", None, _ts("2022-01-01"), 2022.0, 1,
                                          True, False)],
    "licitaciones_completo_2024_2025": [
        ("L1", "Servicio de AMBULANCIAS", "ES300", _ts("2024-01-01"), 2024.0, 2, False, False),   # versión anterior
        ("L1", "Servicio de AMBULANCIAS", "ES300", _ts("2024-03-01"), 2024.0, 2, True, False)],
    "licitaciones_completo_desde_2026": [("L3", "Suministro de papel", "ES300", _ts("2026-02-01"), 2026.0, 1, True,
                                          False)],
}
COL_RES = {"id": S, "lote": S, "fecha_updated": TS_UTC, "nif_adjudicatario": S, "adjudicatario": S,
           "importe_adjudicacion": F, "fecha_adjudicacion": pa.timestamp("us"), "es_ultima_version": B,
           "entrada_repetida": B}
_FA = dt.datetime(2024, 2, 1)
RES = [
    ("L1", None, _ts("2024-01-01"), "B12345678", "EMPRESA UNO SL", 999.0, _FA, False, False),   # versión anterior
    ("L1", None, _ts("2024-03-01"), "B-12345678", "EMPRESA UNO SL", 100.0, _FA, True, False),
    # L2, lote 1: acuerdo marco con tres empresas y el mismo tope en cada fila
    ("L2", "1", _ts("2020-05-01"), "B12345678", "EMPRESA UNO, S.L.", 3000.0, _FA, True, False),
    ("L2", "1", _ts("2020-05-01"), "A87654321", "OTRA SA", 3000.0, _FA, True, False),
    ("L2", "1", _ts("2020-05-01"), "ESB11111111", "TERCERA SL", 3000.0, _FA, True, False),
    ("L2", "2", _ts("2020-05-01"), "b12345678", "EMPRESA UNO SL", 50.0, dt.datetime(2020, 6, 1), True, False),
    ("L3", None, _ts("2026-02-01"), "*** 9265 **", "PERSONA FÍSICA", 10.0, _FA, True, False),   # enmascarado
    ("L4", None, _ts("2018-06-01"), "-", "SIN NIF", 5.0, _FA, True, False),                     # marcador
    ("L5", None, _ts("2022-01-01"), None, None, None, None, True, False),                       # desierto
]
COL_TED = {"ted_notice_id": S, "year": pa.int64(), "cancelled": S, "_en_ultima_descarga": B, "_ultima_descarga": S}
TED = [
    ("1-2024", 2024, "0", True, "2026-09-01"), ("1-2024", 2024, "0", True, "2026-09-01"),   # 2 lotes
    ("1-2024", 2024, "0", False, "2026-01-01"),                                               # versión anterior
    ("2-2024", 2024, "1", True, "2026-09-01"),        # cancelado en la última descarga...
    ("2-2024", 2024, "0", False, "2026-01-01"),       # ...no vuelve con la versión anterior
    ("3-2023", 2023, "0", False, "2025-06-01"), ("3-2023", 2023, "0", False, "2025-06-01"),   # retirado
    ("3-2023", 2023, "0", False, "2025-01-01"),       # versión aún más antigua del retirado
    (None, 2023, None, True, "2026-09-01"),           # sin id: cuenta como un aviso
]
COL_MAD = {c: pa.string() for c in ("Tipo de Publicación", "Estado", "Entidad Adjudicadora", "Nº Expediente",
                                     "Referencia", "Título del contrato", "Tipo de contrato",
                                     "Procedimiento de adjudicación", "Presupuesto de licitación", "Nº de ofertas",
                                     "Resultado", "NIF del adjudicatario", "Adjudicatario", "Fecha del contrato",
                                     "Importe de adjudicación")}
COL_MAD["_en_ultima_descarga"] = B
_M = "Contratos menores"


def _mad(tipo, ref, exp, fecha, importe, en_ultima=True, entidad="Hospital X"):
    return (tipo, "Adjudicado", entidad, exp, ref, "Material", "Suministros", "Menor", "1.000,00", "1", "Adjudicado",
            "B12345678", "EMPRESA UNO SL", fecha, importe, en_ultima)


MAD = [
    _mad(_M, "R1", "E1", "19 de mayo del 2023", "1.234,56"),
    _mad(_M, "R2", "E1", "19 de mayo del 2023", "1.234,56"),                # el mismo menor con otra Referencia
    _mad(_M, "R3", "E3", "1 de enero del 2024", "9.999,00", en_ultima=False),  # versión anterior de R3
    _mad(_M, "R3", "E3", "1 de enero del 2024", "2.000,00"),
    _mad(_M, "R4", "E4", "3 de marzo del 2024", "500,00", en_ultima=False),    # retirado: se conserva
    _mad("Convocatoria anunciada a licitación", "R5", "E5", "", "100.000,00"),
    _mad("", "", "", "", "0,00"),                                               # fila de continuación
]
COL_PSCP = {"procediment": S, "fase_publicacio": S, "identificacio_adjudicatari": S, "denominacio_adjudicatari": S,
            "import_adjudicacio_sense_iva": S, "enllac_publicacio": S, "_en_ultima_descarga": B,
            "_ultima_descarga": S}
_CM, _AG = "Contracte menor", "Publicació agregada de contractes"


def _url(u):
    return f"https://contractaciopublica.cat/ca/detall-publicacio/{u}/123"


PSCP = [
    (_CM, _AG, "B11111111", "ACME SL", "100.50", _url("u1"), True, "2026-09-01"),
    (_CM, "Adjudicació", "B11111111", "ACME SL", "999", _url("u1"), False, "2026-02-01"),   # fase anterior de u1
    (_CM, _AG, "B11111111", "ACME, S.L.", "200", _url("u2"), True, "2026-09-01"),
    (_CM, "Anul·lació", "B11111111", "ACME SL", "70", _url("u3"), True, "2026-09-01"),        # anulado
    (_CM, _AG, "B11111111||A22222222", "ACME SL||BETA SA", "1||2", _url("u4"), True, "2026-09-01"),
    (_CM, _AG, "A22222222", "BETA SA", "", _url("u5"), True, "2026-09-01"),                  # sin importe
    (_CM, _AG, "*** 1234 **", "PERSONA", "30", _url("u6"), True, "2026-09-01"),             # enmascarado
    (_CM, _AG, "B11111111", "ACME SL", "50", _url("u7"), False, "2026-02-01"),              # retirado: cuenta
    ("Obert", "Formalització", "B11111111", "ACME SL", "5000", _url("u8"), True, "2026-09-01"),
]
COL_BE = {"fecha_borme": pa.timestamp("ns"), "actos": S, "capital_euros": F}
BE = [
    (dt.datetime(2020, 1, 2), "Constitución|Nombramientos", 3000.0),
    (dt.datetime(2020, 5, 5), "Constitución", 6000.0),
    (dt.datetime(2021, 3, 3), "Nombramientos", None),
    (dt.datetime(2021, 4, 4), "Constitución|Declaración de unipersonalidad", None),
    (dt.datetime(2021, 4, 5), "Reconstitución", 1.0),      # otra palabra: no es una constitución
    (dt.datetime(2021, 4, 6), None, 2.0),
]
COL_BC = {"empresa_norm": S, "tipo_acto": S, "persona_hash": S}
BC = [("ALFA", "nombramiento", "h1"), ("ALFA", "nombramiento", "h1"), ("ALFA", "nombramiento", "h2"),
      ("BETA", "nombramiento", "h3"), ("ALFA", "cese", "h4"), (None, "nombramiento", "h5")]

ZIP_PARTE = {"licitaciones_completo_sin_ano": "nacional_licitaciones_hasta_2019",
             "licitaciones_completo_hasta_2019": "nacional_licitaciones_hasta_2019",
             "licitaciones_completo_2020_2021": "nacional_licitaciones_2020_2021",
             "licitaciones_completo_2022_2023": "nacional_licitaciones_2022_2023",
             "licitaciones_completo_2024_2025": "nacional_licitaciones_2024_2025",
             "licitaciones_completo_desde_2026": "nacional_licitaciones_desde_2026"}


def crear_release(raiz: Path, un_solo_directorio=False, sin=()):
    """La release v2026.10 diminuta, con la misma estructura de carpetas y nombres que los ZIP."""
    def carpeta(zip_):
        return raiz if un_solo_directorio else raiz / zip_
    for parte, filas in LIC.items():
        _escribir(carpeta(ZIP_PARTE[parte]) / "licitaciones_completo" / f"{parte}.parquet", COL_LIC, filas)
    _escribir(carpeta("nacional_resultados") / "licitaciones_completo_resultados.parquet", COL_RES, RES)
    if "ted" not in sin:
        _escribir(carpeta("ted") / "ted_es_can.parquet", COL_TED, TED)
    _escribir(carpeta("comunidad_madrid") / "contratacion_comunidad_madrid_completo.parquet", COL_MAD, MAD)
    _escribir(carpeta("catalunya") / "contratacion" / "publicaciones_pscp.parquet", COL_PSCP, PSCP)
    if "borme" not in sin:
        _escribir(carpeta("borme") / "borme_empresas_pub.parquet", COL_BE, BE)
        _escribir(carpeta("borme") / "borme_cargos_pub.parquet", COL_BC, BC)
    # Lo que no se debe leer: la copia de v2026.02 dentro del ZIP y el _historico/ de los scripts
    _escribir(carpeta("ted") / "v2026.02" / "ted_es_can.parquet", COL_TED, TED * 3)
    _escribir(carpeta("catalunya") / "v2026.02" / "contratacion" / "publicaciones_pscp.parquet", COL_PSCP, PSCP * 2)
    _escribir(carpeta("nacional_resultados") / "_historico" / "licitaciones_completo_resultados.parquet", COL_RES,
              RES * 2)
    return raiz


def medir_json(datos: Path, salida: Path, *extra):
    assert mc.main(["--datos", str(datos), "--repeticiones", "1", "--json", str(salida), *extra]) == 0
    return json.loads(salida.read_text(encoding="utf-8"))


def por_id(res, motor="duckdb"):
    return {c["id"]: c for c in res["consultas"] if c["motor"] == motor}


@pytest.fixture(scope="module")
def medida(tmp_path_factory):
    raiz = crear_release(tmp_path_factory.mktemp("release"))
    res = medir_json(raiz, raiz.parent / "resultado.json", "--motor", "ambos", "--perfil", "prueba",
                     "--etiqueta", "release diminuta", "--nif", "b-12345678")
    return raiz, res


# ── Descubrimiento ─────────────────────────────────────────────────────────────────────────────────────────────────

def test_encuentra_las_partes_de_la_release_y_salta_las_copias(tmp_path):
    d = mc.buscar_tablas(crear_release(tmp_path))
    assert sorted(p.name for p in d.tablas["licitaciones"]) == sorted(f"{x}.parquet" for x in LIC)
    assert d.tablas["resultados"] == [tmp_path / "nacional_resultados" / "licitaciones_completo_resultados.parquet"]
    assert d.tablas["ted"] == [tmp_path / "ted" / "ted_es_can.parquet"]
    assert "pscp" in d.tablas and "borme_cargos" in d.tablas and "madrid" in d.tablas
    assert sorted(d.saltadas) == sorted(["catalunya/v2026.02", "nacional_resultados/_historico", "ted/v2026.02"])
    assert not any("v2026.02" in str(p) or "_historico" in str(p) for p in d.todos)
    assert len(d.todos) == 6 + 6
    assert d.avisos == []


def test_todos_los_zip_en_la_misma_carpeta(tmp_path):
    d = mc.buscar_tablas(crear_release(tmp_path, un_solo_directorio=True))
    assert len(d.tablas["licitaciones"]) == 6
    assert all(p.parent == tmp_path / "licitaciones_completo" for p in d.tablas["licitaciones"])
    assert d.tablas["ted"] == [tmp_path / "ted_es_can.parquet"]


def test_salida_del_scraper_un_solo_fichero(tmp_path):
    filas = [f for v in LIC.values() for f in v]
    _escribir(tmp_path / "salida" / "licitaciones_completo.parquet", COL_LIC, filas)
    _escribir(tmp_path / "salida" / "_historico" / "licitaciones_completo.parquet", COL_LIC, filas * 2)
    d = mc.buscar_tablas(tmp_path)
    assert d.tablas["licitaciones"] == [tmp_path / "salida" / "licitaciones_completo.parquet"]
    # con partes y fichero único a la vez, las partes (y se avisa)
    crear_release(tmp_path / "rel")
    d = mc.buscar_tablas(tmp_path)
    assert len(d.tablas["licitaciones"]) == 6
    assert any("se usan las partes" in a for a in d.avisos)


def test_dos_copias_del_mismo_fichero_avisa_y_usa_la_mas_cercana(tmp_path):
    crear_release(tmp_path)
    _escribir(tmp_path / "copia" / "otra" / "borme_cargos_pub.parquet", COL_BC, BC * 5)
    d = mc.buscar_tablas(tmp_path)
    assert d.tablas["borme_cargos"] == [tmp_path / "borme" / "borme_cargos_pub.parquet"]
    assert any("borme_cargos_pub.parquet: 2 copias" in a for a in d.avisos)


# ── Lo que cuenta cada consulta (DuckDB) ───────────────────────────────────────────────────────────────────────────

def test_ninguna_consulta_falla_ni_se_salta(medida):
    _, res = medida
    for c in res["consultas"]:
        assert "error" not in c and "saltada" not in c, c
    assert {c["id"] for c in res["consultas"]} == {c.id for c in mc.CONSULTAS}


def test_todo_cuenta_ficheros_y_filas_sin_las_copias(medida):
    _, res = medida
    filas = sum(len(v) for v in LIC.values()) + len(RES) + len(TED) + len(MAD) + len(PSCP) + len(BE) + len(BC)
    assert por_id(res)["todo"]["resultado"] == [[12, filas]]


def test_placsp_ultima_version_una_por_licitacion(medida):
    _, res = medida
    c = por_id(res)
    # L4 2018, L2 2020, L5 2022, L1 2024 (una vez) y L3 2026 (la primera leída de la entrada repetida, no la sin año)
    esperado = [[2018, 1], [2020, 1], [2022, 1], [2024, 1], [2026, 1]]
    assert c["placsp_ultima_version"]["resultado"] == esperado
    assert c["placsp_ultima_version_calculada"]["resultado"] == esperado
    assert c["placsp_ultima_version_calculada"]["coincide_con_placsp_ultima_version"] is True


def test_texto_y_nif_cuentan_solo_la_ultima_version(medida):
    _, res = medida
    c = por_id(res)
    assert c["placsp_texto"]["resultado"] == [[2]]          # L1 una vez (no su versión anterior) y L5
    lotes, licitaciones, primera, ultima = c["placsp_un_nif"]["resultado"][0]
    assert (lotes, licitaciones) == (3, 2)                    # B-12345678, B12345678 y b12345678 son el mismo
    assert primera.startswith("2020-06-01") and ultima.startswith("2024-02-01")


def test_top_adjudicatarios_sin_duplicados(medida):
    _, res = medida
    top = por_id(res)["placsp_top_adjudicatarios"]["resultado"]
    assert top == [
        # 100 (L1) + 3000/3 (tope del acuerdo marco, repartido) + 50; sin la versión anterior de L1 (999)
        ["B12345678", "EMPRESA UNO SL", 2, 3, 1150.0],
        ["A87654321", "OTRA SA", 1, 1, 1000.0],
        ["B11111111", "TERCERA SL", 1, 1, 1000.0],           # ESB11111111: sin el prefijo de país
    ]                                                          # sin el enmascarado ni el '-'


def test_cruce_cuenta_el_tope_del_acuerdo_marco_una_vez(medida):
    _, res = medida
    assert por_id(res)["placsp_cruce_comunidad"]["resultado"] == [
        ["ES51", 4, 1, 3050.0],    # el tope de 3000 una vez, no tres
        ["ES30", 2, 2, 110.0],
        ["ES61", 1, 1, 5.0],
    ]


def test_ted_cada_aviso_una_vez_sin_cancelados(medida):
    _, res = medida
    # 2023: el retirado (sus filas de la última descarga en que salió) y el que no tiene id; 2024: 1-2024 (el 2-2024
    # está cancelado en su última versión y no vuelve con la anterior)
    assert por_id(res)["ted_avisos"]["resultado"] == [[2023, 2, 3], [2024, 1, 2]]


def test_ted_sql_igual_que_avisos_para_cruce_del_repo():
    import pandas as pd
    spec = importlib.util.spec_from_file_location("ted_module_medir", REPO / "ted" / "ted_module.py")
    tm = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(REPO))
    spec.loader.exec_module(tm)
    df = pd.DataFrame(TED, columns=list(COL_TED))
    a = tm.avisos_para_cruce(df)
    assert len(a) == 5
    assert a["ted_notice_id"].nunique() + a["ted_notice_id"].isna().sum() == 3


def test_madrid_menores_sin_repetidos_ni_versiones_anteriores(medida):
    _, res = medida
    assert por_id(res)["madrid_menores"]["resultado"] == [[2023, 1, 1234.56], [2024, 2, 2500.0]]


def test_pscp_menores_vigentes(medida):
    _, res = medida
    # B11111111: u1 (no su fase anterior), u2 y u7 (retirado); fuera u3 (anulado), u4 (||), u6 (enmascarado), u8
    assert por_id(res)["pscp_top_menores"]["resultado"] == [["B11111111", "ACME SL", 3, 350.5],
                                                            ["A22222222", "BETA SA", 1, None]]


def test_borme(medida):
    _, res = medida
    c = por_id(res)
    assert c["borme_constituciones"]["resultado"] == [[2020, 2, 9000.0], [2021, 1, None]]
    assert c["borme_nombramientos"]["resultado"] == [["ALFA", 3, 2], ["BETA", 1, 1]]


# ── pandas da lo mismo ─────────────────────────────────────────────────────────────────────────────────────────────

def test_pandas_coincide_con_duckdb(medida):
    _, res = medida
    pd_ = por_id(res, "pandas")
    assert set(pd_) == set(mc.PANDAS)
    for cid, c in pd_.items():
        assert "error" not in c, c
        assert c["coincide_con_duckdb"] is True, (cid, c["resultado"], por_id(res)[cid]["resultado"])


# ── Cero resultados, tablas y columnas que faltan ──────────────────────────────────────────────────────────────────

def test_cero_resultados(tmp_path):
    raiz = crear_release(tmp_path / "rel")
    res = medir_json(raiz, tmp_path / "r.json", "--nif", "Z9999999Z", "--palabra", "palabraquenoesta",
                     "--solo", "placsp_un_nif,placsp_texto")
    c = por_id(res)
    assert c["placsp_un_nif"]["resultado"] == [[0, 0, None, None]]
    assert c["placsp_texto"]["resultado"] == [[0]]
    assert res["parametros"] == {"nif": "Z9999999Z", "palabra": "palabraquenoesta"}


def test_cero_resultados_en_un_top_con_duckdb_y_pandas(tmp_path):
    raiz = crear_release(tmp_path / "rel")
    solo_enmascarados = [f[:3] + ("*** 1 **",) + f[4:] if f[3] else f for f in RES]
    _escribir(raiz / "nacional_resultados" / "licitaciones_completo_resultados.parquet", COL_RES, solo_enmascarados)
    res = medir_json(raiz, tmp_path / "r.json", "--motor", "ambos", "--solo", "placsp_top_adjudicatarios")
    for motor in ("duckdb", "pandas"):
        c = por_id(res, motor)["placsp_top_adjudicatarios"]
        assert "error" not in c, c
        assert c["filas_resultado"] == 0 and c["resultado"] == []
    assert por_id(res, "pandas")["placsp_top_adjudicatarios"]["coincide_con_duckdb"] is True


def test_tabla_o_columnas_que_faltan_se_saltan(tmp_path):
    raiz = crear_release(tmp_path / "rel", sin=("borme", "ted"))
    _escribir(raiz / "ted" / "ted_es_can.parquet", {"ted_notice_id": S, "year": pa.int64(), "cancelled": S},
              [("1-2024", 2024, "0")])
    res = medir_json(raiz, tmp_path / "r.json", "--solo", "borme_constituciones,ted_avisos,placsp_ultima_version")
    c = por_id(res)
    assert c["borme_constituciones"]["saltada"] == "falta la tabla borme_empresas"
    assert c["ted_avisos"]["saltada"] == "faltan columnas: ted._en_ultima_descarga, ted._ultima_descarga"
    assert c["placsp_ultima_version"]["filas_resultado"] == 5


def test_sin_datos_no_falla(tmp_path):
    (tmp_path / "vacia").mkdir()
    res = medir_json(tmp_path / "vacia", tmp_path / "r.json")
    assert res["tablas"] == {}
    assert all("saltada" in c for c in res["consultas"])


# ── El JSON y la tabla ─────────────────────────────────────────────────────────────────────────────────────────────

def test_json_sin_rutas_y_con_el_entorno(medida, tmp_path):
    raiz, res = medida
    texto = json.dumps(res, ensure_ascii=False)
    assert str(raiz) not in texto and str(raiz.parent) not in texto
    for k in ("python", "duckdb", "pandas", "pyarrow", "numpy", "cpu", "cpus_disponibles", "memoria_disponible_gb"):
        assert k in res["entorno"]
    assert res["perfil"] == "prueba" and res["datos"] == "release diminuta"
    assert res["parametros"] == {"nif": "B12345678", "palabra": "ambulancia"}
    assert res["tablas"]["licitaciones"]["filas"] == sum(len(v) for v in LIC.values())
    assert res["tablas"]["licitaciones"]["ficheros"] == sorted(f"{x}.parquet" for x in LIC)
    assert res["duckdb"]["threads"] >= 1
    for c in res["consultas"]:
        assert c["primera_s"] >= 0 and len(c["repeticiones_s"]) == 1
        assert c["memoria_pico_mb"] is None or c["memoria_pico_mb"] > 0


def test_tabla_markdown_con_dos_perfiles(medida, tmp_path):
    _, res = medida
    otro = json.loads(json.dumps(res))
    otro["perfil"], otro["entorno"]["cpus_disponibles"] = "otro", 999
    for i, x in enumerate((res, otro)):
        (tmp_path / f"{i}.json").write_text(json.dumps(x), encoding="utf-8")
    md = mc.tabla_markdown([tmp_path / "0.json", tmp_path / "1.json"])
    assert "| Consulta | prueba | otro |" in md
    assert "**DuckDB**" in md and "**pandas + pyarrow**" in md
    assert md.count("| PLACSP: licitaciones por año, la última versión de cada una |") == 2


def test_nif_e_iguales():
    assert mc.nif_py("esb-12345678") == "B12345678"
    assert mc.nif_py("B 83029439") == "B83029439"
    assert mc.nif_py("ES12") == "ES12"
    assert mc.iguales([[1, 2.0000000001, "a"]], [[1, 2.0, "a"]])
    assert not mc.iguales([[1, 2.1, "a"]], [[1, 2.0, "a"]])
    assert not mc.iguales([[1]], [[1], [2]])
    assert mc.iguales([[None]], [[None]])


def test_script_suelto_sin_el_repositorio_salta_la_del_ted_con_pandas(tmp_path, monkeypatch):
    raiz = crear_release(tmp_path / "rel")
    monkeypatch.setattr(mc, "RAIZ_REPO", tmp_path / "sin_repo")
    res = medir_json(raiz, tmp_path / "r.json", "--motor", "ambos", "--solo", "ted_avisos")
    assert por_id(res)["ted_avisos"]["resultado"] == [[2023, 2, 3], [2024, 1, 2]]          # DuckDB, igual
    assert por_id(res, "pandas")["ted_avisos"]["saltada"] == (
        "hace falta el repositorio entero (ted/ted_module.py), no solo este script")


def test_el_resultado_entero_en_el_json_hasta_el_tope(medida):
    _, res = medida
    for c in res["consultas"]:
        assert c["resultado_recortado"] is False and len(c["resultado"]) == c["filas_resultado"]
