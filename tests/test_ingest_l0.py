"""
Tests del módulo ingest_l0: registro de conjuntos, rutas de parquet, y (con BD) idempotencia.
Ejecutar: pytest tests/ -v
Con BD: definir DB_*, DB_SCHEMA y opcionalmente usar schema l0_test para no tocar datos reales.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from etl.ingest_l0 import (
    CONJUNTOS_REGISTRY,
    CATALUNYA_PARQUET_PATHS,
    SUBCONJUNTOS_ANDALUCIA,
    SUBCONJUNTOS_CATALUNYA,
    SUBCONJUNTOS_NACIONAL,
    SUBCONJUNTOS_VALENCIA,
    derive_cpv_prefixes,
    format_conjunto_help,
    get_parquet_path_andalucia,
    get_parquet_path_catalunya,
    get_parquet_path_galicia,
    get_parquet_path_nacional,
    get_parquet_path_valencia,
    get_table_name,
    infer_column_defs_from_parquet,
    load_parquet_to_l0,
)


# ---- Tests unitarios (sin BD) ----


def test_registry_has_all_conjuntos():
    assert "nacional" in CONJUNTOS_REGISTRY
    assert "catalunya" in CONJUNTOS_REGISTRY
    assert "valencia" in CONJUNTOS_REGISTRY
    assert "andalucia" in CONJUNTOS_REGISTRY
    assert "euskadi" in CONJUNTOS_REGISTRY
    assert "madrid" in CONJUNTOS_REGISTRY
    assert "ted" in CONJUNTOS_REGISTRY
    assert "galicia" in CONJUNTOS_REGISTRY


def test_registry_nacional_has_five_subconjuntos():
    reg = CONJUNTOS_REGISTRY["nacional"]
    assert len(reg["subconjuntos"]) == 5
    assert "consultas_preliminares" in reg["subconjuntos"]
    assert reg.get("requires_anos") is True


def test_registry_catalunya_valencia_andalucia_subconjuntos():
    assert len(CONJUNTOS_REGISTRY["catalunya"]["subconjuntos"]) >= 5
    assert len(CONJUNTOS_REGISTRY["valencia"]["subconjuntos"]) >= 10
    assert CONJUNTOS_REGISTRY["andalucia"]["subconjuntos"] == ("licitaciones", "menores")


def test_registry_euskadi_madrid_ted_subconjuntos():
    assert len(CONJUNTOS_REGISTRY["euskadi"]["subconjuntos"]) == 6
    assert "contratos_master" in CONJUNTOS_REGISTRY["euskadi"]["subconjuntos"]
    assert CONJUNTOS_REGISTRY["madrid"]["subconjuntos"] == ("comunidad", "ayuntamiento")
    assert CONJUNTOS_REGISTRY["ted"]["subconjuntos"] == ("ted_es_can",)


def test_registry_euskadi_madrid_ted_scripts():
    """Fase A: Euskadi tiene scripts (relativos a script_cwd) + script_cwd; Madrid script_commands; TED scripts + requires_anos."""
    e = CONJUNTOS_REGISTRY["euskadi"]
    assert "scripts" in e and len(e["scripts"]) == 2
    assert e["scripts"][0] == "ccaa_euskadi.py"
    assert e.get("script_cwd") == "Euskadi"
    m = CONJUNTOS_REGISTRY["madrid"]
    assert "script_commands" in m
    assert "comunidad" in m["script_commands"] and "ayuntamiento" in m["script_commands"]
    assert any("csv_to_parquet_comunidad" in str(c) for c in m["script_commands"]["comunidad"])
    t = CONJUNTOS_REGISTRY["ted"]
    assert "scripts" in t and len(t["scripts"]) >= 1
    assert t.get("requires_anos") is True


def test_registry_has_galicia():
    assert "galicia" in CONJUNTOS_REGISTRY
    reg = CONJUNTOS_REGISTRY["galicia"]
    assert reg["subconjuntos"] == ("contratos",)
    assert reg.get("requires_anos") is False
    assert "scripts" in reg


def test_get_parquet_path_galicia():
    p = get_parquet_path_galicia("contratos")
    assert isinstance(p, Path)
    assert p.suffix == ".parquet"
    assert "galicia" in str(p)


def test_get_table_name():
    assert get_table_name("nacional", "consultas_preliminares") == "nacional_consultas_preliminares"
    assert get_table_name("catalunya", "contratacion_registro") == "catalunya_contratacion_registro"


def test_get_parquet_path_nacional():
    p = get_parquet_path_nacional("contratos_menores", 2023, 2023)
    assert p.name == "licitaciones_menores_2023_2023.parquet"
    assert "output" in str(p) or "tmp" in str(p)


def test_get_parquet_path_catalunya():
    p = get_parquet_path_catalunya("contratacion_registro")
    assert p.name == "contratos_registro.parquet"
    assert "catalunya_parquet" in str(p)


def test_get_parquet_path_valencia():
    p = get_parquet_path_valencia("contratacion")
    assert "valencia_parquet" in str(p)
    assert "contratacion" in str(p)
    assert p.name == "datos.parquet"


def test_get_parquet_path_andalucia():
    p_lic = get_parquet_path_andalucia("licitaciones")
    assert p_lic.name == "licitaciones_andalucia.parquet"
    assert "ccaa_Andalucia" in str(p_lic) or "Andalucia" in p_lic.parts
    p_men = get_parquet_path_andalucia("menores")
    assert p_men.name == "licitaciones_menores.parquet"


def test_format_conjunto_help_catalunya():
    """Q2: format_conjunto_help incluye líneas esperadas para catalunya."""
    reg = CONJUNTOS_REGISTRY["catalunya"]
    text = format_conjunto_help("catalunya", reg)
    assert "parquet-rel" in text or "parquet" in text.lower()
    assert "Opcional" in text
    assert "solo-descargar" in text or "solo-procesar" in text


def test_format_conjunto_help_euskadi():
    """Q2/Q3: format_conjunto_help incluye directorio Euskadi para euskadi."""
    reg = CONJUNTOS_REGISTRY["euskadi"]
    text = format_conjunto_help("euskadi", reg)
    assert "Euskadi" in text
    assert "Ejecución desde directorio" in text or "desde Euskadi" in text or "Scripts" in text
    assert "Opcional" in text


def test_format_conjunto_help_nacional():
    """format_conjunto_help para nacional incluye Obligatorio --anos y Script."""
    reg = CONJUNTOS_REGISTRY["nacional"]
    text = format_conjunto_help("nacional", reg)
    assert "Obligatorio" in text
    assert "anos" in text or "--anos" in text
    assert "nacional.licitaciones" in text or "Script" in text


def test_derive_cpv_prefixes():
    p4, p6, sec = derive_cpv_prefixes("45233120", "45233120;45233130")
    assert p4 == 4523
    assert p6 == 452331
    assert len(sec) >= 1
    assert 452331 in sec

    p4, p6, sec = derive_cpv_prefixes(None, None)
    assert p4 is None
    assert p6 is None
    assert sec == []


def test_derive_cpv_prefixes_handles_float_nan():
    p4, p6, sec = derive_cpv_prefixes(float("nan"), None)
    assert p4 is None
    assert p6 is None


def test_null_guard_safe_for_jsonb_list_and_dict():
    """Regression: pd.isna(list|dict) raises ValueError (ambiguous truth value).
    The ingest_l0 null guard must short-circuit on container types before calling pd.isna().
    This tests the exact guard expression used in load_parquet_to_l0."""
    import math

    def _is_null(v):
        """Mirrors the guard introduced in load_parquet_to_l0 line 616."""
        return v is None or (not isinstance(v, (list, dict)) and pd.isna(v))

    assert _is_null(None) is True
    assert _is_null(float("nan")) is True
    assert _is_null(math.nan) is True
    assert _is_null("") is False
    assert _is_null(0) is False
    assert _is_null([]) is False
    assert _is_null([{"nombre": "PCAP", "url": "https://example.com", "hash": None}]) is False
    assert _is_null({"solvencia_tecnica": {"nombre": "Solvencia técnica", "requisitos": []}}) is False
    assert _is_null({"criterios": []}) is False


@pytest.mark.parametrize("conjunto,subconjunto", [
    ("nacional", "consultas_preliminares"),
    ("nacional", "contratos_menores"),
    ("catalunya", "contratacion_registro"),
    ("catalunya", "subvenciones_raisc"),
    ("valencia", "contratacion"),
    ("andalucia", "licitaciones"),
    ("andalucia", "menores"),
    ("euskadi", "contratos_master"),
    ("madrid", "comunidad"),
    ("ted", "ted_es_can"),
    ("galicia", "contratos"),
])
def test_parquet_path_resolution(conjunto, subconjunto):
    reg = CONJUNTOS_REGISTRY[conjunto]
    get_path = reg["get_parquet_path"]
    if conjunto == "nacional":
        path = get_path(subconjunto, 2023, 2023)
    else:
        path = get_path(subconjunto)
    assert isinstance(path, Path)
    assert path.suffix == ".parquet"


# ---- Inferencia de esquema ----


def test_infer_column_defs_from_parquet():
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        df = pd.DataFrame({
            "id": ["a", "b"],
            "texto": ["x", "y"],
            "numero": [1, 2],
        })
        df.to_parquet(f.name, index=False)
    try:
        defs = infer_column_defs_from_parquet(Path(f.name))
        names = [c for c, _ in defs]
        assert "id" in names
        assert "texto" in names
        assert "numero" in names
    finally:
        Path(f.name).unlink(missing_ok=True)


# ---- Tests de integración (requieren BD) ----


@pytest.mark.integration
def test_load_parquet_to_l0_idempotency(require_db, require_db_schema, db_url, db_schema):
    """Carga un parquet mínimo dos veces y comprueba que la segunda no inserta filas nuevas."""
    import psycopg2

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        df = pd.DataFrame([
            {"id": "test-id-1", "expediente": "EXP1", "objeto": "Objeto test"},
        ])
        df.to_parquet(f.name, index=False)
    parquet_path = Path(f.name)

    try:
        with psycopg2.connect(db_url) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{db_schema}"')
        table_name = "test_ingest_idempotency"
        batch_size = 1000

        inserted1, skipped1 = load_parquet_to_l0(
            db_url,
            db_schema,
            table_name,
            parquet_path,
            batch_size,
            column_defs=[("id", "TEXT"), ("expediente", "TEXT"), ("objeto", "TEXT")],
            natural_id_col="id",
        )
        assert inserted1 >= 1
        assert skipped1 == 0

        inserted2, skipped2 = load_parquet_to_l0(
            db_url,
            db_schema,
            table_name,
            parquet_path,
            batch_size,
            column_defs=[("id", "TEXT"), ("expediente", "TEXT"), ("objeto", "TEXT")],
            natural_id_col="id",
        )
        assert inserted2 == 0
        assert skipped2 >= 1

        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) FROM "{db_schema}"."{table_name}"')
                total = cur.fetchone()[0]
                cur.execute(f'DROP TABLE IF EXISTS "{db_schema}"."{table_name}"')
        assert total >= 1
    finally:
        parquet_path.unlink(missing_ok=True)
