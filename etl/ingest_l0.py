"""
Ingesta L0: carga parquets nacional (y futuros conjuntos) en tablas del schema de trabajo (DB_SCHEMA).
PK surrogada: l0_id BIGSERIAL PRIMARY KEY + natural_id TEXT UNIQUE NOT NULL (URL/identificador de fuente).
Deriva prefijos CPV (principal_prefix4/6, secondary_prefix6), asegura tablas con índices e inserta con ON CONFLICT (natural_id) DO NOTHING.
"""

import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import psycopg2
import psycopg2.extras

LOG_PREFIX = "[ingest_l0]"
logger = logging.getLogger("etl.ingest_l0")

CONJUNTO_NACIONAL = "nacional"
SUBCONJUNTOS_NACIONAL = (
    "licitaciones",
    "agregacion_ccaa",
    "contratos_menores",
    "encargos_medios_propios",
    "consultas_preliminares",
)

SCRIPT_CONJUNTO_TO_NACIONAL = {
    "licitaciones": "licitaciones",
    "agregacion_ccaa": "agregacion",
    "contratos_menores": "menores",
    "encargos_medios_propios": "encargos",
    "consultas_preliminares": "consultas",
}

# Columnas del parquet nacional. TEXT implica NULL permitido; en ingesta, NaN/float se normalizan a NULL/vacío (p. ej. cpv_principal, cpvs).
NACIONAL_PARQUET_COLUMNS = [
    ("id", "TEXT"),
    ("expediente", "TEXT"),
    ("objeto", "TEXT"),
    ("organo_contratante", "TEXT"),
    ("nif_organo", "TEXT"),
    ("dir3_organo", "TEXT"),
    ("id_plataforma", "TEXT"),
    ("ciudad_organo", "TEXT"),
    ("dependencia", "TEXT"),
    ("tipo_contrato_code", "TEXT"),
    ("tipo_contrato", "TEXT"),
    ("subtipo_code", "TEXT"),
    ("procedimiento_code", "TEXT"),
    ("procedimiento", "TEXT"),
    ("estado_code", "TEXT"),
    ("estado", "TEXT"),
    ("valor_estimado_contrato", "NUMERIC(18,2)"),
    ("importe_sin_iva", "NUMERIC(18,2)"),
    ("importe_con_iva", "NUMERIC(18,2)"),
    ("importe_adjudicacion", "NUMERIC(18,2)"),
    ("importe_adj_con_iva", "NUMERIC(18,2)"),
    ("adjudicatario", "TEXT"),
    ("nif_adjudicatario", "TEXT"),
    ("num_ofertas", "INTEGER"),
    ("es_pyme", "BOOLEAN"),
    ("cpv_principal", "TEXT"),
    ("cpvs", "TEXT"),
    ("ubicacion", "TEXT"),
    ("nuts", "TEXT"),
    ("duracion", "NUMERIC(18,2)"),
    ("duracion_unidad", "TEXT"),
    ("financiacion_ue", "TEXT"),
    ("urgencia", "TEXT"),
    ("fecha_limite", "TIMESTAMPTZ"),
    ("hora_limite", "TEXT"),
    ("fecha_adjudicacion", "TIMESTAMPTZ"),
    ("fecha_publicacion", "TIMESTAMPTZ"),
    ("fecha_updated", "TIMESTAMPTZ"),
    ("doc_legal_nombre", "TEXT"),
    ("doc_legal_url", "TEXT"),
    ("doc_tecnico_nombre", "TEXT"),
    ("doc_tecnico_url", "TEXT"),
    ("docs_adicionales", "JSONB"),
    ("criterios_adjudicacion", "JSONB"),
    ("requisitos_solvencia", "JSONB"),
    ("url", "TEXT"),
    ("conjunto", "TEXT"),
    ("ano", "INTEGER"),
]

# Nombre de la columna que en el parquet contiene el identificador único (URL); en la tabla L0 se persiste como natural_id.
NATURAL_ID_PARQUET_COL = "id"


def _configure_logging() -> None:
    if logger.handlers:
        return
    h = logging.StreamHandler(sys.stderr)
    h.setFormatter(logging.Formatter(LOG_PREFIX + " %(message)s"))
    logger.addHandler(h)
    logger.setLevel(logging.INFO)


def _tmp_dir() -> Path:
    raw = os.environ.get("LICITACIONES_TMP_DIR", "")
    if not raw:
        raw = str(Path(__file__).resolve().parent.parent.parent / "tmp")
    return Path(raw)


def _tmp_output_dir() -> Path:
    return _tmp_dir() / "output"


def get_parquet_path_nacional(subconjunto: str, ano_inicio: int, ano_fin: int) -> Path:
    script_conjunto = SCRIPT_CONJUNTO_TO_NACIONAL.get(subconjunto, subconjunto)
    name = f"licitaciones_{script_conjunto}_{ano_inicio}_{ano_fin}.parquet"
    return _tmp_output_dir() / name


def get_table_name(conjunto: str, subconjunto: str) -> str:
    return f"{conjunto}_{subconjunto}"


# Catalunya: subconjunto → ruta relativa bajo tmp/catalunya_parquet/
CATALUNYA_PARQUET_PATHS = {
    "contratacion_registro": "contratacion/contratos_registro.parquet",
    "subvenciones_raisc": "subvenciones/raisc_concesiones.parquet",
    "convenios": "convenios/convenios.parquet",
    "presupuestos_aprobados": "presupuestos/presupuestos_aprobados.parquet",
    "rrhh_altos_cargos": "rrhh/altos_cargos.parquet",
}
# Subconjunto → Socrata dataset ID(s) para --limit-datasets del script ccaa_cataluna.py (solo descarga ese dataset)
CATALUNYA_SUBCONJUNTO_TO_LIMIT_DATASETS = {
    "contratacion_registro": "hb6v-jcbf",
    "subvenciones_raisc": "s9xt-n979",
    "convenios": "exh2-diuf",
    "presupuestos_aprobados": "yd9k-7jhw",
    "rrhh_altos_cargos": "x9au-abcn",
}
SUBCONJUNTOS_CATALUNYA = tuple(CATALUNYA_PARQUET_PATHS.keys())


def get_parquet_path_catalunya(subconjunto: str) -> Path:
    rel = CATALUNYA_PARQUET_PATHS.get(subconjunto)
    if not rel:
        raise ValueError(f"Subconjunto catalunya no reconocido: {subconjunto}")
    return _tmp_dir() / "catalunya_parquet" / rel


# Valencia: subconjunto = categoría; un único datos.parquet por categoría en tmp/valencia_parquet/<categoria>/datos.parquet
SUBCONJUNTOS_VALENCIA = (
    "contratacion",
    "subvenciones",
    "presupuestos",
    "convenios",
    "empleo",
    "paro",
    "lobbies",
    "siniestralidad",
    "patrimonio",
    "entidades",
    "territorio",
    "turismo",
    "sanidad",
    "transporte",
)


def get_parquet_path_valencia(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_VALENCIA:
        raise ValueError(f"Subconjunto valencia no reconocido: {subconjunto}")
    return _tmp_dir() / "valencia_parquet" / subconjunto / "datos.parquet"


# Andalucía: script scripts/ccaa_andalucia.py (scrape / scrape-men) escribe CSV en ccaa_Andalucia/;
# scripts/ccaa_andalucia_parquet.py convierte a parquet por subconjunto.
SUBCONJUNTOS_ANDALUCIA = ("licitaciones", "menores")

ANDALUCIA_SCRIPT_SUBCONJUNTO_ARG = {
    "licitaciones": "scrape",
    "menores": "scrape-men",
}


def get_parquet_path_andalucia(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_ANDALUCIA:
        raise ValueError(f"Subconjunto andalucia no reconocido: {subconjunto}")
    etl_root = Path(__file__).resolve().parent.parent
    name = "licitaciones_andalucia.parquet" if subconjunto == "licitaciones" else "licitaciones_menores.parquet"
    return etl_root / "ccaa_Andalucia" / name


def _etl_root() -> Path:
    """Raíz del microservicio ETL (services/etl)."""
    return Path(__file__).resolve().parent.parent


# Euskadi: consolidación en Euskadi/euskadi_parquet/ (Euskadi/ccaa_euskadi.py o consolidacion_euskadi.py)
SUBCONJUNTOS_EUSKADI = (
    "contratos_master",
    "poderes_adjudicadores",
    "empresas_licitadoras",
    "revascon_historico",
    "bilbao_contratos",
    "ultimos_90d",
)


def get_parquet_path_euskadi(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_EUSKADI:
        raise ValueError(f"Subconjunto euskadi no reconocido: {subconjunto}")
    root = _etl_root()
    # Consolidador puede escribir en Euskadi/euskadi_parquet/ (cwd=Euskadi) o euskadi_parquet/ (cwd=etl)
    for base in (root / "Euskadi" / "euskadi_parquet", root / "euskadi_parquet"):
        p = base / f"{subconjunto}.parquet"
        if p.exists():
            return p
    return root / "Euskadi" / "euskadi_parquet" / f"{subconjunto}.parquet"


# Galicia: scraper contratos públicos (contratosdegalicia.gal)
SUBCONJUNTOS_GALICIA = ("contratos",)


def get_parquet_path_galicia(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_GALICIA:
        raise ValueError(f"Subconjunto galicia no reconocido: {subconjunto}")
    return _tmp_dir() / "galicia" / "contratos_galicia.parquet"


# Madrid: Comunidad de Madrid (comunidad_madrid/) y Ayuntamiento (datos_madrid_contratacion_completa/)
SUBCONJUNTOS_MADRID = ("comunidad", "ayuntamiento")

MADRID_PARQUET_PATHS = {
    "comunidad": "comunidad_madrid/contratacion_comunidad_madrid_completo.parquet",
    "ayuntamiento": "datos_madrid_contratacion_completa/actividad_contractual_madrid_completo.parquet",
}


def get_parquet_path_madrid(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_MADRID:
        raise ValueError(f"Subconjunto madrid no reconocido: {subconjunto}")
    rel = MADRID_PARQUET_PATHS[subconjunto]
    return _etl_root() / rel


# TED: datos TED España (ted_module.py → data/ted/ o ted/data/ted/)
SUBCONJUNTOS_TED = ("ted_es_can",)


def get_parquet_path_ted(subconjunto: str) -> Path:
    if subconjunto not in SUBCONJUNTOS_TED:
        raise ValueError(f"Subconjunto ted no reconocido: {subconjunto}")
    root = _etl_root()
    # ted_module usa DATA_DIR = Path("data/ted") (relativo a cwd; a menudo etl o ted/)
    for base in (root / "data" / "ted", root / "ted" / "data" / "ted"):
        p = base / "ted_es_can.parquet"
        if p.exists():
            return p
    return root / "data" / "ted" / "ted_es_can.parquet"


def get_cleanup_dirs(conjunto: str, subconjunto: str) -> list[Path]:
    """
    Directorios de artefactos (descargas, parquets intermedios) que deben eliminarse
    tras un ingest exitoso. Solo incluye rutas que los scripts crean (repo o tmp).
    """
    root = _etl_root()
    out: list[Path] = []
    if conjunto == CONJUNTO_NACIONAL:
        out.append(_tmp_output_dir())
    elif conjunto == "euskadi":
        out.extend([
            root / "Euskadi" / "datos_euskadi_contratacion_v4",
            root / "Euskadi" / "euskadi_parquet",
        ])
    elif conjunto == "andalucia":
        out.append(root / "ccaa_Andalucia")
    elif conjunto == "galicia":
        out.append(_tmp_dir() / "galicia")
    elif conjunto == "madrid" and subconjunto == "ayuntamiento":
        out.append(root / "datos_madrid_contratacion_completa")
    return out


# Registro de conjuntos: subconjuntos, resolución de parquet, si requiere --anos, scripts a ejecutar.
CONJUNTOS_REGISTRY: dict[str, dict[str, Any]] = {
    CONJUNTO_NACIONAL: {
        "subconjuntos": SUBCONJUNTOS_NACIONAL,
        "get_parquet_path": get_parquet_path_nacional,
        "requires_anos": True,
        "column_defs": NACIONAL_PARQUET_COLUMNS,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "script_module": "nacional.licitaciones",
        "script_conjunto_arg": SCRIPT_CONJUNTO_TO_NACIONAL,
    },
    "catalunya": {
        "subconjuntos": SUBCONJUNTOS_CATALUNYA,
        "get_parquet_path": get_parquet_path_catalunya,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "scripts": ["scripts/ccaa_cataluna.py", "scripts/ccaa_cataluna_parquet.py"],
        "subconjunto_to_limit_datasets": CATALUNYA_SUBCONJUNTO_TO_LIMIT_DATASETS,
    },
    "valencia": {
        "subconjuntos": SUBCONJUNTOS_VALENCIA,
        "get_parquet_path": get_parquet_path_valencia,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "scripts": ["scripts/ccaa_valencia.py", "scripts/ccaa_valencia_parquet.py"],
    },
    "andalucia": {
        "subconjuntos": SUBCONJUNTOS_ANDALUCIA,
        "get_parquet_path": get_parquet_path_andalucia,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": "id_expediente",
        "scripts": ["scripts/ccaa_andalucia.py", "scripts/ccaa_andalucia_parquet.py"],
        "script_subconjunto_arg": ANDALUCIA_SCRIPT_SUBCONJUNTO_ARG,
    },
    "euskadi": {
        "subconjuntos": SUBCONJUNTOS_EUSKADI,
        "get_parquet_path": get_parquet_path_euskadi,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "scripts": ["ccaa_euskadi.py", "consolidacion_euskadi.py"],
        "script_cwd": "Euskadi",
    },
    "galicia": {
        "subconjuntos": SUBCONJUNTOS_GALICIA,
        "get_parquet_path": get_parquet_path_galicia,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "scripts": ["galicia/scraper_galicia.py"],
    },
    "madrid": {
        "subconjuntos": SUBCONJUNTOS_MADRID,
        "get_parquet_path": get_parquet_path_madrid,
        "requires_anos": False,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "script_commands": {
            "comunidad": [
                ["comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py", "todo"],
                ["comunidad_madrid/descarga_contratacion_comunidad_madrid_v1.py", "unificar"],
                ["comunidad_madrid/csv_to_parquet_comunidad.py"],
            ],
            "ayuntamiento": [
                ["comunidad_madrid/ccaa_madrid_ayuntamiento.py"],
            ],
        },
    },
    "ted": {
        "subconjuntos": SUBCONJUNTOS_TED,
        "get_parquet_path": get_parquet_path_ted,
        "requires_anos": True,
        "column_defs": None,
        "natural_id_col": NATURAL_ID_PARQUET_COL,
        "scripts": ["ted/ted_module.py"],
    },
}


def format_conjunto_help(conjunto: str, reg: dict[str, Any]) -> str:
    """Genera un bloque de help con argumentos y reglas del ingest para este conjunto."""
    lines: list[str] = []
    if reg.get("requires_anos"):
        lines.append("  Obligatorio: --anos X-Y (ej. 2020-2024).")
    if "script_module" in reg:
        lines.append(f"  Script: {reg['script_module']}. Recibe --conjunto, --anos.")
    if "script_subconjunto_arg" in reg:
        mapeo = ", ".join(f"{k}→{v}" for k, v in reg["script_subconjunto_arg"].items())
        lines.append(f"  Subconjuntos → subcomandos: {mapeo}.")
    if "script_commands" in reg:
        sc = reg["script_commands"]
        resumen = "; ".join(
            f"{s} ({len(cmd)} paso(s))" for s, cmd in sc.items()
        )
        lines.append(f"  Comandos por subconjunto: {resumen}.")
    if reg.get("script_cwd"):
        lines.append(f"  Ejecución desde directorio {reg['script_cwd']}/.")
    if "scripts" in reg and conjunto == "valencia":
        lines.append("  El CLI pasa --categories al script de descarga y parquet.")
    if "scripts" in reg and conjunto == "catalunya":
        lines.append("  El CLI pasa --limit-datasets al script de descarga (solo el dataset del subconjunto) y --parquet-rel al de parquet.")
    if "scripts" in reg and conjunto == "euskadi":
        lines.append(
            "  Scripts: descarga y consolidación desde Euskadi/. Siempre se ejecuta descarga/consolidación completas; el subconjunto solo determina qué parquet se carga en L0."
        )
    if "scripts" in reg and conjunto == "galicia":
        lines.append("  Scraper: descarga contratos de contratosdegalicia.gal. El subconjunto se carga en L0.")
    if "scripts" in reg and conjunto == "ted":
        lines.append("  El script recibe: download --years X-Y.")
        lines.append("  Puede omitir el subconjunto (solo hay ted_es_can).")
    lines.append("  Opcional: --solo-descargar (solo generar parquet), --solo-procesar (solo cargar parquet existente).")
    return "\n".join(lines) if lines else "  (Sin argumentos específicos.)"


def _to_str_for_re(s: Any) -> str:
    """Convierte a str para uso en re; None y NaN devuelven ''."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    if isinstance(s, (int, float)):
        return str(int(s)) if s == int(s) else str(s)
    return str(s) if s else ""


def derive_cpv_prefixes(
    cpv_principal_str: Optional[Any],
    cpvs_str: Optional[Any],
) -> tuple[Optional[int], Optional[int], list[int]]:
    def normalize_to_8(s: Any) -> Optional[str]:
        raw = _to_str_for_re(s)
        digits = re.sub(r"\D", "", raw)
        if len(digits) >= 8:
            return digits[:8]
        return digits if digits else None

    prefix4: Optional[int] = None
    prefix6: Optional[int] = None
    principal_8 = normalize_to_8(cpv_principal_str)
    if principal_8:
        try:
            prefix4 = int(principal_8[:4])
            prefix6 = int(principal_8[:6])
        except ValueError:
            pass

    secondary: list[int] = []
    cpvs_plain = _to_str_for_re(cpvs_str) if cpvs_str is not None else ""
    if cpvs_plain:
        for part in cpvs_plain.split(";"):
            part = part.strip()
            if not part:
                continue
            code_8 = normalize_to_8(part)
            if code_8 and len(code_8) >= 6:
                try:
                    secondary.append(int(code_8[:6]))
                except ValueError:
                    pass

    return (prefix4, prefix6, secondary)


def _infer_pg_type(dtype) -> str:
    """Mapea dtype de pandas a tipo PostgreSQL (TEXT por defecto)."""
    name = getattr(dtype, "name", str(dtype))
    if "int" in name:
        return "BIGINT"
    if "float" in name:
        return "NUMERIC(18,2)"
    if "bool" in name:
        return "BOOLEAN"
    if "datetime" in name or "date" in name:
        return "TIMESTAMPTZ"
    return "TEXT"


def infer_column_defs_from_parquet(parquet_path: Path) -> list[tuple[str, str]]:
    """Infiere (nombre, tipo_pg) a partir del esquema del parquet (leyendo una muestra)."""
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as e:
        _configure_logging()
        err_msg = str(e).lower()
        if "magic" in err_msg or "arrowinvalid" in type(e).__name__.lower() or "parquet" in err_msg:
            logger.error(
                "Parquet inválido (magic bytes no encontrados o fichero corrupto). "
                "Comprobar que la ruta es un .parquet válido; si acabas de descargar, reintentar la descarga. %s",
                e,
            )
        raise
    return [(c, _infer_pg_type(df.dtypes[c])) for c in df.columns]


def ensure_l0_table(
    conn: "psycopg2.extensions.connection",
    schema: str,
    table_name: str,
    column_defs: Optional[list[tuple[str, str]]] = None,
    natural_id_col: str = NATURAL_ID_PARQUET_COL,
) -> None:
    """Crea la tabla L0 con PK surrogada (l0_id, natural_id UNIQUE) y columnas de la fuente + extensiones CPV."""
    if column_defs is None:
        column_defs = NACIONAL_PARQUET_COLUMNS
    col_defs = [
        '"l0_id" BIGSERIAL PRIMARY KEY',
        '"natural_id" TEXT UNIQUE NOT NULL',
    ]
    col_defs.extend(
        f'"{c}" {t}' for c, t in column_defs if c != natural_id_col
    )
    col_defs.extend([
        '"principal_prefix4" INTEGER',
        '"principal_prefix6" INTEGER',
        '"secondary_prefix6" INTEGER[]',
        '"ingested_at" TIMESTAMPTZ DEFAULT NOW()',
    ])
    full_table = f'"{schema}"."{table_name}"'
    create_sql = f"CREATE TABLE IF NOT EXISTS {full_table} (\n  " + ",\n  ".join(col_defs) + "\n)"
    with conn.cursor() as cur:
        cur.execute(create_sql)
        for col in ("principal_prefix4", "principal_prefix6", "secondary_prefix6"):
            iname = f"idx_{table_name}_{col}"
            if col == "secondary_prefix6":
                cur.execute(f'CREATE INDEX IF NOT EXISTS {iname} ON {full_table} USING GIN ("{col}")')
            else:
                cur.execute(f'CREATE INDEX IF NOT EXISTS {iname} ON {full_table} ("{col}")')
        col_names = [c[0] for c in column_defs]
        if "expediente" in col_names and "id_plataforma" in col_names:
            iname = f"idx_{table_name}_org_key"
            cur.execute(f'CREATE INDEX IF NOT EXISTS {iname} ON {full_table} ("expediente", "id_plataforma")')


def load_parquet_to_l0(
    db_url: str,
    schema: str,
    table_name: str,
    parquet_path: Path,
    batch_size: int,
    column_defs: Optional[list[tuple[str, str]]] = None,
    natural_id_col: Optional[str] = None,
) -> tuple[int, int]:
    """Carga un parquet en la tabla L0. Si column_defs es None, se usa el esquema nacional."""
    _configure_logging()
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet no encontrado: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    logger.info("Parquet cargado: %s filas.", len(df))

    if column_defs is None:
        column_defs = NACIONAL_PARQUET_COLUMNS
    if natural_id_col is None:
        natural_id_col = NATURAL_ID_PARQUET_COL

    # Aceptar primera columna como natural_id si no existe la esperada
    if natural_id_col not in df.columns and len(df.columns):
        natural_id_col = df.columns[0]
        logger.info("Usando primera columna como natural_id: %s", natural_id_col)
    if natural_id_col not in df.columns:
        raise ValueError(f"El parquet debe tener columna identificadora (ej. '{NATURAL_ID_PARQUET_COL}' o primera columna).")

    # Comprobar cuántas filas tienen natural_id válido; si ninguna, usar id sintético (table_name + índice)
    nid_series = df[natural_id_col]
    nid_valid = nid_series.notna() & (nid_series.astype(str).str.strip() != "") & (nid_series.astype(str) != "nan")
    num_valid_nid = int(nid_valid.sum())
    use_synthetic_id = num_valid_nid == 0
    if use_synthetic_id:
        logger.info(
            "Columna '%s' sin valores válidos; usando natural_id sintético (%s_0, %s_1, ...).",
            natural_id_col,
            table_name,
            table_name,
        )
    elif num_valid_nid < len(df):
        logger.info("Filas con natural_id válido: %s de %s (se omiten filas sin id).", num_valid_nid, len(df))
    else:
        logger.info("Filas con natural_id válido: %s de %s.", num_valid_nid, len(df))

    cpv_principal_col = "cpv_principal"
    cpvs_col = "cpvs"
    for c in (cpv_principal_col, cpvs_col):
        if c not in df.columns:
            df[c] = None

    with psycopg2.connect(db_url) as conn:
        ensure_l0_table(conn, schema, table_name, column_defs=column_defs, natural_id_col=natural_id_col)
        conn.commit()

    table_base_cols = ["natural_id"] + [c for c, _ in column_defs if c != natural_id_col]
    insert_cols = table_base_cols + ["principal_prefix4", "principal_prefix6", "secondary_prefix6"]
    placeholders = ", ".join(["%s"] * len(insert_cols))
    quoted = ", ".join(f'"{c}"' for c in insert_cols)
    full_table = f'"{schema}"."{table_name}"'
    insert_sql = f"""
        INSERT INTO {full_table} ({quoted})
        VALUES ({placeholders})
        ON CONFLICT (natural_id) DO NOTHING
    """

    parquet_cols_ordered = [c for c, _ in column_defs]
    is_nacional = column_defs is NACIONAL_PARQUET_COLUMNS

    inserted = 0
    total_candidates = 0
    with psycopg2.connect(db_url) as conn:
        with conn.cursor() as cur:
            for start in range(0, len(df), batch_size):
                batch = df.iloc[start : start + batch_size]
                rows = []
                for batch_idx, (_, row) in enumerate(batch.iterrows()):
                    if use_synthetic_id:
                        nid = f"{table_name}_{start + batch_idx}"
                    else:
                        nid = row.get(natural_id_col)
                        if pd.isna(nid) or nid is None:
                            continue
                        nid = _to_str_for_re(nid).strip() or None
                        if not nid:
                            continue
                    prefix4, prefix6, sec6 = derive_cpv_prefixes(
                        row.get(cpv_principal_col),
                        row.get(cpvs_col),
                    )
                    vals = [nid]
                    for idx, c in enumerate(parquet_cols_ordered):
                        if c == natural_id_col:
                            continue
                        v = row.get(c)
                        if pd.isna(v):
                            vals.append(None)
                            continue
                        if is_nacional:
                            if c in ("importe_sin_iva", "importe_con_iva", "importe_adjudicacion", "importe_adj_con_iva", "duracion"):
                                try:
                                    vals.append(float(v))
                                except (TypeError, ValueError):
                                    vals.append(None)
                            elif c == "num_ofertas":
                                try:
                                    vals.append(int(v))
                                except (TypeError, ValueError):
                                    vals.append(None)
                            elif c == "es_pyme":
                                vals.append(bool(v))
                            elif c == "ano":
                                try:
                                    vals.append(int(v))
                                except (TypeError, ValueError):
                                    vals.append(None)
                            else:
                                vals.append(v)
                        else:
                            pg_type = next((t for col, t in column_defs if col == c), "TEXT")
                            if "INT" in pg_type or "BIGINT" in pg_type:
                                try:
                                    vals.append(int(v))
                                except (TypeError, ValueError):
                                    vals.append(None)
                            elif "NUMERIC" in pg_type:
                                try:
                                    vals.append(float(v))
                                except (TypeError, ValueError):
                                    vals.append(None)
                            elif "BOOL" in pg_type:
                                vals.append(bool(v))
                            else:
                                vals.append(v)
                    vals.append(prefix4)
                    vals.append(prefix6)
                    vals.append(sec6 if sec6 else None)
                    rows.append(tuple(vals))

                total_candidates += len(rows)
                if rows:
                    for row_tuple in rows:
                        cur.execute(insert_sql, row_tuple)
                        inserted += cur.rowcount or 0
                    conn.commit()
    skipped = total_candidates - inserted

    logger.info("Ingesta completada: %s filas insertadas, %s omitidas (ya existentes).", inserted, skipped)
    return (inserted, skipped)
