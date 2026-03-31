
#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 CONSOLIDACIÓN BASE — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Convierte la salida de descarga (JSON + XLSX + CSV) en la capa base parquet
 sobre la que luego trabaja el enriquecimiento incremental de detalle.

 ENTRADA:  datos_euskadi_contratacion_v4/
 SALIDA:   euskadi_parquet/

 Estrategia:
   · B1 XLSX anuales (2011-2026)  → contratos_master.parquet   ← FUENTE PRINCIPAL
   · A3 JSON poderes (completo)   → poderes_adjudicadores.parquet
   · A4 JSON empresas (completo)  → empresas_licitadoras.parquet
   · B2 REVASCON (2013-2018)      → revascon_historico.parquet  (pre-API)
   · C1 Bilbao CSVs               → bilbao_contratos.parquet
   · A1/A2 muestras API           → IGNORAR (redundante con B1)
   · B3 últimos 90d / C2 Vitoria  → IGNORAR si 404

 Salida final:
   euskadi_parquet/
   ├── contratos_master.parquet        ← Base consolidada sector público (B1)
   ├── poderes_adjudicadores.parquet   ← Catálogo A3
   ├── empresas_licitadoras.parquet    ← Catálogo A4
   ├── revascon_historico.parquet      ← Serie 2013-2018 (B2)
   ├── bilbao_contratos.parquet        ← Contratos municipales (C1)
   ├── stats.json                      ← Estadísticas consolidación
   └── README.md                       ← Documentación

 Nota:
   · El parquet enriquecido final (`contratos_master_detallado.parquet`) se
     genera después en `detalle_euskadi.py`.
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import logging
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = SCRIPT_DIR / "datos_euskadi_contratacion_v4"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "euskadi_parquet"
DEFAULT_LOG_PATH = SCRIPT_DIR / "consolidar_euskadi_v4.log"


def build_paths(input_dir: Path) -> dict[str, Path]:
    return {
        "api_contracts": input_dir / "A1_api_contratos",
        "api_notices": input_dir / "A2_api_anuncios",
        "api_authorities": input_dir / "A3_api_poderes",
        "api_companies": input_dir / "A4_api_empresas",
        "xlsx_anual": input_dir / "B1_xlsx_sector_publico_anual",
        "revascon_hist": input_dir / "B2_revascon_historico",
        "ultimos_90d": input_dir / "B3_ultimos_90_dias",
        "bilbao": input_dir / "C1_bilbao",
        "vitoria": input_dir / "C2_vitoria_gasteiz",
    }


def build_logger(log_path: Path | None = None) -> logging.Logger:
    logger = logging.getLogger("euskadi.consolidacion")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def configure_runtime(
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    log_path: Path | None = None,
) -> None:
    global INPUT_DIR, OUTPUT_DIR, PATHS, LOG_PATH, log, stats

    INPUT_DIR = (input_dir or DEFAULT_INPUT_DIR).resolve()
    OUTPUT_DIR = (output_dir or DEFAULT_OUTPUT_DIR).resolve()
    PATHS = build_paths(INPUT_DIR)
    LOG_PATH = (log_path or DEFAULT_LOG_PATH).resolve()
    log = build_logger(LOG_PATH)
    stats = {}


INPUT_DIR = DEFAULT_INPUT_DIR
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
PATHS = build_paths(INPUT_DIR)
LOG_PATH = DEFAULT_LOG_PATH
log = logging.getLogger("euskadi.consolidacion")
log.addHandler(logging.NullHandler())
stats = {}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def safe_str_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas object a string para evitar tipos mixtos en Parquet."""
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).replace({"nan": None, "None": None, "": None})
    return df


def load_json_pages(directory: Path) -> pd.DataFrame:
    """
    Carga todos los JSON paginados de la API y extrae los items.
    Estructura esperada: {totalItems, totalPages, items: [...]}
    """
    all_items = []
    json_files = sorted(directory.glob("*.json"))

    if not json_files:
        log.warning("  Sin ficheros JSON en %s", directory)
        return pd.DataFrame()

    for f in json_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            items = data.get("items", [])
            if isinstance(items, list):
                all_items.extend(items)
        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not all_items:
        return pd.DataFrame()

    df = pd.json_normalize(all_items, sep="_")
    log.info("  %d registros de %d páginas JSON", len(df), len(json_files))
    return df


def load_xlsx_files(directory: Path, pattern: str = "*.xlsx") -> pd.DataFrame:
    """Carga y concatena todos los XLSX de un directorio."""
    frames = []
    xlsx_files = sorted(directory.glob(pattern))

    if not xlsx_files:
        log.warning("  Sin ficheros XLSX en %s", directory)
        return pd.DataFrame()

    for f in xlsx_files:
        try:
            # Intentar leer con openpyxl (xlsx)
            df = pd.read_excel(f, engine="openpyxl")
            if len(df) > 0:
                # Añadir columna de origen (año del fichero)
                year_str = f.stem.split("_")[-1]
                try:
                    df["_archivo_origen"] = f.name
                    df["_year"] = int(year_str)
                except ValueError:
                    df["_archivo_origen"] = f.name

                frames.append(df)
                log.info("  %s: %d filas × %d cols", f.name, len(df), len(df.columns))
            else:
                log.info("  %s: vacío — saltando", f.name)
        except Exception as e:
            # Fallback: intentar con xlrd (xls)
            try:
                df = pd.read_excel(f, engine="xlrd")
                if len(df) > 0:
                    df["_archivo_origen"] = f.name
                    frames.append(df)
                    log.info("  %s: %d filas × %d cols (xlrd)", f.name, len(df), len(df.columns))
            except Exception as e2:
                log.warning("  Error leyendo %s: %s / %s", f.name, e, e2)

    if not frames:
        return pd.DataFrame()

    # Concatenar con unión de columnas (pueden variar entre años)
    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL: %d filas × %d cols", len(df), len(df.columns))
    return df


def load_csv_files(directory: Path, pattern: str = "*.csv",
                   encoding: str = "utf-8") -> pd.DataFrame:
    """Carga y concatena todos los CSV de un directorio."""
    frames = []
    csv_files = sorted(directory.glob(pattern))

    if not csv_files:
        log.warning("  Sin ficheros CSV en %s", directory)
        return pd.DataFrame()

    for f in csv_files:
        if f.stat().st_size < 100:
            log.info("  %s: demasiado pequeño — saltando", f.name)
            continue
        try:
            # Detectar separador
            head = f.read_bytes()[:2000].decode(encoding, errors="replace")
            sep = ";" if head.count(";") > head.count(",") else ","

            df = pd.read_csv(f, sep=sep, encoding=encoding, low_memory=False,
                             on_bad_lines="skip")
            if len(df) > 0:
                df["_archivo_origen"] = f.name
                frames.append(df)
                log.info("  %s: %d filas × %d cols (sep='%s')",
                         f.name, len(df), len(df.columns), sep)
        except UnicodeDecodeError:
            # Reintentar con latin-1
            try:
                df = pd.read_csv(f, sep=sep, encoding="latin-1", low_memory=False,
                                 on_bad_lines="skip")
                if len(df) > 0:
                    df["_archivo_origen"] = f.name
                    frames.append(df)
                    log.info("  %s: %d filas × %d cols (latin-1)",
                             f.name, len(df), len(df.columns))
            except Exception as e2:
                log.warning("  Error leyendo %s: %s", f.name, e2)
        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL: %d filas × %d cols", len(df), len(df.columns))
    return df


def save_parquet(df: pd.DataFrame, dest: Path, label: str) -> dict:
    """Guarda DataFrame como Parquet y devuelve estadísticas."""
    if df.empty:
        log.warning("  %s: DataFrame vacío — no se genera Parquet", label)
        return {"registros": 0, "columnas": 0, "tamaño_mb": 0}

    df = safe_str_columns(df)

    # Eliminar columnas completamente vacías
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        log.info("  Eliminadas %d columnas vacías", len(empty_cols))

    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dest, index=False, engine="pyarrow")

    size_mb = dest.stat().st_size / (1024 * 1024)
    log.info("  ✓ %s: %d filas × %d cols → %.1f MB",
             label, len(df), len(df.columns), size_mb)

    return {
        "registros": len(df),
        "columnas": len(df.columns),
        "tamaño_mb": round(size_mb, 2),
        "lista_columnas": df.columns.tolist(),
    }


# ═══════════════════════════════════════════════════════════════
# CONSOLIDADORES POR MÓDULO
# ═══════════════════════════════════════════════════════════════

def consolidar_B1_contratos_master() -> dict:
    """
    B1 → contratos_master.parquet
    FUENTE PRINCIPAL: XLSX anuales de contratos del sector público.
    655K+ contratos, 2011-2026.

    Los XLSX de 2011-2013 están vacíos (solo cabeceras). Los datos de esos
    años están en JSON (Open Data Euskadi). Este consolidador carga ambos.
    """
    log.info("=" * 60)
    log.info("B1. CONTRATOS MASTER (XLSX + JSON anuales → Parquet)")
    log.info("=" * 60)

    src = PATHS["xlsx_anual"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    frames = []

    # ── XLSX (2014-2026, los que tienen datos) ───────────────
    df_xlsx = load_xlsx_files(src, "contratos_*.xlsx")
    if not df_xlsx.empty:
        frames.append(df_xlsx)

    # ── JSON fallback (2011-2013, XLSX vacíos) ───────────────
    json_files = sorted(src.glob("contratos_*.json"))
    for f in json_files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))

            # El JSON de Open Data puede tener varias estructuras:
            # 1. Lista directa de contratos: [{"campo": "valor"}, ...]
            # 2. Objeto con key "items" o "contracts": {"items": [...]}
            # 3. Estructura anidada del CMS de Euskadi

            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Buscar la lista de items en las keys del dict
                for key in ("items", "contracts", "contratos", "data",
                            "opendata", "anuncios"):
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break
                # Si no encuentra una lista, puede ser un dict de dicts
                if items is None:
                    # Estructura tipo {id1: {campos...}, id2: {campos...}}
                    first_val = next(iter(data.values()), None)
                    if isinstance(first_val, dict):
                        items = list(data.values())

            if items and len(items) > 0:
                df_json = pd.json_normalize(items, sep="_")
                year_str = f.stem.split("_")[-1]
                df_json["_archivo_origen"] = f.name
                try:
                    df_json["_year"] = int(year_str)
                except ValueError:
                    pass
                frames.append(df_json)
                log.info("  %s: %d filas × %d cols (JSON)",
                         f.name, len(df_json), len(df_json.columns))
            else:
                log.warning("  %s: no se encontraron items en el JSON", f.name)

        except Exception as e:
            log.warning("  Error leyendo %s: %s", f.name, e)

    if not frames:
        return {"registros": 0, "error": "sin datos"}

    df = pd.concat(frames, ignore_index=True, sort=False)
    log.info("  TOTAL combinado: %d filas × %d cols", len(df), len(df.columns))

    # ── Limpieza básica ──────────────────────────────────────
    # Normalizar nombres de columnas (minúsculas, sin espacios extra)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Eliminar filas completamente vacías
    df = df.dropna(how="all")

    # Eliminar duplicados exactos si los hay
    n_antes = len(df)
    df = df.drop_duplicates()
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados exactos", n_dupes)

    # ── Tipado de columnas comunes ───────────────────────────
    # Intentar convertir columnas de importe a numérico
    for col in df.columns:
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto",
                                     "iva", "canon", "monto")):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Intentar parsear fechas
    for col in df.columns:
        if any(kw in col for kw in ("fecha", "date", "data")):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            except Exception:
                pass

    # Añadir metadatos de fuente
    df["_fuente"] = "B1_xlsx_sector_publico"

    dest = OUTPUT_DIR / "contratos_master.parquet"
    info = save_parquet(df, dest, "contratos_master")
    info["duplicados_eliminados"] = n_dupes
    info["rango_años"] = f"2011-{datetime.now().year}"
    return info


def consolidar_A3_poderes() -> dict:
    """
    A3 → poderes_adjudicadores.parquet
    919 poderes adjudicadores del registro público.
    """
    log.info("=" * 60)
    log.info("A3. PODERES ADJUDICADORES (JSON API → Parquet)")
    log.info("=" * 60)

    src = PATHS["api_authorities"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_json_pages(src)
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Convertir columnas con listas/dicts a string (no son hashables)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False)
                                    if isinstance(x, (list, dict)) else x)

    # El campo 'id' de la API es el índice dentro de la página (1-10),
    # NO un identificador único. Usamos dedup por contenido completo.
    content_cols = [c for c in df.columns
                    if c not in ("id", "_fuente", "_archivo_origen")
                    and not c.startswith("_")]
    n_antes = len(df)
    df = df.drop_duplicates(subset=content_cols if content_cols else None)
    if len(df) < n_antes:
        log.info("  Deduplicados %d → %d (contenido completo)", n_antes, len(df))

    df["_fuente"] = "A3_api_poderes"
    dest = OUTPUT_DIR / "poderes_adjudicadores.parquet"
    return save_parquet(df, dest, "poderes_adjudicadores")


def consolidar_A4_empresas() -> dict:
    """
    A4 → empresas_licitadoras.parquet
    9042 empresas del Registro de Licitadores.
    """
    log.info("=" * 60)
    log.info("A4. EMPRESAS LICITADORAS (JSON API → Parquet)")
    log.info("=" * 60)

    src = PATHS["api_companies"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_json_pages(src)
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Convertir columnas con listas/dicts a string (no son hashables)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False)
                                    if isinstance(x, (list, dict)) else x)

    # Deduplicamos por contenido completo para no perder registros.
    content_cols = [c for c in df.columns
                    if c not in ("id", "_fuente", "_archivo_origen")
                    and not c.startswith("_")]
    n_antes = len(df)
    df = df.drop_duplicates(subset=content_cols if content_cols else None)
    if len(df) < n_antes:
        log.info("  Deduplicados %d → %d (contenido completo)", n_antes, len(df))

    df["_fuente"] = "A4_api_empresas"
    dest = OUTPUT_DIR / "empresas_licitadoras.parquet"
    return save_parquet(df, dest, "empresas_licitadoras")


def consolidar_B2_revascon() -> dict:
    """
    B2 → revascon_historico.parquet
    Registro de contratos 2013-2018 (serie pre-API).
    """
    log.info("=" * 60)
    log.info("B2. REVASCON HISTÓRICO (CSV/XLSX → Parquet)")
    log.info("=" * 60)

    src = PATHS["revascon_hist"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    frames = []

    # Cargar CSVs
    df_csv = load_csv_files(src, "revascon_*.csv")
    if not df_csv.empty:
        frames.append(df_csv)

    # Cargar XLSXs
    df_xlsx = load_xlsx_files(src, "revascon_*.xlsx")
    if not df_xlsx.empty:
        frames.append(df_xlsx)

    if not frames:
        return {"registros": 0, "error": "sin datos"}

    df = pd.concat(frames, ignore_index=True, sort=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Eliminar duplicados
    n_antes = len(df)
    df = df.drop_duplicates()
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados", n_dupes)

    df["_fuente"] = "B2_revascon_historico"
    dest = OUTPUT_DIR / "revascon_historico.parquet"
    info = save_parquet(df, dest, "revascon_historico")
    info["duplicados_eliminados"] = n_dupes
    return info


def consolidar_C1_bilbao() -> dict:
    """
    C1 → bilbao_contratos.parquet
    Contratos municipales de Bilbao (2005-presente).
    """
    log.info("=" * 60)
    log.info("C1. BILBAO CONTRATOS MUNICIPALES (CSV → Parquet)")
    log.info("=" * 60)

    src = PATHS["bilbao"]
    if not src.exists():
        log.warning("  Directorio no encontrado: %s", src)
        return {"registros": 0, "error": "directorio no encontrado"}

    df = load_csv_files(src, "bilbao_*.csv")
    if df.empty:
        return {"registros": 0, "error": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Bilbao descarga por año Y por tipo → posibles duplicados
    n_antes = len(df)
    # Excluir columna de origen para comparar
    compare_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.drop_duplicates(subset=compare_cols)
    n_dupes = n_antes - len(df)
    if n_dupes:
        log.info("  Eliminados %d duplicados (solapamiento año/tipo)", n_dupes)

    # Tipado
    for col in df.columns:
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto")):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        if any(kw in col for kw in ("fecha", "date")):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            except Exception:
                pass

    df["_fuente"] = "C1_bilbao"
    dest = OUTPUT_DIR / "bilbao_contratos.parquet"
    info = save_parquet(df, dest, "bilbao_contratos")
    info["duplicados_eliminados"] = n_dupes
    return info


def consolidar_B3_ultimos_90d() -> dict:
    """
    B3 → últimos 90 días (si existe, puede dar 404).
    """
    log.info("=" * 60)
    log.info("B3. ÚLTIMOS 90 DÍAS (si disponible)")
    log.info("=" * 60)

    src = PATHS["ultimos_90d"]
    if not src.exists():
        log.info("  No disponible (404 en descarga)")
        return {"registros": 0, "nota": "no disponible (404)"}

    df = load_xlsx_files(src, "ultimos_*.xlsx")
    if df.empty:
        log.info("  Sin datos")
        return {"registros": 0, "nota": "sin datos"}

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["_fuente"] = "B3_ultimos_90d"

    dest = OUTPUT_DIR / "ultimos_90d.parquet"
    return save_parquet(df, dest, "ultimos_90d")


# ═══════════════════════════════════════════════════════════════
# GENERACIÓN DE DOCUMENTACIÓN
# ═══════════════════════════════════════════════════════════════

def generar_readme(all_stats: dict):
    """Genera README.md con la documentación del dataset."""
    total_regs = sum(v.get("registros", 0) for v in all_stats.values())
    total_mb = sum(v.get("tamaño_mb", 0) for v in all_stats.values())

    readme = f"""# Contratación Pública de Euskadi — Dataset Consolidado

## Resumen

| Métrica | Valor |
|---------|-------|
| **Fecha consolidación** | {datetime.now().strftime('%Y-%m-%d %H:%M')} |
| **Total registros** | {total_regs:,} |
| **Tamaño Parquet** | {total_mb:.1f} MB |
| **Archivos generados** | {len([v for v in all_stats.values() if v.get('registros', 0) > 0])} |

## Archivos

| Archivo | Registros | Tamaño | Fuente | Descripción |
|---------|-----------|--------|--------|-------------|
"""

    file_docs = {
        "contratos_master": {
            "fuente": "B1 (XLSX Open Data)",
            "desc": "Contratos del Sector Público Vasco 2011-2026 (FUENTE PRINCIPAL)",
        },
        "poderes_adjudicadores": {
            "fuente": "A3 (API KontratazioA)",
            "desc": "800+ poderes adjudicadores (GV, Diputaciones, Aytos, OOAA)",
        },
        "empresas_licitadoras": {
            "fuente": "A4 (API KontratazioA)",
            "desc": "Empresas del Registro de Licitadores de Euskadi",
        },
        "revascon_historico": {
            "fuente": "B2 (Open Data)",
            "desc": "REVASCON agregado 2013-2018 (serie pre-API)",
        },
        "bilbao_contratos": {
            "fuente": "C1 (Portal Bilbao)",
            "desc": "Contratos municipales Bilbao 2005-2026",
        },
        "ultimos_90d": {
            "fuente": "B3 (Open Data)",
            "desc": "Snapshot contratos últimos 90 días (ventana móvil)",
        },
    }

    for key, info in all_stats.items():
        regs = info.get("registros", 0)
        if regs == 0:
            continue
        mb = info.get("tamaño_mb", 0)
        doc = file_docs.get(key, {"fuente": "?", "desc": "?"})
        readme += f"| `{key}.parquet` | {regs:,} | {mb:.1f} MB | {doc['fuente']} | {doc['desc']} |\n"

    readme += """
## Notas sobre redundancia

- **contratos_master** (B1) es la fuente principal de contratos y subsume los
  datos que la API expone en A1/A2 (solo muestras de 1K registros). Las
  muestras API **no se incluyen** en la consolidación.
- **revascon_historico** (B2) contiene datos 2013-2018 con formato más rico
  que B1 para ese período. Hay solapamiento con contratos_master.
- **bilbao_contratos** (C1) puede incluir contratos menores municipales que
  no están en KontratazioA/REVASCON.

## Fuentes

- **KontratazioA API**: `https://api.euskadi.eus/procurements/`
- **Open Data Euskadi**: `https://opendata.euskadi.eus/`
- **Portal Bilbao**: `https://www.bilbao.eus/opendata/`

## Esquema de columnas

"""

    for key, info in all_stats.items():
        cols = info.get("lista_columnas", [])
        if cols:
            readme += f"### {key}.parquet\n\n"
            readme += f"Columnas ({len(cols)}): "
            readme += ", ".join(f"`{c}`" for c in cols[:30])
            if len(cols) > 30:
                readme += f" ... (+{len(cols)-30} más)"
            readme += "\n\n"

    (OUTPUT_DIR / "README.md").write_text(readme, encoding="utf-8")
    log.info("  ✓ README.md generado")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consolidación de contratación pública de Euskadi a Parquet",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help="Directorio con JSON/XLSX/CSV descargados por ccaa_euskadi.py.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directorio de salida de los Parquet consolidados.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="Ruta del fichero de log de consolidación.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    import time as _time

    args = build_parser().parse_args(argv)
    configure_runtime(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        log_path=args.log_path,
    )

    t0 = _time.time()

    log.info("╔═══════════════════════════════════════════════════════════╗")
    log.info("║  CONSOLIDACIÓN EUSKADI v4 → PARQUET                     ║")
    log.info("║  Fecha: %s                                  ║",
             datetime.now().strftime("%Y-%m-%d"))
    log.info("╚═══════════════════════════════════════════════════════════╝")

    # Verificar dependencias
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        log.error("Falta pyarrow. Instala con: pip install pyarrow")
        return 1

    try:
        import openpyxl  # noqa: F401
    except ImportError:
        log.error("Falta openpyxl. Instala con: pip install openpyxl")
        return 1

    # Verificar que exista el directorio de entrada
    if not INPUT_DIR.exists():
        log.error("Directorio de entrada no encontrado: %s", INPUT_DIR)
        log.error("Ejecuta primero el scraper: python Euskadi/ccaa_euskadi.py")
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # ── Consolidar cada módulo ──────────────────────────────
    all_stats = {}

    all_stats["contratos_master"]       = consolidar_B1_contratos_master()
    all_stats["poderes_adjudicadores"]  = consolidar_A3_poderes()
    all_stats["empresas_licitadoras"]   = consolidar_A4_empresas()
    all_stats["revascon_historico"]      = consolidar_B2_revascon()
    all_stats["bilbao_contratos"]       = consolidar_C1_bilbao()
    all_stats["ultimos_90d"]            = consolidar_B3_ultimos_90d()

    # ── Generar documentación ───────────────────────────────
    log.info("=" * 60)
    log.info("DOCUMENTACIÓN")
    log.info("=" * 60)

    # Stats JSON
    stats_out = {
        "fecha": datetime.now().isoformat(),
        "input_dir": str(INPUT_DIR),
        "output_dir": str(OUTPUT_DIR),
        "datasets": all_stats,
    }
    (OUTPUT_DIR / "stats.json").write_text(
        json.dumps(stats_out, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    log.info("  ✓ stats.json generado")

    generar_readme(all_stats)

    # ── Resumen final ───────────────────────────────────────
    elapsed = _time.time() - t0
    total_regs = sum(v.get("registros", 0) for v in all_stats.values())
    total_mb = sum(v.get("tamaño_mb", 0) for v in all_stats.values())
    n_files = len([v for v in all_stats.values() if v.get("registros", 0) > 0])

    log.info("═" * 60)
    log.info("RESUMEN CONSOLIDACIÓN")
    log.info("─" * 60)
    log.info("  Archivos Parquet:  %d", n_files)
    log.info("  Total registros:   %s", f"{total_regs:,}")
    log.info("  Tamaño Parquet:    %.1f MB", total_mb)
    log.info("  Tiempo:            %.0f s", elapsed)
    log.info("─" * 60)

    for key, info in all_stats.items():
        regs = info.get("registros", 0)
        mb = info.get("tamaño_mb", 0)
        if regs > 0:
            log.info("  ✓ %-30s %8s regs  %6.1f MB",
                     f"{key}.parquet", f"{regs:,}", mb)
        else:
            nota = info.get("nota", info.get("error", "sin datos"))
            log.info("  ✗ %-30s %s", key, nota)

    log.info("═" * 60)
    log.info("\nSalida: %s/", OUTPUT_DIR)
    log.info("  Uso:")
    log.info('    df = pd.read_parquet("%s")', OUTPUT_DIR / "contratos_master.parquet")
    log.info("    df.info()")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
