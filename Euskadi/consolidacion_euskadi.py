#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
 CONSOLIDACIÓN — CONTRATACIÓN PÚBLICA DE EUSKADI  v4
═══════════════════════════════════════════════════════════════════════════════
 Convierte la salida del scraper v4 (JSON + XLSX + CSV) en Parquets limpios.

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
   ├── contratos_master.parquet        ← 655K+ contratos (B1)
   ├── poderes_adjudicadores.parquet   ← 919 poderes (A3)
   ├── empresas_licitadoras.parquet    ← 9042 empresas (A4)
   ├── revascon_historico.parquet      ← Serie 2013-2018 (B2)
   ├── bilbao_contratos.parquet        ← Contratos municipales (C1)
   ├── stats.json                      ← Estadísticas consolidación
   └── README.md                       ← Documentación
═══════════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import sys
import warnings
from pathlib import Path
from datetime import datetime

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ─────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────

# Rutas relativas al script (no al cwd), igual que en ccaa_euskadi.py
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "datos_euskadi_contratacion_v4"
OUTPUT_DIR = SCRIPT_DIR / "euskadi_parquet"

# Subdirectorios de entrada (del scraper v4)
PATHS = {
    "api_contracts":   INPUT_DIR / "A1_api_contratos",
    "api_notices":     INPUT_DIR / "A2_api_anuncios",
    "api_authorities": INPUT_DIR / "A3_api_poderes",
    "api_companies":   INPUT_DIR / "A4_api_empresas",
    "xlsx_anual":      INPUT_DIR / "B1_xlsx_sector_publico_anual",
    "revascon_hist":   INPUT_DIR / "B2_revascon_historico",
    "ultimos_90d":     INPUT_DIR / "B3_ultimos_90_dias",
    "bilbao":          INPUT_DIR / "C1_bilbao",
    "vitoria":         INPUT_DIR / "C2_vitoria_gasteiz",
}

# Los JSON 2011-2013 de B1 traen los mismos campos que el XLSX con otros
# nombres (comprobado con los ficheros reales): se renombran antes de
# concatenar para que esos años no queden en columnas aparte.
B1_JSON_A_XLSX = {
    "documentName": "Nombre",
    "documentDescription": "Descripción",
    "procedureCollection": "Colección",
    "friendlyUrl": "URL amigable",
    "physicalUrl": "URL física",
    "dataXML": "XML datos",
    "metadataXML": "XML metadatos",
    "contratacion_titulo_contrato": "Titulo del Contrato",
    "contratacion_objeto_contrato": "Objeto del Contrato",
    "contratacion_tipo_anuncio": "Tipo de Anuncio",
    "contratacion_fecha_de_publicacion_documento": "Fecha de publicación documento",
    "contratacion_expediente": "Expediente",
    "contratacion_estado_tramitacion": "Estado de la tramitacion",
    "contratacion_contrato_menor": "Contrato menor",
    "contratacion_adjudicacion": "Adjudicación",
    "contratacion_subsanacion": "Subsanación",
    "contratacion_apertura_plicas": "Apertura de plicas",
    "contratacion_acuerdos_mesa_contratacion": "Acuerdos de la mesa de contratacion",
    "contratacion_ambito_geografico": "Ámbito geográfico del poder adjudicador",
    "contratacion_poder_adjudicador_url": "URL del Logo del Poder Adjudicador",
    "contratacion_poder_adjudicador_titulo": "Título del Logo del Poder Adjudicador",
    "contratacion_entidad_impulsora": "Entidad que impulsa la contratación",
    "contratacion_organo_contratacion": "Órgano de Contratación",
    "contratacion_fecha_limite_presentacion": "Fecha límite de presentación",
}

# Cabeceras con mojibake en los XLSX B1 2022-2026 (sin corregir, esos años
# quedan en columnas distintas a las de 2014-2021)
CABECERAS_MOJIBAKE = {
    "Colecciï¿½n": "Colección",
    "Acrï¿½nimo (nombre corto) del procedimiento":
        "Acrónimo (nombre corto) del procedimiento",
    "Fecha de resoluciï¿½n": "Fecha de resolución",
}

# contratos_2021.xlsx (verificado sobre las 64.826 filas del fichero de 2026-02):
# - 18.000 filas traen desde "Fecha límite de presentación" los valores de
#   "URL física" en adelante (URL física, XML datos, XML metadatos, zip, fecha
#   de creación, institución…): corridos 2 columnas a la izquierda.
# - 826 filas los traen desde "Órgano de Contratación": corridos 3.
# - 21 de esas 826 traen además, desde "Fecha de publicación documento", los
#   valores de "Expediente" en adelante (expediente, estado, contrato menor,
#   entidad, órgano): corridos 1.
# Ningún otro fichero B1 (2011-2026) tiene filas corridas.
B1_COLS_CORRIDAS = [
    "órgano_de_contratación", "fecha_límite_de_presentación", "url_amigable",
    "url_física", "xml_datos", "xml_metadatos", "zip", "fecha_de_creación",
    "id._institución", "institución", "id._departamento", "departamento",
]
B1_COLS_CABECERA_CORRIDA = [
    "fecha_de_publicación_documento", "expediente", "estado_de_la_tramitacion",
    "contrato_menor", "entidad_que_impulsa_la_contratación", "órgano_de_contratación",
]
_FECHA_DMY = r"\d{1,2}/\d{1,2}/\d{4}(?: \d{1,2}:\d{2}(?::\d{2})?)?"

# REVASCON 2013-2014 (CSV) llama distinto a 3 campos del XLSX 2015-2018
REVASCON_CSV_A_XLSX = {
    "estado_de_contrato": "estado_contrato",
    "título": "título_de_contrato",
    "código_de_contrato": "código_identificador_del_contrato",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(SCRIPT_DIR / "consolidar_euskadi_v4.log",
                            encoding="utf-8", delay=True),
    ],
)
log = logging.getLogger(__name__)

stats = {}


# ─────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────

def safe_str_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas object a string para evitar tipos mixtos en Parquet."""
    for col in df.columns:
        # pandas 3 lee el texto con dtype "str" (no "object"): sin la 2ª
        # condición sus "" no se convierten en nulos como en pandas 2.
        if df[col].dtype == "object" or pd.api.types.is_string_dtype(df[col]):
            # en pandas 2 astype(str) convierte pd.NA en el texto "<NA>"
            nulo = df[col].isna().to_numpy()
            df[col] = df[col].astype(str).replace({"nan": None, "None": None, "": None})
            df.loc[nulo, col] = None
    return df


def parse_importes(s: pd.Series) -> pd.Series:
    """
    Convierte importes en formato español ("1.455.954,56", "52.990") a float.
    pd.to_numeric() leería "52.990" como 52,99 y dejaría en NaN todo lo que
    lleve coma decimal o varios puntos de miles.
    """
    if pd.api.types.is_numeric_dtype(s):
        return s
    txt = s.astype("string").str.replace("€", "", regex=False).str.strip()
    es = txt.str.fullmatch(r"-?\d{1,3}(?:\.\d{3})+(?:,\d+)?|-?\d+,\d+")
    es = es.fillna(False).astype(bool)
    txt[es] = txt[es].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(txt.astype(object), errors="coerce")


FORMATOS_FECHA = ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y", "ISO8601")


def parse_fechas(s: pd.Series) -> pd.Series:
    """
    Convierte fechas de texto a datetime probando varios formatos valor a valor.
    pd.to_datetime(dayfirst=True) deduce UN formato del primer valor y deja en
    NaT los que no lo siguen (p.ej. "24/09/2013" en una columna con
    "07/11/2014 10:00"). Si la columna viene en mes/día/año (hay valores con el
    2º campo > 12 y ninguno con el 1º > 12) se interpreta así y no día/mes.
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    txt = s.astype("string").str.strip()
    partes = txt.str.extract(r"^(\d{1,2})/(\d{1,2})/\d{4}")
    p1 = pd.to_numeric(partes[0].astype(object), errors="coerce")
    p2 = pd.to_numeric(partes[1].astype(object), errors="coerce")
    formatos = FORMATOS_FECHA
    if (p2 > 12).sum() > (p1 > 12).sum():
        formatos = tuple(f.replace("%d/%m", "%m/%d") for f in formatos)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    for fmt in formatos:
        falta = out.isna() & txt.notna()
        if not falta.any():
            break
        out[falta] = pd.to_datetime(txt[falta].astype(object), format=fmt,
                                    errors="coerce")
    return out


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
            # REVASCON 2015-2018 trae filas de título antes de la cabecera: la
            # 1ª fila (vacía) se toma como cabecera y todo sale "Unnamed: N".
            # Se relee usando como cabecera la 1ª fila con ≥ mitad de celdas.
            if len(df.columns) and all(str(c).startswith("Unnamed") for c in df.columns):
                llenas = df.notna().sum(axis=1)
                filas = llenas.index[llenas >= len(df.columns) / 2]
                if len(filas):
                    df = pd.read_excel(f, engine="openpyxl", header=int(filas[0]) + 1)
            df = df.rename(columns=CABECERAS_MOJIBAKE)
            if len(df) > 0:
                # Añadir columna de origen (año del fichero)
                year_str = f.stem.split("_")[-1]
                df["_archivo_origen"] = f.name
                # Solo si es un año (no la fecha AAAAMMDD de una instantánea)
                if year_str.isdigit() and len(year_str) == 4:
                    df["_year"] = int(year_str)

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

            # Todo como texto: si no, read_csv convierte expedientes como
            # "080617000001" en número (pierde el 0 inicial) e importes como
            # "52.990" en 52,99 antes de poder tratarlos como formato español
            df = pd.read_csv(f, sep=sep, encoding=encoding, low_memory=False,
                             on_bad_lines="skip", dtype=str)
            if len(df) > 0:
                df["_archivo_origen"] = f.name
                frames.append(df)
                log.info("  %s: %d filas × %d cols (sep='%s')",
                         f.name, len(df), len(df.columns), sep)
        except UnicodeDecodeError:
            # Reintentar con latin-1
            try:
                df = pd.read_csv(f, sep=sep, encoding="latin-1", low_memory=False,
                                 on_bad_lines="skip", dtype=str)
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
                df_json = df_json.rename(columns=B1_JSON_A_XLSX)
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
    # "" y nulo cuentan igual (las claves ausentes en un JSON son "" en otro)
    df = safe_str_columns(df)
    datos = [c for c in df.columns if not c.startswith("_")]

    # Recolocar las filas corridas (ver B1_COLS_CORRIDAS). Sin esto el tipado
    # de fechas convertiría en NaT las URL, zip, instituciones y expedientes
    # que el fichero trae en columnas de fecha. _columnas_corridas describe
    # cómo venía la fila en el fichero publicado ("columna:n" = desde esa
    # columna los valores estaban n columnas a la izquierda); nulo = sin tocar.
    cols = B1_COLS_CORRIDAS
    if all(c in df.columns for c in cols):
        limite = df[cols[1]].astype("string")
        corrida = limite.str.startswith("http").fillna(False).astype(bool)
        salto3 = corrida & limite.str.contains("/es_doc/data/").fillna(False).astype(bool)
        df["_columnas_corridas"] = pd.Series(pd.NA, index=df.index, dtype="string")
        for salto, filas in ((3, salto3), (2, corrida & ~salto3)):
            if filas.any():
                # "URL física" y lo que la sigue están en cols[3 - salto:]
                df.loc[filas, cols[3:]] = df.loc[filas, cols[3 - salto:len(cols) - salto]].to_numpy()
                df.loc[filas, cols[3 - salto:3]] = None
                df.loc[filas, "_columnas_corridas"] = f"{cols[3 - salto]}:{salto}"
        cab = B1_COLS_CABECERA_CORRIDA
        if all(c in df.columns for c in cab):
            pub = df[cab[0]].astype("string")
            no_es_fecha = pub.notna() & ~pub.str.fullmatch(_FECHA_DMY).fillna(False).astype(bool)
            filas = salto3 & no_es_fecha
            if filas.any():
                df.loc[filas, cab[1:]] = df.loc[filas, cab[:-1]].to_numpy()
                df.loc[filas, cab[0]] = None
                df.loc[filas, "_columnas_corridas"] = f"{cab[0]}:1," + df.loc[filas, "_columnas_corridas"]
        if corrida.any():
            log.info("  Recolocadas %d filas con columnas corridas", int(corrida.sum()))

    # Eliminar filas completamente vacías (sin contar _archivo_origen/_year)
    df = df.dropna(how="all", subset=datos)

    # Filas idénticas a otra anterior (sin contar el fichero de origen): se
    # conservan todas, tal como se publican, y se marcan con _duplicado. Los
    # JSON 2012 y 2013 repiten filas del de 2011.
    df["_duplicado"] = df.duplicated(subset=datos)
    n_dupes = int(df["_duplicado"].sum())
    if n_dupes:
        log.info("  %d filas repiten otra idéntica (marcadas en _duplicado)", n_dupes)

    # ── Tipado de columnas comunes ───────────────────────────
    # Intentar convertir columnas de importe a numérico
    for col in df.columns:
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto",
                                     "iva", "canon", "monto")):
            df[col] = parse_importes(df[col])

    # Intentar parsear fechas ("data" no: casaba con las URL dataXML/metadataXML)
    for col in df.columns:
        if any(kw in col for kw in ("fecha", "date")):
            try:
                df[col] = parse_fechas(df[col])
            except Exception:
                pass

    # Añadir metadatos de fuente
    df["_fuente"] = "B1_xlsx_sector_publico"

    dest = OUTPUT_DIR / "contratos_master.parquet"
    info = save_parquet(df, dest, "contratos_master")
    info["duplicados_marcados"] = n_dupes
    if "_year" in df.columns and df["_year"].notna().any():
        info["rango_años"] = f"{int(df['_year'].min())}-{int(df['_year'].max())}"
    else:
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

    # 'id' es el identificador del poder adjudicador (p.ej. 27191, el de
    # _links.self.href), no el índice en la página: dos poderes con distinto
    # id son distintos aunque coincida el resto. Si un id sale más de una vez
    # (la API lo repite o la paginación se desplaza durante la descarga) se
    # conservan todas las filas y _duplicado marca las que no son la última.
    if "id" in df.columns:
        df["_duplicado"] = df.duplicated(subset=["id"], keep="last")
    else:
        df["_duplicado"] = df.duplicated()
    if df["_duplicado"].any():
        log.info("  %d filas con id repetido (marcadas en _duplicado)", int(df["_duplicado"].sum()))

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

    # La API repite algunas empresas idénticas en posiciones consecutivas de
    # la misma página (25 de 9.042 en la descarga de 2026-02): se conservan y
    # se marcan en _duplicado.
    content_cols = [c for c in df.columns
                    if c not in ("id", "_fuente", "_archivo_origen")
                    and not c.startswith("_")]
    df["_duplicado"] = df.duplicated(subset=content_cols if content_cols else None)
    if df["_duplicado"].any():
        log.info("  %d filas repiten otra idéntica (marcadas en _duplicado)", int(df["_duplicado"].sum()))

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

    # Cargar CSVs (nombres normalizados antes de concatenar para que casen
    # con los del XLSX y no queden columnas duplicadas)
    df_csv = load_csv_files(src, "revascon_*.csv")
    if not df_csv.empty:
        df_csv.columns = [c.strip().lower().replace(" ", "_") for c in df_csv.columns]
        frames.append(df_csv.rename(columns=REVASCON_CSV_A_XLSX))

    # Cargar XLSXs
    df_xlsx = load_xlsx_files(src, "revascon_*.xlsx")
    if not df_xlsx.empty:
        df_xlsx.columns = [c.strip().lower().replace(" ", "_") for c in df_xlsx.columns]
        frames.append(df_xlsx)

    if not frames:
        return {"registros": 0, "error": "sin datos"}

    df = pd.concat(frames, ignore_index=True, sort=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Importes: texto "1.455.954,56" en el XLSX y numéricos en el CSV → float
    # (solo "importe*": "iva_del_importe_de_licitación" es texto "21%(...)")
    for col in df.columns:
        if col.startswith("importe"):
            df[col] = parse_importes(df[col])

    # El mismo contrato aparece idéntico en varios XLSX anuales: se conservan
    # todas las filas y se marcan las que repiten otra (sin contar el fichero)
    df["_duplicado"] = df.duplicated(subset=[c for c in df.columns if not c.startswith("_")])
    n_dupes = int(df["_duplicado"].sum())
    if n_dupes:
        log.info("  %d filas repiten otra idéntica (marcadas en _duplicado)", n_dupes)

    df["_fuente"] = "B2_revascon_historico"
    dest = OUTPUT_DIR / "revascon_historico.parquet"
    info = save_parquet(df, dest, "revascon_historico")
    info["duplicados_marcados"] = n_dupes
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

    # Cada ejecución guarda una instantánea bilbao_abiertas_AAAAMMDD.csv:
    # solo se usa la más reciente (si no, se acumulan versiones del mismo contrato)
    snaps = sorted(f for f in df["_archivo_origen"].unique()
                   if f.startswith("bilbao_abiertas_"))
    if len(snaps) > 1:
        df = df[~df["_archivo_origen"].isin(snaps[:-1])].copy()

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # El scraper descarga Bilbao por año, por tipo y "abiertas": la misma fila
    # llega en varias descargas. Se descartan esas repeticiones entre ficheros,
    # no las que el Ayuntamiento publica dentro de un mismo fichero: de cada
    # fila se conservan tantas copias como tenga el fichero que más tenga.
    # Los ficheros difieren en espacios finales ("VICONSA, S.A. " vs
    # "VICONSA, S.A."): se comparan sin ellos, pero los valores no se tocan.
    compare_cols = [c for c in df.columns if not c.startswith("_")]
    sin_espacios = df[compare_cols].apply(
        lambda s: s.str.strip() if pd.api.types.is_string_dtype(s) else s)
    clave = pd.util.hash_pandas_object(sin_espacios, index=False)
    copia = clave.groupby([df["_archivo_origen"].to_numpy(), clave.to_numpy()]).cumcount()
    repetida = pd.DataFrame({"clave": clave, "copia": copia}).duplicated().to_numpy()
    n_dupes = int(repetida.sum())
    df = df[~repetida].copy()
    if n_dupes:
        log.info("  Descartadas %d filas repetidas entre descargas (año/tipo/abiertas)", n_dupes)

    # Tipado (importes "1.234.567,89" y fechas dd/mm o mm/dd según la columna)
    for col in df.columns:
        if col == "lote":   # el CSV se lee como texto; numérico si no se pierde nada
            num = pd.to_numeric(df[col], errors="coerce")
            if num.notna().sum() == df[col].notna().sum():
                df[col] = num
        if any(kw in col for kw in ("importe", "valor", "precio", "presupuesto")):
            df[col] = parse_importes(df[col])
        if any(kw in col for kw in ("fecha", "date")):
            try:
                df[col] = parse_fechas(df[col])
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

    # Cada ejecución guarda una instantánea (ultimos_90d_AAAAMMDD.xlsx) y las
    # ventanas se solapan: solo se consolida la más reciente
    snaps = sorted(src.glob("ultimos_*.xlsx"))
    df = load_xlsx_files(src, snaps[-1].name) if snaps else pd.DataFrame()
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

    readme += f"""
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

def main():
    import time as _time
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
        sys.exit(1)

    try:
        import openpyxl  # noqa: F401
    except ImportError:
        log.error("Falta openpyxl. Instala con: pip install openpyxl")
        sys.exit(1)

    # Verificar que exista el directorio de entrada
    if not INPUT_DIR.exists():
        log.error("Directorio de entrada no encontrado: %s", INPUT_DIR)
        log.error("Ejecuta primero el scraper: python ccaa_euskadi.py")
        sys.exit(1)

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
        "input_dir": INPUT_DIR.name,     # relativo al script (sin rutas locales)
        "output_dir": OUTPUT_DIR.name,
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
    log.info('    df = pd.read_parquet("euskadi_parquet/contratos_master.parquet")')
    log.info("    df.info()")


if __name__ == "__main__":
    main()


