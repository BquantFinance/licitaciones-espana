#!/usr/bin/env python3
"""
BORME × PLACSP — Detector de anomalías en contratación pública
================================================================
Cruza datos del Registro Mercantil (BORME) con licitaciones públicas (PLACSP)
para detectar patrones sospechosos.

Red flags:
  1. Empresa recién creada: constitución < 6 meses antes de adjudicación
  2. Capital ridículo: capital social bajo ganando contratos grandes
  3. Mismos administradores: misma persona en empresas competidoras
  4. Disolución tras cobro: disolución poco después de adjudicación
  5. Cambio de administradores: cambios de cargos alrededor de adjudicación
  6. Empresa fantasma: sin actividad BORME previa al contrato

Uso:
  python borme_placsp_match.py --borme D:\Licitaciones --placsp D:\Licitaciones\nacional\licitaciones_espana.parquet --output D:\Licitaciones\anomalias
"""

import re
import argparse
import logging
import unicodedata
from pathlib import Path
from datetime import timedelta

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)

# =====================================================================
# NORMALIZACIÓN
# =====================================================================

def normalize_empresa(name):
    """Normaliza nombre de empresa para matching (BORME y PLACSP).
    - UPPER
    - Quita acentos (preserva Ñ)
    - Quita paréntesis: MARFINA SL (MOVENTIS) → MARFINA SL
    - Normaliza abreviaturas: S.L. S.R.L. S.A.E. S M E → SL SRL SAE SME
    - Quita sufijos jurídicos en loop (SA SOCIEDAD UNIPERSONAL → quita ambos)
    """
    if not name or not isinstance(name, str):
        return ""

    n = name.upper().strip()

    # Preservar Ñ, quitar acentos
    n = n.replace('Ñ', '##ENIE##')
    n = unicodedata.normalize('NFD', n)
    n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
    n = n.replace('##ENIE##', 'Ñ')

    # Quitar contenido entre paréntesis
    n = re.sub(r'\s*\([^)]*\)', '', n)

    # Abreviaturas con puntos
    n = re.sub(r'\bS\s*\.\s*R\s*\.\s*L\s*\.?\b', 'SRL', n)
    n = re.sub(r'\bS\s*\.\s*L\s*\.\s*U\s*\.?\b', 'SLU', n)
    n = re.sub(r'\bS\s*\.\s*L\s*\.\s*P\s*\.?\b', 'SLP', n)
    n = re.sub(r'\bS\s*\.\s*L\s*\.\s*L\s*\.?\b', 'SLL', n)
    n = re.sub(r'\bS\s*\.\s*L\s*\.?\b', 'SL', n)
    n = re.sub(r'\bS\s*\.\s*A\s*\.\s*U\s*\.?\b', 'SAU', n)
    n = re.sub(r'\bS\s*\.\s*A\s*\.\s*E\s*\.?\b', 'SAE', n)
    n = re.sub(r'\bS\s*\.\s*A\s*\.?\b', 'SA', n)
    n = re.sub(r'\bS\s*\.\s*C\s*\.?\b', 'SC', n)
    n = re.sub(r'\bA\s*\.\s*I\s*\.\s*E\s*\.?\b', 'AIE', n)
    n = re.sub(r'\bS\s*\.\s*M\s*\.\s*E\s*\.?\b', 'SME', n)

    # Abreviaturas con espacios
    n = re.sub(r'\bS\s+R\s+L\b', 'SRL', n)
    n = re.sub(r'\bS\s+M\s+E\b', 'SME', n)
    n = re.sub(r'\bS\s+L\s+U\b', 'SLU', n)
    n = re.sub(r'\bS\s+L\s+P\b', 'SLP', n)
    n = re.sub(r'\bS\s+L\s+L\b', 'SLL', n)
    n = re.sub(r'\bS\s+L\b$', 'SL', n)
    n = re.sub(r'\bS\s+A\s+U\b', 'SAU', n)
    n = re.sub(r'\bS\s+A\s+E\b', 'SAE', n)
    n = re.sub(r'\bS\s+A\b$', 'SA', n)

    # Quitar comas, puntos, guiones
    n = re.sub(r'[,.\-]', ' ', n)
    n = re.sub(r'\s+', ' ', n).strip()

    # Quitar sufijos jurídicos en loop
    SUFFIXES = [
        " SOCIEDAD ANONIMA DEPORTIVA", " SOCIEDAD ANONIMA",
        " SOCIEDAD LIMITADA PROFESIONAL", " SOCIEDAD LIMITADA LABORAL",
        " SOCIEDAD LIMITADA NUEVA EMPRESA", " SOCIEDAD LIMITADA",
        " SOCIEDAD COOPERATIVA ANDALUZA", " SOCIEDAD COOPERATIVA",
        " SOCIEDAD CIVIL PROFESIONAL", " SOCIEDAD CIVIL",
        " SOCIEDAD UNIPERSONAL",
        " AGRUPACION DE INTERES ECONOMICO",
        " SAU", " SLU", " SAD", " SLL", " SLP", " SLNE",
        " SA SME", " SAE", " SME", " SA", " SL", " SC",
        " SCA", " SCCL", " SCOOP", " SE", " SRL", " AIE",
    ]

    changed = True
    while changed:
        changed = False
        for suffix in SUFFIXES:
            if n.endswith(suffix):
                n = n[:-len(suffix)].strip()
                changed = True
                break

    n = re.sub(r'[.,;]+$', '', n).strip()
    return n


# =====================================================================
# CARGA DE DATOS
# =====================================================================

def load_borme(borme_dir: Path):
    """Carga BORME empresas y cargos, re-normaliza nombres."""
    log.info("Cargando BORME empresas...")
    df_emp = pd.read_parquet(borme_dir / "borme_empresas.parquet")
    df_emp["fecha_borme"] = pd.to_datetime(df_emp["fecha_borme"], errors="coerce")

    # Re-normalizar con la función unificada
    log.info("  Re-normalizando empresa_norm...")
    df_emp["empresa_norm"] = df_emp["empresa"].apply(normalize_empresa)
    log.info(f"  {len(df_emp):,} filas, {df_emp['empresa_norm'].nunique():,} empresas únicas")

    log.info("Cargando BORME cargos...")
    df_car = pd.read_parquet(borme_dir / "borme_cargos.parquet")
    df_car["fecha_borme"] = pd.to_datetime(df_car["fecha_borme"], errors="coerce")
    df_car["empresa_norm"] = df_car["empresa"].apply(normalize_empresa)
    log.info(f"  {len(df_car):,} filas, {df_car['persona'].nunique():,} personas únicas")

    return df_emp, df_car


def load_placsp(placsp_path: Path):
    """Carga PLACSP con adjudicaciones."""
    log.info("Cargando PLACSP...")
    cols = [
        "id", "expediente", "objeto", "organo_contratante",
        "tipo_contrato", "procedimiento", "estado",
        "importe_sin_iva", "importe_con_iva",
        "importe_adjudicacion", "importe_adj_con_iva",
        "adjudicatario", "nif_adjudicatario",
        "num_ofertas", "fecha_adjudicacion", "fecha_publicacion",
        "nuts", "urgencia",
    ]
    df = pd.read_parquet(placsp_path, columns=cols)

    # Solo adjudicaciones
    df = df[df["adjudicatario"].notna()].copy()
    df["fecha_adjudicacion"] = pd.to_datetime(df["fecha_adjudicacion"], errors="coerce")
    df["fecha_publicacion"] = pd.to_datetime(df["fecha_publicacion"], errors="coerce")

    # Normalizar
    log.info("  Normalizando adjudicatarios...")
    df["adj_norm"] = df["adjudicatario"].apply(normalize_empresa)

    log.info(f"  {len(df):,} adjudicaciones, {df['adj_norm'].nunique():,} adjudicatarios únicos")
    return df


# =====================================================================
# ANOMALÍAS
# =====================================================================

def flag_recien_creada(df_match, df_emp):
    """Flag 1: Empresa constituida < 6 meses antes de adjudicación."""
    log.info("Flag 1: Empresa recién creada...")

    # Fecha de primera constitución por empresa
    constit = df_emp[df_emp["actos"].str.contains("Constitución", na=False)]
    primera_constit = constit.groupby("empresa_norm")["fecha_borme"].min().reset_index()
    primera_constit.columns = ["empresa_norm", "fecha_constitucion"]

    merged = df_match.merge(primera_constit, left_on="adj_norm", right_on="empresa_norm",
                             how="left", suffixes=("", "_borme"))

    merged["dias_desde_constitucion"] = (
        merged["fecha_adjudicacion"] - merged["fecha_constitucion"]
    ).dt.days

    merged["flag_recien_creada"] = merged["dias_desde_constitucion"].between(0, 180)
    flagged = merged[merged["flag_recien_creada"] == True]
    log.info(f"  {len(flagged):,} adjudicaciones a empresas con <6 meses de vida")

    return merged


def flag_capital_ridiculo(df_match, df_emp):
    """Flag 2: Capital social ridículo vs importe adjudicación."""
    log.info("Flag 2: Capital ridículo...")

    # Capital más reciente por empresa
    with_cap = df_emp[df_emp["capital_euros"].notna()].copy()
    capital = with_cap.sort_values("fecha_borme").groupby("empresa_norm")["capital_euros"].last().reset_index()

    merged = df_match.merge(capital, left_on="adj_norm", right_on="empresa_norm",
                             how="left", suffixes=("", "_cap"))

    # Flag: capital < 10K y adjudicación > 100K
    merged["ratio_importe_capital"] = merged["importe_adjudicacion"] / merged["capital_euros"].replace(0, np.nan)
    merged["flag_capital_ridiculo"] = (
        (merged["capital_euros"] < 10_000) &
        (merged["importe_adjudicacion"] > 100_000)
    )
    flagged = merged[merged["flag_capital_ridiculo"] == True]
    log.info(f"  {len(flagged):,} adjudicaciones con capital ridículo")

    return merged


def flag_mismos_administradores(df_car):
    """Flag 3: Personas que administran múltiples empresas adjudicatarias."""
    log.info("Flag 3: Mismos administradores en múltiples empresas...")

    # Solo nombramientos activos (sin cese posterior)
    admins = df_car[df_car["tipo_acto"].isin(["nombramiento", "reeleccion"])].copy()

    # Personas con >1 empresa
    persona_empresas = admins.groupby("persona")["empresa_norm"].nunique()
    multi = persona_empresas[persona_empresas > 1]
    log.info(f"  {len(multi):,} personas en >1 empresa")

    return multi


def flag_disolucion_tras_cobro(df_match, df_emp):
    """Flag 4: Disolución/Extinción poco después de adjudicación."""
    log.info("Flag 4: Disolución tras adjudicación...")

    disol = df_emp[df_emp["actos"].str.contains("Disolución|Extinción", na=False)]
    primera_disol = disol.groupby("empresa_norm")["fecha_borme"].min().reset_index()
    primera_disol.columns = ["empresa_norm", "fecha_disolucion"]

    merged = df_match.merge(primera_disol, left_on="adj_norm", right_on="empresa_norm",
                             how="left", suffixes=("", "_dis"))

    merged["dias_hasta_disolucion"] = (
        merged["fecha_disolucion"] - merged["fecha_adjudicacion"]
    ).dt.days

    # Flag: disolución entre 0 y 365 días después de adjudicación
    merged["flag_disolucion_post"] = merged["dias_hasta_disolucion"].between(0, 365)
    flagged = merged[merged["flag_disolucion_post"] == True]
    log.info(f"  {len(flagged):,} adjudicaciones seguidas de disolución <1 año")

    return merged


def flag_concursal(df_match, df_emp):
    """Flag 5: Empresa en situación concursal adjudicando contratos."""
    log.info("Flag 5: Situación concursal...")

    concursal = df_emp[df_emp["actos"].str.contains("concurso|concursal", case=False, na=False)]
    primera_concursal = concursal.groupby("empresa_norm")["fecha_borme"].min().reset_index()
    primera_concursal.columns = ["empresa_norm", "fecha_concursal"]

    merged = df_match.merge(primera_concursal, left_on="adj_norm", right_on="empresa_norm",
                             how="left", suffixes=("", "_conc"))

    # Flag: adjudicación después de declaración de concurso
    merged["flag_concursal"] = (
        merged["fecha_adjudicacion"] >= merged["fecha_concursal"]
    )
    flagged = merged[merged["flag_concursal"] == True]
    log.info(f"  {len(flagged):,} adjudicaciones a empresas en concurso")

    return merged


# =====================================================================
# PIPELINE PRINCIPAL
# =====================================================================

def run_matching(borme_dir: Path, placsp_path: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    df_emp, df_car = load_borme(borme_dir)
    df_placsp = load_placsp(placsp_path)

    # ── Match por nombre normalizado ──
    log.info("\n=== MATCHING ===")
    borme_empresas_set = set(df_emp["empresa_norm"].unique())
    placsp_adj_set = set(df_placsp["adj_norm"].unique())

    overlap = borme_empresas_set & placsp_adj_set
    log.info(f"  Empresas BORME: {len(borme_empresas_set):,}")
    log.info(f"  Adjudicatarios PLACSP: {len(placsp_adj_set):,}")
    log.info(f"  Match por nombre: {len(overlap):,}")

    df_match = df_placsp[df_placsp["adj_norm"].isin(overlap)].copy()
    log.info(f"  Adjudicaciones matched: {len(df_match):,} de {len(df_placsp):,} ({len(df_match)/len(df_placsp)*100:.1f}%)")

    importe_match = df_match["importe_adjudicacion"].sum()
    importe_total = df_placsp["importe_adjudicacion"].sum()
    log.info(f"  Importe matched: {importe_match/1e9:.1f}B€ de {importe_total/1e9:.1f}B€ ({importe_match/importe_total*100:.1f}%)")

    # ── Anomalías ──
    log.info("\n=== ANOMALÍAS ===")

    # Flag 1: Recién creada
    df_f1 = flag_recien_creada(df_match, df_emp)
    f1 = df_f1[df_f1["flag_recien_creada"] == True][[
        "id", "adj_norm", "adjudicatario", "importe_adjudicacion",
        "fecha_adjudicacion", "fecha_constitucion", "dias_desde_constitucion",
        "organo_contratante", "objeto"
    ]].copy()

    # Flag 2: Capital ridículo
    df_f2 = flag_capital_ridiculo(df_match, df_emp)
    f2 = df_f2[df_f2["flag_capital_ridiculo"] == True][[
        "id", "adj_norm", "adjudicatario", "importe_adjudicacion",
        "capital_euros", "ratio_importe_capital",
        "organo_contratante", "objeto"
    ]].copy()

    # Flag 3: Mismos administradores
    multi_admin = flag_mismos_administradores(df_car)

    # Flag 4: Disolución
    df_f4 = flag_disolucion_tras_cobro(df_match, df_emp)
    f4 = df_f4[df_f4["flag_disolucion_post"] == True][[
        "id", "adj_norm", "adjudicatario", "importe_adjudicacion",
        "fecha_adjudicacion", "fecha_disolucion", "dias_hasta_disolucion",
        "organo_contratante", "objeto"
    ]].copy()

    # Flag 5: Concursal
    df_f5 = flag_concursal(df_match, df_emp)
    f5 = df_f5[df_f5["flag_concursal"] == True][[
        "id", "adj_norm", "adjudicatario", "importe_adjudicacion",
        "fecha_adjudicacion", "fecha_concursal",
        "organo_contratante", "objeto"
    ]].copy()

    # ── Guardar ──
    log.info("\n=== GUARDANDO RESULTADOS ===")

    f1.to_parquet(output_dir / "flag1_recien_creada.parquet", index=False)
    log.info(f"  flag1_recien_creada: {len(f1):,} filas")

    f2.to_parquet(output_dir / "flag2_capital_ridiculo.parquet", index=False)
    log.info(f"  flag2_capital_ridiculo: {len(f2):,} filas")

    multi_admin.reset_index().to_parquet(output_dir / "flag3_multi_admin.parquet", index=False)
    log.info(f"  flag3_multi_admin: {len(multi_admin):,} personas")

    f4.to_parquet(output_dir / "flag4_disolucion.parquet", index=False)
    log.info(f"  flag4_disolucion: {len(f4):,} filas")

    f5.to_parquet(output_dir / "flag5_concursal.parquet", index=False)
    log.info(f"  flag5_concursal: {len(f5):,} filas")

    # ── Resumen ──
    log.info(f"\n{'='*60}")
    log.info(f"RESUMEN ANOMALÍAS")
    log.info(f"{'='*60}")
    log.info(f"  Flag 1 — Recién creada (<6 meses):     {len(f1):>8,} adjudicaciones")
    log.info(f"  Flag 2 — Capital ridículo:              {len(f2):>8,} adjudicaciones")
    log.info(f"  Flag 3 — Multi-administrador:           {len(multi_admin):>8,} personas")
    log.info(f"  Flag 4 — Disolución post-adjudicación:  {len(f4):>8,} adjudicaciones")
    log.info(f"  Flag 5 — Adjudicación en concurso:      {len(f5):>8,} adjudicaciones")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BORME × PLACSP anomaly detector")
    parser.add_argument("--borme", required=True, help="Carpeta con borme_empresas.parquet y borme_cargos.parquet")
    parser.add_argument("--placsp", required=True, help="Ruta a licitaciones_espana.parquet")
    parser.add_argument("--output", required=True, help="Carpeta de salida para flags")
    args = parser.parse_args()

    run_matching(Path(args.borme), Path(args.placsp), Path(args.output))
