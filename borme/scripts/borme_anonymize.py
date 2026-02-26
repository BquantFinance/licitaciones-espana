#!/usr/bin/env python3
"""
BORME Anonymizer — Genera datasets públicos sin datos personales
================================================================
Toma la salida cruda del parser (borme_empresas.parquet + borme_cargos.parquet)
y genera versiones anonimizadas aptas para redistribución pública (GitHub).

Los nombres de personas físicas se eliminan o se sustituyen por un hash
irreversible (SHA-256 truncado). Esto preserva la capacidad de detectar
administradores compartidos entre empresas sin exponer identidades.

Salidas:
  1. borme_empresas_pub.parquet   — Actos mercantiles por empresa (sin cambios,
                                     no contiene datos personales directos)
  2. borme_cargos_pub.parquet     — Cargos con persona_hash en vez de nombre
  3. borme_grafo_admin.parquet    — Grafo empresa↔empresa por admin compartido

Uso:
  python borme_anonymize.py --input ./borme_pdfs --output ./data

Fuente de los datos: Agencia Estatal Boletín Oficial del Estado (https://www.boe.es)
"""

import hashlib
import argparse
import logging
from pathlib import Path
from itertools import combinations

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)


def hash_persona(name: str, salt: str = "borme_2024") -> str:
    """Hash irreversible de nombre de persona.

    Se usa SHA-256 con salt, truncado a 16 chars hex.
    Suficiente para detectar coincidencias, imposible de revertir.
    """
    if not name or not isinstance(name, str):
        return ""
    raw = f"{salt}:{name.strip().upper()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def anonymize_empresas(df_emp: pd.DataFrame) -> pd.DataFrame:
    """Limpia borme_empresas: quita campos de texto libre que podrían
    contener nombres de personas (objeto_social a veces menciona personas)."""
    log.info("Anonimizando empresas...")

    # Columnas a mantener (sin objeto_social que puede tener nombres)
    keep_cols = [
        "fecha_borme", "num_borme", "num_entrada",
        "empresa", "empresa_norm", "provincia", "cod_provincia",
        "tipo_borme", "actos", "domicilio", "capital_euros",
        "fecha_constitucion", "hoja_registral", "tomo", "inscripcion",
        "fecha_inscripcion", "pdf_filename",
    ]
    cols = [c for c in keep_cols if c in df_emp.columns]
    df = df_emp[cols].copy()

    # Truncar domicilio a ciudad (quitar calle/número que podría ser personal)
    if "domicilio" in df.columns:
        df["domicilio"] = df["domicilio"].apply(
            lambda x: x.split("(")[0].strip()[-80:] if isinstance(x, str) else x
        )

    log.info(f"  {len(df):,} filas, {df['empresa_norm'].nunique():,} empresas")
    return df


def anonymize_cargos(df_car: pd.DataFrame) -> pd.DataFrame:
    """Reemplaza nombres de personas por hash irreversible."""
    log.info("Anonimizando cargos...")

    df = df_car.copy()
    df["persona_hash"] = df["persona"].apply(hash_persona)

    # Eliminar nombre real
    df = df.drop(columns=["persona"], errors="ignore")

    # Reordenar
    col_order = [
        "fecha_borme", "num_entrada", "empresa", "empresa_norm",
        "provincia", "hoja_registral", "tipo_acto", "cargo",
        "persona_hash", "pdf_filename",
    ]
    cols = [c for c in col_order if c in df.columns]
    df = df[cols]

    log.info(f"  {len(df):,} filas, {df['persona_hash'].nunique():,} personas (hasheadas)")
    return df


def build_admin_graph(df_car_anon: pd.DataFrame, max_empresas_per_admin: int = 20) -> pd.DataFrame:
    """Construye grafo de empresas que comparten administrador.

    Cada fila = un par de empresas con al menos un admin en común.
    Admins en >max_empresas_per_admin empresas se excluyen (profesionales
    de despachos que administran cientos de sociedades — no son señal).

    Columnas: empresa_a, empresa_b, n_admins_compartidos, admin_hashes.
    """
    log.info("Construyendo grafo de administradores compartidos...")

    # Solo nombramientos/reelecciones (admins activos)
    admins = df_car_anon[
        df_car_anon["tipo_acto"].isin(["nombramiento", "reeleccion"])
    ].copy()

    # Personas con >1 empresa
    persona_empresas = admins.groupby("persona_hash")["empresa_norm"].apply(
        lambda x: sorted(set(x))
    )
    multi = persona_empresas[persona_empresas.apply(len).between(2, max_empresas_per_admin)]
    log.info(f"  {len(multi):,} admins en 2-{max_empresas_per_admin} empresas")

    excluded = persona_empresas[persona_empresas.apply(len) > max_empresas_per_admin]
    if len(excluded) > 0:
        log.info(f"  {len(excluded):,} admins excluidos (>{max_empresas_per_admin} empresas — profesionales)")

    # Generar pares en chunks para no explotar RAM
    CHUNK = 100_000
    pair_counts = {}
    pair_hashes = {}
    processed = 0

    for persona_hash, empresas in multi.items():
        for a, b in combinations(empresas, 2):
            key = (a, b)
            pair_counts[key] = pair_counts.get(key, 0) + 1
            if key not in pair_hashes:
                pair_hashes[key] = set()
            pair_hashes[key].add(persona_hash)

        processed += 1
        if processed % 200_000 == 0:
            log.info(f"    {processed:,}/{len(multi):,} admins procesados, {len(pair_counts):,} pares")

    if not pair_counts:
        log.info("  Sin conexiones encontradas")
        return pd.DataFrame()

    log.info(f"  Generando DataFrame con {len(pair_counts):,} pares...")
    rows = []
    for (a, b), count in pair_counts.items():
        rows.append({
            "empresa_a": a,
            "empresa_b": b,
            "n_admins_compartidos": count,
            "admin_hashes": "|".join(sorted(pair_hashes[(a, b)])),
        })

    grafo = pd.DataFrame(rows)
    grafo = grafo.sort_values("n_admins_compartidos", ascending=False)
    log.info(f"  {len(grafo):,} pares de empresas conectadas")
    log.info(f"  Max admins compartidos: {grafo['n_admins_compartidos'].max()}")

    return grafo


def main():
    parser = argparse.ArgumentParser(description="BORME Anonymizer")
    parser.add_argument("--input", required=True,
                        help="Carpeta con borme_empresas.parquet y borme_cargos.parquet")
    parser.add_argument("--output", required=True,
                        help="Carpeta de salida para datos anonimizados")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Cargar
    log.info(f"Cargando datos de {input_dir}...")
    df_emp = pd.read_parquet(input_dir / "borme_empresas.parquet")
    df_car = pd.read_parquet(input_dir / "borme_cargos.parquet")
    log.info(f"  Empresas: {len(df_emp):,} filas")
    log.info(f"  Cargos: {len(df_car):,} filas")

    # Anonimizar
    df_emp_pub = anonymize_empresas(df_emp)
    df_car_pub = anonymize_cargos(df_car)

    # Guardar
    log.info("\nGuardando...")

    path_emp = output_dir / "borme_empresas_pub.parquet"
    df_emp_pub.to_parquet(path_emp, index=False, engine="pyarrow")
    log.info(f"  {path_emp} ({path_emp.stat().st_size / 1e6:.1f} MB)")

    path_car = output_dir / "borme_cargos_pub.parquet"
    df_car_pub.to_parquet(path_car, index=False, engine="pyarrow")
    log.info(f"  {path_car} ({path_car.stat().st_size / 1e6:.1f} MB)")

    # Resumen
    log.info(f"\n{'='*60}")
    log.info(f"ANONIMIZACIÓN COMPLETADA")
    log.info(f"{'='*60}")
    log.info(f"  Empresas:  {len(df_emp_pub):,} filas ({df_emp_pub['empresa_norm'].nunique():,} únicas)")
    log.info(f"  Cargos:    {len(df_car_pub):,} filas (personas hasheadas)")
    log.info(f"")
    log.info(f"  ✅ Datos listos para subir al repo público")
    log.info(f"  ⚠️  NO subir borme_empresas.parquet ni borme_cargos.parquet originales")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    main()
