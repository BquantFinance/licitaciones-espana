#!/usr/bin/env python3
"""
Convierte CSV de Andalucía (generado por ccaa_andalucia.py) a Parquet.
Uso: python ccaa_andalucia_parquet.py --subconjunto licitaciones|menores
  licitaciones -> lee licitaciones_all.csv (o licitaciones_std.csv), escribe licitaciones_andalucia.parquet
  menores      -> lee licitaciones_menores.csv, escribe licitaciones_menores.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Mismo directorio que el scraper (cwd = raíz del repo)
ETL_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ETL_ROOT / "ccaa_Andalucia"

# Subconjunto -> (archivo CSV, archivo parquet salida)
SUBCONJUNTO_FILES = {
    "licitaciones": (["licitaciones_all.csv", "licitaciones_std.csv"], "licitaciones_andalucia.parquet"),
    "menores": (["licitaciones_menores.csv"], "licitaciones_menores.parquet"),
}


def main():
    parser = argparse.ArgumentParser(description="Andalucía: CSV a Parquet por subconjunto")
    parser.add_argument("--subconjunto", type=str, required=True, choices=list(SUBCONJUNTO_FILES),
                        help="Subconjunto: licitaciones o menores")
    args = parser.parse_args()
    subconjunto = args.subconjunto
    csv_candidates, parquet_name = SUBCONJUNTO_FILES[subconjunto]

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = None
    for name in csv_candidates:
        p = DATA_DIR / name
        if p.exists():
            csv_path = p
            break
    if not csv_path:
        print(f"Error: no se encontró ninguno de {csv_candidates} en {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False, on_bad_lines="skip")
    # Asegurar columna natural_id para L0 (id_expediente)
    if "id_expediente" not in df.columns and len(df.columns):
        df["id_expediente"] = df.iloc[:, 0].astype(str) if "id_expediente" not in df.columns else df.get("id_expediente")

    out_path = DATA_DIR / parquet_name
    df.to_parquet(out_path, index=False, compression="snappy")
    print(f"OK: {len(df):,} filas -> {out_path}")


if __name__ == "__main__":
    main()
