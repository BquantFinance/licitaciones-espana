#!/usr/bin/env python3
"""
Convierte el CSV unificado de contratación Comunidad de Madrid a Parquet.
Entrada: comunidad_madrid/contratacion_comunidad_madrid_completo.csv
Salida: comunidad_madrid/contratacion_comunidad_madrid_completo.parquet
Ejecutar con cwd = raíz del repo.
"""

import sys
from pathlib import Path

import pandas as pd

DIR = Path(__file__).resolve().parent
CSV_PATH = DIR / "contratacion_comunidad_madrid_completo.csv"
PARQUET_PATH = DIR / "contratacion_comunidad_madrid_completo.parquet"


def main() -> int:
    if not CSV_PATH.exists():
        print(f"Error: no existe {CSV_PATH}. Ejecute antes: descarga_contratacion_comunidad_madrid_v1.py todo y unificar", file=sys.stderr)
        return 1
    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig", low_memory=False, on_bad_lines="skip")
    df.to_parquet(PARQUET_PATH, index=False, compression="snappy")
    print(f"OK: {len(df):,} filas -> {PARQUET_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
