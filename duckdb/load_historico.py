#!/usr/bin/env python3
"""Carga el histórico de licitaciones en DuckDB en minutos.

Lee los parquet liberados (nacional, CCAA, TED), los materializa como
tablas por fuente y genera una vista unificada lista para consultar. Sin
normalización: los esquemas originales de cada fuente se conservan (el
consumidor deduplica según su caso — ver qa/verify_datasets.py para la
guía de inflación multi-registro).

En producción (Elicita) este patrón carga 71M+ filas en minutos y alimenta
perfilado por CIF, análisis sectorial y buscador.

Requires: pip install duckdb
Uso:
    python duckdb/load_historico.py --raiz /ruta/a/los/datos [--salida licitaciones.duckdb]

Las fuentes no descargadas se omiten con un aviso.
"""
import argparse

import duckdb

FUENTES = {
    "calidad":   "calidad/**/*.parquet",
    "nacional":  "nacional/**/*.parquet",
    "ted":       "ted/**/*.parquet",
    "andalucia": "andalucia/**/*.parquet",
    "asturias":  "asturias/**/*.parquet",
    "catalunya": "catalunya/**/*.parquet",
    "euskadi":   "euskadi/**/*.parquet",
    "madrid_cm": "comunidad_madrid/**/*.parquet",
    "valencia":  "valencia/**/*.parquet",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raiz", required=True, help="directorio con los datos descargados")
    ap.add_argument("--salida", default="licitaciones.duckdb", help="fichero DuckDB de salida")
    args = ap.parse_args()

    con = duckdb.connect(args.salida)
    cargadas = []
    for fuente, patron in FUENTES.items():
        try:
            con.execute(
                f"CREATE OR REPLACE TABLE {fuente} AS "
                f"SELECT * FROM read_parquet('{args.raiz}/{patron}', union_by_name=true)"
            )
            n = con.execute(f"SELECT count(*) FROM {fuente}").fetchone()[0]
            print(f"  {fuente}: {n:,} filas")
            cargadas.append(fuente)
        except Exception as e:
            print(f"  {fuente}: omitida ({str(e)[:70]})")

    if not cargadas:
        raise SystemExit("ninguna fuente cargada — revisa --raiz")

    # Vista unificada por nombre (las columnas que falten quedan a NULL)
    uniones = "\n  UNION ALL BY NAME\n".join(
        f"SELECT '{f}' AS fuente, * FROM {f}" for f in cargadas
    )
    con.execute(f"CREATE OR REPLACE VIEW contratos AS {uniones}")

    total = con.execute("SELECT count(*) FROM contratos").fetchone()[0]
    print(f"\n{total:,} filas en la vista `contratos` ({', '.join(cargadas)})")
    print(f"base de datos: {args.salida}")


if __name__ == "__main__":
    main()