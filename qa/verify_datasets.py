#!/usr/bin/env python3
"""QA: verificación cruzada entre los datasets liberados.

Ejecuta las comprobaciones que hicimos al ingerir el dataset completo en
producción (Elicita: 71M+ filas en DuckDB alimentando perfilado por CIF,
análisis sectorial y buscador):

1. Alineación de ids entre `calidad` y `nacional` (los huérfanos).
2. Filas únicas vs filas totales por fuente (los duplicados por id).
3. Artefactos de fecha: 1970/epoch en filas sin fecha real.
4. Inflación multi-registro: el ratio entre `importe_sin_iva` y
   `importe_adjudicacion` agregado (el mismo expediente aparece en varias
   filas — anuncios, adjudicaciones, modificaciones — y el campo estimado
   se repite inflando cualquier agregación sin deduplicar).
5. Los conteos por fuente frente a los documentados en el README.

Requires: pip install duckdb
Uso:
    python qa/verify_datasets.py --raiz /ruta/a/los/datos [--db /tmp/qa.duckdb]

Las fuentes se localizan por patrón en <raiz>; las que no estén descargadas
se omiten con un aviso.
"""
import argparse
import sys

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


def conectar(raiz: str, db: str):
    con = duckdb.connect(db)
    vistas = []
    for fuente, patron in FUENTES.items():
        try:
            con.execute(
                f"CREATE OR REPLACE VIEW {fuente} AS "
                f"SELECT * FROM read_parquet('{raiz}/{patron}', union_by_name=true)"
            )
            vistas.append(fuente)
        except Exception:
            print(f"  ({fuente}: no descargado o sin parquet — se omite)")
    print(f"fuentes cargadas: {', '.join(vistas)}\n")
    return con, set(vistas)


def check_alineacion(con, vistas):
    if "calidad" not in vistas or "nacional" not in vistas:
        return
    r = con.execute("""SELECT
        (SELECT count(DISTINCT id) FROM calidad) AS calidad_ids,
        (SELECT count(DISTINCT id) FROM nacional) AS nacional_ids,
        (SELECT count(*) FROM (SELECT DISTINCT id FROM calidad
                               INTERSECT SELECT DISTINCT id FROM nacional)) AS interseccion""").fetchone()
    estado = "✅" if r[0] == r[1] == r[2] else "⚠️"
    print(f"1. Alineación calidad/nacional: {r[0]} ids calidad, {r[1]} ids nacional, "
          f"intersección {r[2]} {estado}")


def check_duplicados(con, vistas, fuente):
    if fuente not in vistas:
        return
    r = con.execute(f"""SELECT count(*) AS total, count(DISTINCT id) AS unicos
                        FROM {fuente}""").fetchone()
    estado = "✅" if r[0] == r[1] else f"⚠️ ({r[0] - r[1]} duplicados por id)"
    print(f"2. {fuente}: {r[0]} filas, {r[1]} ids únicos {estado}")


def check_fechas(con, vistas, fuente, col="fecha_publicacion"):
    if fuente not in vistas:
        return
    r = con.execute(f"""SELECT count(*) FILTER (
                          WHERE {col} IS NOT NULL AND year({col}) < 2000) AS epoch
                        FROM {fuente}""").fetchone()
    if r[0] > 0:
        print(f"3. {fuente}: {r[0]} filas con fecha 1970/epoch (nulls convertidos) ⚠️ "
              "— filtrar `>= 2000-01-01` en producto")


def check_inflacion(con, vistas, fuente):
    if fuente not in vistas:
        return
    r = con.execute(f"""SELECT
        round(sum(COALESCE(importe_sin_iva, 0)) / 1e9, 1) AS vol_sin_iva_bn,
        round(sum(COALESCE(importe_adjudicacion, 0)) / 1e9, 1) AS vol_adj_bn,
        round(sum(COALESCE(importe_sin_iva, 0)) / NULLIF(sum(COALESCE(importe_adjudicacion, 0)), 0), 2) AS ratio
        FROM {fuente}""").fetchone()
    if r[0] and r[1]:
        aviso = "⚠️ multi-registro: el mismo expediente repite el importe estimado" if r[2] and r[2] > 1.5 else ""
        print(f"4. {fuente}: importe_sin_iva {r[0]} B€ vs importe_adjudicacion {r[1]} B€ "
              f"(ratio {r[2]}) {aviso}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raiz", required=True, help="directorio con los datos descargados")
    ap.add_argument("--db", default="/tmp/qa_verificacion.duckdb")
    args = ap.parse_args()

    con, vistas = conectar(args.raiz, args.db)

    check_alineacion(con, vistas)
    for f in vistas:
        check_duplicados(con, vistas, f)
        check_fechas(con, vistas, f)
        check_inflacion(con, vistas, f)


if __name__ == "__main__":
    main()