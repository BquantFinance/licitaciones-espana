"""Consolida particiones completas, marca versiones y compara con el publicado."""
import argparse
import json
from pathlib import Path
import tempfile

import duckdb

from nacional.regenerar_historico import digest


def consolidar(inventario, particiones, original, output):
    output = Path(output)
    if output.exists():
        raise ValueError("El parquet de salida debe ser nuevo")
    files, reports = [], []
    for item in json.loads(Path(inventario).read_text()):
        folder = Path(particiones) / "particiones" / item["conjunto"] / Path(item["archivo_origen"]).stem
        report = json.loads((folder / "informe.json").read_text())
        if report["estado"] != "ok" or report["descartadas"]:
            raise ValueError(f"Partición incompleta: {folder}")
        reports.append(report)
        if report["filas"]:
            file = folder / "nacional.parquet"
            if digest(file) != report["salidas"][file.name]:
                raise ValueError(f"Hash no coincide: {file}")
            files.append(str(file.resolve()))
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=output.parent, prefix=".consolidar-") as tmp:
        with duckdb.connect() as db:
            db.execute("SET memory_limit='24GB'")
            db.execute("SET threads=4")
            db.execute("SET temp_directory=?", [tmp])
            db.read_parquet(files, union_by_name=True, hive_partitioning=False).create_view("raw")
            db.read_parquet(str(original)).create_view("anterior")
            count = db.sql("SELECT count(*) FROM raw").fetchone()[0]
            if count != sum(r["filas"] for r in reports):
                raise ValueError("El recuento de particiones no coincide")
            db.execute("""CREATE VIEW nuevo AS SELECT *,
                count(DISTINCT fecha_updated) OVER (PARTITION BY conjunto, id) AS n_versiones,
                row_number() OVER (PARTITION BY conjunto, id ORDER BY fecha_updated DESC, archivo_origen DESC, entrada_origen DESC)=1 AS es_ultima_version,
                row_number() OVER (PARTITION BY conjunto, id, fecha_updated ORDER BY archivo_origen, entrada_origen)>1 AS entrada_repetida
                FROM raw""")
            dest = str(Path(tmp) / "nacional.parquet")
            db.execute("COPY nuevo TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)", [dest])
            db.read_parquet(dest).create_view("resultado")
            summary = db.sql("""SELECT count(*) filas, count(*) FILTER(WHERE fecha_updated IS NULL) fechas_updated_nulas,
                sum(CASE WHEN es_ultima_version THEN 1 ELSE 0 END) licitaciones_distintas,
                sum(CASE WHEN entrada_repetida THEN 1 ELSE 0 END) entradas_repetidas,
                count(importe_sin_iva) importe_sin_iva_informado,
                count(valor_estimado_contrato) valor_estimado_informado
                FROM resultado""").df().iloc[0].to_dict()
            coverage = db.sql("""WITH a AS (SELECT conjunto, archivo_origen, count(*) filas_antes FROM anterior GROUP BY ALL),
                 b AS (SELECT conjunto, archivo_origen, count(*) filas_despues FROM resultado GROUP BY ALL)
                 SELECT coalesce(a.conjunto,b.conjunto) conjunto, coalesce(a.archivo_origen,b.archivo_origen) archivo_origen,
                    coalesce(filas_antes,0) filas_antes, coalesce(filas_despues,0) filas_despues,
                    coalesce(filas_despues,0)-coalesce(filas_antes,0) diferencia
                 FROM a FULL OUTER JOIN b USING(conjunto,archivo_origen) ORDER BY 1,2""").df().to_dict("records")
            # Compare version sets rather than a many-to-many join over repeated entries.
            keys = db.sql("""WITH a AS (SELECT DISTINCT conjunto,id,fecha_updated FROM anterior WHERE fecha_updated IS NOT NULL),
                b AS (SELECT DISTINCT conjunto,id,fecha_updated FROM resultado)
                SELECT count(*) FILTER(WHERE a.id IS NOT NULL AND b.id IS NOT NULL) versiones_comunes,
                       count(*) FILTER(WHERE a.id IS NOT NULL AND b.id IS NULL) versiones_solo_anterior,
                       count(*) FILTER(WHERE a.id IS NULL AND b.id IS NOT NULL) versiones_solo_nuevo
                FROM a FULL OUTER JOIN b USING(conjunto,id,fecha_updated)""").df().iloc[0].to_dict()
            nulls = db.sql("SELECT count(*) FROM anterior WHERE fecha_updated IS NULL").fetchone()[0]
            # Monetary comparison is bounded to unique version+archive records.
            amounts = db.sql("""WITH a AS (SELECT *, count(*) OVER(PARTITION BY conjunto,id,fecha_updated,archivo_origen) n FROM anterior),
                b AS (SELECT *, count(*) OVER(PARTITION BY conjunto,id,fecha_updated,archivo_origen) n FROM resultado)
                SELECT count(*) filas_comparables,
                    count(*) FILTER(WHERE a.importe_sin_iva IS DISTINCT FROM b.importe_sin_iva) netos_modificados,
                    count(*) FILTER(WHERE a.importe_sin_iva IS DISTINCT FROM b.valor_estimado_contrato) valor_estimado_difiere_del_antiguo_sin_iva,
                    count(*) FILTER(WHERE a.importe_con_iva IS DISTINCT FROM b.importe_con_iva) totales_modificados
                FROM a JOIN b USING(conjunto,id,fecha_updated,archivo_origen) WHERE a.n=1 AND b.n=1""").df().iloc[0].to_dict()
            report = {"resumen": {k:int(v) for k,v in summary.items()}, "cobertura_archivos": coverage,
                      "versiones": {k:int(v) for k,v in keys.items()}, "importes_versiones_unicas": {k:int(v) for k,v in amounts.items()},
                      "fechas_updated_nulas_anterior": nulls, "sha256_original": digest(original),
                      "sha256_nacional": digest(dest), "fuentes": reports}
            Path(dest).replace(output)
            output.with_suffix(".json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventario", type=Path, required=True)
    p.add_argument("--particiones", type=Path, required=True)
    p.add_argument("--original", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    report = consolidar(args.inventario,args.particiones,args.original,args.output)
    print(json.dumps({k:v for k,v in report.items() if k not in ("fuentes","cobertura_archivos")},ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
