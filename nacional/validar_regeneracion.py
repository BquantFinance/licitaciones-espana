"""Verifica la entrega completa: identidad, importes, versiones e indicadores."""
import argparse
import json
from pathlib import Path
import tempfile

import duckdb
import pyarrow.parquet as pq

from nacional.regenerar_historico import digest


def verificar(nacional, calidad, informe):
    informe=Path(informe)
    schema_n=pq.read_schema(nacional).names
    schema_q=pq.read_schema(calidad).names
    missing=set(schema_n)-set(schema_q)
    if missing:
        raise ValueError(f"Calidad perdió campos nacionales: {sorted(missing)}")
    indicators=[c for c in schema_q if c.startswith("INT-")]
    if len(indicators)!=20:
        raise ValueError(f"Se esperaban 20 indicadores, hay {len(indicators)}")
    informe.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(dir=informe.parent,prefix=".validar-") as tmp, duckdb.connect() as db:
        db.execute("SET threads=4")
        db.execute("SET memory_limit='24GB'")
        db.execute("SET temp_directory=?",[tmp])
        db.read_parquet(str(nacional)).create_view("n")
        db.read_parquet(str(calidad)).create_view("q")
        checks={}
        checks["filas_nacional"]=db.sql("SELECT count(*) FROM n").fetchone()[0]
        checks["filas_calidad"]=db.sql("SELECT count(*) FROM q").fetchone()[0]
        for view in ("n","q"):
            checks[f"claves_repetidas_{view}"]=db.sql(f"SELECT count(*) FROM (SELECT conjunto,archivo_origen,entrada_origen FROM {view} GROUP BY ALL HAVING count(*)<>1)").fetchone()[0]
        checks["fechas_updated_nulas"]=db.sql("SELECT count(*) FROM n WHERE fecha_updated IS NULL").fetchone()[0]
        checks["licitaciones_sin_una_ultima_version"]=db.sql("SELECT count(*) FROM (SELECT conjunto,id FROM n GROUP BY ALL HAVING count(*) FILTER(WHERE es_ultima_version)<>1)").fetchone()[0]
        checks["ultimas_marcadas_repetidas"]=db.sql("SELECT count(*) FROM n WHERE es_ultima_version AND entrada_repetida").fetchone()[0]
        checks["ultimas_no_recientes"]=db.sql("""SELECT count(*) FROM n JOIN
            (SELECT conjunto,id,max(fecha_updated) ultima FROM n GROUP BY ALL) m USING(conjunto,id)
            WHERE es_ultima_version AND fecha_updated IS DISTINCT FROM ultima""").fetchone()[0]
        checks["recuentos_versiones_incoherentes"]=db.sql("""SELECT count(*) FROM
            (SELECT conjunto,id FROM n GROUP BY ALL
             HAVING min(n_versiones)<>count(DISTINCT fecha_updated) OR max(n_versiones)<>count(DISTINCT fecha_updated))""").fetchone()[0]
        checks["marcas_repeticion_incoherentes"]=db.sql("""SELECT count(*) FROM
            (SELECT conjunto,id,fecha_updated FROM n GROUP BY ALL
             HAVING count(*) FILTER(WHERE entrada_repetida)<>count(*)-1)""").fetchone()[0]
        # Exact value comparisons, including all original fields and provenance.
        different=" OR ".join(f'n."{c}" IS DISTINCT FROM q."{c}"' for c in schema_n)
        checks["filas_con_campos_nacionales_modificados"]=db.sql(f"""SELECT count(*) FROM n FULL OUTER JOIN q
            USING(conjunto,archivo_origen,entrada_origen) WHERE n.id IS NULL OR q.id IS NULL OR ({different})""").fetchone()[0]
        passed=" + ".join(f'CASE WHEN "{c}" THEN 1 ELSE 0 END' for c in indicators)
        evaluated=" + ".join(f'CASE WHEN "{c}" IS NOT NULL THEN 1 ELSE 0 END' for c in indicators)
        checks["scores_incoherentes"]=db.sql(f"""SELECT count(*) FROM q WHERE score_calidad IS NULL
             OR score_calidad<0 OR score_calidad>100
             OR abs(score_calidad-100.0*({passed})/nullif(({evaluated}),0))>0.05000001""").fetchone()[0]
        indicator_summary=db.sql("SELECT "+", ".join(f'count("{c}") AS "{c}"' for c in indicators)+" FROM q").df().iloc[0].to_dict()
        report={"comprobaciones":checks,"campos_nacionales_comparados":len(schema_n),
                "indicadores_evaluados":{k:int(v) for k,v in indicator_summary.items()},
                "sha256_nacional":digest(nacional),"sha256_calidad":digest(calidad),"sha256_verificador":digest(__file__)}
        passed_all=checks["filas_nacional"]==checks["filas_calidad"] and all(v==0 for k,v in checks.items() if k not in ("filas_nacional","filas_calidad"))
        report["resultado"]="PASS" if passed_all else "FAIL"
        informe.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
        if not passed_all:
            raise ValueError(f"La entrega no supera la validación: {checks}")
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--nacional",type=Path,required=True)
    p.add_argument("--calidad",type=Path,required=True)
    p.add_argument("--informe",type=Path,required=True)
    args=p.parse_args()
    print(json.dumps(verificar(args.nacional,args.calidad,args.informe),ensure_ascii=False,indent=2))


if __name__=="__main__":
    main()
