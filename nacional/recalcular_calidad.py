"""Ejecuta TED corregido y calidad sobre el nacional reconstruido.

El resultado TED se une por id y versión, nunca solo por expediente/adjudicatario.
Las versiones históricas no evaluadas en TED quedan como no evaluadas.
"""
import argparse
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

from nacional.regenerar_historico import digest


def cargar(root, relative, name):
    sys.path.insert(0,str(Path(root).resolve()))
    spec=importlib.util.spec_from_file_location(name,Path(root)/relative)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cons20_por_version(df, path):
    ted=pd.read_parquet(path,columns=["id","fecha_updated","_ted_validated"])
    if ted.duplicated(["id","fecha_updated"]).any():
        raise ValueError("El cruce TED contiene más de un resultado por versión")
    keys=df[["id","fecha_updated"]].copy()
    keys["_pos"]=range(len(df))
    joined=keys.merge(ted,on=["id","fecha_updated"],how="left",validate="many_to_one",sort=False)
    joined=joined.sort_values("_pos")
    return pd.Series(joined["_ted_validated"].array,index=df.index,dtype="boolean")


def run(args):
    args.output.mkdir(parents=True,exist_ok=True)
    path_ted=args.output/"ted"
    path_ted.mkdir(exist_ok=True)
    matching=path_ted/"validacion_por_version.parquet"
    marker=path_ted/"informe.json"
    provenance={"sha256_nacional":digest(args.nacional),"sha256_ted":digest(args.ted),
                "sha256_parser":digest(args.parser_root/"nacional/licitaciones.py"),
                "sha256_pipeline_ted":digest(args.parser_root/"ted/run_ted_crossvalidation.py")}
    if not marker.exists():
        ted=cargar(args.parser_root,"ted/run_ted_crossvalidation.py","ted_regeneracion")
        ted.OUTPUT_DIR=path_ted
        df=ted.load_placsp(args.nacional)
        source=ted.load_ted(args.ted)
        matched,data,n1,n2,n2b,e2b,consumed=ted.run_e1_e2(df,source)
        advanced=ted.run_advanced_matching(df,source,matched,data,consumed)
        df,missing,hc=ted.apply_results_and_report(df,matched,data,n1,n2,n2b,e2b,advanced)
        ted.save_outputs(df,missing,hc)
        selected=df.loc[df["_es_sara"],["id","fecha_updated","_ted_validated"]]
        selected.to_parquet(matching,index=False,compression="zstd")
        marker.write_text(json.dumps(provenance|{"sara_evaluados":len(selected),"sha256_resultado":digest(matching)},indent=2)+"\n")
        del df,source,missing,hc,selected,matched,data,advanced
    else:
        report=json.loads(marker.read_text())
        if any(report.get(k)!=v for k,v in provenance.items()) or digest(matching)!=report["sha256_resultado"]:
            raise ValueError("El cruce TED existente pertenece a otras entradas o revisión de código")
    quality=cargar(args.parser_root,"calidad/calidad_licitaciones.py","calidad_regeneracion")
    quality.calcular_cons20=cons20_por_version
    expected=(args.parser_root/"nacional/licitaciones.py").resolve()
    if Path(quality.leer_placsp.__code__.co_filename).resolve()!=expected:
        raise ValueError("Se ha cargado una revisión distinta del lector nacional")
    result=args.output/"calidad"/"calidad_licitaciones_resultado.parquet"
    if result.exists():
        raise ValueError("El resultado de calidad ya existe; no se sobrescribe")
    quality.run(SimpleNamespace(input=args.nacional,output=result.parent,sample=None,
                                ted=matching,borme=args.borme,solo_ultima_version=False))
    report=provenance|{"sha256_borme":digest(args.borme),"sha256_calidad":digest(result),
                       "sha256_pipeline_calidad":digest(args.parser_root/"calidad/calidad_licitaciones.py"),
                       "sha256_driver":digest(__file__),"union_ted":"id + fecha_updated; solo versiones evaluadas"}
    (args.output/"calidad"/"informe.json").write_text(json.dumps(report,indent=2)+"\n")


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--nacional",type=Path,required=True)
    p.add_argument("--ted",type=Path,required=True)
    p.add_argument("--borme",type=Path,required=True)
    p.add_argument("--parser-root",type=Path,required=True)
    p.add_argument("--output",type=Path,required=True)
    run(p.parse_args())


if __name__=="__main__":
    main()
