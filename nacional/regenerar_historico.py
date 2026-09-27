"""Reconstrucción por ZIP, reanudable, usando una revisión explícita del parser.

No elimina entradas ni versiones. Cada fuente tiene un parquet, detalle JSON
en un parquet separado, registro de tombstones e informe verificable.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def cargar_parser(root):
    path = Path(root).resolve() / "nacional" / "licitaciones.py"
    sys.path.insert(0, str(path.parent.parent))
    spec = importlib.util.spec_from_file_location("parser_regeneracion", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


NUMEROS = {"valor_estimado_contrato", "importe_sin_iva", "importe_con_iva",
           "importe_adjudicacion", "importe_adj_con_iva", "num_ofertas", "duracion",
           "n_lotes", "n_resultados", "n_modificaciones"}
BOOLEANOS = {"es_pyme", "sara"}
FORMATO = 2  # microsegundos: conserva fechas originales fuera del rango ns


def tabla(filas, parser):
    df = pd.DataFrame(filas)
    for col in parser.FECHAS:
        if col in df:
            df[col] = parser.parsear_fechas(df[col])
    df["fecha_updated"] = parser.parsear_fecha_updated(df["fecha_updated"])
    if df["id"].isna().any() or df["fecha_updated"].isna().any():
        raise ValueError("Entrada sin id o fecha_updated válida en el XML original")
    df["ano"] = df["fecha_publicacion"].dt.year
    df = parser._normalizar_columnas(df)
    fields = []
    for col in df:
        if col == "fecha_updated":
            tipo = pa.timestamp("us", tz="UTC")
        elif col in parser.FECHAS:
            tipo = pa.timestamp("us")
        elif col in NUMEROS or col == "ano":
            df[col] = pd.to_numeric(df[col], errors="raise")
            tipo = pa.float64()
        elif col in BOOLEANOS:
            tipo = pa.bool_()
        elif col == "entrada_origen":
            tipo = pa.int64()
        else:
            df[col] = df[col].astype("string")
            tipo = pa.string()
        fields.append(pa.field(col, tipo))
    return pa.Table.from_pandas(df, schema=pa.schema(fields), preserve_index=False)


def procesar(item, fuentes, output, parser_root):
    conjunto, nombre = item["conjunto"], item["archivo_origen"]
    source = Path(fuentes) / conjunto / nombre
    dest = Path(output) / "particiones" / conjunto / Path(nombre).stem
    manifest = dest / "informe.json"
    parser_path = Path(parser_root) / "nacional" / "licitaciones.py"
    source_hash, parser_hash = digest(source), digest(parser_path)
    if manifest.exists():
        report = json.loads(manifest.read_text())
        if report.get("formato") == FORMATO and report["sha256_fuente"] == source_hash and report["sha256_parser"] == parser_hash:
            if all((dest / name).is_file() and digest(dest / name) == sha for name, sha in report["salidas"].items()):
                return report
        raise ValueError(f"Partición existente incompatible: {dest}; utiliza otra carpeta de salida")
    dest.mkdir(parents=True, exist_ok=True)
    parser = cargar_parser(parser_root)
    start = time.monotonic()
    report = {**item, "formato": FORMATO, "sha256_fuente": source_hash, "sha256_parser": parser_hash,
              "filas": 0, "borrados": 0, "atom": 0, "descartadas": {}, "estado": "en_proceso"}
    with tempfile.TemporaryDirectory(dir=dest, prefix=".reconstruccion-") as tmp:
        tmp = Path(tmp)
        writer = None
        detail_writer = None
        buffer, detalles = [], []

        def flush():
            nonlocal writer, detail_writer
            if not buffer:
                return
            t = tabla(buffer, parser)
            if writer is None:
                writer = pq.ParquetWriter(tmp / "nacional.parquet", t.schema, compression="zstd")
            writer.write_table(t)
            dt = pa.Table.from_pylist(detalles, schema=pa.schema([
                ("id", pa.string()), ("fecha_updated", pa.string()), ("conjunto", pa.string()),
                ("archivo_origen", pa.string()), ("entrada_origen", pa.int64()), ("detalle_json", pa.string())]))
            if detail_writer is None:
                detail_writer = pq.ParquetWriter(tmp / "detalle.parquet", dt.schema, compression="zstd")
            detail_writer.write_table(dt)
            report["filas"] += len(buffer)
            buffer.clear()
            detalles.clear()

        try:
            with zipfile.ZipFile(source) as z, (tmp / "borrados.jsonl").open("w") as borrados:
                for name in sorted(z.namelist()):
                    if not name.lower().endswith((".atom", ".xml")):
                        continue
                    report["atom"] += 1
                    with z.open(name) as stream:
                        context = ET.iterparse(stream, events=("start", "end"))
                        _, root = next(context)
                        for event, elem in context:
                            if event != "end":
                                continue
                            if elem.tag == parser.TAG_ENTRY:
                                row = parser.parsear_entry(elem, report["descartadas"])
                                if row is None:
                                    raise ValueError(f"Entrada no parseada en {source}/{name}: {report['descartadas']}")
                                row.update(conjunto=conjunto, archivo_origen=nombre,
                                           entrada_origen=report["filas"] + len(buffer))
                                detail = {key: row.pop(key) for key in list(row) if key.startswith("_")}
                                detalles.append({key: row[key] for key in ("id", "fecha_updated", "conjunto", "archivo_origen", "entrada_origen")}
                                                | {"detalle_json": json.dumps(detail, ensure_ascii=False, separators=(",", ":"))})
                                buffer.append(row)
                                if len(buffer) >= 10000:
                                    flush()
                                elem.clear()
                                root.clear()
                            elif elem.tag == parser.TAG_BORRADO:
                                borrados.write(json.dumps(parser.parsear_borrado(elem) | {"conjunto": conjunto, "archivo_origen": nombre}, ensure_ascii=False)+"\n")
                                report["borrados"] += 1
                                elem.clear()
                                root.clear()
            flush()
        finally:
            if writer is not None:
                writer.close()
            if detail_writer is not None:
                detail_writer.close()
        if not report["atom"]:
            raise ValueError(f"ZIP sin ATOM/XML: {source}")
        report.update(estado="ok", segundos=round(time.monotonic()-start, 2))
        report["salidas"] = {p.name: digest(p) for p in tmp.iterdir() if p.is_file()}
        for file in tmp.iterdir():
            file.replace(dest / file.name)
        temp = dest / "informe.json.tmp"
        temp.write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
        temp.replace(manifest)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventario", type=Path, required=True)
    p.add_argument("--fuentes", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--parser-root", type=Path, required=True)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--esperar-descargas", action="store_true")
    args = p.parse_args()
    pending = json.loads(args.inventario.read_text())
    reports, errors = [], []
    args.output.mkdir(parents=True, exist_ok=True)
    while pending:
        available = [i for i in pending if (args.fuentes / i["conjunto"] / i["archivo_origen"]).exists()]
        if not available:
            download_report = args.fuentes / "descargas.json"
            if not args.esperar_descargas or (download_report.exists() and len(json.loads(download_report.read_text())) >= len(pending)+len(reports)+len(errors)):
                errors.extend({**i, "error": "fuente no disponible"} for i in pending)
                break
            time.sleep(5)
            continue
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(procesar, i, args.fuentes, args.output, args.parser_root): i for i in available}
            for future in as_completed(futures):
                item = futures[future]
                pending.remove(item)
                try:
                    report = future.result()
                    reports.append(report)
                    print(f"[{len(reports)}] {item['archivo_origen']}: {report['filas']:,} filas, {report['segundos']} s", flush=True)
                except Exception as exc:
                    errors.append({**item, "error": f"{type(exc).__name__}: {exc}"})
                    print(f"ERROR {item['archivo_origen']}: {exc}", flush=True)
                (args.output / "estado.json").write_text(json.dumps({"completadas": reports, "errores": errors, "pendientes": pending}, ensure_ascii=False, indent=2)+"\n")
    (args.output / "estado.json").write_text(json.dumps({"completadas": reports, "errores": errors, "pendientes": pending}, ensure_ascii=False, indent=2)+"\n")
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
