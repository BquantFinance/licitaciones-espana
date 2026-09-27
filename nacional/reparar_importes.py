"""Recupera BudgetAmount de los ATOM originales sin cambiar filas ni versiones.

Las fuentes deben estar en <fuentes>/<conjunto>/**/*.atom (o .xml/.zip).
No descarga datos, no infiere IVA y no acepta versiones históricas sin fuente.
"""

import argparse
import hashlib
import json
import math
import sqlite3
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from nacional.licitaciones import CONJUNTOS, NS, safe_text


IMPORTES = {
    "valor_estimado_contrato": "EstimatedOverallContractAmount",
    "importe_sin_iva": "TaxExclusiveAmount",
    "importe_con_iva": "TotalAmount",
}
CLAVES = ["conjunto", "id", "fecha_updated"]


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def clave(conjunto, identificador, fecha):
    if any(pd.isna(v) or not str(v).strip() for v in (conjunto, identificador, fecha)):
        raise ValueError("Falta conjunto, id o fecha_updated; no se puede identificar la versión")
    fecha = pd.Timestamp(fecha)
    if fecha.tzinfo is None:
        raise ValueError("fecha_updated debe incluir zona horaria")
    return str(conjunto), str(identificador), fecha.tz_convert("UTC").isoformat()


def entradas(stream):
    context = ET.iterparse(stream, events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == f"{{{NS['atom']}}}entry":
            yield elem
            elem.clear()
            root.clear()


def indexar_stream(db, stream, conjunto):
    count = 0
    for entry in entradas(stream):
        key = clave(conjunto, safe_text(entry, "atom:id"), safe_text(entry, "atom:updated"))
        status = entry.find("cac-place-ext:ContractFolderStatus", NS)
        consulta = entry.find("cac-place-ext:PreliminaryMarketConsultationStatus", NS)
        if status is None and (conjunto != "consultas" or consulta is None):
            raise ValueError(f"Entrada sin ContractFolderStatus: {key}")
        budget = status.find("cac:ProcurementProject/cac:BudgetAmount", NS) if status is not None else None
        values = []
        for tag in IMPORTES.values():
            text = safe_text(budget, f"cbc:{tag}")
            value = float(text) if text is not None else None
            if value is not None and not math.isfinite(value):
                raise ValueError(f"Importe no finito en {key}: {tag}")
            values.append(value)
        previous = db.execute("SELECT vec, neto, total FROM presupuestos WHERE conjunto=? AND id=? AND fecha=?", key).fetchone()
        if previous is not None and previous != tuple(values):
            raise ValueError(f"Importes contradictorios para la misma versión: {key}")
        db.execute("INSERT OR IGNORE INTO presupuestos VALUES (?, ?, ?, ?, ?, ?)", (*key, *values))
        count += 1
    return count


def indexar(db, fuentes):
    db.execute("CREATE TABLE presupuestos (conjunto TEXT, id TEXT, fecha TEXT, vec REAL, neto REAL, total REAL, PRIMARY KEY (conjunto, id, fecha)) WITHOUT ROWID")
    sources = []
    for conjunto in CONJUNTOS:
        for path in sorted((Path(fuentes) / conjunto).rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".atom", ".xml", ".zip"}:
                continue
            count = 0
            if path.suffix.lower() == ".zip":
                with zipfile.ZipFile(path) as archive:
                    for name in sorted(archive.namelist()):
                        if Path(name).suffix.lower() in {".atom", ".xml"}:
                            with archive.open(name) as stream:
                                count += indexar_stream(db, stream, conjunto)
            else:
                with path.open("rb") as stream:
                    count = indexar_stream(db, stream, conjunto)
            db.commit()
            sources.append({"archivo": str(path.resolve()), "sha256": sha256(path), "entradas": count})
    if not sources or not db.execute("SELECT COUNT(*) FROM presupuestos").fetchone()[0]:
        raise ValueError("No hay entradas ATOM en las carpetas de conjuntos de la fuente")
    return sources


def reparar_importes(entrada, fuentes, salida):
    entrada, salida = Path(entrada), Path(salida)
    informe = salida.with_suffix(".json")
    if entrada.resolve() == salida.resolve() or salida.exists() or informe.exists():
        raise ValueError("La salida y su informe deben ser archivos nuevos")
    pf = pq.ParquetFile(entrada)
    schema = pf.schema_arrow
    if set(CLAVES + ["importe_sin_iva", "importe_con_iva"]) - set(schema.names):
        raise ValueError("La entrada debe ser un parquet nacional con claves e importes")
    if "score_calidad" in schema.names or any(c.startswith("INT-") for c in schema.names):
        raise ValueError("Usa el parquet nacional y recalcula calidad después; no reutilices indicadores antiguos")
    if not pf.metadata.num_rows:
        raise ValueError("El parquet nacional está vacío")
    missing = dict.fromkeys(CLAVES, 0)
    for batch in pf.iter_batches(columns=CLAVES):
        for col in CLAVES:
            missing[col] += batch.column(col).null_count
    if any(missing.values()):
        raise ValueError(f"Claves nulas en el parquet: {missing}. No se puede identificar cada versión; no se omiten filas")
    # Solo cambian los dos campos afectados por #6. TotalAmount se verifica.
    for col in ("valor_estimado_contrato", "importe_sin_iva"):
        field = pa.field(col, pa.float64())
        pos = schema.get_field_index(col)
        schema = schema.set(pos, field) if pos >= 0 else schema.append(field)
    metadata = dict(schema.metadata or {})
    # El esquema pandas original ya no describe las columnas corregidas.
    metadata.pop(b"pandas", None)
    metadata[b"budget_mapping"] = b"EstimatedOverallContractAmount=valor_estimado_contrato;TaxExclusiveAmount=importe_sin_iva;TotalAmount=importe_con_iva"
    metadata[b"budget_source"] = b"atom_exact_version"
    schema = schema.with_metadata(metadata)
    salida.parent.mkdir(parents=True, exist_ok=True)
    rows = changed = 0
    present = dict.fromkeys(IMPORTES, 0)
    with tempfile.TemporaryDirectory(dir=salida.parent, prefix=".budget-") as tmp:
        tmp = Path(tmp)
        with sqlite3.connect(tmp / "presupuestos.sqlite3") as db:
            sources = indexar(db, fuentes)
            with pq.ParquetWriter(tmp / "resultado.parquet", schema, compression="snappy") as writer:
                for batch in pf.iter_batches(batch_size=50_000):
                    table = pa.Table.from_batches([batch])
                    keys = table.select(CLAVES).to_pydict()
                    old = table["importe_sin_iva"].to_pylist()
                    totals = table["importe_con_iva"].to_pylist()
                    values = []
                    for i, raw_key in enumerate(zip(*(keys[c] for c in CLAVES))):
                        key = clave(*raw_key)
                        budget = db.execute("SELECT vec, neto, total FROM presupuestos WHERE conjunto=? AND id=? AND fecha=?", key).fetchone()
                        if budget is None:
                            raise ValueError(f"No hay ATOM para la versión exacta: {key}. No se publica una reparación parcial")
                        current_total = None if pd.isna(totals[i]) else float(totals[i])
                        if current_total != budget[2]:
                            raise ValueError(f"TotalAmount difiere del parquet original: {key}")
                        old_net = None if pd.isna(old[i]) else float(old[i])
                        changed += old_net != budget[1]
                        values.append(budget)
                        for col, value in zip(IMPORTES, budget):
                            present[col] += value is not None
                    for pos, col in enumerate(("valor_estimado_contrato", "importe_sin_iva")):
                        array = pa.array([v[pos] for v in values], type=pa.float64())
                        index = table.schema.get_field_index(col)
                        table = table.set_column(index, col, array) if index >= 0 else table.append_column(col, array)
                    table = table.select(schema.names).cast(schema)
                    writer.write_table(table)
                    rows += table.num_rows
            report = {
                "entrada": str(entrada.resolve()), "sha256_entrada": sha256(entrada),
                "salida": str(salida.resolve()), "sha256_salida": sha256(tmp / "resultado.parquet"),
                "filas_entrada": pf.metadata.num_rows, "filas_salida": rows,
                "filas_con_atom_exacto": rows, "importe_sin_iva_modificado": changed,
                "importes_informados": present, "mapeo": IMPORTES, "fuentes": sources,
                "alcance": "Solo importes: filas, orden, versiones y demás campos conservados; calidad pendiente de recalcular",
            }
            (tmp / "informe.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
            (tmp / "resultado.parquet").replace(salida)
            (tmp / "informe.json").replace(informe)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--fuentes", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        report = reparar_importes(args.input, args.fuentes, args.output)
    except (ValueError, ET.ParseError, zipfile.BadZipFile) as exc:
        parser.exit(1, f"Error: {exc}\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
