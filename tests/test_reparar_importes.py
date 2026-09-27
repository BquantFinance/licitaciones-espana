import json
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from calidad.calidad_licitaciones import run
from nacional.licitaciones import NS, parsear_entry
from nacional.reparar_importes import reparar_importes, sha256


FIXTURE = Path(__file__).parent / "fixtures" / "entry_budget.xml"


def entry(date="2026-03-25T10:00:00+01:00", net="4685950.41"):
    root = ET.parse(FIXTURE).getroot()
    root.find("atom:updated", NS).text = date
    budget = root.find(".//cac:BudgetAmount", NS)
    if net is None:
        budget.remove(budget.find("cbc:TaxExclusiveAmount", NS))
    else:
        budget.find("cbc:TaxExclusiveAmount", NS).text = net
    return root


def legacy(entries):
    rows = []
    for item in entries:
        row = parsear_entry(item)
        row["importe_sin_iva"] = row.pop("valor_estimado_contrato")
        row["conjunto"] = "licitaciones"
        rows.append(row)
    df = pd.DataFrame(rows)
    df["fecha_updated"] = pd.to_datetime(df["fecha_updated"], utc=True)
    return df


def setup(tmp_path, entries=None):
    entries = entries or [entry()]
    sources = tmp_path / "fuentes"
    directory = sources / "licitaciones"
    directory.mkdir(parents=True)
    feed = ET.Element(f"{{{NS['atom']}}}feed")
    feed.extend(entries)
    ET.ElementTree(feed).write(directory / "feed.atom", encoding="utf-8")
    old = tmp_path / "old.parquet"
    legacy(entries).to_parquet(old, index=False)
    return old, sources, tmp_path / "new.parquet"


def test_preserves_rows_order_duplicates_types_and_exact_versions(tmp_path):
    # Two versions of one id; duplicate source and parquet rows are retained.
    old, sources, new = setup(tmp_path, [entry(), entry("2026-03-26T10:00:00+01:00", "12"), entry()])
    before = pq.read_table(old)
    report = reparar_importes(old, sources, new)
    after = pq.read_table(new)
    assert after.num_rows == before.num_rows == report["filas_con_atom_exacto"] == 3
    assert after["importe_sin_iva"].to_pylist() == [4685950.41, 12, 4685950.41]
    assert after["valor_estimado_contrato"].to_pylist() == [7809917.35] * 3
    for col in before.column_names:
        if col != "importe_sin_iva":
            assert after[col].equals(before[col]), col
    assert report["sha256_entrada"] == sha256(old)
    assert report["sha256_salida"] == sha256(new)
    assert json.loads(new.with_suffix(".json").read_text()) == report


def test_zip_source_and_missing_tax_exclusive_stays_null(tmp_path):
    old, sources, new = setup(tmp_path, [entry(net=None)])
    atom = sources / "licitaciones" / "feed.atom"
    with zipfile.ZipFile(atom.with_suffix(".zip"), "w") as archive:
        archive.write(atom, "nested/feed.atom")
    atom.unlink()
    reparar_importes(old, sources, new)
    result = pd.read_parquet(new)
    assert pd.isna(result.iloc[0].importe_sin_iva)
    assert result.iloc[0].valor_estimado_contrato == 7809917.35


@pytest.mark.parametrize("failure", ["missing_version", "wrong_total", "conflict", "malformed", "nonfinite", "no_sources"])
def test_incomplete_or_inconsistent_repair_does_not_publish(tmp_path, failure):
    old, sources, new = setup(tmp_path)
    atom = sources / "licitaciones" / "feed.atom"
    if failure == "missing_version":
        df = pd.read_parquet(old)
        df["fecha_updated"] += pd.Timedelta(days=1)
        df.to_parquet(old)
    elif failure == "wrong_total":
        df = pd.read_parquet(old)
        df["importe_con_iva"] = 123.
        df.to_parquet(old)
    elif failure == "conflict":
        ET.ElementTree(entry(net="1")).write(atom.with_name("conflict.xml"))
    elif failure == "malformed":
        with atom.open("a") as stream:
            stream.write("<broken>")
    elif failure == "nonfinite":
        ET.ElementTree(entry(net="NaN")).write(atom)
    else:
        atom.unlink()
    with pytest.raises((ValueError, ET.ParseError)):
        reparar_importes(old, sources, new)
    assert not new.exists()
    assert not new.with_suffix(".json").exists()
    assert not list(tmp_path.glob(".budget-*"))


def test_output_cannot_overwrite_source_or_existing_result(tmp_path):
    old, sources, new = setup(tmp_path)
    original = old.read_bytes()
    with pytest.raises(ValueError):
        reparar_importes(old, sources, old)
    new.write_bytes(b"keep me")
    with pytest.raises(ValueError):
        reparar_importes(old, sources, new)
    assert old.read_bytes() == original
    assert new.read_bytes() == b"keep me"


def test_quality_recalculated_from_recovered_budget(tmp_path):
    old, sources, new = setup(tmp_path)
    df = pd.read_parquet(old)
    # Award exceeds the net budget but not the (wrongly mapped) estimated value.
    df["importe_adjudicacion"] = 6_000_000.
    df.to_parquet(old, index=False)
    reparar_importes(old, sources, new)
    output = tmp_path / "calidad"
    run(SimpleNamespace(input=new, output=output, sample=None, ted=None, borme=None))
    result = pd.read_parquet(output / "calidad_licitaciones_resultado.parquet")
    assert len(result) == 1
    assert not result.iloc[0]["INT-CONS-08"]
    assert result.iloc[0].importe_sin_iva == 4685950.41
    assert result.iloc[0].valor_estimado_contrato == 7809917.35
    # Already corrected national inputs can be reverified without changing values.
    other = tmp_path / "verified.parquet"
    assert reparar_importes(new, sources, other)["importe_sin_iva_modificado"] == 0


def test_quality_rejects_legacy_before_creating_output(tmp_path):
    old, _, _ = setup(tmp_path)
    output = tmp_path / "calidad"
    with pytest.raises(ValueError, match="anterior al arreglo #6"):
        run(SimpleNamespace(input=old, output=output, sample=None, ted=None, borme=None))
    assert not output.exists()


def test_quality_result_cannot_be_repaired_or_used_as_national(tmp_path):
    old, sources, new = setup(tmp_path)
    df = pd.read_parquet(old)
    df["valor_estimado_contrato"] = df["importe_sin_iva"]
    df["score_calidad"] = 100.
    df.to_parquet(old)
    with pytest.raises(ValueError, match="recalcula calidad"):
        reparar_importes(old, sources, new)
    with pytest.raises(ValueError, match="indicadores antiguos"):
        run(SimpleNamespace(input=old))


def test_missing_published_timestamp_fails_before_source_indexing(tmp_path):
    old, sources, new = setup(tmp_path)
    df = pd.read_parquet(old)
    df["fecha_updated"] = pd.NaT
    df.to_parquet(old)
    with pytest.raises(ValueError, match="Claves nulas"):
        reparar_importes(old, sources, new)
    assert not new.exists()


def test_consultation_without_budget_is_preserved(tmp_path):
    old, sources, new = setup(tmp_path)
    root = entry()
    status = root.find("cac-place-ext:ContractFolderStatus", NS)
    root.remove(status)
    ET.SubElement(root, f"{{{NS['cac-place-ext']}}}PreliminaryMarketConsultationStatus")
    (sources / "licitaciones" / "feed.atom").unlink()
    (sources / "consultas").mkdir()
    ET.ElementTree(root).write(sources / "consultas" / "feed.atom")
    df = pd.read_parquet(old)
    df["conjunto"] = "consultas"
    df["importe_sin_iva"] = None
    df["importe_con_iva"] = None
    df.to_parquet(old)
    reparar_importes(old, sources, new)
    result = pd.read_parquet(new)
    assert len(result) == 1
    assert result[["importe_sin_iva", "importe_con_iva", "valor_estimado_contrato"]].isna().all().all()
