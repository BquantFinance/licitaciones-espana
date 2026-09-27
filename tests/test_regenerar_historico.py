import json
from pathlib import Path
from types import SimpleNamespace
import zipfile

import pandas as pd
import pyarrow.parquet as pq
import pytest

from nacional import regenerar_historico as regen
pytest.importorskip("duckdb")  # Dependencia del flujo de regeneración completo.
from nacional.consolidar_historico import consolidar
from nacional.licitaciones import NS, parsear_entry

pytestmark = pytest.mark.skipif(int(pd.__version__.split(".")[0]) < 3,
                               reason="El flujo completo requiere requirements-regeneracion.txt")


def fake_parser():
    return SimpleNamespace(
        FECHAS=["fecha_publicacion", "fecha_limite", "fecha_adjudicacion"],
        parsear_fechas=lambda s: pd.to_datetime(s.astype("string").str[:10], format="%Y-%m-%d", errors="coerce"),
        parsear_fecha_updated=lambda s: pd.to_datetime(s, format="ISO8601", utc=True),
        _normalizar_columnas=lambda df: df,
        parsear_entry=lambda e, _: parsear_entry(e),
        TAG_ENTRY=f"{{{NS['atom']}}}entry", TAG_BORRADO="unused")


def test_dates_outside_nanosecond_range_and_mixed_precision_are_preserved():
    rows = [dict(id="a",fecha_updated="2026-01-01T00:00:00+01:00",fecha_publicacion="0001-01-01"),
            dict(id="b",fecha_updated="2026-01-01T00:00:00.123+01:00",fecha_publicacion="3232-01-01")]
    table = regen.tabla(rows, fake_parser())
    assert table["fecha_updated"].null_count == 0
    assert [d.year for d in table["fecha_publicacion"].to_pylist()] == [1,3232]


def test_zip_rebuild_preserves_duplicate_versions_and_resumes(tmp_path, monkeypatch):
    monkeypatch.setattr(regen,"cargar_parser",lambda _:fake_parser())
    fixture = (Path(__file__).parent / "fixtures" / "entry_budget.xml").read_text()
    source = tmp_path / "fuentes" / "licitaciones"
    source.mkdir(parents=True)
    with zipfile.ZipFile(source / "test.zip","w") as z:
        z.writestr("one.atom",fixture)
        z.writestr("two.atom",fixture)
    parser_root = tmp_path / "parser" / "nacional"
    parser_root.mkdir(parents=True)
    (parser_root / "licitaciones.py").write_text("# test parser revision\n")
    item={"conjunto":"licitaciones","archivo_origen":"test.zip"}
    args=(item,tmp_path/"fuentes",tmp_path/"out",parser_root.parent)
    report=regen.procesar(*args)
    assert report["filas"]==2
    assert report==regen.procesar(*args)
    file=tmp_path/"out"/"particiones"/"licitaciones"/"test"/"nacional.parquet"
    df=pd.read_parquet(file)
    assert df.importe_sin_iva.tolist()==[4685950.41]*2
    assert df.entrada_origen.tolist()==[0,1]
    file.write_bytes(b"broken")
    with pytest.raises(ValueError,match="incompatible"):
        regen.procesar(*args)


def test_consolidation_counts_duplicates_without_dropping_rows(tmp_path):
    item={"conjunto":"licitaciones","archivo_origen":"test.zip"}
    folder=tmp_path/"particiones"/"licitaciones"/"test"
    folder.mkdir(parents=True)
    df=pd.DataFrame({"id":["a","a","a"],"conjunto":["licitaciones"]*3,
                     "archivo_origen":["test.zip"]*3,"entrada_origen":[0,1,2],
                     "fecha_updated":pd.to_datetime(["2026-01-01","2026-01-02","2026-01-02"],utc=True),
                     "importe_sin_iva":[100.,120.,120.],"valor_estimado_contrato":[200.]*3,
                     "importe_con_iva":[121.,145.2,145.2]})
    file=folder/"nacional.parquet"
    df.to_parquet(file,index=False)
    manifest={**item,"estado":"ok","descartadas":{},"filas":3,"salidas":{"nacional.parquet":regen.digest(file)}}
    (folder/"informe.json").write_text(json.dumps(manifest))
    inv=tmp_path/"inventario.json"
    inv.write_text(json.dumps([item]))
    old=tmp_path/"old.parquet"
    df.drop(columns="valor_estimado_contrato").assign(importe_sin_iva=200.).to_parquet(old,index=False)
    output=tmp_path/"new.parquet"
    report=consolidar(inv,tmp_path,old,output)
    assert report["resumen"]["filas"]==3
    result=pd.read_parquet(output)
    assert result.es_ultima_version.sum()==1
    assert result.entrada_repetida.sum()==1
    assert result.n_versiones.tolist()==[2]*3
    assert report["versiones"]["versiones_solo_anterior"]==0
    assert report["importes_versiones_unicas"]["filas_comparables"]==1
    (folder/"informe.json").unlink()
    with pytest.raises(FileNotFoundError):
        consolidar(inv,tmp_path,old,tmp_path/"incomplete.parquet")
    assert not (tmp_path/"incomplete.parquet").exists()
