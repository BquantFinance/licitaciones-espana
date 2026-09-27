import pandas as pd
import pytest
from nacional.recalcular_calidad import cons20_por_version, limitar_cobertura_ted


def test_ted_does_not_propagate_to_other_versions_or_contracting_bodies(tmp_path):
    dates=pd.to_datetime(["2025-01-01","2025-02-01","2025-02-01"],utc=True)
    df=pd.DataFrame({"id":["a","a","b"],"fecha_updated":dates,
                     "expediente":["1/2025"]*3,"nif_adjudicatario":["B00000000"]*3},index=[2,5,7])
    path=tmp_path/"ted.parquet"
    pd.DataFrame({"id":["a"],"fecha_updated":[dates[1]],"_ted_validated":[True]}).to_parquet(path)
    result=cons20_por_version(df,path)
    assert result.index.tolist()==[2,5,7]
    assert pd.isna(result.loc[2]) and pd.isna(result.loc[7])
    assert result.loc[5]


def test_ted_rejects_duplicate_version_keys(tmp_path):
    df=pd.DataFrame({"id":["a","a"],"fecha_updated":pd.to_datetime(["2025-01-01"]*2,utc=True),"_ted_validated":[True,False]})
    path=tmp_path/"ted.parquet"
    df.to_parquet(path)
    with pytest.raises(ValueError,match="más de un resultado"):
        cons20_por_version(df,path)


def test_absence_outside_ted_year_coverage_is_unknown_not_false():
    df=pd.DataFrame({"id":["a","b","c"],"fecha_updated":pd.to_datetime(["2026-01-01"]*3,utc=True),
                     "_ano":[2026,2026,2025],"_es_sara":[True]*3,"_ted_validated":[False,True,False]})
    result,unknown=limitar_cobertura_ted(df,{2025})
    assert unknown==1
    assert pd.isna(result.iloc[0]["_ted_validated"])
    assert result.iloc[1]["_ted_validated"]
    assert not result.iloc[2]["_ted_validated"]
