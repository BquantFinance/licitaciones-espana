"""--semilla de scripts/ccaa_cataluna_parquet.py: las filas del Parquet publicado en el release
v2026.02 que la descarga ya no trae (la ventana móvil del RPC, publicaciones retiradas de la PSCP...)
se añaden con _origen y _en_ultima_descarga=False, sin duplicar las que siguen."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ccaa_cataluna_parquet_semilla", REPO_ROOT / "scripts" / "ccaa_cataluna_parquet.py")
cp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cp)

CLAVE_RPC = cp.SEMILLAS["contratacion/contratos_registro.parquet"]
CSV_RPC = "01_transparencia_catalunya/01_contratacion/registro_publico_contratos.csv"
PQ_RPC = "contratacion/contratos_registro.parquet"


def test_clave_texto_compara_tipos_distintos_y_el_vacio_es_un_valor():
    a = pd.DataFrame({"x": ["7515090354", " 12 ", None, None], "lot": [1, 2, None, None],
                      "mod": [None, "3", None, None]})
    b = pd.DataFrame({"x": ["7515090354", "12", "", None], "lot": [1.0, 2.0, np.nan, None],
                      "mod": [np.nan, 3.0, "", None]})
    ka, kb = cp.clave_texto(a, ["x", "lot", "mod"]), cp.clave_texto(b, ["x", "lot", "mod"])
    assert ka.iloc[0] == kb.iloc[0] == "7515090354\x1f1\x1f"          # sin modificación: '' como valor
    assert ka.iloc[1] == kb.iloc[1] == "12\x1f2\x1f3"                 # 2 == 2.0, '3' == 3.0, sin espacios
    assert ka.iloc[2] is None and kb.iloc[2] is None and kb.iloc[3] is None   # todo vacío: sin clave
    assert list(cp.clave_texto(a.set_index(pd.Index([5, 6, 7, 8])), ["x"]).index) == [5, 6, 7, 8]


def _descarga(filas):
    df = pd.DataFrame(filas)
    df["_primera_descarga"] = df["_ultima_descarga"] = "2026-09-28T13:53:55Z"
    df["_en_ultima_descarga"] = True
    return df


def test_sembrar_release_anade_solo_las_claves_ausentes(tmp_path):
    df = _descarga({"id": ["A", "B"], "importe": [10.0, 20.0]})
    ruta = tmp_path / "publicado.parquet"
    pd.DataFrame({"id": ["A", "C", "C"], "importe": [11.0, 30.0, 31.0]}).to_parquet(ruta)
    out = cp.sembrar_release(df, ruta, ["id"])
    assert out["id"].tolist() == ["A", "B", "C", "C"]                 # A no se duplica (aunque cambió)
    assert out["_origen"].tolist()[:2] == [None, None] and set(out["_origen"][2:]) == {"release v2026.02"}
    assert out["_en_ultima_descarga"].tolist() == [True, True, False, False]
    assert out["_primera_descarga"][2:].isna().all()
    assert "_clave_semilla" not in out.columns
    # Sembrar otra vez no añade nada
    assert len(cp.sembrar_release(out, ruta, ["id"])) == 4


def test_sin_fichero_o_sin_columnas_de_la_clave_no_se_siembra(tmp_path, caplog):
    df = _descarga({"id": ["A"], "importe": [10.0]})
    assert cp.sembrar_release(df, tmp_path / "no_existe.parquet", ["id"]) is df
    ruta = tmp_path / "publicado.parquet"
    pd.DataFrame({"otra": ["A"]}).to_parquet(ruta)
    assert cp.sembrar_release(df, ruta, ["id"]) is df


def _rpc(filas):
    cab = ",".join(["Identificador organisme contractant", "Codi de l’expedient", "Número de lot", "Situació contractual",
                    "Número de modificació", "Número de pròrroga", "Exercici", "Import d’adjudicació", "Adjudicatari"])
    return cab + "\n" + "".join(",".join(f) + "\n" for f in filas)


@pytest.fixture
def repo_cat(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "INPUT_DIR", cp.INPUT_DIR)
    monkeypatch.setattr(cp, "OUTPUT_DIR", cp.OUTPUT_DIR)
    monkeypatch.setattr(cp, "CATEGORIAS", cp.CATEGORIAS)
    for nombre in ("menores", "contratistas", "perfil", "modificaciones", "resumen", "autorizacion"):
        monkeypatch.setattr(cp, f"consolidate_barcelona_{nombre}", lambda i, o: (0, 0))
    crudo = tmp_path / "crudo" / CSV_RPC
    crudo.parent.mkdir(parents=True)
    # Una adjudicación y su modificación, que siguen; la de 2021 ya no se sirve
    crudo.write_text(_rpc([["0817120002", "F/2024/1", "1", "adjudicació", "", "", "2024", "1000.5", "EMPRESA A"],
                           ["0817120002", "F/2024/1", "1", "modificació", "1", "", "2024", "200", "EMPRESA A"]]),
                     encoding="utf-8")
    semilla = tmp_path / "release" / "catalunya"
    (semilla / "contratacion").mkdir(parents=True)
    pd.DataFrame({
        "Identificador organisme contractant": ["0817120002", "0817120002", "1535"],
        "Codi de l’expedient": ["F/2024/1", "F/2024/1", "4070224773"],
        "Número de lot": [1, 1, 1],                                   # entero en el publicado
        "Situació contractual": ["adjudicació", "modificació", "menor"],
        "Número de modificació": [np.nan, 1.0, np.nan],               # float con nulos en el publicado
        "Número de pròrroga": [np.nan, np.nan, np.nan],
        "Exercici": [2024, 2024, 2021],
        "Import d’adjudicació": [1000.5, 200.0, 99.0],
        "Adjudicatari": ["EMPRESA A", "EMPRESA A", "EMPRESA B"],
    }).to_parquet(semilla / PQ_RPC, index=False)
    return tmp_path, semilla


def test_main_con_semilla_anade_lo_que_la_ventana_ya_no_sirve(repo_cat):
    tmp_path, semilla = repo_cat
    salida = tmp_path / "pq"
    assert cp.main(["--entrada", str(tmp_path / "crudo"), "--salida", str(salida),
                    "--categorias", "contratacion", "--semilla", str(semilla)]) == 0
    df = pd.read_parquet(salida / PQ_RPC)
    assert len(df) == 3                                               # las dos que siguen no se duplican
    sembrada = df[df["_origen"].notna()]
    assert sembrada["Codi de l’expedient"].tolist() == ["4070224773"]
    assert sembrada["_origen"].tolist() == ["release v2026.02"] and not sembrada["_en_ultima_descarga"].any()
    assert sembrada["_primera_descarga"].isna().all()
    descargadas = df[df["_origen"].isna()]                            # nulo, no '' (no pasan por texto_sin_nulos)
    assert len(descargadas) == 2 and descargadas["_en_ultima_descarga"].all()
    assert sorted(df["Import d’adjudicació"].tolist()) == [99.0, 200.0, 1000.5]


def test_main_sin_semilla_da_lo_de_siempre(repo_cat):
    tmp_path, _ = repo_cat
    salida = tmp_path / "pq"
    assert cp.main(["--entrada", str(tmp_path / "crudo"), "--salida", str(salida), "--categorias", "contratacion"]) == 0
    df = pd.read_parquet(salida / PQ_RPC)
    assert len(df) == 2 and "_origen" not in df.columns


def test_main_con_semilla_inexistente_o_errores_sale_con_1(repo_cat, monkeypatch):
    tmp_path, _ = repo_cat
    assert cp.main(["--entrada", str(tmp_path / "crudo"), "--salida", str(tmp_path / "pq"),
                    "--semilla", str(tmp_path / "no_existe")]) == 1
    assert not (tmp_path / "pq").exists()
    # Un error al convertir un fichero: antes el script salía con 0
    monkeypatch.setattr(cp, "construir_registros", lambda *a, **k: (_ for _ in ()).throw(ValueError("roto")))
    assert cp.main(["--entrada", str(tmp_path / "crudo"), "--salida", str(tmp_path / "pq"),
                    "--categorias", "contratacion"]) == 1


def test_pscp_se_siembra_por_el_uuid_del_procedimiento(tmp_path):
    """La URL de la PSCP cambia con cada fase (su último número) y entre /ca/ y /es/: casar por la
    URL entera añadía fases antiguas de procedimientos que siguen publicados (102.180 en la descarga de producción)."""
    base = "https://contractaciopublica.cat/{}/detall-publicacio/{}/{}"
    a, b = "992d779c-6a90-5a7f-047e-a86a9ed3d999", "0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9"
    df = _descarga({"enllac_publicacio": [base.format("ca", a, "300138374")], "fase_publicacio": ["Formalització"]})
    ruta = tmp_path / "publicaciones_pscp.parquet"
    pd.DataFrame({"enllac_publicacio": [base.format("es", a.upper(), "39498108"),      # otra fase e idioma
                                        base.format("ca", b, "5"), base.format("ca", b, "6")],
                  "fase_publicacio": ["Anunci", "Adjudicació", "Formalització"]}).to_parquet(ruta)
    out = cp.sembrar_release(df, ruta, cp.SEMILLAS["contratacion/publicaciones_pscp.parquet"])
    assert out["enllac_publicacio"].tolist()[1:] == [base.format("ca", b, "5"), base.format("ca", b, "6")]
    assert out["_origen"].tolist() == [None, "release v2026.02", "release v2026.02"]
    assert cp.uuid_publicacio(pd.DataFrame({"enllac_publicacio": [None, "https://otra/url"]})).tolist() == [None, None]
