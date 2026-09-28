"""--salida / --entrada de scripts/ccaa_cataluna.py y ccaa_cataluna_parquet.py: poder
ejecutarlos fuera del repo sin escribir en el directorio actual."""
import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ccaa_cataluna = _load("ccaa_cataluna")
cat_parquet = _load("ccaa_cataluna_parquet")


def test_descarga_con_salida_escribe_solo_ahi(monkeypatch, tmp_path):
    monkeypatch.setattr(ccaa_cataluna, "OUTPUT_DIR", ccaa_cataluna.OUTPUT_DIR)
    recibidas = []
    for nombre in ("download_socrata_datasets", "download_socrata_metadata",
                   "download_barcelona_datasets", "download_gencat_adicional"):
        monkeypatch.setattr(ccaa_cataluna, nombre, lambda carpeta: recibidas.append(carpeta))
    actual = tmp_path / "actual"
    actual.mkdir()
    monkeypatch.chdir(actual)
    destino = tmp_path / "datos" / "cat"          # la carpeta madre no existe

    ccaa_cataluna.main(["--salida", str(destino)])

    assert recibidas == [destino] * 4
    assert (destino / "INFORME.md").exists()
    assert list(actual.iterdir()) == []


def test_parquet_con_entrada_y_salida(monkeypatch, tmp_path):
    monkeypatch.setattr(cat_parquet, "INPUT_DIR", cat_parquet.INPUT_DIR)
    monkeypatch.setattr(cat_parquet, "OUTPUT_DIR", cat_parquet.OUTPUT_DIR)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "catalunya_datos_completos").mkdir()   # la entrada por defecto existe…
    entrada = tmp_path / "no_existe"

    cat_parquet.main(["--entrada", str(entrada), "--salida", str(tmp_path / "pq")])

    # …pero se usa la de --entrada: al no existir, termina antes de crear la salida
    assert (cat_parquet.INPUT_DIR, cat_parquet.OUTPUT_DIR) == (str(entrada), str(tmp_path / "pq"))
    assert not (tmp_path / "pq").exists() and not (tmp_path / "catalunya_parquet").exists()


def test_descarga_solo_las_categorias_pedidas(monkeypatch, tmp_path):
    """--categorias contratacion: solo los datasets de 01_contratacion y Open Data Barcelona."""
    monkeypatch.setattr(ccaa_cataluna, "OUTPUT_DIR", ccaa_cataluna.OUTPUT_DIR)
    monkeypatch.setattr(ccaa_cataluna, "CATEGORIAS", ccaa_cataluna.CATEGORIAS)
    descargados, bcn = [], []
    monkeypatch.setattr(ccaa_cataluna, "fecha_actualizacion_socrata", lambda dataset_id: None)
    monkeypatch.setattr(ccaa_cataluna, "download_with_progress",
                        lambda url, *a, **k: descargados.append(url.split("/views/")[1].split("/")[0]) and False)
    monkeypatch.setattr(ccaa_cataluna, "download_socrata_metadata", lambda carpeta: None)
    monkeypatch.setattr(ccaa_cataluna, "download_barcelona_datasets", lambda carpeta: bcn.append(carpeta))
    monkeypatch.setattr(ccaa_cataluna, "download_gencat_adicional", lambda carpeta: None)
    monkeypatch.setattr(ccaa_cataluna.time, "sleep", lambda s: None)

    def de(*carpetas):
        return sorted(k for k, (s, _) in ccaa_cataluna.SOCRATA_DATASETS.items() if s.split("/")[0] in carpetas)

    ccaa_cataluna.main(["--salida", str(tmp_path / "a"), "--categorias", "contratacion"])
    assert sorted(descargados) == de("01_contratacion") and len(bcn) == 1

    descargados.clear()
    bcn.clear()
    ccaa_cataluna.main(["--salida", str(tmp_path / "b"), "--categorias", "subvenciones, convenios"])
    assert sorted(descargados) == de("02_subvenciones", "03_convenios") and bcn == []

    descargados.clear()   # sin --categorias, todo, como antes
    ccaa_cataluna.main(["--salida", str(tmp_path / "c")])
    assert sorted(descargados) == sorted(ccaa_cataluna.SOCRATA_DATASETS) and len(bcn) == 1


@pytest.mark.parametrize("modulo", [ccaa_cataluna, cat_parquet])
def test_categoria_desconocida_es_un_error_de_argumentos(modulo, capsys):
    with pytest.raises(SystemExit) as salida:
        modulo.argumentos(["--categorias", "contratacion,contratos"])
    assert salida.value.code == 2 and "contratos" in capsys.readouterr().err
    assert modulo.argumentos(["--categorias", "contratacion"]).categorias == {"contratacion"}


def test_parquet_solo_las_categorias_pedidas(monkeypatch, tmp_path):
    monkeypatch.setattr(cat_parquet, "INPUT_DIR", cat_parquet.INPUT_DIR)
    monkeypatch.setattr(cat_parquet, "OUTPUT_DIR", cat_parquet.OUTPUT_DIR)
    monkeypatch.setattr(cat_parquet, "CATEGORIAS", cat_parquet.CATEGORIAS)
    entrada, salida = tmp_path / "crudo", tmp_path / "pq"
    for rel in ("01_transparencia_catalunya/01_contratacion/resoluciones_tribunal.csv",
                "01_transparencia_catalunya/02_subvenciones/raisc_convocatorias.csv"):
        (entrada / rel).parent.mkdir(parents=True, exist_ok=True)
        (entrada / rel).write_text("a,b\n1,x\n2,y\n", encoding="utf-8")
    consolidaciones = []
    for nombre in ("menores", "contratistas", "perfil", "modificaciones", "resumen", "autorizacion"):
        monkeypatch.setattr(cat_parquet, f"consolidate_barcelona_{nombre}",
                            lambda i, o, nombre=nombre: consolidaciones.append(nombre) or (0, 0))

    cat_parquet.main(["--entrada", str(entrada), "--salida", str(salida), "--categorias", "subvenciones"])
    assert sorted(p.relative_to(salida).as_posix() for p in salida.rglob("*.parquet")) == \
        ["subvenciones/raisc_convocatorias.parquet"]
    assert consolidaciones == []

    cat_parquet.main(["--entrada", str(entrada), "--salida", str(salida), "--categorias", "contratacion"])
    assert (salida / "contratacion" / "resoluciones_tribunal.parquet").exists()
    assert len(consolidaciones) == 6


def test_contar_registros_por_trozos(monkeypatch, tmp_path):
    """count_csv_records ya no carga el CSV entero (el de RAISC, 19 GB, tumbaba la descarga):
    cuenta registros, no líneas, aunque un campo entrecomillado ocupe varias."""
    monkeypatch.setattr(ccaa_cataluna, "FILAS_POR_TROZO_CONTEO", 2)
    leer = ccaa_cataluna.pd.read_csv
    trozos = []
    monkeypatch.setattr(ccaa_cataluna.pd, "read_csv", lambda *a, **k: trozos.append(k.get("chunksize")) or leer(*a, **k))
    coma = tmp_path / "coma.csv"
    coma.write_text('a,b\n1,"x\ny"\n2,z\n3,w\n4,v\n5,u\n', encoding="utf-8")
    assert ccaa_cataluna.count_csv_records(coma) == 5
    punto_y_coma = tmp_path / "pyc.csv"
    punto_y_coma.write_text("a;b\n1;x\n2;y\n3;z\n", encoding="latin-1")
    assert ccaa_cataluna.count_csv_records(punto_y_coma) == 3
    una_columna = tmp_path / "una.csv"
    una_columna.write_text("a\n1\n2\n", encoding="utf-8")
    assert ccaa_cataluna.count_csv_records(una_columna) == 0
    assert ccaa_cataluna.count_csv_records(tmp_path / "no_existe.csv") == 0
    assert trozos and set(trozos) == {2}   # siempre por trozos
