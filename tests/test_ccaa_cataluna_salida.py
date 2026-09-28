"""--salida / --entrada de scripts/ccaa_cataluna.py y ccaa_cataluna_parquet.py: poder
ejecutarlos fuera del repo sin escribir en el directorio actual."""
import importlib.util
from pathlib import Path

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
