import importlib.util
import logging
import os
import runpy
import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "scripts" / "ccaa_asturias.py"
SPEC = importlib.util.spec_from_file_location("ccaa_asturias", MODULE_PATH)
ccaa_asturias = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ccaa_asturias)

BASE_URL = "https://descargas.asturias.es/asturias/opendata/SectorPublico/contratacion"
YEARS = [2019, 2020, 2021, 2022, 2023, 2024]  # años publicados en el parquet del repo
# El script pide además los años posteriores hasta el actual (404 = aún no publicado)
REQUESTED_YEARS = list(range(2019, max(date.today().year, 2024) + 1))
HEADER = [
    "Nº INSCRIPCION",
    "{year_col}",
    "CLASIFICACION GENERAL",
    "OBJETO",
    "PRESUPUESTO",
    "IVA",
    "CARACTERISTICAS CONTRATO",
    "ENTE CONTRATANTE",
    "ORGANO CONTRATANTE",
    "IMP. ADJ. (CON IVA)",
    "F. ADJ.",
    "NIF/CIF CONTRATISTA",
]


def build_year_csv(year):
    """CSV separado por '§' y codificado en latin-1, como los ficheros anuales del Principado.

    Devuelve los bytes y los valores esperados tras el parseo. El IVA de 2019 lleva coma
    decimal (read_csv lo deja como texto) y el de 2023 son enteros con un hueco (read_csv
    lo parsea como float): es la combinacion que multiplicaba por 10 el IVA de 2023.
    """
    year_col = "ANO" if year <= 2021 else "AÑO"
    header = [column.format(year_col=year_col) for column in HEADER]
    if year == 2020:
        header.append("OBJETO ")  # cabecera duplicada tras strip() -> OBJETO_dup2
    iva = {2019: ["21,00", "10", "21"], 2023: ["21", "", "10"]}.get(year, ["21", "10", "21"])
    rows = [
        [f" 00000001-{year % 100:02d}", str(year), "MENORES 5000", "Suministro de señalización", "1.234,56", iva[0],
         "SUMINISTROS", "SERVICIO DE SALUD", "CONSEJERÍA DE SALUD", "1.493,82", f"07/02/{year}", "B12345678"],
        [f" 00000002-{year % 100:02d}", str(year), "MENOR", "Reparación de daños", "100,5", iva[1],
         "OBRAS", "ADMINISTRACIÓN DEL PRINCIPADO", "DIRECCIÓN GENERAL DE PATRIMONIO", "121,61", f"08/03/{year}", "A87654321"],
        [f" 00000003-{year % 100:02d}", str(year), "MAYOR", "Servicio de limpieza", "12.959.100,00", iva[2],
         "SERVICIOS", "SERVICIO DE SALUD", "CONSEJERÍA DE SALUD", "15.680.511,00", f"09/04/{year}", "B11111111"],
    ]
    if year == 2020:
        for row in rows:
            row.append("texto duplicado")
    lines = ["§".join(header)] + ["§".join(row) for row in rows]
    content = ("\r\n".join(lines) + "\r\n").encode("latin-1")
    expected_iva = [float(value.replace(",", ".")) if value else None for value in iva]
    return content, expected_iva


def make_response(url, status_code, content):
    response = requests.Response()
    response.url = url
    response.status_code = status_code
    response._content = content
    return response


class FakeDownloads:
    """Sirve los CSV anuales por URL; lo que no esta en `contents` responde 404 HTML."""

    def __init__(self, contents):
        self.contents = contents
        self.calls = []

    def get(self, url, timeout=None, **kwargs):
        self.calls.append((url, timeout))
        filename = url.rsplit("/", 1)[-1]
        if filename in self.contents:
            return make_response(url, 200, self.contents[filename])
        return make_response(url, 404, b"<html><head><title>404 Not Found</title></head><body></body></html>")


def all_year_contents():
    return {
        f"dataset-contratacion-centralizada-{year}.csv": build_year_csv(year)[0]
        for year in YEARS
    }


class AsturiasScraperTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def test_default_output_dir_is_repo_ccaa_asturias_independent_of_cwd(self):
        previous_cwd = os.getcwd()
        os.chdir(self.tmpdir)
        try:
            processor = ccaa_asturias.AsturiasToParquet()
        finally:
            os.chdir(previous_cwd)

        self.assertEqual(processor.output_dir, REPO_ROOT / "ccaa_asturias")
        self.assertFalse((Path(self.tmpdir) / "asturias_data").exists())

    def test_process_year_rejects_http_error_page(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        downloads = FakeDownloads({})

        with patch.object(ccaa_asturias.requests, "get", side_effect=downloads.get):
            ok = processor.process_year(2019, "dataset-contratacion-centralizada-2019.csv")

        self.assertFalse(ok)
        self.assertEqual(processor.all_dfs, [])

    def test_process_year_rejects_html_served_with_200(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        html = b"<html><head><title>Mantenimiento</title></head>\n<body>Volvemos pronto</body></html>\n"
        downloads = FakeDownloads({"dataset-contratacion-centralizada-2019.csv": html})

        with patch.object(ccaa_asturias.requests, "get", side_effect=downloads.get):
            ok = processor.process_year(2019, "dataset-contratacion-centralizada-2019.csv")

        self.assertFalse(ok)
        self.assertEqual(processor.all_dfs, [])

    def test_force_compatible_types_parses_spanish_amounts_stored_as_text(self):
        # Con pandas 3 las columnas de texto tienen dtype "str", no "object"
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        content, _ = build_year_csv(2021)
        dataframe = processor.force_compatible_types(processor.parse_year(content, 2021))

        self.assertTrue(pd.api.types.is_float_dtype(dataframe["PRESUPUESTO"]))
        self.assertEqual(dataframe["PRESUPUESTO"].tolist(), [1234.56, 100.5, 12959100.0])
        self.assertEqual(dataframe["IMP. ADJ. (CON IVA)"].tolist(), [1493.82, 121.61, 15680511.0])
        self.assertEqual(dataframe["ORGANO CONTRATANTE"].iloc[0], "CONSEJERÍA DE SALUD")

    def test_force_compatible_types_keeps_values_already_parsed_as_numbers(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        frames = []
        for year in (2019, 2023):
            content, _ = build_year_csv(year)
            frames.append(processor.parse_year(content, year))
        # read_csv deja el IVA de 2019 como texto ("21,00") y el de 2023 como float
        self.assertFalse(pd.api.types.is_numeric_dtype(frames[0]["IVA"]))
        self.assertTrue(pd.api.types.is_float_dtype(frames[1]["IVA"]))

        combined = pd.concat(frames, axis=0, join="outer", ignore_index=True)
        dataframe = processor.force_compatible_types(combined)

        iva = dataframe["IVA"].tolist()
        self.assertEqual(iva[:3], [21.0, 10.0, 21.0])
        self.assertEqual(iva[3], 21.0)  # antes 210.0
        self.assertTrue(pd.isna(iva[4]))
        self.assertEqual(iva[5], 10.0)  # antes 100.0

    def test_run_does_not_overwrite_parquet_when_a_year_fails(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        parquet_path = Path(self.tmpdir) / "asturias_contracts_ALL_YEARS.parquet"
        parquet_path.write_bytes(b"dataset previo completo")
        contents = all_year_contents()
        del contents["dataset-contratacion-centralizada-2022.csv"]
        downloads = FakeDownloads(contents)

        with patch.object(ccaa_asturias.requests, "get", side_effect=downloads.get), patch.object(
            ccaa_asturias.time, "sleep"
        ):
            result = processor.run()

        self.assertIsNone(result)
        self.assertEqual(parquet_path.read_bytes(), b"dataset previo completo")
        self.assertFalse((Path(self.tmpdir) / "sample_1000_rows.csv").exists())

    # ── Completitud ──────────────────────────────────────────────────────────

    def test_run_downloads_years_after_2024_and_skips_the_unpublished_last_one(self):
        # Antes la lista acababa en 2024 fija: el CSV de 2025 no se pedía nunca
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir, last_year=2026)
        contents = all_year_contents()
        contents["dataset-contratacion-centralizada-2025.csv"] = build_year_csv(2025)[0]
        downloads = FakeDownloads(contents)  # 2026 -> 404: aún no publicado

        with patch.object(ccaa_asturias.requests, "get", side_effect=downloads.get), patch.object(
            ccaa_asturias.time, "sleep"
        ):
            result = processor.run()

        self.assertEqual(
            [url.rsplit("-", 1)[-1] for url, _ in downloads.calls],
            [f"{year}.csv" for year in range(2019, 2027)],
        )
        self.assertIsNotNone(result)
        self.assertEqual(sorted(result["year"].unique().tolist()), YEARS + [2025])

    def test_run_fails_when_a_recent_year_is_missing_but_a_later_one_exists(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir, last_year=2026)
        contents = all_year_contents()
        contents["dataset-contratacion-centralizada-2026.csv"] = build_year_csv(2026)[0]
        downloads = FakeDownloads(contents)  # 2025 -> 404 con 2026 publicado: hueco

        with patch.object(ccaa_asturias.requests, "get", side_effect=downloads.get), patch.object(
            ccaa_asturias.time, "sleep"
        ):
            result = processor.run()

        self.assertIsNone(result)
        self.assertFalse((Path(self.tmpdir) / "asturias_contracts_ALL_YEARS.parquet").exists())

    def test_parse_year_decodes_windows_1252(self):
        # Los CSV del Principado son cp1252: como latin-1, “ ” – € quedaban como \x93 \x94 \x96 \x80
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        content = "OBJETO§PRESUPUESTO\r\nObra “Puente” – fase 2 (5.000 €)§5.000,00\r\n".encode("cp1252")

        dataframe = processor.parse_year(content, 2024)

        self.assertEqual(dataframe["OBJETO"].iloc[0], "Obra “Puente” – fase 2 (5.000 €)")

    def test_parse_year_keeps_lines_with_extra_fields_aside_and_warns(self):
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        content = (
            "Nº INSCRIPCION§OBJETO§PRESUPUESTO\r\n"
            " 00000001-24§Uno§10,00\r\n"
            " 00000002-24§Dos § con separador§20,00\r\n"
            " 00000003-24§Tres§30,00\r\n"
        ).encode("cp1252")

        logging.disable(logging.NOTSET)
        with self.assertLogs(ccaa_asturias.logger, level="WARNING") as logs:
            dataframe = processor.parse_year(content, 2024)

        self.assertEqual(dataframe["Nº INSCRIPCION"].tolist(), [" 00000001-24", " 00000003-24"])
        self.assertIn("1 líneas", "\n".join(logs.output))
        saved = (Path(self.tmpdir) / "lineas_descartadas_2024.csv").read_text(encoding="utf-8")
        self.assertEqual(saved, " 00000002-24§Dos § con separador§20,00\n")

    def test_force_compatible_types_does_not_turn_text_values_into_nan(self):
        # 'Nº EXPEDIENTE ORGANO' se convertía a float: los expedientes con letras (todos los
        # contratos MAYOR del parquet publicado) quedaban NaN y "00123" perdía los ceros
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir)
        dataframe = pd.DataFrame(
            {
                "Nº EXPEDIENTE ORGANO": ["4501518123", "SUM/2019/12", "00123", None],
                "ACUERDO MARCO DEL QUE DERIVA EL CONTRATO BASADO": ["123", "456", "AM/2021/7", " "],
                "PRESUPUESTO": ["1.234,56", "100,5", " ", None],
            },
            dtype=object,
        )

        dataframe = processor.force_compatible_types(dataframe)

        expedientes = [None if pd.isna(value) else value for value in dataframe["Nº EXPEDIENTE ORGANO"]]
        self.assertEqual(expedientes, ["4501518123", "SUM/2019/12", "00123", None])
        self.assertEqual(
            dataframe["ACUERDO MARCO DEL QUE DERIVA EL CONTRATO BASADO"].tolist(), ["123", "456", "AM/2021/7", " "]
        )
        self.assertTrue(pd.api.types.is_float_dtype(dataframe["PRESUPUESTO"]))
        self.assertEqual(dataframe["PRESUPUESTO"].tolist()[:2], [1234.56, 100.5])


class AsturiasEndToEndTests(unittest.TestCase):
    """Ejecuta el script como en el README (`python scripts/ccaa_asturias.py`) con las
    descargas simuladas, sobre una copia en un arbol temporal para no tocar el repo."""

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        (self.tmpdir / "scripts").mkdir()
        self.script = self.tmpdir / "scripts" / "ccaa_asturias.py"
        shutil.copy(MODULE_PATH, self.script)
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def run_script(self, downloads):
        other_cwd = self.tmpdir / "otro_directorio"
        other_cwd.mkdir(exist_ok=True)
        previous_cwd = os.getcwd()
        os.chdir(other_cwd)
        try:
            with patch("requests.get", side_effect=downloads.get), patch("time.sleep"):
                runpy.run_path(str(self.script), run_name="__main__")
        finally:
            os.chdir(previous_cwd)

    def test_main_downloads_every_year_and_writes_documented_parquet(self):
        downloads = FakeDownloads(all_year_contents())

        self.run_script(downloads)

        self.assertEqual(
            downloads.calls,
            [(f"{BASE_URL}/dataset-contratacion-centralizada-{year}.csv", 180) for year in REQUESTED_YEARS],
        )
        output_dir = self.tmpdir / "ccaa_asturias"
        parquet_path = output_dir / "asturias_contracts_ALL_YEARS.parquet"
        self.assertTrue(parquet_path.exists())
        self.assertTrue((output_dir / "sample_1000_rows.csv").exists())
        self.assertFalse((self.tmpdir / "otro_directorio" / "asturias_data").exists())

        df_ast = pd.read_parquet(parquet_path)
        self.assertEqual(len(df_ast), 3 * len(YEARS))
        self.assertEqual(sorted(df_ast["year"].unique().tolist()), YEARS)
        self.assertEqual(
            set(df_ast["source_file"]),
            {f"dataset-contratacion-centralizada-{year}.csv" for year in YEARS},
        )
        self.assertIn("OBJETO_dup2", df_ast.columns)
        self.assertEqual(df_ast["OBJETO"].iloc[0], "Suministro de señalización")
        self.assertEqual(set(df_ast["ORGANO CONTRATANTE"]), {"CONSEJERÍA DE SALUD", "DIRECCIÓN GENERAL DE PATRIMONIO"})
        self.assertEqual(df_ast["Nº INSCRIPCION"].iloc[0], " 00000001-19")

        for column in ("PRESUPUESTO", "IMP. ADJ. (CON IVA)", "IVA"):
            self.assertTrue(pd.api.types.is_float_dtype(df_ast[column]), column)
        for year in YEARS:
            rows = df_ast[df_ast["year"] == year]
            self.assertEqual(rows["PRESUPUESTO"].tolist(), [1234.56, 100.5, 12959100.0])
            self.assertEqual(rows["IMP. ADJ. (CON IVA)"].tolist(), [1493.82, 121.61, 15680511.0])
            expected_iva = build_year_csv(year)[1]
            got_iva = [None if pd.isna(value) else value for value in rows["IVA"]]
            self.assertEqual(got_iva, expected_iva, year)

        # Ejemplos de analisis del README
        by_type = df_ast.groupby(["year", "CARACTERISTICAS CONTRATO"])["IMP. ADJ. (CON IVA)"].sum()
        self.assertAlmostEqual(by_type[(2024, "SERVICIOS")], 15680511.0)
        top = df_ast.groupby("ENTE CONTRATANTE")["IMP. ADJ. (CON IVA)"].sum().nlargest(10)
        self.assertAlmostEqual(top["SERVICIO DE SALUD"], (1493.82 + 15680511.0) * len(YEARS), places=2)

    def test_main_exits_with_error_and_keeps_previous_dataset_when_a_download_fails(self):
        contents = all_year_contents()
        del contents["dataset-contratacion-centralizada-2021.csv"]
        downloads = FakeDownloads(contents)
        output_dir = self.tmpdir / "ccaa_asturias"
        output_dir.mkdir()
        parquet_path = output_dir / "asturias_contracts_ALL_YEARS.parquet"
        parquet_path.write_bytes(b"dataset previo completo")

        with self.assertRaises(SystemExit) as raised:
            self.run_script(downloads)

        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(len(downloads.calls), len(REQUESTED_YEARS))
        self.assertEqual(parquet_path.read_bytes(), b"dataset previo completo")


if __name__ == "__main__":
    unittest.main()
