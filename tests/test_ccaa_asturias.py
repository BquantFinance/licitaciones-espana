import importlib.util
import logging
import os
import runpy
import shutil
import sys
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


def csv_bytes(header, rows):
    """CSV anual como los del Principado ('§', CRLF, Windows-1252)."""
    lines = ["§".join(header)] + ["§".join(row) for row in rows]
    return ("\r\n".join(lines) + "\r\n").encode("cp1252")


HEADER_H = ["Nº INSCRIPCION", "AÑO", "OBJETO", "IVA", "IMP. ADJ. (CON IVA)", "NIF/CIF CONTRATISTA"]
FILA_A = [" 00000001-24", "2024", "Suministro de papel", "21", "121,00", "B12345678"]
FILA_B = [" 00000002-24", "2024", "Reparación de tejado", "10", "1.100,00", "A87654321"]
FILA_C = [" 00000003-24", "2024", "Servicio de limpieza", "21", "2.420,00", "B11111111"]


class AsturiasHistoricoTests(unittest.TestCase):
    """Sesgo del superviviente: los CSV se guardan con versiones (raw/ y raw/_historico/)
    y el Parquet se construye desde todas ellas (comun/historico.py)."""

    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmpdir, ignore_errors=True)
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def run_once(self, content_2024, last_year=2024, extra=None, semillas=(), checked_at=None):
        """Una ejecución con los años 2019-2023 fijos y el CSV de 2024 indicado
        (None = 404). extra: {nombre_fichero: bytes} de años posteriores."""
        contents = {name: data for name, data in all_year_contents().items() if not name.endswith("2024.csv")}
        if content_2024 is not None:
            contents["dataset-contratacion-centralizada-2024.csv"] = content_2024
        contents.update(extra or {})
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir, last_year=last_year, semillas=semillas)
        if checked_at:
            processor.checked_at = checked_at
        with patch.object(ccaa_asturias.requests, "get", side_effect=FakeDownloads(contents).get), patch.object(
            ccaa_asturias.time, "sleep"
        ):
            return processor.run()

    def filas(self, result, year=2024):
        rows = result[result["year"] == year]
        return {(r["Nº INSCRIPCION"], r["OBJETO"]): bool(r["_en_ultima_descarga"]) for _, r in rows.iterrows()}

    def historico(self, year):
        carpeta = self.tmpdir / "raw" / "_historico"
        return sorted(carpeta.glob(f"dataset-contratacion-centralizada-{year}__*.csv")) if carpeta.is_dir() else []

    def test_second_run_without_changes_keeps_every_row_current(self):
        first = self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B, FILA_C]))
        second = self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B, FILA_C]))

        self.assertEqual(len(second), len(first))
        self.assertTrue(second["_en_ultima_descarga"].all())
        self.assertEqual(self.historico(2024), [])
        self.assertTrue((self.tmpdir / "raw" / "dataset-contratacion-centralizada-2024.csv").exists())
        self.assertEqual(set(first["_primera_descarga"]), set(second["_primera_descarga"]))

    def test_withdrawn_and_changed_rows_are_kept_as_no_longer_published(self):
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B, FILA_C]))
        cambiada = FILA_C[:2] + ["Servicio de limpieza (lote 2)"] + FILA_C[3:]
        result = self.run_once(csv_bytes(HEADER_H, [FILA_A, cambiada]))

        self.assertEqual(self.filas(result), {
            (" 00000001-24", "Suministro de papel"): True,
            (" 00000002-24", "Reparación de tejado"): False,
            (" 00000003-24", "Servicio de limpieza"): False,
            (" 00000003-24", "Servicio de limpieza (lote 2)"): True,
        })
        self.assertEqual(len(self.historico(2024)), 1)
        # los importes se siguen leyendo como siempre, también en las filas retiradas
        importes = result[result["year"] == 2024].set_index("OBJETO")["IMP. ADJ. (CON IVA)"]
        self.assertEqual(importes["Reparación de tejado"], 1100.0)
        self.assertTrue(pd.api.types.is_float_dtype(result["IMP. ADJ. (CON IVA)"]))

    def test_a_type_pandas_infers_differently_is_not_a_change(self):
        # IVA sin huecos se lee como entero y con un hueco como float (21 frente a 21.0)
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]))
        nueva = [" 00000004-24", "2024", "Obra menor", "", "500,00", "B22222222"]
        result = self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B, nueva]))

        filas = self.filas(result)
        self.assertEqual(len(filas), 3)
        self.assertTrue(all(filas.values()))

    def test_a_new_column_does_not_duplicate_rows_and_keeps_its_values(self):
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]))
        header = HEADER_H + ["ID. PLACE"]
        result = self.run_once(csv_bytes(header, [FILA_A + ["111"], FILA_B + ["222"]]))

        rows = result[result["year"] == 2024]
        self.assertEqual(len(rows), 2)
        self.assertTrue(rows["_en_ultima_descarga"].all())
        # el valor de la columna nueva llega también a las filas que ya existían (float: los
        # demás años no la tienen, como al concatenar años hasta ahora)
        self.assertEqual(sorted(rows["ID. PLACE"].astype(float)), [111.0, 222.0])

    def test_a_year_that_stops_being_served_keeps_its_rows(self):
        extra = {"dataset-contratacion-centralizada-2025.csv": build_year_csv(2025)[0]}
        first = self.run_once(csv_bytes(HEADER_H, [FILA_A]), last_year=2026, extra=extra)
        self.assertEqual(int((first["year"] == 2025).sum()), 3)

        # 2025 pasa a dar 404 (y 2026 sigue sin publicar): no es un error, pero sus filas no se pierden
        result = self.run_once(csv_bytes(HEADER_H, [FILA_A]), last_year=2026)

        self.assertIsNotNone(result)
        filas_2025 = result[result["year"] == 2025]
        self.assertEqual(len(filas_2025), 3)
        self.assertFalse(filas_2025["_en_ultima_descarga"].any())
        self.assertTrue(result.loc[result["year"] != 2025, "_en_ultima_descarga"].all())

    def test_rows_still_published_carry_the_date_of_the_last_check(self):
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]), checked_at="2026-09-01T00:00:00+00:00")
        result = self.run_once(csv_bytes(HEADER_H, [FILA_A]), checked_at="2026-09-28T00:00:00+00:00")

        rows = result[result["year"] == 2024].set_index("OBJETO")
        self.assertEqual(rows.loc["Suministro de papel", "_ultima_descarga"], "2026-09-28T00:00:00+00:00")
        self.assertNotEqual(rows.loc["Reparación de tejado", "_ultima_descarga"], "2026-09-28T00:00:00+00:00")
        self.assertFalse(rows.loc["Reparación de tejado", "_en_ultima_descarga"])

    def test_a_year_not_requested_in_this_run_keeps_its_rows_unchanged(self):
        extra = {"dataset-contratacion-centralizada-2025.csv": build_year_csv(2025)[0]}
        self.run_once(csv_bytes(HEADER_H, [FILA_A]), last_year=2025, extra=extra)

        # una ejecución que solo llega a 2024 no sabe nada de 2025: ni lo pierde ni lo retira
        result = self.run_once(csv_bytes(HEADER_H, [FILA_A]), last_year=2024)

        filas_2025 = result[result["year"] == 2025]
        self.assertEqual(len(filas_2025), 3)
        self.assertTrue(filas_2025["_en_ultima_descarga"].all())

    def test_the_previous_parquet_is_kept_in_historico(self):
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]))
        primero = (self.tmpdir / "asturias_contracts_ALL_YEARS.parquet").read_bytes()
        self.run_once(csv_bytes(HEADER_H, [FILA_A]))

        guardados = sorted((self.tmpdir / "_historico").glob("asturias_contracts_ALL_YEARS__*.parquet"))
        self.assertEqual([g.read_bytes() for g in guardados], [primero])

    def test_a_failed_run_does_not_touch_the_previous_parquet_or_raw_versions(self):
        self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]))
        parquet = self.tmpdir / "asturias_contracts_ALL_YEARS.parquet"
        antes = parquet.read_bytes()

        contents = {name: data for name, data in all_year_contents().items() if not name.endswith("2021.csv")}
        processor = ccaa_asturias.AsturiasToParquet(output_dir=self.tmpdir, last_year=2024)
        with patch.object(ccaa_asturias.requests, "get", side_effect=FakeDownloads(contents).get), patch.object(
            ccaa_asturias.time, "sleep"
        ):
            self.assertIsNone(processor.run())

        self.assertEqual(parquet.read_bytes(), antes)
        self.assertTrue((self.tmpdir / "raw" / "dataset-contratacion-centralizada-2021.csv").exists())

    def test_seed_adds_only_missing_registrations_and_keeps_them_in_later_runs(self):
        semilla = pd.DataFrame({
            "Nº INSCRIPCION": [" 00000001-24", " 99999999-24", " 00000009-18"],
            "OBJETO": ["Suministro de papel", "Contrato retirado del portal", "Fuera del ámbito"],
            "IMP. ADJ. (CON IVA)": [121.0, 50.0, 10.0],
            "year": [2024, 2024, 2018],
            "source_file": ["dataset-contratacion-centralizada-2024.csv"] * 2
                           + ["dataset-contratacion-centralizada-2018.csv"],
        })
        ruta = self.tmpdir / "semilla.parquet"
        semilla.to_parquet(ruta, index=False)

        result = self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]), semillas=[ruta])

        sembradas = result[result["_origen"].notna()]
        self.assertEqual(sembradas["Nº INSCRIPCION"].tolist(), [" 99999999-24"])
        self.assertEqual(sembradas["_origen"].tolist(), ["release v2026.02"])
        self.assertFalse(sembradas["_en_ultima_descarga"].any())
        self.assertEqual(int((result["Nº INSCRIPCION"] == " 00000001-24").sum()), 1)

        # sin --semilla, la siguiente ejecución conserva la fila sembrada y no la duplica
        again = self.run_once(csv_bytes(HEADER_H, [FILA_A, FILA_B]))
        self.assertEqual(again.loc[again["_origen"].notna(), "Nº INSCRIPCION"].tolist(), [" 99999999-24"])
        self.assertEqual(len(again), len(result))

    def test_cli_accepts_output_folder_and_seed(self):
        with patch.object(ccaa_asturias.AsturiasToParquet, "run", return_value=None) as run, patch.object(
            ccaa_asturias.AsturiasToParquet, "__init__", return_value=None
        ) as init:
            ccaa_asturias.main(["--salida", str(self.tmpdir), "--semilla", "a.parquet", "--semilla", "b.parquet"])
        init.assert_called_once_with(output_dir=self.tmpdir, semillas=[Path("a.parquet"), Path("b.parquet")])
        run.assert_called_once_with()


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
            with patch("requests.get", side_effect=downloads.get), patch("time.sleep"), patch.object(
                sys, "argv", [str(self.script)]
            ):
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
