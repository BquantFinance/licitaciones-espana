import json
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "comunidad_madrid" / "ccaa_madrid_ayuntamiento.py"
SPEC = importlib.util.spec_from_file_location("madrid_ayuntamiento", MODULE_PATH)
madrid_ayuntamiento = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(madrid_ayuntamiento)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class MadridAyuntamientoTests(unittest.TestCase):
    def test_defaults_live_under_comunidad_madrid_directory(self):
        script_dir = MODULE_PATH.parent
        self.assertEqual(
            madrid_ayuntamiento.DEFAULT_OUTPUT_DIR,
            script_dir / "datos_madrid_contratacion_completa",
        )
        self.assertEqual(
            madrid_ayuntamiento.DEFAULT_LOG_PATH,
            script_dir / "descarga_madrid_ayuntamiento.log",
        )

    def test_descubrir_recursos_dataset_filtra_csv_y_nombra_desde_api(self):
        payload = {
            "success": True,
            "result": {
                "resources": [
                    {
                        "name": "csv-2026",
                        "description": "Actividad contractual. 2026. Contratos inscritos en el Registro de Contratos. 2026",
                        "format": "CSV",
                        "url": "https://datos.madrid.es/fake/formalizados_2026.csv",
                    },
                    {
                        "name": "xlsx-2026",
                        "description": "Actividad contractual. 2026. Contratos inscritos en el Registro de Contratos. 2026",
                        "format": "XLSX",
                        "url": "https://datos.madrid.es/fake/formalizados_2026.xlsx",
                    },
                    {
                        "name": "csv-prorrogas",
                        "description": "Actividad contractual. 2025. Contratos prorrogados inscritos en el Registro de Contratos. 2025",
                        "format": "CSV",
                        "url": "https://datos.madrid.es/fake/prorrogados_2025.csv",
                    },
                ]
            },
        }

        with patch.object(
            madrid_ayuntamiento.requests,
            "get",
            return_value=FakeResponse(payload),
        ):
            urls = madrid_ayuntamiento.descubrir_recursos_dataset(
                madrid_ayuntamiento.PACKAGE_ID_ACTIVIDAD,
                madrid_ayuntamiento._extraer_nombre_actividad,
            )

        self.assertEqual(
            urls,
            {
                "formalizados_2026": "https://datos.madrid.es/fake/formalizados_2026.csv",
                "prorrogados_2025": "https://datos.madrid.es/fake/prorrogados_2025.csv",
            },
        )

    def test_extraer_nombre_menores_resuelve_2021_y_2026(self):
        self.assertEqual(
            madrid_ayuntamiento._extraer_nombre_menores(
                "Contratos menores. 2021 - Contratos menores (hasta febrero)",
                "https://datos.madrid.es/fake.csv",
            ),
            "menores_2021_hasta_febrero",
        )
        self.assertEqual(
            madrid_ayuntamiento._extraer_nombre_menores(
                "Contratos menores. 2026 Contratos menores",
                "https://datos.madrid.es/fake.csv",
            ),
            "menores_2026",
        )

    def test_extraer_nombre_actividad_resuelve_2021_y_2026(self):
        self.assertEqual(
            madrid_ayuntamiento._extraer_nombre_actividad(
                "Actividad contractual. 2021. Contratos basados en un acuerdo marco inscritos en el Registro de Contratos. 2021. Contratos 2020",
                "https://datos.madrid.es/fake.csv",
            ),
            "acuerdo_marco_2021_anteriores",
        )
        self.assertEqual(
            madrid_ayuntamiento._extraer_nombre_actividad(
                "Actividad contractual. 2026. Contratos inscritos en el Registro de Contratos. 2026",
                "https://datos.madrid.es/fake.csv",
            ),
            "formalizados_2026",
        )

    def test_urls_respaldo_incluyen_catalogo_actual(self):
        menores = madrid_ayuntamiento._urls_respaldo_menores()
        actividad = madrid_ayuntamiento._urls_respaldo_actividad()

        self.assertEqual(len(menores), 13)
        self.assertEqual(len(actividad), 67)
        self.assertIn("menores_2026", menores)
        self.assertIn("formalizados_2026", actividad)
        self.assertIn("acuerdo_marco_2021_anteriores", actividad)

    def test_detectar_estructura_menores_y_actividad(self):
        est_menores = madrid_ayuntamiento.detectar_estructura_menores(
            "menores_2024",
            [
                "Nº RECON",
                "NÚMERO EXPEDIENTE",
                "SECCIÓN",
                "OBJETO DEL CONTRATO",
            ],
        )
        est_actividad = madrid_ayuntamiento.detectar_estructura_actividad(
            "formalizados_2025",
            [
                "N. DE REGISTRO",
                "N. DE EXPEDIENTE",
                "ORGANISMO_CONTRATANTE",
                "OBJETO DEL EXPEDIENTE",
            ],
        )

        self.assertEqual(est_menores, "C")
        self.assertEqual(est_actividad, "AC_2025")

    def test_normalizar_importe_handles_common_formats(self):
        self.assertEqual(madrid_ayuntamiento.normalizar_importe("3.145,16€"), 3145.16)
        self.assertEqual(madrid_ayuntamiento.normalizar_importe("108.798,07 \x80"), 108798.07)
        self.assertIsNone(madrid_ayuntamiento.normalizar_importe(""))

    def test_limpiar_dataframe_fills_anio_from_alternative_dates_and_filename(self):
        df = pd.DataFrame(
            {
                "fuente_fichero": ["formalizados_2025", "modificados_2019"],
                "categoria": ["contratos_formalizados", "modificados"],
                "estructura": ["AC_2025", "AC_OLD_MOD"],
                "objeto_contrato": ["Contrato 1", "Contrato 2"],
                "importe_adjudicacion_iva_inc": ["100,00", "200,00"],
                "fecha_adjudicacion": [None, None],
                "fecha_formalizacion": ["15/02/2025", None],
                "fecha_formalizacion_incidencia": [None, "01/03/2019"],
                "importe_modificacion": [None, "20,00"],
            }
        )

        cleaned = madrid_ayuntamiento.limpiar_dataframe(df)

        self.assertEqual(cleaned["anio"].tolist(), [2025.0, 2019.0])
        self.assertEqual(cleaned["importe_adjudicacion_iva_inc"].tolist(), [100.0, 200.0])
        self.assertTrue(pd.isna(cleaned["importe_modificacion"].iloc[0]))
        self.assertEqual(cleaned["importe_modificacion"].iloc[1], 20.0)

    def test_configure_runtime_updates_output_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "out"
            log_path = Path(tmpdir) / "madrid.log"
            madrid_ayuntamiento.configure_runtime(output_dir=output_dir, log_path=log_path)

            self.assertEqual(madrid_ayuntamiento.OUTPUT_DIR, output_dir.resolve())
            self.assertEqual(
                madrid_ayuntamiento.CSV_DIR,
                output_dir.resolve() / "csv_originales",
            )
            self.assertTrue(madrid_ayuntamiento.CSV_DIR.exists())
            self.assertEqual(madrid_ayuntamiento.LOG_PATH, log_path.resolve())

    def test_exportar_manifiesto_incluye_categorias_y_resources(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "out"
            madrid_ayuntamiento.configure_runtime(output_dir=output_dir)

            manifest_path = madrid_ayuntamiento.exportar_manifiesto(
                {
                    "menores_2026": "https://datos.madrid.es/fake/menores_2026.csv",
                    "formalizados_2026": "https://datos.madrid.es/fake/formalizados_2026.csv",
                    "acuerdo_marco_2026": "https://datos.madrid.es/fake/acuerdo_marco_2026.csv",
                }
            )

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["total_csvs"], 3)
            self.assertEqual(
                manifest["categorias"],
                {
                    "contratos_menores": 1,
                    "contratos_formalizados": 1,
                    "acuerdo_marco": 1,
                },
            )
            self.assertEqual(len(manifest["resources"]), 3)
            self.assertEqual(manifest["resources"][0]["nombre"], "acuerdo_marco_2026")

if __name__ == "__main__":
    unittest.main()
