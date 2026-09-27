"""Tests offline de los scrapers de Catalunya (HTTP simulado, sin red).

Cubre:
- scripts/ccaa_cataluna.py                  (descarga Socrata + Open Data BCN)
- scripts/ccaa_cataluna_parquet.py          (CSV -> Parquet)
- scripts/ccaa_cataluna_contratosmenores.py (API contractaciopublica.cat, con API falsa local)
"""

import asyncio
import importlib.util
import json
import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import requests
from aiohttp import web


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ccaa_cataluna = _load("ccaa_cataluna")
cat_parquet = _load("ccaa_cataluna_parquet")
cat_menores = _load("ccaa_cataluna_contratosmenores")

STATS_VACIAS = {'downloaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0, 'records': 0}


# =============================================================================
# Utilidades: respuestas HTTP simuladas (requests)
# =============================================================================

def _resp(status=200, body=b"", json_data=None, cortar_tras_primer_chunk=False):
    r = MagicMock()
    r.status_code = status
    r.headers = {}
    if status >= 400:
        r.raise_for_status.side_effect = requests.exceptions.HTTPError(f"{status} Error")
    else:
        r.raise_for_status.return_value = None

    def iter_content(chunk_size=8192):
        yield body
        if cortar_tras_primer_chunk:
            raise requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead")

    r.iter_content.side_effect = iter_content
    r.json.return_value = json_data
    return r


def _csv_socrata(dataset_id):
    # Texto con huecos para comprobar que los nulos de texto se guardan como ''
    return (
        "Codi,Nom,Import adjudicació,Data formalització\n"
        f"{dataset_id}-1,Educació,1234.5,01/15/2020 12:00:00 AM\n"
        f"{dataset_id}-2,,99,\n"
    ).encode("utf-8")


BCN_RECURSOS = {
    'perfil-contractant': [
        {'id': 'p0000001-a', 'name': 'perfil_contractant', 'format': 'CSV', 'url': 'https://bcn.test/perfil.csv'},
    ],
    'contractes-menors': [
        {'id': 'm0000001-a', 'name': '2019_contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/cm2019.csv'},
        {'id': 'm0000002-a', 'name': '1T_2020_contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/cm2020.csv'},
        {'id': 'm0000003-a', 'name': '2019_contractes_menors', 'format': 'JSON', 'url': 'https://bcn.test/cm2019.json'},
    ],
    'relacio-contractistes': [
        {'id': 'c0000001-a', 'name': 'contractistes_2012', 'format': 'CSV', 'url': 'https://bcn.test/ct2012.csv'},
    ],
    'resums-trimestrals-contractacio': [
        {'id': 'r0000001-a', 'name': 'Resum_2n_trimestre_2019', 'format': 'CSV', 'url': 'https://bcn.test/rs2019.csv'},
    ],
    'modificacions-de-contractes': [
        {'id': 'x0000001-a', 'name': 'modificacions_2021', 'format': 'CSV', 'url': 'https://bcn.test/md2021.csv'},
    ],
    'contractes-menors-a-generica': [
        {'id': 'g0000001-a', 'name': '2022_contractes_menors_a_generica', 'format': 'CSV', 'url': 'https://bcn.test/ag2022.csv'},
    ],
}


def _fake_get_factory(urls_pedidas):
    """session.get simulado que imita Socrata + CKAN de Open Data BCN"""
    ids_socrata = set(ccaa_cataluna.SOCRATA_DATASETS)

    def fake_get(url, timeout=None, stream=False, **kwargs):
        urls_pedidas.append(url)
        assert timeout is not None, f"petición sin timeout: {url}"
        base = ccaa_cataluna.SOCRATA_BASE + "/api/views/"
        if url.startswith(base) and url.endswith("/rows.csv?accessType=DOWNLOAD"):
            dataset_id = url[len(base):].split("/")[0]
            if dataset_id in ids_socrata:
                return _resp(body=_csv_socrata(dataset_id))
        if url.startswith(base) and url.endswith(".json"):
            dataset_id = url[len(base):-len(".json")]
            return _resp(json_data={'id': dataset_id, 'name': dataset_id})
        if url.startswith(ccaa_cataluna.BCN_BASE + "/data/api/3/action/package_show?id="):
            slug = url.split("id=", 1)[1]
            if slug in BCN_RECURSOS:
                return _resp(json_data={'success': True, 'result': {'resources': BCN_RECURSOS[slug]}})
        if url.startswith("https://bcn.test/"):
            nombre = url.rsplit("/", 1)[1]
            if nombre.endswith(".json"):
                return _resp(body=b'[{"id": 1}]')
            return _resp(body=f"Id,Descripcio,Import\n1,{nombre},10.5\n2,,\n".encode("utf-8"))
        return _resp(status=404)

    return fake_get


# =============================================================================
# ccaa_cataluna.py
# =============================================================================

class DescargaCatalunyaTests(unittest.TestCase):
    def test_menores_de_la_generalitat_piden_el_dataset_vigente(self):
        # ydq4-xy5b da 404; qjue-2pk9 son los menores 2020-2024 (importes en céntimos)
        self.assertIn('qjue-2pk9', ccaa_cataluna.SOCRATA_DATASETS)
        self.assertNotIn('ydq4-xy5b', ccaa_cataluna.SOCRATA_DATASETS)
        ruta = ccaa_cataluna.SOCRATA_DATASETS['qjue-2pk9'][0]
        self.assertIn(f'01_transparencia_catalunya/{ruta}.csv', cat_parquet.ARCHIVOS)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        patcher = patch.dict(ccaa_cataluna.stats, STATS_VACIAS)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.tmp.cleanup)

    def test_descarga_cortada_no_queda_como_ya_descargada(self):
        destino = self.dir / "datos.csv"
        cortada = _resp(body=b"id,nom\n1,a\n", cortar_tras_primer_chunk=True)
        with patch.object(ccaa_cataluna.session, "get", return_value=cortada):
            self.assertFalse(ccaa_cataluna.download_with_progress("https://x.test/a.csv", destino, "d"))
        self.assertFalse(destino.exists(), "un archivo truncado no debe quedar con el nombre final")
        self.assertEqual(list(self.dir.iterdir()), [], "tampoco debe quedar el temporal .part")
        self.assertEqual(ccaa_cataluna.stats['failed'], 1)

        # La siguiente ejecución debe volver a descargar (antes se saltaba el archivo truncado)
        completa = _resp(body=b"id,nom\n1,a\n2,b\n")
        with patch.object(ccaa_cataluna.session, "get", return_value=completa) as get:
            self.assertTrue(ccaa_cataluna.download_with_progress("https://x.test/a.csv", destino, "d"))
        get.assert_called_once()
        self.assertEqual(destino.read_bytes(), b"id,nom\n1,a\n2,b\n")

    def test_descarga_existente_se_salta(self):
        destino = self.dir / "datos.csv"
        destino.write_bytes(b"id\n1\n")
        with patch.object(ccaa_cataluna.session, "get") as get:
            self.assertTrue(ccaa_cataluna.download_with_progress("https://x.test/a.csv", destino, "d"))
        get.assert_not_called()
        self.assertEqual(ccaa_cataluna.stats['skipped'], 1)

    def test_respuesta_vacia_cuenta_como_fallo(self):
        destino = self.dir / "vacio.csv"
        with patch.object(ccaa_cataluna.session, "get", return_value=_resp(body=b"")):
            self.assertFalse(ccaa_cataluna.download_with_progress("https://x.test/v.csv", destino, "v"))
        self.assertFalse(destino.exists())
        self.assertEqual(ccaa_cataluna.stats['failed'], 1)

    def test_error_http_no_deja_archivo(self):
        destino = self.dir / "404.csv"
        with patch.object(ccaa_cataluna.session, "get", return_value=_resp(status=404)):
            self.assertFalse(ccaa_cataluna.download_with_progress("https://x.test/404.csv", destino, "x"))
        self.assertEqual(list(self.dir.iterdir()), [])
        self.assertEqual(ccaa_cataluna.stats['failed'], 1)

    def _socrata_con_fecha(self, rows_updated_at, contenido=b"id,nom\n1,nou\n"):
        pedidas = []

        def fake_get(url, timeout=None, stream=False, **kwargs):
            pedidas.append(url)
            if url.endswith("/hb6v-jcbf.json"):
                return _resp(json_data={'id': 'hb6v-jcbf', 'rowsUpdatedAt': rows_updated_at})
            return _resp(body=contenido)

        with patch.object(ccaa_cataluna, "SOCRATA_DATASETS", {'hb6v-jcbf': ('01_contratacion/registro', 'x')}), \
                patch.object(ccaa_cataluna.session, "get", side_effect=fake_get), \
                patch.object(ccaa_cataluna.time, "sleep"):
            ccaa_cataluna.download_socrata_datasets(self.dir)
        return pedidas

    def test_socrata_actualizado_en_el_portal_se_vuelve_a_descargar(self):
        # Antes un CSV ya descargado no se actualizaba nunca (registro de contratos,
        # PSCP... se quedaban congelados en la primera descarga)
        csv = self.dir / "01_transparencia_catalunya" / "01_contratacion" / "registro.csv"
        csv.parent.mkdir(parents=True)
        csv.write_bytes(b"id,nom\n1,vell\n")
        os.utime(csv, (1_000_000, 1_000_000))

        pedidas = self._socrata_con_fecha(500_000)  # la copia local es posterior: no se descarga
        self.assertFalse(any("rows.csv" in u for u in pedidas))
        self.assertEqual(csv.read_bytes(), b"id,nom\n1,vell\n")

        pedidas = self._socrata_con_fecha(2_000_000)  # el portal lo actualizó después
        self.assertTrue(any("rows.csv" in u for u in pedidas))
        self.assertEqual(csv.read_bytes(), b"id,nom\n1,nou\n")

        # Sin fecha en los metadatos se mantiene el comportamiento anterior (no se descarga)
        os.utime(csv, (1_000_000, 1_000_000))
        pedidas = self._socrata_con_fecha(None, contenido=b"id,nom\n1,otro\n")
        self.assertEqual(csv.read_bytes(), b"id,nom\n1,nou\n")

    def test_bcn_recurso_modificado_se_vuelve_a_descargar(self):
        carpeta = self.dir / "02_barcelona" / "contratos_menores"
        carpeta.mkdir(parents=True)
        viejo = carpeta / "2019_contractes_menors.csv"
        viejo.write_bytes(b"Id\n0\n")
        os.utime(viejo, (1_000_000, 1_000_000))
        self._descargar_bcn([
            {'id': 'a1', 'name': '2019_contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/a2019.csv',
             'last_modified': '2025-11-03T10:15:00.123456'},
        ])
        self.assertIn("a2019.csv", viejo.read_text())

    def test_epoch_ckan(self):
        self.assertEqual(ccaa_cataluna.epoch_ckan('1970-01-02T00:00:00'), 86400.0)
        self.assertEqual(ccaa_cataluna.epoch_ckan('1970-01-02T00:00:00Z'), 86400.0)
        self.assertIsNone(ccaa_cataluna.epoch_ckan(None))
        self.assertIsNone(ccaa_cataluna.epoch_ckan('no es fecha'))

    def test_metadatos_fallidos_se_registran(self):
        with patch.object(ccaa_cataluna, "SOCRATA_DATASETS", {'hb6v-jcbf': ('01_contratacion/registro', 'x')}), \
                patch.object(ccaa_cataluna.session, "get", return_value=_resp(status=503)), \
                patch.object(ccaa_cataluna.time, "sleep"), \
                self.assertLogs(level="INFO") as logs:
            ccaa_cataluna.download_socrata_metadata(self.dir)
        self.assertTrue(any("hb6v-jcbf" in m and "503" in m for m in logs.output), logs.output)

    def _descargar_bcn(self, recursos):
        with patch.object(ccaa_cataluna, "BCN_DATASETS", {'contractes-menors': 'contratos_menores'}), \
                patch.object(ccaa_cataluna.session, "get", side_effect=_fake_get_factory([])), \
                patch.object(ccaa_cataluna.time, "sleep"), \
                patch.dict(BCN_RECURSOS, {'contractes-menors': recursos}):
            ccaa_cataluna.download_barcelona_datasets(self.dir)
        return self.dir / "02_barcelona" / "contratos_menores"

    def test_bcn_recursos_con_mismo_nombre_no_se_pisan(self):
        carpeta = self._descargar_bcn([
            {'id': 'aaaa1111-1', 'name': 'contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/a2019.csv'},
            {'id': 'bbbb2222-2', 'name': 'contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/b2020.csv'},
            # El mismo recurso listado dos veces no debe duplicarse
            {'id': 'aaaa1111-1', 'name': 'contractes_menors', 'format': 'CSV', 'url': 'https://bcn.test/a2019.csv'},
        ])
        archivos = sorted(p.name for p in carpeta.iterdir())
        self.assertEqual(archivos, ['contractes_menors.csv', 'contractes_menors_bbbb2222.csv'])
        self.assertIn("a2019.csv", (carpeta / 'contractes_menors.csv').read_text())
        self.assertIn("b2020.csv", (carpeta / 'contractes_menors_bbbb2222.csv').read_text())

    def test_bcn_recurso_con_nombre_o_formato_null_no_aborta_el_dataset(self):
        carpeta = self._descargar_bcn([
            {'id': 'n1', 'name': None, 'format': 'CSV', 'url': 'https://bcn.test/sin_nombre.csv'},
            {'id': 'n2', 'name': 'raro', 'format': None, 'url': 'https://bcn.test/raro.bin'},
            {'id': 'n3', 'name': '2019_contractes', 'format': 'CSV', 'url': 'https://bcn.test/c2019.csv'},
        ])
        self.assertEqual(sorted(p.name for p in carpeta.iterdir()), ['2019_contractes.csv', 'unknown.csv'])

    def test_main_offline_descarga_todo_y_el_conversor_lo_procesa(self):
        """Pipeline completo documentado: ccaa_cataluna.py -> ccaa_cataluna_parquet.py"""
        salida = self.dir / "catalunya_datos_completos"
        urls = []
        with patch.object(ccaa_cataluna, "OUTPUT_DIR", str(salida)), \
                patch.object(ccaa_cataluna.session, "get", side_effect=_fake_get_factory(urls)), \
                patch.object(ccaa_cataluna.time, "sleep"):
            ccaa_cataluna.main()

        # --- Descarga: URLs, rutas y estadísticas ---
        self.assertEqual(ccaa_cataluna.stats['failed'], 0)
        for dataset_id, (subpath, _) in ccaa_cataluna.SOCRATA_DATASETS.items():
            self.assertIn(f"{ccaa_cataluna.SOCRATA_BASE}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD", urls)
            csv = salida / "01_transparencia_catalunya" / f"{subpath}.csv"
            self.assertTrue(csv.exists(), csv)
            meta = salida / "01_transparencia_catalunya" / "_metadata" / f"{subpath.split('/')[-1]}_metadata.json"
            self.assertEqual(json.loads(meta.read_text(encoding="utf-8"))['id'], dataset_id)
        for slug in ccaa_cataluna.BCN_DATASETS:
            self.assertIn(f"{ccaa_cataluna.BCN_BASE}/data/api/3/action/package_show?id={slug}", urls)
        menores = salida / "02_barcelona" / "contratos_menores"
        self.assertEqual(sorted(p.name for p in menores.iterdir()),
                         ['1T_2020_contractes_menors.csv', '2019_contractes_menors.csv', '2019_contractes_menors.json'])
        for nombre in ("INFORME.md", "INDICE.txt", "03_gencat_adicional/FUENTES_ADICIONALES.txt"):
            self.assertTrue((salida / nombre).exists(), nombre)
        self.assertFalse(list(salida.rglob("*.part")))
        # 2 filas por CSV Socrata + 2 por cada CSV de BCN
        n_csv_bcn = sum(1 for rs in BCN_RECURSOS.values() for r in rs if r['format'] == 'CSV')
        self.assertEqual(ccaa_cataluna.stats['records'], 2 * len(ccaa_cataluna.SOCRATA_DATASETS) + 2 * n_csv_bcn)

        # --- Todas las entradas del conversor apuntan a archivos que genera el descargador ---
        for csv_rel in cat_parquet.ARCHIVOS:
            self.assertTrue((salida / csv_rel).exists(), f"ARCHIVOS apunta a un CSV que no se descarga: {csv_rel}")

        # --- Conversión a Parquet ---
        parquet_dir = self.dir / "catalunya_parquet"
        with patch.object(cat_parquet, "INPUT_DIR", str(salida)), \
                patch.object(cat_parquet, "OUTPUT_DIR", str(parquet_dir)):
            cat_parquet.main()

        for parquet_rel, _ in cat_parquet.ARCHIVOS.values():
            self.assertTrue((parquet_dir / parquet_rel).exists(), parquet_rel)
        # Documentado en catalunya/README.md (antes nunca se generaba)
        self.assertTrue((parquet_dir / "contratacion" / "licitaciones_adjudicaciones.parquet").exists())
        for nombre in ("contratos_menores_bcn", "contratistas_bcn", "perfil_contratante_bcn",
                       "modificaciones_bcn", "resumen_trimestral_bcn", "contratos_menores_autorizacion_bcn"):
            self.assertTrue((parquet_dir / "contratacion" / f"{nombre}.parquet").exists(), nombre)
        self.assertTrue((parquet_dir / "README.md").exists())

        registro = pd.read_parquet(parquet_dir / "contratacion" / "contratos_registro.parquet")
        self.assertEqual(list(registro.columns), ["Codi", "Nom", "Import adjudicació", "Data formalització"])
        self.assertEqual(list(registro["Nom"]), ["Educació", ""])
        self.assertEqual(list(registro["Import adjudicació"]), [1234.5, 99.0])
        self.assertEqual(pd.to_datetime(registro["Data formalització"]).dt.year.iloc[0], 2020)

        menores_bcn = pd.read_parquet(parquet_dir / "contratacion" / "contratos_menores_bcn.parquet")
        self.assertEqual(len(menores_bcn), 4)
        self.assertEqual(sorted(set(menores_bcn["_año"])), [2019, 2020])
        autorizacion = pd.read_parquet(parquet_dir / "contratacion" / "contratos_menores_autorizacion_bcn.parquet")
        self.assertEqual((len(autorizacion), set(autorizacion["_año"])), (2, {2022}))
        # Todo lo que se descarga de Socrata se convierte (antes 4 datasets se quedaban en CSV)
        convertidos = {Path(csv_rel).relative_to("01_transparencia_catalunya").with_suffix("").as_posix()
                       for csv_rel in cat_parquet.ARCHIVOS}
        for dataset_id, (subpath, _) in ccaa_cataluna.SOCRATA_DATASETS.items():
            self.assertIn(subpath, convertidos, f"{dataset_id} se descarga pero no se convierte")
        resumen = pd.read_parquet(parquet_dir / "contratacion" / "resumen_trimestral_bcn.parquet")
        self.assertEqual(set(resumen["_año"]), {2019})
        perfil = pd.read_parquet(parquet_dir / "contratacion" / "perfil_contratante_bcn.parquet")
        self.assertEqual(set(perfil["_archivo_origen"]), {"perfil_contractant.csv"})


# =============================================================================
# ccaa_cataluna_parquet.py
# =============================================================================

class ConversorParquetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def _escribir(self, nombre, texto, encoding="utf-8"):
        path = self.dir / nombre
        path.write_bytes(texto.encode(encoding))
        return path

    def test_load_csv_punto_y_coma_con_coma_en_la_cabecera(self):
        path = self._escribir("semi.csv", "Any;Import (sense IVA, EUR);Adjudicatari\n2019;1234,56;ACME, SL\n2020;99;Foo\n")
        df = cat_parquet.load_csv(path)
        self.assertEqual(list(df.columns), ["Any", "Import (sense IVA, EUR)", "Adjudicatari"])
        self.assertEqual(list(df["Adjudicatari"]), ["ACME, SL", "Foo"])
        self.assertEqual(list(df["Any"]), [2019, 2020])

    def test_load_csv_coma_tabulador_y_latin1(self):
        coma = cat_parquet.load_csv(self._escribir("c.csv", 'a,b,"c;d"\n1,2,3\n'))
        self.assertEqual(list(coma.columns), ["a", "b", "c;d"])
        tab = cat_parquet.load_csv(self._escribir("t.csv", "a\tb\n1\t2\n"))
        self.assertEqual(list(tab.columns), ["a", "b"])
        latin = cat_parquet.load_csv(self._escribir("l.csv", "Nom;Població\nx;Girona\n", encoding="latin-1"))
        self.assertEqual(list(latin.columns), ["Nom", "Població"])

    def test_load_csv_ilegible_lanza_error(self):
        with self.assertRaises(ValueError):
            cat_parquet.load_csv(self._escribir("una_columna.csv", "solo\n1\n"))

    def test_textos_vacios_se_guardan_como_cadena_vacia_en_pandas_2_y_3(self):
        path = self._escribir("d.csv", "Nom,Import,Actiu,Data\nA,10.5,true,2020-01-01\n,,,\nC,3,false,\n")
        salida = self.dir / "out" / "d.parquet"
        n, _ = cat_parquet.convert_to_parquet(path, salida, "d")
        self.assertEqual(n, 3)
        tabla = pq.read_table(salida)
        self.assertEqual(tabla.column("Nom").to_pylist(), ["A", "", "C"])
        self.assertEqual(tabla.column("Actiu").to_pylist(), ["True", "", "False"])
        self.assertEqual(tabla.column("Data").to_pylist(), ["2020-01-01", "", ""])
        # Los numéricos siguen siendo numéricos con nulos
        self.assertEqual(tabla.column("Import").to_pylist(), [10.5, None, 3.0])

    def test_anio_de_nombre(self):
        casos = {
            "2019_contractes_menors": 2019,
            "1T_2019_resum": 2019,
            "Resum_2n_trimestre_2019": 2019,
            "OD_2019_1T": 2019,
            "contractistes_2012-2023": 2012,
            "20190101_export": 2019,
            "perfil_contractant": None,
        }
        for nombre, esperado in casos.items():
            self.assertEqual(cat_parquet.anio_de_nombre(nombre), esperado, nombre)

    def test_codigos_con_ceros_a_la_izquierda_se_guardan_tal_cual(self):
        # En los parquet publicados CODIPOSTAL empieza en 8002 y CODI_INE10 en 801930008
        filas = ["CODIPOSTAL,Nom,Import,CODI_INE10,Any"]
        filas += [f"4389{i % 10},m{i},{i}.5,{4300000000 + i},2020" for i in range(7)]
        filas += ["08002,Barcelona,1.5,0801930008,2021", "25001,Lleida,,,2022"]
        path = self._escribir("ens.csv", "\n".join(filas) + "\n")
        with patch.object(cat_parquet, "FILAS_POR_TROZO", 3):  # el 0 aparece en un trozo posterior
            df = cat_parquet.load_csv(path)
        self.assertEqual(list(df["CODIPOSTAL"])[-2:], ["08002", "25001"])
        self.assertEqual(df["CODI_INE10"].iloc[-2], "0801930008")
        self.assertTrue(pd.isna(df["CODI_INE10"].iloc[-1]))
        # Las columnas numéricas sin ceros a la izquierda no cambian
        self.assertTrue(pd.api.types.is_float_dtype(df["Import"]))
        self.assertTrue(pd.api.types.is_integer_dtype(df["Any"]))

        salida = self.dir / "out" / "ens.parquet"
        cat_parquet.convert_to_parquet(path, salida, "ens")
        tabla = pq.read_table(salida)
        self.assertEqual(tabla.column("CODIPOSTAL").to_pylist()[-2:], ["08002", "25001"])
        self.assertEqual(tabla.column("CODI_INE10").to_pylist()[-2:], ["0801930008", ""])

    def test_ceros_con_columnas_duplicadas_y_cero_decimal(self):
        path = self._escribir("dup.csv", "Codi,Codi,Valor\n01,5,0.5\n2,6,0\n")
        df = cat_parquet.load_csv(path)
        self.assertEqual(list(df.iloc[:, 0]), ["01", "2"])
        self.assertEqual(list(df.iloc[:, 1]), [5, 6])
        self.assertEqual(list(df["Valor"]), [0.5, 0.0])

    def test_lineas_mal_formadas_se_cuentan_y_se_avisa(self):
        path = self._escribir("malo.csv", "a,b,c\n1,2,3\n4,5,6,7\n8,9,10\n")
        with patch.object(cat_parquet, "log") as log:
            df = cat_parquet.load_csv(path)
        self.assertEqual(list(df["a"]), [1, 8])
        mensajes = [str(c.args[0]) for c in log.call_args_list]
        if tuple(int(x) for x in pd.__version__.split(".")[:2]) >= (2, 1):  # pandas 2.0 lo escribe en stderr
            self.assertTrue(any("1 líneas mal formadas" in m for m in mensajes), mensajes)

    def test_consolidacion_bcn_incluye_excel_y_json_sin_csv(self):
        entrada = self.dir / "in"
        carpeta = entrada / "02_barcelona" / "contratos_menores"
        carpeta.mkdir(parents=True)
        (carpeta / "2019_contractes_menors.csv").write_text("Id,Nom\n1,csv\n", encoding="utf-8")
        # Mismo recurso en JSON: no se duplica
        (carpeta / "2019_contractes_menors.json").write_text('[{"Id": 1, "Nom": "csv"}]', encoding="utf-8")
        # Recursos publicados solo en Excel o JSON: antes no llegaban al parquet
        pd.DataFrame({"Id": [2, 3], "Nom": ["xlsx", "xlsx"]}).to_excel(carpeta / "2020_contractes_menors.xlsx", index=False)
        (carpeta / "2021_contractes_menors.json").write_text(
            json.dumps({"result": {"records": [{"Id": 4, "Nom": "json"}]}}), encoding="utf-8")
        (carpeta / "llegeix-me.txt").write_text("no es un recurso", encoding="utf-8")

        n, _ = cat_parquet.consolidate_barcelona_menores(entrada, self.dir / "out")
        self.assertEqual(n, 4)
        df = pd.read_parquet(self.dir / "out" / "contratacion" / "contratos_menores_bcn.parquet")
        self.assertEqual(sorted(zip(df["Nom"], df["_año"])),
                         [("csv", 2019), ("json", 2021), ("xlsx", 2020), ("xlsx", 2020)])

    def test_consolidacion_bcn_sin_anio_no_escribe_none_literal(self):
        entrada = self.dir / "in"
        carpeta = entrada / "02_barcelona" / "contratos_menores"
        carpeta.mkdir(parents=True)
        (carpeta / "1T_2019_contractes.csv").write_text("Id,Nom\n1,a\n2,\n", encoding="utf-8")
        (carpeta / "contractes_historic.csv").write_text("Id,Nom\n3,c\n", encoding="utf-8")
        n, _ = cat_parquet.consolidate_barcelona_menores(entrada, self.dir / "out")
        self.assertEqual(n, 3)
        df = pd.read_parquet(self.dir / "out" / "contratacion" / "contratos_menores_bcn.parquet")
        self.assertEqual(list(df["_año"]), ["2019", "2019", ""])
        self.assertEqual(list(df["Nom"]), ["a", "", "c"])


# =============================================================================
# ccaa_cataluna_contratosmenores.py con una API falsa local (aiohttp)
# =============================================================================

NOMBRES_FASE = {10: 'ANUNCI_PREVI', 20: 'ADJUDICACIO', 800: 'FORMALITZACIO'}
FILTROS = {'faseVigent': '_fase', 'ambit': '_ambit', 'tipusContracte': '_tipus',
           'procedimentAdjudicacio': '_proc', 'organ': '_organ'}


def _registros(n, fase, inicio, organs=(1,), proc=401, tipus=393):
    regs = []
    for i in range(n):
        rid = inicio + i
        organ = organs[i % len(organs)]
        regs.append({
            'id': rid, 'titol': f'T{rid}', 'descripcio': f'Desc {rid}',
            'pressupostLicitacio': 100.0 + i, 'pressupostAdjudicacio': 90.0 + i,
            'organ': f'Organ {organ}', 'idOrgan': organ, 'codiExpedient': f'EXP-{rid}',
            'fasesVigents': {NOMBRES_FASE[fase]: {
                'lotsActius': 1, 'dataPublicacio': f'2024-01-{i % 28 + 1:02d}T10:00:00', 'idPublicacio': rid * 10}},
            # Atributos internos para filtrar/ordenar en la API falsa (no se devuelven)
            '_fase': fase, '_ambit': 1500001, '_tipus': tipus, '_proc': proc, '_organ': organ,
            '_orden': f'{i:08d}',
        })
    return regs


class APIFalsa:
    """Imita /cerca-avancada (filtros, orden, paginación, ventana de 10k) y /organs/noms"""

    def __init__(self, registros, organs=None, fallo=None):
        self.registros = registros
        self.organs = organs or {}
        self.fallo = fallo or (lambda query: None)
        self.peticiones = []

    async def cerca(self, request):
        q = request.query
        self.peticiones.append(('cerca', dict(q)))
        error = self.fallo(q)
        if error is not None:
            return error
        regs = self.registros
        for param, campo in FILTROS.items():
            if param in q:
                regs = [r for r in regs if r[campo] == int(q[param])]
        regs = sorted(regs, key=lambda r: r['_orden'], reverse=q.get('sortOrder') == 'desc')
        page, size = int(q['page']), int(q['size'])
        if (page + 1) * size > 10000:
            return web.json_response({'errorData': {'missatge': 'Result window is too large'}})
        contenido = [{k: v for k, v in r.items() if not k.startswith('_')} for r in regs[page * size:(page + 1) * size]]
        return web.json_response({'content': contenido, 'totalElements': len(regs)})

    async def organs_noms(self, request):
        q = request.query
        self.peticiones.append(('organs', dict(q)))
        error = self.fallo(q)
        if error is not None:
            return error
        ids = self.organs.get(int(q['ambitId']), [])
        page, size = int(q['page']), int(q['size'])
        return web.json_response([{'id': o, 'nom': f'Organ {o}'} for o in ids[page * size:(page + 1) * size]])


class ContratosMenoresTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.out = self.dir / "cm.parquet"
        self.addCleanup(self.tmp.cleanup)

    def _ejecutar(self, api, **kwargs):
        sleep_real = asyncio.sleep

        async def sleep_rapido(*args, **kw):
            await sleep_real(0)

        async def run():
            app = web.Application()
            app.router.add_get('/portal-api/cerca-avancada', api.cerca)
            app.router.add_get('/portal-api/organs/noms', api.organs_noms)
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '127.0.0.1', 0)
            await site.start()
            base = f"http://127.0.0.1:{runner.addresses[0][1]}/portal-api"
            try:
                with patch.object(cat_menores, 'BASE_URL', base), \
                        patch.object(cat_menores.asyncio, 'sleep', sleep_rapido), \
                        patch.object(cat_menores, 'FASES_NORMAL', [10, 20]), \
                        patch.object(cat_menores, 'FASES_AGREGADAS', [800]), \
                        patch.object(cat_menores, 'FASES_ALL', [10, 20, 800]):
                    await cat_menores.main(str(self.out), **kwargs)
            finally:
                await runner.cleanup()

        asyncio.run(run())

    def _checkpoint(self):
        return json.loads((self.dir / "cm_checkpoint.json").read_text())

    @staticmethod
    def _basicos():
        return _registros(250, 10, 1) + _registros(300, 20, 100000) + _registros(30, 800, 900000)

    def test_main_segmenta_hasta_organo_y_ambos_ordenes_sin_perder_registros(self):
        # Fase 20: 10.060 registros -> ambit -> tipus -> procediment -> organ;
        # el órgano 7 sigue >10k (10.020) y se descarga en orden desc + asc
        regs = (_registros(250, 10, 1)
                + _registros(10020, 20, 100000, organs=(7,))
                + _registros(40, 20, 200000, organs=(8,))
                + _registros(30, 800, 900000))
        duplicado = dict(regs[0], _fase=800)  # mismo (id, descripcio) en dos fases
        api = APIFalsa(regs + [duplicado], organs={1500001: [7, 8]})
        self._ejecutar(api)

        esperados = {r['id'] for r in regs}
        limpio = pd.read_parquet(self.out)
        crudo = pd.read_parquet(self.dir / "cm_raw.parquet")
        self.assertEqual(set(limpio['id']), esperados)
        self.assertEqual(len(limpio), len(esperados))
        self.assertEqual(len(crudo), len(esperados) + 1)
        self.assertFalse(limpio.duplicated(['id', 'descripcio']).any())

        for columna in ('id', 'descripcio', 'pressupostLicitacio', 'pressupostAdjudicacio', 'organ', 'idOrgan',
                        'codiExpedient', 'fasesVigents_ADJUDICACIO_dataPublicacio',
                        'fasesVigents_ANUNCI_PREVI_lotsActius', 'fasesVigents_FORMALITZACIO_idPublicacio'):
            self.assertIn(columna, limpio.columns)
        fila = limpio.set_index('id').loc[1]
        self.assertEqual(fila['pressupostLicitacio'], 100.0)
        self.assertEqual(fila['pressupostAdjudicacio'], 90.0)
        # El duplicado entre fases queda en una sola fila
        self.assertEqual(fila['descripcio'], 'Desc 1')

        cp = self._checkpoint()
        self.assertEqual(cp['completed_fases'], [10, 20, 800])
        analisis = json.loads((self.dir / "cm_duplicate_analysis.json").read_text())
        self.assertEqual((analisis['duplicate_rows'], analisis['duplicate_groups']), (2, 1))
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()),
                         ['cm.parquet', 'cm_checkpoint.json', 'cm_duplicate_analysis.json', 'cm_fase_10.parquet',
                          'cm_fase_20.parquet', 'cm_fase_800.parquet', 'cm_raw.parquet'])

        cerca = [q for tipo, q in api.peticiones if tipo == 'cerca']
        for q in cerca:
            self.assertEqual(q['inclourePublicacionsPlacsp'], 'false')
            self.assertEqual(q['sortField'], 'dataUltimaPublicacio')
            self.assertIn(q['size'], ('1', '100'))
        self.assertTrue(any(q.get('organ') == '7' and q['sortOrder'] == 'asc' for q in cerca))
        self.assertTrue(any(tipo == 'organs' and q['ambitId'] == '1500001' for tipo, q in api.peticiones))

    def test_recuento_fallido_no_marca_la_fase_como_completa_y_resume_la_recupera(self):
        def falla_recuento_fase_20(q):
            if q.get('faseVigent') == '20' and q.get('size') == '1':
                return web.Response(status=503)

        with self.assertRaises(RuntimeError):
            self._ejecutar(APIFalsa(self._basicos(), fallo=falla_recuento_fase_20))
        self.assertEqual(self._checkpoint()['completed_fases'], [10])
        self.assertFalse(self.out.exists())

        api = APIFalsa(self._basicos())
        self._ejecutar(api, resume=True)
        self.assertEqual(len(pd.read_parquet(self.out)), 580)
        self.assertFalse(any(q.get('faseVigent') == '10' for _, q in api.peticiones), "la fase 10 no se repite")

    def test_pagina_fallida_aborta_en_vez_de_devolver_segmento_truncado(self):
        def falla_pagina_1(q):
            if q.get('faseVigent') == '10' and q.get('size') == '100' and q.get('page') == '1':
                return web.Response(status=500)

        with self.assertRaises(RuntimeError):
            self._ejecutar(APIFalsa(self._basicos(), fallo=falla_pagina_1))
        self.assertFalse((self.dir / "cm_checkpoint.json").exists())

    def test_error_no_reintentable_tambien_aborta(self):
        def prohibido(q):
            if q.get('faseVigent') == '800':
                return web.Response(status=403)

        with self.assertRaises(RuntimeError):
            self._ejecutar(APIFalsa(self._basicos(), fallo=prohibido))
        self.assertEqual(self._checkpoint()['completed_fases'], [10, 20])

    def test_lista_de_organos_fallida_aborta(self):
        def falla_organos(q):
            if 'ambitId' in q:
                return web.Response(status=500)

        regs = _registros(10050, 20, 100000, organs=(7, 8))
        with self.assertRaises(RuntimeError):
            self._ejecutar(APIFalsa(regs, organs={1500001: [7, 8]}, fallo=falla_organos))
        self.assertEqual(self._checkpoint()['completed_fases'], [10])

    def test_fase_vacia_elimina_parquet_obsoleto_de_ejecucion_anterior(self):
        self._ejecutar(APIFalsa(self._basicos()))
        self.assertEqual(len(pd.read_parquet(self.out)), 580)
        # Nueva ejecución completa (sin --resume): la fase 800 ya no tiene registros
        self._ejecutar(APIFalsa(_registros(250, 10, 1) + _registros(300, 20, 100000)))
        self.assertEqual(len(pd.read_parquet(self.out)), 550)
        self.assertFalse((self.dir / "cm_fase_800.parquet").exists())

    def test_formato_csv_usa_extension_csv(self):
        self._ejecutar(APIFalsa(self._basicos()), output_format='csv')
        self.assertFalse(self.out.exists(), "no debe escribir un CSV con extensión .parquet")
        limpio = pd.read_csv(self.dir / "cm.csv", encoding="utf-8-sig")
        self.assertEqual(len(limpio), 580)
        self.assertTrue((self.dir / "cm_raw.csv").exists())

    def test_no_agregadas_y_cleanup(self):
        api = APIFalsa(self._basicos())
        self._ejecutar(api, include_agregadas=False, cleanup=True)
        self.assertEqual(len(pd.read_parquet(self.out)), 550)
        self.assertFalse(any(q.get('faseVigent') == '800' for _, q in api.peticiones))
        self.assertEqual(sorted(p.name for p in self.dir.iterdir()),
                         ['cm.parquet', 'cm_duplicate_analysis.json', 'cm_raw.parquet'])

    def test_hueco_de_cobertura_en_segmentacion_se_avisa_y_se_recupera(self):
        # 60 registros con un procediment que no está en PROCEDIMENTS: no salen en ningún
        # sub-segmento; el segmento (10.020 <= 2 ventanas) se pide entero en los dos órdenes
        regs = _registros(9960, 20, 100000) + _registros(60, 20, 300000, proc=999999)
        api = APIFalsa(regs)
        with self.assertLogs(cat_menores.logger, level=logging.INFO) as logs:
            self._ejecutar(api)
        self.assertTrue(any("coverage gap" in m and "9960 of 10020" in m for m in logs.output), logs.output)
        self.assertTrue(any("Gap recovery: +60" in m for m in logs.output), logs.output)
        self.assertFalse(any("Coverage gap remains" in m for m in logs.output), logs.output)
        limpio = pd.read_parquet(self.out)
        self.assertEqual(len(limpio), 10020)
        self.assertEqual(set(limpio['id']), {r['id'] for r in regs})
        # La recuperación pide el segmento padre (sin procediment) en orden ascendente
        self.assertTrue(any(q.get('tipusContracte') == '393' and 'procedimentAdjudicacio' not in q
                            and q['sortOrder'] == 'asc' for t, q in api.peticiones if t == 'cerca'))

    def test_hueco_grande_se_recupera_segmentando_por_organo(self):
        # 20.110 registros (> 2 ventanas): 50 con un tipus fuera de TIPUS_CONTRACTE.
        # No caben en dos ventanas: se vuelve a pedir el segmento por órgano.
        regs = (_registros(10030, 20, 100000, organs=(7,))
                + _registros(10030, 20, 200000, organs=(8,))
                + _registros(50, 20, 300000, organs=(8,), tipus=999999))
        api = APIFalsa(regs, organs={1500001: [7, 8]})
        with self.assertLogs(cat_menores.logger, level=logging.INFO) as logs:
            self._ejecutar(api)
        self.assertTrue(any("Gap recovery by organ" in m for m in logs.output), logs.output)
        self.assertFalse(any("Coverage gap remains" in m for m in logs.output), logs.output)
        limpio = pd.read_parquet(self.out)
        self.assertEqual(len(limpio), 20110)
        self.assertEqual(set(limpio['id']), {r['id'] for r in regs})
        self.assertFalse(limpio.duplicated().any())

    def test_contratos_distintos_con_mismo_id_y_descripcio_no_se_pierden(self):
        # Publicación agregada: varios contratos (expedientId distintos) con la misma
        # descripción. Antes se deduplicaba por (id, descripcio) y quedaba uno.
        def contrato(exp, importe, fase):
            return {'id': 500, 'descripcio': 'Material oficina', 'expedientId': f'uuid;{exp}',
                    'pressupostAdjudicacio': importe, 'esAgregatContractes': True,
                    'fasesVigents': {'ADJUDICACIO': {'lotsActius': 0, 'dataPublicacio': '2024-02-19T11:18:05.000Z',
                                                     'idPublicacio': 500}},
                    '_fase': fase, '_ambit': 1500001, '_tipus': 393, '_proc': 403, '_organ': 1, '_orden': exp}
        regs = [contrato('1', 100.0, 20), contrato('2', 250.0, 20), contrato('3', 100.0, 20),
                contrato('1', 100.0, 800)]  # la misma publicación vuelve en otra fase: copia idéntica
        self._ejecutar(APIFalsa(regs))
        limpio = pd.read_parquet(self.out)
        crudo = pd.read_parquet(self.dir / "cm_raw.parquet")
        self.assertEqual(len(crudo), 4)
        self.assertEqual(sorted(limpio['expedientId']), ['uuid;1', 'uuid;2', 'uuid;3'])
        self.assertEqual(sorted(limpio['pressupostAdjudicacio']), [100.0, 100.0, 250.0])
        analisis = json.loads((self.dir / "cm_duplicate_analysis.json").read_text())
        self.assertEqual(analisis['identical_rows_removed'], 1)
        self.assertEqual(analisis['rows_clean'], 3)

    def test_total_sin_filtro_de_fase_detecta_fases_no_consultadas(self):
        # 40 registros cuya fase (30) no está en FASES_ALL: la API los cuenta sin filtro
        regs = self._basicos() + _registros(40, 10, 700000)
        for r in regs[-40:]:
            r['_fase'] = 30
        api = APIFalsa(regs)
        with self.assertLogs(cat_menores.logger, level=logging.WARNING) as logs:
            self._ejecutar(api)
        self.assertEqual(len(pd.read_parquet(self.out)), 580)
        analisis = json.loads((self.dir / "cm_duplicate_analysis.json").read_text())
        self.assertEqual(analisis['api_total_without_phase_filter'], 620)
        self.assertTrue(any("620 records without phase filter" in m for m in logs.output), logs.output)
        self.assertTrue(any('faseVigent' not in q for t, q in api.peticiones if t == 'cerca'))

    def test_incloure_placsp_se_envia_a_la_api(self):
        api = APIFalsa(self._basicos())
        with patch.object(cat_menores, 'INCLOURE_PLACSP', 'true'):
            self._ejecutar(api)
        cerca = [q for t, q in api.peticiones if t == 'cerca']
        self.assertTrue(cerca)
        self.assertTrue(all(q['inclourePublicacionsPlacsp'] == 'true' for q in cerca))


if __name__ == "__main__":
    unittest.main()
