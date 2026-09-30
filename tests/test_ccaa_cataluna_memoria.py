"""Consolidación de ccaa_cataluna_contratosmenores.py con poca memoria.

La primera descarga completa (4,8 M de filas y 43 columnas en el crudo) se quedó sin memoria
en el análisis de duplicados (pico de 10,6 GiB); al repetir la consolidación, comparar el crudo
nuevo con el anterior llegaba a 11 GiB. Los pasos reescritos tienen que dar
exactamente lo mismo que el código anterior, que se copia aquí tal cual como referencia
(main en c889c2d), y no volver a copiar la tabla entera.
"""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "ccaa_cataluna_contratosmenores", REPO_ROOT / "scripts" / "ccaa_cataluna_contratosmenores.py")
cat_menores = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cat_menores)


# =============================================================================
# Referencia: el código anterior, sin cambios
# =============================================================================

def analisis_anterior(df, key_cols):
    dupes_mask = df.duplicated(subset=key_cols, keep=False)
    dupes = df[dupes_mask].copy()
    if len(dupes) == 0:
        return {'duplicate_rows': 0, 'duplicate_groups': 0, 'differing_columns': []}
    n_dupe_rows = len(dupes)
    n_dupe_groups = dupes.groupby(key_cols).ngroups
    differing_cols = []
    non_key_cols = [c for c in df.columns if c not in key_cols]
    for col in non_key_cols:
        try:
            nunique = dupes.groupby(key_cols)[col].nunique()
            if (nunique > 1).any():
                n_groups_differ = (nunique > 1).sum()
                differing_cols.append({
                    'column': col,
                    'groups_with_differences': int(n_groups_differ),
                    'pct_groups': float(n_groups_differ / n_dupe_groups * 100)
                })
        except Exception:
            pass
    differing_cols.sort(key=lambda x: x['groups_with_differences'], reverse=True)
    return {'duplicate_rows': int(n_dupe_rows), 'duplicate_groups': int(n_dupe_groups),
            'differing_columns': differing_cols}


def comparables_anterior(df):
    def a_texto(v):
        if isinstance(v, np.ndarray):
            v = v.tolist()
        return json.dumps(v, sort_keys=True, ensure_ascii=False, default=str)
    out = df
    for col in df.columns:
        if df[col].dtype != object:
            continue
        anidado = df[col].map(lambda v: isinstance(v, (list, dict, np.ndarray)))
        if anidado.any():
            if out is df:
                out = df.copy()
            out[col] = df[col].where(~anidado, df[col].map(a_texto))
    return out


def copias_anterior(df):
    copias = comparables_anterior(df).duplicated(keep='first')
    return df[~copias].reset_index(drop=True)


def huellas_anterior(*tablas):
    columnas = sorted({str(c) for t in tablas for c in t.columns if not str(c).startswith('_')})
    huellas = []
    for tabla in tablas:
        h = np.full(len(tabla), 0x345678, dtype=np.uint64)
        mult = np.uint64(1000003)
        for i, columna in enumerate(columnas):
            if columna in tabla.columns:
                texto = cat_menores._texto_comparable(tabla[columna])
            else:
                texto = np.full(len(tabla), None, dtype=object)
            h = (h ^ pd.util.hash_array(texto)) * mult
            mult = np.uint64(int(mult) + 82520 + 2 * (len(columnas) - i))
        huellas.append(h)
    return huellas


def mismas_filas_anterior(df, ruta):
    import pyarrow.parquet as pq
    try:
        fichero = pq.ParquetFile(ruta)
        if (fichero.metadata.num_rows != len(df)
                or set(fichero.schema_arrow.names) != set(map(str, df.columns))):
            return False
        anterior = pd.read_parquet(ruta)
    except Exception:
        return False
    if len(anterior) != len(df) or set(map(str, anterior.columns)) != set(map(str, df.columns)):
        return False
    h_anterior, h_nuevo = huellas_anterior(anterior, df)
    return bool(np.array_equal(np.sort(h_anterior), np.sort(h_nuevo)))


# =============================================================================
# Datos: crudos con copias, claves repetidas, nulos y tipos mezclados
# =============================================================================

def crudo_aleatorio(semilla, n=600, anidadas=False, mixto=True):
    """Un crudo como el de la API: pocas publicaciones repetidas muchas veces (copias
    idénticas), la misma clave con contenido distinto, nulos en la clave y en los datos,
    y columnas object con tipos mezclados (1, 1.0, '1', True...; mixto=False sin ellas, que
    no se pueden escribir en parquet)."""
    rng = np.random.default_rng(semilla)
    base = 60
    ids = rng.integers(0, 25, base).astype(float)
    ids[rng.random(base) < 0.08] = np.nan
    exp = rng.choice(np.array(['u-1', 'u-2', 'u-3;1', 'u-3;2', ''], dtype=object), base)
    exp[rng.random(base) < 0.08] = None
    pool = pd.DataFrame({
        'id': ids,
        'expedientId': exp,
        'titol': rng.choice(np.array(['Obra', 'Servei', 'Subministrament', None], dtype=object), base),
        'pressupostAdjudicacio': rng.choice([100.0, 250.5, 0.0, -0.0, np.nan], base),
        'esAgregatContractes': rng.choice(np.array([True, False, None], dtype=object), base),
        'mixto': rng.choice(np.array([1, 1.0, '1', True, 'x', None, np.nan], dtype=object), base),
        'idOrgan': rng.integers(1, 6, base),
        'fecha': pd.to_datetime(rng.choice(['2024-01-01', '2025-06-30', None], base)),
    })
    if not mixto:
        pool = pool.drop(columns='mixto')
    if anidadas:
        valores = [[1, 2], {'b': 1, 'a': 2}, np.array([3, 4]), 'texto', None, [1, 2]]
        pool['fasesAnidadas'] = pd.Series([valores[i % len(valores)] for i in rng.integers(0, 6, base)],
                                          dtype=object)
    filas = rng.integers(0, base, n)
    df = pool.iloc[filas].reset_index(drop=True)
    # Algunas copias con un valor cambiado: misma clave, contenido distinto
    cambiar = rng.random(n) < 0.15
    df.loc[cambiar, 'titol'] = 'Cambiado'
    return df


def sin_copiar_la_tabla(df):
    """Contexto en el que copiar un DataFrame del tamaño de df (la tabla entera) falla; las
    copias de lo que queda tras filtrar (reset_index) no cuentan."""
    copia_real = pd.DataFrame.copy

    def copia(self, *args, **kwargs):
        if len(self) == len(df):
            raise AssertionError('copia de la tabla entera')
        return copia_real(self, *args, **kwargs)

    return patch.object(pd.DataFrame, 'copy', autospec=True, side_effect=copia)


def ida_y_vuelta(df, carpeta, nombre='t.parquet'):
    """El crudo tal como lo lee la consolidación (de los ficheros de fase)."""
    ruta = Path(carpeta) / nombre
    df.to_parquet(ruta, index=False)
    return pd.read_parquet(ruta)


class AnalisisDuplicadosTests(unittest.TestCase):

    def test_da_lo_mismo_que_el_codigo_anterior(self):
        with tempfile.TemporaryDirectory() as tmp:
            for semilla in range(12):
                for df in (crudo_aleatorio(semilla), ida_y_vuelta(crudo_aleatorio(semilla, mixto=False), tmp)):
                    for clave in (['id', 'expedientId'], ['id', 'titol'], ['expedientId']):
                        with self.subTest(semilla=semilla, clave=clave, dtypes=str(df.dtypes.tolist())):
                            self.assertEqual(cat_menores.analyze_duplicates(df, clave),
                                             analisis_anterior(df, clave))

    def test_nulos_en_la_clave_repetidas_pero_sin_grupo(self):
        # duplicated cuenta nulo == nulo; groupby no hace grupo de una clave con nulo
        df = pd.DataFrame({'id': [1, 1, np.nan, np.nan, 2], 'expedientId': ['a', 'a', 'b', 'b', 'c'],
                           'titol': ['x', 'y', 'p', 'q', 'z']})
        esperado = analisis_anterior(df, ['id', 'expedientId'])
        self.assertEqual(esperado['duplicate_rows'], 4)
        self.assertEqual(esperado['duplicate_groups'], 1)
        self.assertEqual(cat_menores.analyze_duplicates(df, ['id', 'expedientId']), esperado)
        solo_nulos = df.iloc[2:4].reset_index(drop=True)
        self.assertEqual(cat_menores.analyze_duplicates(solo_nulos, ['id', 'expedientId']),
                         analisis_anterior(solo_nulos, ['id', 'expedientId']))

    def test_columnas_no_comparables_y_sin_repetidas(self):
        df = crudo_aleatorio(3, anidadas=True)
        # Una lista solo en una fila sin repetir: nunique solo veía las repetidas
        unica = pd.DataFrame({'id': [99.0], 'expedientId': ['única'], 'fasesAnidadas': [[9]]})
        df = pd.concat([df, unica], ignore_index=True)
        resultado = cat_menores.analyze_duplicates(df, ['id', 'expedientId'])
        self.assertEqual(resultado, analisis_anterior(df, ['id', 'expedientId']))
        self.assertNotIn('fasesAnidadas', [c['column'] for c in resultado['differing_columns']])
        sin = df.drop_duplicates(subset=['id', 'expedientId'])
        self.assertEqual(cat_menores.analyze_duplicates(sin, ['id', 'expedientId']),
                         {'duplicate_rows': 0, 'duplicate_groups': 0, 'differing_columns': []})

    def test_no_copia_la_tabla_ni_agrupa_por_columna(self):
        df = crudo_aleatorio(5)
        esperado = analisis_anterior(df, ['id', 'expedientId'])
        with sin_copiar_la_tabla(df), patch.object(pd.DataFrame, 'groupby', side_effect=AssertionError('groupby')):
            self.assertEqual(cat_menores.analyze_duplicates(df, ['id', 'expedientId']), esperado)


class CopiasIdenticasTests(unittest.TestCase):

    def test_da_lo_mismo_que_el_codigo_anterior(self):
        with tempfile.TemporaryDirectory() as tmp:
            for semilla in range(12):
                for anidadas in (False, True):
                    crudo = crudo_aleatorio(semilla, anidadas=anidadas)
                    tablas = [crudo] if anidadas else [crudo, ida_y_vuelta(crudo.drop(columns='mixto'), tmp)]
                    for df in tablas:
                        with self.subTest(semilla=semilla, anidadas=anidadas):
                            limpio = cat_menores.quitar_copias_identicas(df)
                            pd.testing.assert_frame_equal(limpio, copias_anterior(df))
                            self.assertLess(len(limpio), len(df))

    def test_nulo_igual_a_nulo_y_tipos_mezclados(self):
        df = pd.DataFrame({'a': [1, 1, np.nan, np.nan, 1.0, True],
                           'b': pd.Series([None, None, 'x', 'x', None, None], dtype=object),
                           'c': pd.Series([[1], [1], {'k': 1}, {'k': 1}, [1], [1]], dtype=object)})
        limpio = cat_menores.quitar_copias_identicas(df)
        pd.testing.assert_frame_equal(limpio, copias_anterior(df))
        self.assertEqual(len(limpio), 2)

    def test_tabla_vacia_o_sin_columnas(self):
        for df in (pd.DataFrame({'a': pd.Series([], dtype=float)}), pd.DataFrame()):
            pd.testing.assert_frame_equal(cat_menores.quitar_copias_identicas(df), copias_anterior(df))

    def test_no_copia_la_tabla(self):
        df = crudo_aleatorio(7, anidadas=True)
        esperado = copias_anterior(df)
        with sin_copiar_la_tabla(df), \
                patch.object(pd.DataFrame, 'duplicated', side_effect=AssertionError('duplicated de la tabla')):
            limpio = cat_menores.quitar_copias_identicas(df)
        pd.testing.assert_frame_equal(limpio, esperado)


class MismasFilasTests(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def _escrito(self, df, nombre='crudo.parquet'):
        ruta = self.dir / nombre
        cat_menores._tipos_estables(df).to_parquet(ruta, index=False)
        return ruta

    def _casos(self, crudo):
        otro_orden = crudo.sample(frac=1, random_state=1).reset_index(drop=True)
        cambiado = otro_orden.copy()
        cambiado.loc[3, 'titol'] = 'otro'
        tipo = otro_orden.assign(idOrgan=otro_orden['idOrgan'].astype(float))
        return {'otro orden': otro_orden, 'un valor cambiado': cambiado, 'otro tipo': tipo,
                'una fila menos': otro_orden.iloc[1:], 'otra columna': otro_orden.assign(nueva='x'),
                'sin una columna': otro_orden.drop(columns='titol'),
                'control distinta': otro_orden.assign(_fase=1)}

    def test_da_lo_mismo_que_el_codigo_anterior(self):
        for semilla in range(6):
            crudo = pd.read_parquet(self._escrito(crudo_aleatorio(semilla, mixto=False)))
            ruta = self._escrito(crudo)
            for caso, df in self._casos(crudo).items():
                with self.subTest(semilla=semilla, caso=caso):
                    self.assertEqual(cat_menores._mismas_filas(df, ruta, 'parquet'),
                                     mismas_filas_anterior(df, ruta))
            self.assertTrue(cat_menores._mismas_filas(self._casos(crudo)['otro orden'], ruta, 'parquet'))
            self.assertFalse(cat_menores._mismas_filas(self._casos(crudo)['un valor cambiado'], ruta, 'parquet'))

    def test_lee_el_fichero_una_columna_cada_vez(self):
        crudo = pd.read_parquet(self._escrito(crudo_aleatorio(2, mixto=False)))
        ruta = self._escrito(crudo)
        lecturas = []
        leer = pd.read_parquet

        def espia(ruta, *args, **kwargs):
            lecturas.append(kwargs.get('columns'))
            return leer(ruta, *args, **kwargs)

        with patch.object(cat_menores.pd, 'read_parquet', side_effect=espia):
            self.assertTrue(cat_menores._mismas_filas(crudo.iloc[::-1], ruta, 'parquet'))
        self.assertTrue(lecturas)
        self.assertTrue(all(c is not None and len(c) == 1 for c in lecturas), lecturas)

    def test_fichero_ilegible_no_son_las_mismas_filas(self):
        crudo = pd.read_parquet(self._escrito(crudo_aleatorio(4, mixto=False)))
        ruta = self._escrito(crudo)
        leer = pd.read_parquet

        def falla_en_titol(ruta, *args, **kwargs):
            if kwargs.get('columns') == ['titol']:
                raise OSError('bloque ilegible')
            return leer(ruta, *args, **kwargs)

        with patch.object(cat_menores.pd, 'read_parquet', side_effect=falla_en_titol):
            self.assertFalse(cat_menores._mismas_filas(crudo, ruta, 'parquet'))

    def test_columna_con_otro_nombre_se_compara_leyendo_todo(self):
        # Si el fichero no da una columna con su nombre (etiquetas no de texto en el
        # pandas que lo escribió), se compara como antes, leyéndolo entero
        crudo = pd.read_parquet(self._escrito(crudo_aleatorio(6, mixto=False)))
        ruta = self._escrito(crudo)
        leer = pd.read_parquet

        def renombra(ruta, *args, **kwargs):
            df = leer(ruta, *args, **kwargs)
            return df.rename(columns={'titol': 'otro'}) if kwargs.get('columns') == ['titol'] else df

        for df, esperado in ((crudo.iloc[::-1], True), (self._casos(crudo)['un valor cambiado'], False)):
            with patch.object(cat_menores.pd, 'read_parquet', side_effect=renombra):
                self.assertEqual(cat_menores._mismas_filas(df, ruta, 'parquet'), esperado)

    def test_huellas_contenido_iguales_a_las_de_antes(self):
        a = crudo_aleatorio(8)
        b = pd.read_parquet(self._escrito(crudo_aleatorio(9, mixto=False))).assign(nueva='x', _control=1)
        for nueva, anterior in zip(cat_menores.huellas_contenido(a, b), huellas_anterior(a, b)):
            np.testing.assert_array_equal(nueva, anterior)


if __name__ == "__main__":
    unittest.main()
