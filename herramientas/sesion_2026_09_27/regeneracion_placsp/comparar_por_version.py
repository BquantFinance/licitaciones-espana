"""Contraste por versión (id + fecha_updated) con el publicado v2026.02, sin mirar
el fichero de origen (el publicado usa ZIP mensuales de 2025-2026 y la
regeneración los anuales). Solo claves únicas en los dos lados y con fecha.
Uso: python comparar_por_version.py <regenerado> <v2026.02> <salida.json>"""
import json
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

nuevo_p, viejo_p, salida = sys.argv[1:4]


def leer(ruta, cols):
    df = pq.read_table(ruta, columns=cols).to_pandas()
    df['_t'] = pd.to_datetime(df.pop('fecha_updated'), utc=True, errors='coerce').astype('datetime64[us, UTC]').astype('int64')
    df = df[df['_t'] != np.iinfo('int64').min]
    return df[~df.duplicated(['id', '_t'], keep=False)]


def distinto(a, b):
    # a float64 con NaN: con dtypes nullable (Float64, pandas 3) NaN == x da <NA> y .sum()
    # se saltaba esas filas (publicado vacío frente a regenerado informado)
    a = pd.to_numeric(a, errors='coerce').astype('float64')
    b = pd.to_numeric(b, errors='coerce').astype('float64')
    return ~((a == b) | (a.isna() & b.isna()) | (a.notna() & b.notna() & np.isclose(a.fillna(0), b.fillna(0))))


viejo = leer(viejo_p, ['id', 'fecha_updated', 'importe_sin_iva', 'importe_con_iva'])
nuevo = leer(nuevo_p, ['id', 'fecha_updated', 'importe_sin_iva', 'importe_con_iva', 'valor_estimado_contrato'])
comp = viejo.merge(nuevo, on=['id', '_t'], suffixes=('_pub', '_reg'))
r = {'versiones_unicas_publicado': len(viejo), 'versiones_unicas_regenerado': len(nuevo),
     'filas_comparables': len(comp),
     'publicado_sin_pareja': len(viejo) - len(comp),
     'importe_sin_iva_cambiado': int(distinto(comp['importe_sin_iva_pub'], comp['importe_sin_iva_reg']).sum()),
     'importe_sin_iva_publicado_distinto_de_valor_estimado_regenerado':
         int(distinto(comp['importe_sin_iva_pub'], comp['valor_estimado_contrato']).sum()),
     'importe_con_iva_distinto': int(distinto(comp['importe_con_iva_pub'], comp['importe_con_iva_reg']).sum())}
json.dump(r, open(salida, 'w'), indent=2)
print(json.dumps(r, indent=2))
