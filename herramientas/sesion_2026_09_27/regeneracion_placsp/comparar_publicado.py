"""Contraste de la regeneración con el publicado v2026.02 (y con las cifras de la PR #24).

Sobre los mismos ficheros de origen que el publicado (inventario de sus
`archivo_origen`): filas por fichero, filas comparables sin ambigüedad por
versión (id + fecha_updated + archivo_origen únicos en los dos lados), cambios
de importe_sin_iva, si el importe_sin_iva antiguo es el valor estimado nuevo, y
diferencias en importe_con_iva.
Uso: python comparar_publicado.py <nacional regenerado> <licitaciones_espana.parquet v2026.02> <salida.json>
"""
import json
import sys

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds

nuevo_p, viejo_p, salida = sys.argv[1:4]
COLS = ['id', 'fecha_updated', 'archivo_origen', 'conjunto', 'importe_sin_iva', 'importe_con_iva']
d_viejo, d_nuevo = ds.dataset(viejo_p), ds.dataset(nuevo_p)
cols_nuevo = [c for c in COLS + ['valor_estimado_contrato', '_origen'] if c in d_nuevo.schema.names]
NULO = np.iinfo('int64').min


def leer(d, columnas, archivo):
    """Filas de un fichero de origen (filtro en Arrow: memoria acotada a ese fichero)."""
    df = d.to_table(columns=columnas, filter=ds.field('archivo_origen') == archivo).to_pandas()
    df['_t'] = pd.to_datetime(df['fecha_updated'], utc=True, errors='coerce').astype('datetime64[us, UTC]').astype('int64')
    return df.drop(columns=['fecha_updated'])


def distinto(a, b):
    # a float64 con NaN: con dtypes nullable (Float64, pandas 3) NaN == x da <NA> y .sum()
    # se saltaba esas filas (publicado vacío frente a regenerado informado)
    a = pd.to_numeric(a, errors='coerce').astype('float64')
    b = pd.to_numeric(b, errors='coerce').astype('float64')
    iguales = (a == b) | (a.isna() & b.isna()) | (a.notna() & b.notna() & np.isclose(a.fillna(0), b.fillna(0)))
    return ~iguales


def cuenta(d, filtro=None):
    return d.count_rows(filter=filtro) if filtro is not None else d.count_rows()


archivos_viejo = pc.unique(d_viejo.to_table(columns=['archivo_origen']).column('archivo_origen')).to_pylist()
inventario = sorted(str(a) for a in archivos_viejo if a is not None)
leido_hoy = ds.field('_origen').is_null() if '_origen' in d_nuevo.schema.names else None
r = {'filas_publicado': cuenta(d_viejo), 'filas_regenerado': cuenta(d_nuevo),
     'filas_regenerado_leidas_hoy': cuenta(d_nuevo, leido_hoy),
     'ficheros_inventario': len(inventario)}
r['filas_regenerado_de_la_semilla'] = r['filas_regenerado'] - r['filas_regenerado_leidas_hoy']
filas, suma = [], dict(comparables=0, sin_iva=0, vec=0, con_iva=0, mismo=0)
for archivo in inventario:
    viejo = leer(d_viejo, COLS, archivo)
    nuevo = leer(d_nuevo, cols_nuevo, archivo)
    if '_origen' in nuevo:
        nuevo = nuevo[nuevo['_origen'].isna()]
    suma['mismo'] += len(nuevo)
    filas.append({'archivo_origen': archivo, 'publicado': len(viejo), 'regenerado': len(nuevo)})
    clave = ['id', '_t']
    v1 = viejo[~viejo.duplicated(clave, keep=False) & (viejo['_t'] != NULO)]
    n1 = nuevo[~nuevo.duplicated(clave, keep=False)]
    comp = v1.merge(n1, on=clave, suffixes=('_pub', '_reg'))
    suma['comparables'] += len(comp)
    suma['sin_iva'] += int(distinto(comp['importe_sin_iva_pub'], comp['importe_sin_iva_reg']).sum())
    if 'valor_estimado_contrato' in comp:
        suma['vec'] += int(distinto(comp['importe_sin_iva_pub'], comp['valor_estimado_contrato']).sum())
    suma['con_iva'] += int(distinto(comp['importe_con_iva_pub'], comp['importe_con_iva_reg']).sum())
    del viejo, nuevo, v1, n1, comp
por_fichero = pd.DataFrame(filas).set_index('archivo_origen')
por_fichero['diferencia'] = por_fichero['regenerado'] - por_fichero['publicado']
r.update({'filas_regenerado_mismo_inventario': suma['mismo'],
          'ficheros_con_el_mismo_recuento': int((por_fichero['diferencia'] == 0).sum()),
          'ficheros_distintos': por_fichero[por_fichero['diferencia'] != 0].reset_index().to_dict('records'),
          'filas_comparables': suma['comparables'], 'importe_sin_iva_cambiado': suma['sin_iva'],
          'importe_sin_iva_publicado_distinto_de_valor_estimado_regenerado': suma['vec'],
          'importe_con_iva_distinto': suma['con_iva']})
r['referencia_pr24'] = {'filas_reconstruidas': 8721484, 'filas_comparables': 8612333,
                        'importe_sin_iva_cambiado': 4383029, 'valor_estimado_distinto': 0, 'importe_con_iva_distinto': 0,
                        'ficheros_mismo_recuento': 73}
json.dump(r, open(salida, 'w', encoding='utf-8'), ensure_ascii=False, indent=2, default=int)
print(json.dumps({k: v for k, v in r.items() if k != 'ficheros_distintos'}, ensure_ascii=False, indent=2, default=int))
print(por_fichero[por_fichero['diferencia'] != 0].to_string())
