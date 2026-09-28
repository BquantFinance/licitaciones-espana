"""Contratos menores de PLACSP (feed 1143) por CCAA (NUTS de ejecución), año de
adjudicación y tipo de órgano (prefijo DIR3). Uso: python medir_placsp.py <parquet> <salida_prefijo>"""
import sys

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)
NUTS2 = {'ES11': 'Galicia', 'ES12': 'Asturias', 'ES13': 'Cantabria', 'ES21': 'País Vasco', 'ES22': 'Navarra',
         'ES23': 'La Rioja', 'ES24': 'Aragón', 'ES30': 'Madrid', 'ES41': 'Castilla y León', 'ES42': 'Castilla-La Mancha',
         'ES43': 'Extremadura', 'ES51': 'Catalunya', 'ES52': 'C. Valenciana', 'ES53': 'Illes Balears', 'ES61': 'Andalucía',
         'ES62': 'Murcia', 'ES63': 'Ceuta', 'ES64': 'Melilla', 'ES70': 'Canarias'}
ruta, salida = sys.argv[1], sys.argv[2]
columnas = ['id', 'conjunto', 'dir3_organo', 'nuts', 'ano', 'fecha_adjudicacion']
df = pq.read_table(ruta, columns=columnas, filters=[('conjunto', '=', 'menores')]).to_pandas()
df = df.drop_duplicates('id', keep='last')    # un contrato menor = un id
df['ccaa'] = df['nuts'].astype(str).str[:4].map(NUTS2).fillna('(sin región)')
df['anio'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce').dt.year.fillna(df['ano']).astype('Int64')
d = df['dir3_organo'].astype('string')
def empieza(*p):
    return d.str.startswith(p).fillna(False).to_numpy(bool)
df['tipo'] = np.select([empieza('A0', 'A1'), empieza('L01'), empieza('L02', 'L03'), empieza('L'), empieza('E'),
                        empieza('U'), d.isna().to_numpy(bool)],
                       ['Autonómica', 'Ayuntamiento', 'Diputación/Cabildo/Consell', 'Otra local', 'Estado (AGE)',
                        'Universidad', '(sin DIR3)'], 'Otra')
sub = df[df['anio'].between(2018, 2025)]
t = sub.pivot_table(index='ccaa', columns='anio', values='id', aggfunc='count', fill_value=0)
t['total'] = t.sum(axis=1)
t = t.sort_values('total', ascending=False)
print(f'{len(df):,} contratos menores distintos; {len(sub):,} adjudicados en 2018-2025')
print('\nPOR CCAA (NUTS de ejecución) Y AÑO DE ADJUDICACIÓN')
print(t.to_string())
u = sub.pivot_table(index='ccaa', columns='tipo', values='id', aggfunc='count', fill_value=0).loc[t.index]
print('\nPOR TIPO DE ÓRGANO (2018-2025)')
print(u.to_string())
t.to_csv(f'{salida}_ccaa_anio.csv')
u.to_csv(f'{salida}_ccaa_tipo.csv')
