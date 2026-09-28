"""¿Cuántos ayuntamientos publican contratos menores en la PLACSP (feed 1143)?

Por CCAA y año de adjudicación: municipios distintos con al menos un contrato
menor de un órgano del ayuntamiento, frente al número de municipios del INE.
El municipio sale del DIR3 (L01 + código INE de 5 cifras + control) o, sin DIR3,
de la ruta 'dependencia' (… > ENTIDADES LOCALES > CCAA > Provincia >
Ayuntamientos > Nombre), casada con el INE por los órganos que traen las dos.
Uso: python municipios_placsp.py <parquet PLACSP> <salida.csv>
"""
import sys

import pandas as pd
import pyarrow.parquet as pq

PROV_CCAA = {
    '01': 'País Vasco', '20': 'País Vasco', '48': 'País Vasco', '02': 'Castilla-La Mancha', '13': 'Castilla-La Mancha',
    '16': 'Castilla-La Mancha', '19': 'Castilla-La Mancha', '45': 'Castilla-La Mancha', '03': 'C. Valenciana',
    '12': 'C. Valenciana', '46': 'C. Valenciana', '04': 'Andalucía', '11': 'Andalucía', '14': 'Andalucía',
    '18': 'Andalucía', '21': 'Andalucía', '23': 'Andalucía', '29': 'Andalucía', '41': 'Andalucía',
    '05': 'Castilla y León', '09': 'Castilla y León', '24': 'Castilla y León', '34': 'Castilla y León',
    '37': 'Castilla y León', '40': 'Castilla y León', '42': 'Castilla y León', '47': 'Castilla y León',
    '49': 'Castilla y León', '06': 'Extremadura', '10': 'Extremadura', '07': 'Illes Balears', '08': 'Catalunya',
    '17': 'Catalunya', '25': 'Catalunya', '43': 'Catalunya', '15': 'Galicia', '27': 'Galicia', '32': 'Galicia',
    '36': 'Galicia', '22': 'Aragón', '44': 'Aragón', '50': 'Aragón', '26': 'La Rioja', '28': 'Madrid', '30': 'Murcia',
    '31': 'Navarra', '33': 'Asturias', '35': 'Canarias', '38': 'Canarias', '39': 'Cantabria', '51': 'Ceuta',
    '52': 'Melilla'}
# Municipios por CCAA (INE, padrón 2024)
MUNICIPIOS = {'Andalucía': 785, 'Aragón': 731, 'Asturias': 78, 'Illes Balears': 67, 'Canarias': 88, 'Cantabria': 102,
              'Castilla y León': 2248, 'Castilla-La Mancha': 919, 'Catalunya': 947, 'C. Valenciana': 542,
              'Extremadura': 388, 'Galicia': 313, 'Madrid': 179, 'Murcia': 45, 'Navarra': 272, 'País Vasco': 251,
              'La Rioja': 174, 'Ceuta': 1, 'Melilla': 1}

ruta, salida = sys.argv[1], sys.argv[2]
df = pq.read_table(ruta, columns=['id', 'conjunto', 'dir3_organo', 'dependencia', 'ano', 'fecha_adjudicacion'],
                   filters=[('conjunto', '=', 'menores')]).to_pandas().drop_duplicates('id', keep='last')
df['anio'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce').dt.year.fillna(df['ano']).astype('Int64')
d = df['dir3_organo'].astype('string')
df['ine'] = d.str.extract(r'^L01(\d{5})\d$')[0]
partes = df['dependencia'].astype('string').str.split(' > ')
es_ayto = partes.str[1].eq('ENTIDADES LOCALES').fillna(False) & partes.str[4].eq('Ayuntamientos').fillna(False)
df['clave_dep'] = (partes.str[3] + ' | ' + partes.str[5]).where(es_ayto)
# Casar la ruta con el INE a partir de los contratos que traen las dos
casado = df.dropna(subset=['ine', 'clave_dep']).groupby('clave_dep')['ine'].agg(lambda s: s.mode().iat[0])
df['ine'] = df['ine'].fillna(df['clave_dep'].map(casado))
sin_ine = df['clave_dep'].notna() & df['ine'].isna()
print(f"{len(df):,} menores; de ayuntamientos (DIR3 L01 o ruta): {(df['ine'].notna() | sin_ine).sum():,}; "
      f"con municipio INE: {df['ine'].notna().sum():,}; ruta de ayuntamiento sin casar: {sin_ine.sum():,} "
      f"({df.loc[sin_ine, 'clave_dep'].nunique():,} ayuntamientos)")
df['ccaa'] = df['ine'].str[:2].map(PROV_CCAA)
aytos = df.dropna(subset=['ine'])
t = aytos[aytos['anio'].between(2018, 2025)].groupby(['ccaa', 'anio'])['ine'].nunique().unstack(fill_value=0)
t['municipios INE'] = pd.Series(MUNICIPIOS)
for a in (2022, 2023, 2024, 2025):
    if a in t:
        t[f'% {a}'] = (100 * t[a] / t['municipios INE']).round(1)
n_contratos = aytos[aytos['anio'] == 2024].groupby('ccaa').size().rename('menores 2024 de aytos')
t = t.join(n_contratos).sort_values('municipios INE', ascending=False)
t.loc['TOTAL'] = t.sum(numeric_only=True)
for a in (2022, 2023, 2024, 2025):
    if a in t:
        t.loc['TOTAL', f'% {a}'] = round(100 * t.loc['TOTAL', a] / t.loc['TOTAL', 'municipios INE'], 1)
pd.set_option('display.width', 250)
print(t.to_string())
t.to_csv(salida)
