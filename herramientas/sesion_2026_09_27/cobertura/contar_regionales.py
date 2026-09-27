"""Contratos menores por año en cada fuente regional que ya tenemos.

Cuenta contratos distintos (por su clave en cada fuente) por año. Salida:
tabla fuente × año en regionales_por_anio.csv y por pantalla.
"""
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

S = Path(sys.argv[1])            # scratchpad
C = S / 'cobertura' / 'regionales'
N = S / 'ccaa_nuevas'
pd.set_option('display.width', 250); pd.set_option('display.max_columns', 30)


def leer(ruta, columnas):
    return pq.read_table(ruta, columns=columnas).to_pandas()


def anio(serie, dayfirst=False):
    """Año de una fecha en cualquier formato (ISO, dd/mm/aaaa, '26 de marzo del 2025'...)."""
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie.dt.year
    return pd.to_numeric(pd.Series(serie).astype('string').str.extract(r'((?:19|20)\d{2})')[0], errors='coerce')


filas = []   # (ccaa, fuente, órganos, año, n)


def anotar(ccaa, fuente, organos, anios):
    for a, n in pd.Series(anios).dropna().astype(int).value_counts().items():
        filas.append((ccaa, fuente, organos, a, n))


# Catalunya: RPC (Registre Públic de Contractes), menores por ámbito
df = leer(C / 'catalunya/contratacion/contratos_registro.parquet',
          ["Procediment d’adjudicació", 'Exercici', 'Àmbit organitzatiu', 'Identificador organisme contractant',
           'Codi de l’expedient', 'Número de lot'])
df = df[df["Procediment d’adjudicació"] == 'Menor'].drop_duplicates(
    ['Identificador organisme contractant', 'Codi de l’expedient', 'Número de lot', 'Exercici'])
for ambito, g in df.groupby('Àmbit organitzatiu'):
    anotar('Catalunya', 'RPC (contratos_registro)', ambito, pd.to_numeric(g['Exercici'], errors='coerce'))
# Catalunya: PSCP (contractacio_menors)
cols = [c for c in pq.ParquetFile(C / 'catalunya/contratacion/contractacio_menors.parquet').schema_arrow.names
        if c.endswith('_dataPublicacio')]
df = leer(C / 'catalunya/contratacion/contractacio_menors.parquet', ['id'] + cols).drop_duplicates('id')
fechas = pd.concat([pd.to_datetime(df[c], errors='coerce', utc=True) for c in cols], axis=1).min(axis=1)
anotar('Catalunya', 'PSCP (contractacio_menors)', 'Generalitat y entes locales en la PSCP', fechas.dt.year)
# Barcelona
df = leer(C / 'catalunya/contratacion/contratos_menores_bcn.parquet', ['_año'])
anotar('Catalunya', 'Ayuntamiento de Barcelona (contratos_menores_bcn)', 'Ayuntamiento de Barcelona',
       pd.to_numeric(df['_año'], errors='coerce'))
# Andalucía (Junta: buscador de la Junta, codigo_procedimiento 9)
df = leer(C / 'ccaa_Andalucia/licitaciones_andalucia.parquet', ['id_expediente', 'codigo_procedimiento', 'fecha_publicacion'])
df = df[df['codigo_procedimiento'].astype(str) == '9'].drop_duplicates('id_expediente')
anotar('Andalucía', 'Junta (licitaciones_andalucia)', 'Junta de Andalucía y sus entes', anio(df['fecha_publicacion']))
# Comunidad de Madrid
df = leer(C / 'comunidad_madrid/contratacion_comunidad_madrid_completo.parquet',
          ['Tipo de Publicación', 'Entidad Adjudicadora', 'Nº Expediente', 'Referencia', 'Fecha del contrato'])
df = df[df['Tipo de Publicación'] == 'Contratos menores'].drop_duplicates(
    ['Entidad Adjudicadora', 'Nº Expediente', 'Referencia', 'Fecha del contrato'])
anotar('Madrid', 'Comunidad de Madrid (portal de contratación)', 'Comunidad de Madrid y sus entes',
       anio(df['Fecha del contrato'], dayfirst=True))
# Ayuntamiento de Madrid (ejecución real de hoy)
df = leer(S / 'madrid_real/salida/actividad_contractual_madrid_completo.parquet', ['categoria', 'anio', '_duplicado'])
df = df[(df['categoria'] == 'contratos_menores') & ~df['_duplicado'].fillna(False).astype(bool)]
anotar('Madrid', 'Ayuntamiento de Madrid (datos.madrid.es)', 'Ayuntamiento de Madrid', df['anio'])
# Galicia (contratosdegalicia, CM)
df = leer(C / 'galicia/contratos_galicia.parquet', ['id', '_tipo', 'publicado'])
df = df[df['_tipo'] == 'CM'].drop_duplicates('id')
anotar('Galicia', 'Xunta (contratosdegalicia)', 'Xunta y organismos (418)', anio(df['publicado'], dayfirst=True))
# Asturias (contratación centralizada del Principado)
df = leer(C / 'ccaa_asturias/asturias_contracts_ALL_YEARS.parquet', ['CLASIFICACION GENERAL', 'ANO', 'F. ADJ.'])
df = df[df['CLASIFICACION GENERAL'].isin(['MENORES 5000', 'MENOR'])]
a = pd.to_numeric(df['ANO'], errors='coerce').fillna(anio(df['F. ADJ.'], dayfirst=True))
anotar('Asturias', 'Principado (contratación centralizada)', 'Principado de Asturias', a)
# C. Valenciana (REGCON): adjudicaciones directas y sin procedimiento
for f in sorted((C / 'valencia/contratacion').glob('*.parquet')):
    nombres = {n.upper(): n for n in pq.ParquetFile(f).schema_arrow.names}
    df = leer(f, [nombres['EJERCICIO'], nombres['PROCEDIMIENTO']])
    df.columns = ['EJERCICIO', 'PROCEDIMIENTO']
    proc = df['PROCEDIMIENTO'].astype('string')
    df = df[proc.isna() | proc.str.startswith('Adjudicación directa').fillna(False)]
    anotar('C. Valenciana', 'REGCON (adj. directa o sin procedimiento; aprox.)', 'Generalitat Valenciana',
           pd.to_numeric(df['EJERCICIO'], errors='coerce'))
# Euskadi: anuncios de contratos menores (sin importe ni adjudicatario)
df = leer(C / 'Euskadi/euskadi_parquet/contratos_master.parquet', ['contrato_menor', '_year'])
df = df[df['contrato_menor'].astype(str).isin(['True', 'Sí', 'true'])]
anotar('País Vasco', 'KontratazioA: anuncios de menores (SIN importe/adjudicatario)', 'Sector público vasco',
       pd.to_numeric(df['_year'], errors='coerce'))
# Scrapers nuevos (ejecución en vivo de hoy)
df = leer(N / 'murcia/contratos_menores_carm.parquet', ['_anio_fichero'])
anotar('Murcia', 'CARM (datosabiertos.carm.es)', 'Comunidad Autónoma', pd.to_numeric(df['_anio_fichero'], errors='coerce'))
df = leer(N / 'murcia/contratos_menores_sms.parquet', ['F.FORMALIZACIÓN', '_anio_fichero'])
anotar('Murcia', 'SMS (transparencia.carm.es)', 'Servicio Murciano de Salud',
       pd.to_numeric(df['_anio_fichero'], errors='coerce'))
for nombre, organos in [('contratos-menores', 'Junta de Castilla y León'), ('contratos-menores-sacyl', 'SACYL')]:
    df = leer(N / f'cyl/{nombre}.parquet', ['Fecha Aprobación de Gasto'])
    anotar('Castilla y León', f'Junta ({nombre})', organos, anio(df['Fecha Aprobación de Gasto'], dayfirst=True))
df = leer(N / 'aragon/contratos_gobierno__contratos_menores_gobierno_de_aragon.parquet', ['_anio_recurso'])
anotar('Aragón', 'Gobierno de Aragón (menores por año)', 'Gobierno de Aragón', pd.to_numeric(df['_anio_recurso'], errors='coerce'))
df = leer(N / 'aragon/registro_contratos__contratos_menores.parquet', ['fecha_de_adjudicacion'])
anotar('Aragón', 'Registro de Contratos de Aragón (2023+)', 'Sector público aragonés',
       anio(df['fecha_de_adjudicacion'], dayfirst=True))

t = pd.DataFrame(filas, columns=['ccaa', 'fuente', 'organos', 'anio', 'n'])
t.to_csv(S / 'cobertura' / 'regionales_por_anio.csv', index=False)
tabla = t[t['anio'].between(2018, 2026)].pivot_table(index=['ccaa', 'fuente'], columns='anio', values='n',
                                                     aggfunc='sum', fill_value=0)
tabla['total 2018-2026'] = tabla.sum(axis=1)
print(tabla.to_string())
