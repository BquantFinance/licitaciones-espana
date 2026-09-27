"""
Conversión de CSVs de Valencia a Parquet
Ejecutar después de ccaa_valencia.py

Ejecutar: python ccaa_valencia_parquet.py
Entrada: valencia_datos/
Salida: valencia_parquet/

Sesgo del superviviente: ccaa_valencia.py guarda en <categoría>/_historico/
cada versión anterior de un CSV que el portal ha cambiado. El parquet de cada
recurso se construye con TODAS las versiones (de la más antigua a la actual)
mediante comun.historico.acumular: un registro que la administración retira o
modifica sigue en el parquet con _en_ultima_descarga=False, y cada fila lleva
_primera_descarga/_ultima_descarga (sello de la versión en _historico/, o el
mtime del CSV vigente). Con una sola versión la salida es la de siempre más
esas 3 columnas.

Sin --semilla (a diferencia de otros scrapers): los parquet publicados en
v2026.02 no pueden servir de primera descarga. Los recursos de GVA no tienen
una clave estable común (cada dataset tiene columnas distintas, muchos sin
identificador) y acumular compara filas completas, pero esos parquet los
generó código incompatible con el actual (ceros a la izquierda perdidos:
codigo_postal 3001 en vez de '03001'; cp1252 leído como latin-1; vacíos como
texto 'nan'; recursos con el mismo nombre que se pisaban). Ninguna fila
casaría: todo el publicado aparecería como "retirado" y duplicado.
"""

import codecs
import re
import sys
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from comun.historico import COLUMNAS_META, acumular, versiones  # noqa: E402

# === CONFIGURACIÓN ===
INPUT_DIR = Path("valencia_datos")
OUTPUT_DIR = Path("valencia_parquet")

# Encodings a probar, en orden. cp1252 va antes que latin-1: latin-1 acepta
# cualquier byte, así que si va primero cp1252 no se prueba nunca y los '€', '’',
# '“', '–' de los CSV exportados desde Windows quedan como caracteres de control
# ('\x80', '\x92'...). 'iso-8859-1' es un alias de latin-1.
ENCODINGS = ['utf-8', 'cp1252', 'latin-1']

# Separadores a probar
SEPARATORS = [';', ',', '\t']


def _detect_encoding(filepath: Path) -> str:
    """Primer encoding de ENCODINGS capaz de decodificar el archivo COMPLETO.

    Leer solo unas filas no basta: pandas decodifica por bloques y un CSV latin-1
    cuyo primer carácter acentuado estaba más allá del primer bloque se daba por
    utf-8 y después fallaba la lectura completa (el archivo no se convertía).
    """
    for encoding in ENCODINGS:
        decoder = codecs.getincrementaldecoder(encoding)()
        try:
            with open(filepath, 'rb') as f:
                while bloque := f.read(1024 * 1024):
                    decoder.decode(bloque)
            decoder.decode(b'', final=True)
            return encoding
        except UnicodeDecodeError:
            continue
    return ENCODINGS[-1]


def detect_encoding_and_sep(filepath: Path) -> tuple:
    """Detecta encoding y separador de un CSV."""
    encoding = _detect_encoding(filepath)
    for sep in SEPARATORS:
        try:
            # on_bad_lines='skip' como en la lectura completa: una fila mal
            # formada al principio no debe hacer descartar el separador correcto
            df = pd.read_csv(filepath, encoding=encoding, sep=sep, nrows=5, on_bad_lines='skip')
            if len(df.columns) > 1:
                return encoding, sep
        except Exception:
            continue
    return encoding, ';'


# Texto numérico con ceros a la izquierda: '03001', '03000047', '-01' (no '0', '0,5')
PATRON_CERO_INICIAL = r'^\s*[+-]?0\d'
FILAS_POR_TROZO = 500_000


def _tiene_cero_inicial(serie) -> bool:
    valores = serie.dropna()
    return len(valores) > 0 and bool(valores.astype(str).str.match(PATRON_CERO_INICIAL).any())


def restaurar_ceros_iniciales(df: pd.DataFrame, csv_path: Path, encoding: str, sep: str) -> pd.DataFrame:
    """Columnas que pandas leyó como número pero cuyo texto original lleva ceros a
    la izquierda (códigos postales y de municipio de Alicante '03001', códigos de
    centro '03000047'...): el 0 se perdía (en los parquet publicados codigo_postal
    empieza en 3001). Esas columnas se guardan como texto, tal como las publica GVA.
    """
    # Si pandas usó la 1ª columna como índice las posiciones no casan con el archivo
    if not isinstance(df.index, pd.RangeIndex):
        return df
    tipos = df.dtypes
    numericas = [i for i in range(len(tipos))
                 if pd.api.types.is_numeric_dtype(tipos.iloc[i]) and not pd.api.types.is_bool_dtype(tipos.iloc[i])]
    if not numericas:
        return df

    def trozos():
        # Misma lectura (mismas líneas descartadas) pero todo como texto y por trozos
        return pd.read_csv(csv_path, encoding=encoding, sep=sep, dtype=str, on_bad_lines='skip',
                           chunksize=FILAS_POR_TROZO)

    try:
        con_ceros = set()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.ParserWarning)
            for trozo in trozos():
                if trozo.shape[1] != df.shape[1]:
                    return df
                for i in numericas:
                    if i not in con_ceros and _tiene_cero_inicial(trozo.iloc[:, i]):
                        con_ceros.add(i)
            if not con_ceros:
                return df
            posiciones = sorted(con_ceros)
            texto = pd.concat([t.iloc[:, posiciones] for t in trozos()], ignore_index=True)
    except Exception as e:
        print(f"     ⚠️ No se pudo comprobar ceros a la izquierda: {e}")
        return df
    if len(texto) != len(df):
        print("     ⚠️ No se pudo comprobar ceros a la izquierda (filas distintas)")
        return df
    for j, i in enumerate(posiciones):
        df.isetitem(i, texto.iloc[:, j].array)
    print(f"     🔢 Guardadas como texto (ceros a la izquierda): {', '.join(str(df.columns[i]) for i in posiciones)}")
    return df


def _leer(csv_path: Path, encoding: str, sep: str, **kwargs):
    """read_csv contando las líneas mal formadas (se descartan, pero se avisa:
    antes se perdían en silencio). Devuelve (df, descartadas)."""
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always", pd.errors.ParserWarning)
        df = pd.read_csv(csv_path, encoding=encoding, sep=sep, on_bad_lines='warn', **kwargs)
    descartadas = sum(
        str(a.message).count("Skipping line")
        for a in avisos if issubclass(a.category, pd.errors.ParserWarning)
    )
    return df, descartadas


def leer_csv(csv_path: Path, encoding: str = None, sep: str = None) -> tuple:
    """Lee un CSV con los tipos que infiere pandas, conservando los ceros a la
    izquierda y los vacíos como nulos. Devuelve (df, líneas descartadas)."""
    if encoding is None:
        encoding, sep = detect_encoding_and_sep(csv_path)
    df, descartadas = _leer(csv_path, encoding, sep, low_memory=False)

    df = restaurar_ceros_iniciales(df, csv_path, encoding, sep)

    # Convertir columnas object a string para evitar errores, conservando
    # los vacíos como nulos (astype(str) a secas los convertía en el texto 'nan')
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    return df, descartadas


SELLO = re.compile(r"(\d{8})T(\d{2})(\d{2})(\d{2})Z(_\d+)?")


def versiones_csv(csv_path: Path) -> list:
    """[(ruta, fecha)] de las versiones del CSV, de la más antigua a la vigente.
    fecha: sello de la versión en _historico/ o mtime del CSV vigente (UTC)."""
    salida = []
    for v in versiones(csv_path):
        if v == csv_path:
            momento = datetime.fromtimestamp(v.stat().st_mtime, timezone.utc)
            salida.append((v, momento.strftime("%Y-%m-%dT%H:%M:%SZ")))
            continue
        # glob 'X__*' también casaría con las versiones de otro 'X__algo.csv'
        m = SELLO.fullmatch(v.stem[len(csv_path.stem) + 2:])
        if m:
            d, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4)
            salida.append((v, f"{d[:4]}-{d[4:6]}-{d[6:]}T{hh}:{mm}:{ss}Z"))
    return salida


def construir_registros(csv_path: Path, tmp_csv: Path) -> tuple:
    """Registros de todas las versiones del CSV acumulados (comun.historico).
    Devuelve (df con COLUMNAS_META, líneas descartadas, nº de versiones)."""
    vers = versiones_csv(csv_path)
    if len(vers) == 1:
        # Una sola versión: exactamente la lectura de siempre + columnas meta
        df, descartadas = leer_csv(csv_path)
        return acumular(None, df, vers[0][1], permitir_vacio=True), descartadas, 1

    # Varias versiones: se comparan como texto, tal como las sirvió el portal
    # (leídas con tipos, un 1 en una versión y 1.0 en otra con nulos no casarían)
    acumulado, descartadas = None, 0
    for ruta, fecha in vers:
        encoding, sep = detect_encoding_and_sep(ruta)
        texto, n = _leer(ruta, encoding, sep, dtype=str)
        descartadas += n
        if len(texto) == 0 and acumulado is not None:
            print(f"     ⚠️ Versión vacía ignorada (no se marca nada como retirado): {ruta.name}")
            continue
        # ámbito: todo el recurso (cada parquet es un único recurso)
        acumulado = acumular(acumulado, texto, fecha, permitir_vacio=acumulado is None)

    # Tipos: se vuelve a leer el texto acumulado con la misma lectura que un
    # CSV suelto (misma inferencia de tipos y de ceros a la izquierda)
    datos = acumulado.drop(columns=list(COLUMNAS_META))
    try:
        datos.to_csv(tmp_csv, sep=';', index=False, encoding='utf-8')
        df, _ = leer_csv(tmp_csv, 'utf-8', ';')
    finally:
        if tmp_csv.exists():
            tmp_csv.unlink()
    if list(df.columns) != list(datos.columns) or len(df) != len(datos):
        print("     ⚠️ No se pudieron inferir los tipos: se guarda como texto")
        df = datos
    for c in COLUMNAS_META:
        df[c] = acumulado[c].to_numpy()
    return df, descartadas, len(vers)


def tiene_meta(parquet_path: Path) -> bool:
    try:
        return set(COLUMNAS_META) <= set(pq.read_schema(parquet_path).names)
    except Exception:
        return False


def convert_to_parquet(csv_path: Path, parquet_path: Path) -> bool:
    """Convierte un CSV (todas sus versiones) a Parquet."""
    tmp_path = parquet_path.with_name(parquet_path.name + '.tmp')
    try:
        df, descartadas, n_versiones = construir_registros(
            csv_path, parquet_path.with_name(parquet_path.name + '.csv.tmp'))

        # Columnas totalmente vacías: pandas las infiere como float64. Se guardan
        # como texto para que el esquema coincida con los demás archivos de la
        # serie (si no, pd.read_parquet('valencia/contratacion/') falla al mezclar
        # double y string en la misma columna).
        schema = pa.Schema.from_pandas(df, preserve_index=False)
        for i, col in enumerate(df.columns):
            if col not in COLUMNAS_META and df[col].isna().all():
                schema = schema.set(i, pa.field(schema.field(i).name, pa.string()))

        # Guardar como Parquet en un temporal y renombrar al final: si la
        # escritura falla o se interrumpe no queda un .parquet a medias que la
        # siguiente ejecución daría por bueno ("Ya existe")
        df.to_parquet(tmp_path, index=False, compression='snappy', schema=schema)
        os.replace(tmp_path, parquet_path)

        # Estadísticas
        csv_size = csv_path.stat().st_size / (1024 * 1024)
        parquet_size = parquet_path.stat().st_size / (1024 * 1024)
        reduction = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0

        print(f"  ✅ {parquet_path.name}")
        print(f"     {len(df):,} registros | {csv_size:.1f} MB → {parquet_size:.1f} MB ({reduction:.0f}% reducción)")
        if descartadas:
            print(f"     ⚠️ {descartadas:,} líneas mal formadas descartadas")
        retirados = int((~df['_en_ultima_descarga'].astype(bool)).sum())
        if n_versiones > 1:
            print(f"     📜 {n_versiones} versiones del CSV; {retirados:,} registros ya no servidos (conservados)")

        return True

    except Exception as e:
        print(f"  ❌ Error en {csv_path.name}: {e}")
        return False
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def main():
    print("=" * 60)
    print("CONVERSIÓN CSV → PARQUET - COMUNITAT VALENCIANA")
    print("=" * 60)
    
    if not INPUT_DIR.exists():
        print(f"❌ No existe la carpeta {INPUT_DIR}")
        print("   Ejecuta primero: python ccaa_valencia.py")
        return 1

    OUTPUT_DIR.mkdir(exist_ok=True)

    total_csv = 0
    total_parquet = 0
    total_registros = 0
    fallidos = []
    
    # Procesar cada categoría
    for category_dir in sorted(INPUT_DIR.iterdir()):
        if not category_dir.is_dir():
            continue
        
        csv_files = list(category_dir.glob("*.csv"))
        if not csv_files:
            continue
        
        print(f"\n📁 {category_dir.name.upper()}")
        print("-" * 40)
        
        # Crear subcarpeta de salida
        output_category = OUTPUT_DIR / category_dir.name
        output_category.mkdir(exist_ok=True)
        
        for csv_file in sorted(csv_files):
            total_csv += 1
            
            # Nombre del parquet
            parquet_name = csv_file.stem.replace(" ", "_") + ".parquet"
            parquet_path = output_category / parquet_name
            
            # Solo se salta si el parquet es posterior a todas las versiones del
            # CSV (si la descarga lo actualizó, se vuelve a construir) y ya
            # lleva las columnas del histórico
            reciente = max(v.stat().st_mtime for v, _ in versiones_csv(csv_file))
            if (parquet_path.exists() and parquet_path.stat().st_mtime >= reciente
                    and tiene_meta(parquet_path)):
                print(f"  ⏭️ Ya existe: {parquet_name}")
                total_parquet += 1
                continue
            
            if convert_to_parquet(csv_file, parquet_path):
                total_parquet += 1
            else:
                fallidos.append(f"{category_dir.name}/{csv_file.name}")

    # Resumen final
    print("\n" + "=" * 60)
    if fallidos:
        print(f"⚠️ CONVERSIÓN COMPLETADA CON ERRORES ({len(fallidos)})")
    else:
        print("✅ CONVERSIÓN COMPLETADA")
    print("=" * 60)
    
    total_size_csv = 0
    total_size_parquet = 0
    
    print("\n📊 RESUMEN POR CATEGORÍA:")
    for category_dir in sorted(OUTPUT_DIR.iterdir()):
        if category_dir.is_dir():
            files = list(category_dir.glob("*.parquet"))
            size = sum(f.stat().st_size for f in files) / (1024 * 1024)
            total_size_parquet += size
            
            # Contar registros
            registros = 0
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    registros += len(df)
                except Exception as e:
                    print(f"  ⚠️ No se pudo leer {f.name}: {e}")
            
            total_registros += registros
            print(f"  📁 {category_dir.name}: {len(files)} archivos, {registros:,} registros, {size:.1f} MB")
    
    # CSV original
    for category_dir in sorted(INPUT_DIR.iterdir()):
        if category_dir.is_dir():
            files = list(category_dir.glob("*.csv"))
            total_size_csv += sum(f.stat().st_size for f in files) / (1024 * 1024)
    
    print(f"\n📈 TOTALES:")
    print(f"   Archivos: {total_parquet}")
    print(f"   Registros: {total_registros:,}")
    print(f"   Tamaño CSV: {total_size_csv:.1f} MB")
    print(f"   Tamaño Parquet: {total_size_parquet:.1f} MB")
    if total_size_csv > 0:
        print(f"   Reducción: {(1 - total_size_parquet/total_size_csv)*100:.0f}%")

    if fallidos:
        print(f"\n❌ ARCHIVOS NO CONVERTIDOS ({len(fallidos)}):")
        for nombre in fallidos:
            print(f"   - {nombre}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())