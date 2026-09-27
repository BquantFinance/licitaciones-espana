"""
Conversión de CSVs de Valencia a Parquet
Ejecutar después de ccaa_valencia.py

Ejecutar: python ccaa_valencia_parquet.py
Entrada: valencia_datos/
Salida: valencia_parquet/
"""

import codecs
import sys
import warnings
import pandas as pd
import pyarrow as pa
from pathlib import Path
import os

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


def convert_to_parquet(csv_path: Path, parquet_path: Path) -> bool:
    """Convierte un CSV a Parquet."""
    tmp_path = parquet_path.with_name(parquet_path.name + '.tmp')
    try:
        encoding, sep = detect_encoding_and_sep(csv_path)

        # Leer CSV (las líneas mal formadas se descartan, pero se cuentan y se
        # avisa: antes se perdían en silencio)
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always", pd.errors.ParserWarning)
            df = pd.read_csv(
                csv_path,
                encoding=encoding,
                sep=sep,
                low_memory=False,
                on_bad_lines='warn'
            )
        descartadas = sum(
            str(a.message).count("Skipping line")
            for a in avisos if issubclass(a.category, pd.errors.ParserWarning)
        )

        # Convertir columnas object a string para evitar errores, conservando
        # los vacíos como nulos (astype(str) a secas los convertía en el texto 'nan')
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].where(df[col].isna(), df[col].astype(str))

        # Columnas totalmente vacías: pandas las infiere como float64. Se guardan
        # como texto para que el esquema coincida con los demás archivos de la
        # serie (si no, pd.read_parquet('valencia/contratacion/') falla al mezclar
        # double y string en la misma columna).
        schema = pa.Schema.from_pandas(df, preserve_index=False)
        for i, col in enumerate(df.columns):
            if df[col].isna().all():
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
            
            if parquet_path.exists():
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