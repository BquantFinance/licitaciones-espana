#!/usr/bin/env python3
"""
NORMALIZAR PARQUET PLACSP YA GENERADO / PUBLICADO
=================================================
Corrige un parquet nacional (p. ej. los del release v2026.02) sin volver a
descargar ni procesar los ATOM de la PLACSP, sin eliminar ningún registro:

  1. Marca las versiones: la PLACSP publica una entrada por cada actualización
     de una licitación (licitaciones_espana.parquet: 8,7M entradas de 4,7M
     licitaciones). Se conservan todas y se añaden n_versiones y
     es_ultima_version; sumar importes sin filtrar es_ultima_version cuenta
     varias veces la misma licitación (x4,7 en adjudicación).
  2. Esquema antiguo de importes (issue #6): 'importe_sin_iva' contenía el
     valor estimado (EstimatedOverallContractAmount). Se renombra a
     'valor_estimado_contrato' y 'importe_sin_iva' queda vacía; el presupuesto
     sin IVA real (TaxExclusiveAmount) solo se recupera reprocesando los ATOM
     con nacional/licitaciones.py.
  3. Etiquetas de tipo de contrato / procedimiento / estado recalculadas desde
     los códigos, y códigos guardados como float pasados a texto (CPV con cero
     inicial: 9134100.0 → '09134100').

Procesa el fichero por row groups, así que no necesita cargar las ~9M filas en
memoria a la vez.

Uso:
    python nacional/normalizar_placsp.py -i nacional/licitaciones_espana.parquet \\
        -o nacional/licitaciones_espana_normalizado.parquet
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nacional.licitaciones import (  # noqa: E402
    COLUMNAS_CODIGO,
    IMPORTES_RESUMEN,
    _normalizar_columnas,
    info_versiones,
)

# Columnas que normalizar_placsp devuelve como texto
COLUMNAS_TEXTO = set(COLUMNAS_CODIGO) | {'cpv_principal', 'tipo_contrato', 'procedimiento',
                                         'estado', 'estado_code'}


def esquema_salida(esquema_entrada, df):
    """Esquema Arrow de salida: el de entrada salvo las columnas recalculadas."""
    campos = []
    for col in df.columns:
        if col in COLUMNAS_TEXTO:
            tipo = pa.string()
        elif col in ('importe_sin_iva', 'valor_estimado_contrato'):
            tipo = pa.float64()
        elif col in esquema_entrada.names:
            tipo = esquema_entrada.field(col).type
        else:
            tipo = pa.Schema.from_pandas(df[[col]], preserve_index=False).field(col).type
        campos.append(pa.field(col, tipo))
    return pa.schema(campos)


def normalizar_fichero(entrada, salida):
    """Normaliza 'entrada' y escribe 'salida' con todas sus filas.

    Devuelve (filas, licitaciones_distintas, sumas); las sumas de importes se
    calculan sobre la última versión de cada licitación.
    """
    pf = pq.ParquetFile(entrada)
    nombres = pf.schema_arrow.names

    ultima = n_versiones = None
    if 'id' in nombres:
        claves = pq.read_table(entrada, columns=[c for c in ('id', 'fecha_updated') if c in nombres]).to_pandas()
        ultima, n_versiones = info_versiones(claves['id'], claves.get('fecha_updated'))
        del claves

    writer = None
    escritas = distintas = 0
    sumas = {}
    try:
        for i in range(pf.num_row_groups):
            df = pf.read_row_group(i).to_pandas()
            n = len(df)
            if ultima is not None:
                df['n_versiones'] = n_versiones[escritas:escritas + n]
                df['es_ultima_version'] = ultima[escritas:escritas + n]
            df = _normalizar_columnas(df)
            if writer is None:
                esquema = esquema_salida(pf.schema_arrow, df)
                writer = pq.ParquetWriter(salida, esquema, compression='snappy')
            writer.write_table(pa.Table.from_pandas(df, schema=esquema, preserve_index=False))
            escritas += n

            ultimas = df[df['es_ultima_version']] if 'es_ultima_version' in df.columns else df
            distintas += len(ultimas)
            for col, _ in IMPORTES_RESUMEN:
                if col in ultimas.columns:
                    sumas[col] = sumas.get(col, 0.0) + pd.to_numeric(ultimas[col], errors='coerce').sum()
            print(f"   [{i + 1}/{pf.num_row_groups}] {escritas:,} filas escritas", flush=True)
    finally:
        if writer is not None:
            writer.close()
    return escritas, distintas, sumas


def main():
    parser = argparse.ArgumentParser(description='Normaliza un parquet PLACSP ya generado')
    parser.add_argument('-i', '--input', required=True, type=Path, help='Parquet de entrada')
    parser.add_argument('-o', '--output', required=True, type=Path, help='Parquet de salida')
    args = parser.parse_args()

    if args.input.resolve() == args.output.resolve():
        parser.error('La salida debe ser un fichero distinto de la entrada')
    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"🔧 Normalizando {args.input}")
    filas, distintas, sumas = normalizar_fichero(args.input, args.output)

    print("\n📊 RESULTADO")
    print("=" * 60)
    print(f"   Filas (todas se conservan): {filas:,}")
    print(f"   Licitaciones distintas (es_ultima_version): {distintas:,}")
    for col, etiqueta in IMPORTES_RESUMEN:
        if col in sumas:
            print(f"   {etiqueta} ({col}), última versión: {sumas[col]/1e9:,.1f}B €")
    print(f"\n✓ Escrito {args.output}")


if __name__ == '__main__':
    main()
