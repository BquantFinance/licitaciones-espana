#!/usr/bin/env python3
"""
NORMALIZAR PARQUET PLACSP YA GENERADO / PUBLICADO
=================================================
Corrige un parquet nacional (p. ej. los del release v2026.02) sin volver a
descargar ni procesar los ATOM de la PLACSP:

  1. Una fila por licitación: conserva la versión más reciente (atom:updated).
     licitaciones_espana.parquet tiene 8,7M filas para 4,7M licitaciones y
     cualquier suma sobre él multiplica los importes (x4,8 en adjudicación).
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

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nacional.licitaciones import (  # noqa: E402
    COLUMNAS_CODIGO,
    IMPORTES_RESUMEN,
    indices_ultima_version,
    normalizar_placsp,
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


def normalizar_fichero(entrada, salida, deduplicar=True):
    """Normaliza 'entrada' y escribe 'salida'. Devuelve (filas_leidas, filas_escritas, sumas)."""
    pf = pq.ParquetFile(entrada)
    nombres = pf.schema_arrow.names

    conservar = None
    if deduplicar and 'id' in nombres:
        claves = pq.read_table(entrada, columns=[c for c in ('id', 'fecha_updated') if c in nombres]).to_pandas()
        conservar = indices_ultima_version(claves['id'], claves.get('fecha_updated'))
        del claves

    writer = None
    leidas = escritas = 0
    sumas = {}
    try:
        for i in range(pf.num_row_groups):
            tabla = pf.read_row_group(i)
            n = tabla.num_rows
            if conservar is not None:
                desde, hasta = np.searchsorted(conservar, [leidas, leidas + n])
                tabla = tabla.take(conservar[desde:hasta] - leidas)
            leidas += n

            df = normalizar_placsp(tabla.to_pandas(), deduplicar=False)
            del tabla
            if writer is None:
                esquema = esquema_salida(pf.schema_arrow, df)
                writer = pq.ParquetWriter(salida, esquema, compression='snappy')
            writer.write_table(pa.Table.from_pandas(df, schema=esquema, preserve_index=False))
            escritas += len(df)
            for col, _ in IMPORTES_RESUMEN:
                if col in df.columns:
                    sumas[col] = sumas.get(col, 0.0) + pd.to_numeric(df[col], errors='coerce').sum()
            print(f"   [{i + 1}/{pf.num_row_groups}] {escritas:,} filas escritas", flush=True)
    finally:
        if writer is not None:
            writer.close()
    return leidas, escritas, sumas


def main():
    parser = argparse.ArgumentParser(description='Normaliza un parquet PLACSP ya generado')
    parser.add_argument('-i', '--input', required=True, type=Path, help='Parquet de entrada')
    parser.add_argument('-o', '--output', required=True, type=Path, help='Parquet de salida')
    parser.add_argument('--sin-deduplicar', action='store_true',
                        help='Conservar todas las versiones de cada licitación')
    args = parser.parse_args()

    if args.input.resolve() == args.output.resolve():
        parser.error('La salida debe ser un fichero distinto de la entrada')
    args.output.parent.mkdir(parents=True, exist_ok=True)

    print(f"🔧 Normalizando {args.input}")
    leidas, escritas, sumas = normalizar_fichero(args.input, args.output,
                                                 deduplicar=not args.sin_deduplicar)

    print(f"\n📊 RESULTADO")
    print("=" * 60)
    print(f"   Filas leídas:   {leidas:,}")
    print(f"   Filas escritas: {escritas:,}"
          + (f" ({leidas - escritas:,} versiones anteriores descartadas)" if leidas != escritas else ""))
    for col, etiqueta in IMPORTES_RESUMEN:
        if col in sumas:
            print(f"   {etiqueta} ({col}): {sumas[col]/1e9:,.1f}B €")
    print(f"\n✓ Escrito {args.output}")


if __name__ == '__main__':
    main()
