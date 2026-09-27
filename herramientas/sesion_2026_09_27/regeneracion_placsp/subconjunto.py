"""Copia de un parquet con solo las columnas indicadas, row group a row group
(memoria acotada). Uso: python subconjunto.py <entrada> <salida> col1,col2,..."""
import sys

import pyarrow.parquet as pq

entrada, salida, lista = sys.argv[1:4]
pf = pq.ParquetFile(entrada)
columnas = [c for c in lista.split(',') if c in pf.schema_arrow.names]
escritor = None
for i in range(pf.num_row_groups):
    t = pf.read_row_group(i, columns=columnas)
    if escritor is None:
        escritor = pq.ParquetWriter(salida, t.schema, compression='snappy')
    escritor.write_table(t)
escritor.close()
print(f"{salida}: {pf.metadata.num_rows:,} filas, {len(columnas)} columnas: {', '.join(columnas)}")
