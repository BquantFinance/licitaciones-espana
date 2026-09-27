"""Tabla principal regenerada -> entrada de calidad y del cruce TED: las columnas
de licitaciones_espana.parquet (v2026.02) con la semántica corregida, más
valor_estimado_contrato, sara, las marcas de versión y la procedencia. Por row
groups (memoria acotada). Uso: python reducir.py <principal.parquet> <salida.parquet>"""
import sys

import pyarrow.parquet as pq

V2026 = ['id', 'expediente', 'objeto', 'organo_contratante', 'nif_organo', 'dir3_organo', 'id_plataforma',
         'ciudad_organo', 'dependencia', 'tipo_contrato_code', 'tipo_contrato', 'subtipo_code', 'procedimiento_code',
         'procedimiento', 'estado_code', 'estado', 'importe_sin_iva', 'importe_con_iva', 'importe_adjudicacion',
         'importe_adj_con_iva', 'adjudicatario', 'nif_adjudicatario', 'num_ofertas', 'es_pyme', 'cpv_principal',
         'cpvs', 'ubicacion', 'nuts', 'duracion', 'duracion_unidad', 'financiacion_ue', 'urgencia', 'fecha_limite',
         'hora_limite', 'fecha_adjudicacion', 'fecha_publicacion', 'fecha_updated', 'url', 'conjunto',
         'archivo_origen', 'ano', 'tipo_registro', 'id_consulta', 'nombre_consulta', 'condiciones', 'tipo_condicion',
         'fecha_planificada', 'fecha_limite_respuestas']
EXTRA = ['valor_estimado_contrato', 'sara', 'n_versiones', 'es_ultima_version', 'entrada_repetida', '_origen',
         '_en_ultima_descarga']

pf = pq.ParquetFile(sys.argv[1])
columnas = [c for c in V2026 + EXTRA if c in pf.schema_arrow.names]
faltan = [c for c in V2026 + EXTRA if c not in pf.schema_arrow.names]
escritor = None
filas = 0
for i in range(pf.num_row_groups):
    tabla = pf.read_row_group(i, columns=columnas)
    if escritor is None:
        escritor = pq.ParquetWriter(sys.argv[2], tabla.schema, compression='snappy')
    escritor.write_table(tabla)
    filas += tabla.num_rows
escritor.close()
print(f'{filas:,} filas x {len(columnas)} columnas -> {sys.argv[2]}; faltan: {faltan or "ninguna"}')
