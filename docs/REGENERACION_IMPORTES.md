# Regenerar calidad tras el arreglo de BudgetAmount (#6)

La [PR #7](https://github.com/BquantFinance/licitaciones-espana/pull/7), integrada
el 30 de marzo de 2026, corrigió el parser. El release `v2026.02` es anterior:
sus parquets no se corrigen al actualizar el código.

| XML de origen | Campo correcto |
| --- | --- |
| `EstimatedOverallContractAmount` | `valor_estimado_contrato` |
| `TaxExclusiveAmount` | `importe_sin_iva` |
| `TotalAmount` | `importe_con_iva` |

El importe sin IVA no puede recuperarse renombrando columnas ni dividiendo
el total por 1,21: faltan el dato original y el tipo impositivo aplicable.
Un valor estimado repetido tampoco representa volumen adjudicado.

## Recuperación del mismo snapshot

El comando siguiente conserva **todas las filas, su orden y sus versiones**
del nacional publicado, y cambia únicamente los dos campos afectados.
Usa SQLite y lotes de 50.000 filas para no cargar el histórico entero en memoria.
No usa el exportador nacional antiguo, que elimina versiones por `id`.

1. Conserva el parquet nacional original y obtén los ZIP/ATOM de PLACSP de
   los períodos y conjuntos correspondientes. Colócalos así:

   ```text
   fuentes/
     licitaciones/*.zip
     agregacion/*.zip
     menores/*.zip
     encargos/*.zip
     consultas/*.zip
   ```

   También acepta `.atom` y `.xml`, incluidas subcarpetas. Los archivos ZIP
   se leen sin extraerlos. Por ejemplo, el histórico público de 2012:

   ```bash
   mkdir -p fuentes/licitaciones
   curl --fail --location --retry 3 \
     'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_2012.zip' \
     -o fuentes/licitaciones/licitacionesPerfilesContratanteCompleto3_2012.zip
   ```

   Este archivo **solo cubre una parte del histórico**. Descargar el feed
   actual no garantiza recuperar las versiones de un snapshot antiguo.

2. Desde la raíz del repositorio, con Python 3.11+ y `requirements.txt`:

   ```bash
   python -m nacional.reparar_importes \
     --input nacional/licitaciones_espana.parquet \
     --fuentes fuentes \
     --output regenerado/nacional.parquet
   ```

   Se exige una coincidencia exacta por `(conjunto, id, fecha_updated)` con
   zona horaria normalizada a UTC. Si falta una versión, hay presupuestos
   contradictorios, un XML está truncado o difiere `TotalAmount`, el comando
   falla **sin publicar un parquet parcial**. No sobrescribe archivos existentes.
   La ausencia de `TaxExclusiveAmount` en una entrada encontrada se conserva
   como nulo: no se rellena con el valor estimado.

   `regenerado/nacional.json` incluye SHA-256 de entrada, salida y fuentes,
   filas recuperadas, valores informados y número de importes modificados.
   No se acepta como entrada un resultado de calidad: sus indicadores deben
   recalcularse, no conservarse después de sustituir los importes.

3. Recalcula calidad sobre el nacional recuperado:

   ```bash
   python calidad/calidad_licitaciones.py \
     -i regenerado/nacional.parquet -o regenerado/calidad
   ```

   El pipeline de calidad calcula sobre el conjunto completo; no se divide en
   lotes porque hay indicadores que dependen de cuantiles globales por CPV.
   Este paso sí requiere memoria suficiente para el dataset completo.
   La presencia de `valor_estimado_contrato` evita reutilizar el esquema
   antiguo por accidente, pero **no demuestra por sí sola la procedencia**:
   acompaña el resultado con el informe de recuperación.

## Validación y publicación

- Comprueba que filas de entrada, salida y coincidencias exactas sean iguales.
- Conserva los hashes, las fuentes y la versión de código utilizada.
- Compara los campos ajenos al arreglo: deben permanecer idénticos.
- No agregues importes de todas las versiones como si fueran contratos distintos.
- El comando de calidad sin `--ted` ni `--borme` genera solo los indicadores
  base. No equivale a regenerar todas las validaciones externas.
- No reutilices un cruce TED antiguo: `main` aún usa `importe_sin_iva` como
  proxy del valor estimado. La [PR #23](https://github.com/BquantFinance/licitaciones-espana/pull/23)
  aborda TED, versiones y otras correcciones. Este cambio se centra en recuperar
  los importes desde su fuente y puede complementar esa PR.
- Solo el mantenedor con permisos puede sustituir los assets del release
  oficial. Probar una muestra o preparar el código no actualiza ese release.

Pruebas automatizadas:

```bash
python -m pytest -q tests/test_parsear_entry.py tests/test_reparar_importes.py
```
