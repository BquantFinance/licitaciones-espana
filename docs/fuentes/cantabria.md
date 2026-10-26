# Cantabria: base de datos de contratos del Gobierno

- **En la release:** todavía no (el script es nuevo; la primera descarga completa son unas 290 peticiones).
- **Script:** [`scripts/ccaa_cantabria.py`](../../scripts/ccaa_cantabria.py). Su docstring es la referencia completa.
- **Portal:** «Base de datos de contratos» del [Portal de Transparencia de Cantabria](https://transparencia.cantabria.es/contratacion-publica) (aplicación en `aplicacionesweb.cantabria.es/transparencia`).
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (CC BY 3.0 ES).

## Qué trae

Todos los contratos adjudicados por la Administración de la Comunidad Autónoma de Cantabria y su sector público desde 2015, contrato a contrato, también los menores: 39.661 en la API el 2026-10-26.

| Fichero | Contenido | Filas |
|---|---|--:|
| `contratos.parquet` | La exportación CSV de la API, mes a mes por fecha de formalización: las 273 columnas de cada contrato (bloques de contrato menor, contrato general, basado en acuerdo marco o en sistema dinámico y otros), como texto | 39.436 con fecha de formalización (2015: 1.513 … 2019: 4.201 … 2025: 2.412) |

Lo que no está en el feed 1143 de la [PLACSP](placsp.md): en una muestra de tres trimestres (2.710 contratos), el 92-96 % de los menores ya está en el 1143 por NIF e importe. Lo nuevo son los **invitados y las ofertas de los menores** (`Invitados (detalle)` y `Ofertas (detalle)`, con el NIF de cada licitador, en el 28-39 % de los menores; con dos o más ofertas, el 19-29 %), las prórrogas, modificaciones, penalizaciones y subcontrataciones, y el 4-8 % de menores que no está en el 1143.

## Avisos

- **Para no contar dos veces**, cruza con el 1143 antes de sumar: los no menores llevan el `Número PLACSP` (o `Nº PLACSP` en los bloques de acuerdo marco y sistema dinámico); los menores, no.
- **Nombres repetidos:** la exportación repite el nombre de las columnas en cada bloque; el segundo `Sin lotes` es `Sin lotes.1`, como los pone pandas. Cada contrato rellena solo su bloque.
- **Los detalles van en una columna:** `Ofertas (detalle)` es «1- cif/nif: … razon: … sin_iva: … iva: …; 2- …». Los importes de las ofertas vienen a menudo a cero.
- **225 contratos sin fecha de formalización** (39.661 en la API y 39.436 en las ventanas): la API no deja pedirlos por ventana. El script lo avisa en cada ejecución.
- **Importes y fechas como texto:** '7.825,00 €', '11-02-2026'. El CSV se sirve como ISO-8859-1, pero es Windows-1252 (el euro en 0x80).
- **NIF de personas físicas enmascarado** ('***8328**').

## Columnas añadidas

- De origen: `_fuente`, `_ventana_desde`, `_ventana_hasta`, `_anio`, `_archivo_origen` y `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `scripts/ccaa_cantabria.py` | Portal de Transparencia de Cantabria (API de la base de datos de contratos) | Contratos del Gobierno de Cantabria y su sector público (2015-), menores incluidos: la exportación CSV de cada mes tal cual, con versiones, y un Parquet acumulado |

---

[Todas las fuentes](README.md) · [README](../../README.md)
