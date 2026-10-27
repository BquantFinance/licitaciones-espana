# Melilla: contratos menores de la Ciudad Autónoma

- **En la release:** todavía no (el script es nuevo; la primera descarga son 49 peticiones).
- **Script:** [`melilla/melilla_menores.py`](../../melilla/melilla_menores.py). Su docstring es la referencia completa.
- **Portal:** pestaña «Documentos» del [perfil del contratante de la Consejería de Hacienda de la Ciudad Autónoma de Melilla en la PLACSP](https://contrataciondelestado.es/wps/poc?uri=deeplink:perfilContratante&idBp=QOY179oppWwQK2TEfXGy%2BA%3D%3D), epígrafe «Publicidad de la contratación menor».
- **Licencia o condiciones del portal:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (documentos de publicidad obligatoria del perfil del contratante, art. 63 LCSP; las mismas condiciones que el resto de la PLACSP).

## Qué trae

La relación trimestral de los contratos menores de toda la Ciudad Autónoma, unidad gestora a unidad gestora (art. 63.4 LCSP), desde el primer trimestre de 2019. La Ciudad no carga sus menores contrato a contrato en el feed 1143 de la [PLACSP](placsp.md): en él, la Consejería de Hacienda tiene un menor. Lo publica en el epígrafe como documentos: 47 el 2026-10-27, de 2019-T1 a 2026-T2.

| Fichero | Contenido | Filas |
|---|---|--:|
| `melilla_menores.parquet` | Una fila por contrato y relación: las 30 relaciones trimestrales (2019-T1 a 2026-T2) y las 3 anuales (2019, 2020 y 2021), con los dos formatos de la Ciudad leídos a las mismas columnas, como texto | 24.620: 18.471 trimestrales (2019: 2.671 … 2022: 2.876 … 2025: 2.494; 2026-S1: 1.246) y 6.149 de las anuales |

- **Formato A** (2019-T1 a T3): expediente, fecha de adjudicación, objeto, importe, plazo de ejecución, la cuota de impuestos, tipo, tramitación, CIF y nombre del adjudicatario, adjudicatario extranjero y unidad.
- **Formato B** (desde 2019-T4): expediente, objeto, tipo, duración (texto), **presupuesto**, importe de adjudicación, **nº de empresas invitadas**, **nº de licitadores**, publicado en el tablón, adjudicatario («NOMBRE CIF: X»), fecha de adjudicación, **fecha del informe de necesidad** y unidad.
- 188 códigos de unidad: las consejerías y sus direcciones generales (el código cambia con cada estructura de gobierno: `07.7`, «Dirección General de Cultura y Festejos») y cuatro entes con NIF propio: el Consorcio UNED-Melilla (`14.`), el Instituto de las Culturas (`15.`, `80`), la Fundación Melilla Ciudad Monumental (`16.`) y el Patronato de Turismo (`17.`).

## Cómo se descarga

1. El perfil (un GET con dos redirecciones) y su pestaña «Documentos» (el POST del formulario JSF de la página, con su `ViewState`).
2. Los documentos del epígrafe que no se tienen (un GET cada uno; la PLACSP da a cada uno un `DocumentIdParam` fijo). Cada uno se guarda tal cual en `raw/documentos/`.
3. Qué periodo es cada documento: el que dice su contenido («de 20190101 a 20190331», «Del 01/10/2025 al 31/12/2025») o, si no lo trae, su título y su nombre («4º trimestre 2020», «CME_2025_2T», «2019 completo»).
4. De cada periodo se lee el XLS; el PDF, solo si el periodo no tiene XLS. Donde están los dos (2019-T1 a 2021-T3 y las anuales de 2019 y 2020) se cotejan fila a fila.

Con 5 segundos entre peticiones y sin reintentar un 403 o un 429: la primera descarga son 49 peticiones (unos 5 minutos, más el cotejo de los 13 PDF, otros 5); la semanal, 2 y una más por relación nueva.

## Avisos

- **IPSI, no IVA.** Melilla no tiene IVA: tiene el Impuesto sobre la Producción, los Servicios y la Importación. La cabecera no dice si el importe lo lleva, y el escalón en el umbral del menor dice que sí: de los servicios y suministros de más de 15.000 €, 635 de 662 están por debajo de 16.500 €; de las obras de más de 40.000 €, las 173 están por debajo de 44.000 €. La cuota del formato A no es coherente fila a fila (4 %, 10 %, 2 %, 0,5 % y 8 %, pero también 21 % y 86 cuotas iguales o mayores que el importe): no sirve para sacar la base.
- **El mismo expediente en dos relaciones.** 333 expedientes salen en dos relaciones trimestrales (bordes de trimestre: la de 2022-T3 empieza el 1 de junio), y los de 2019-2021, también en su anual. Salen todas las filas; para contar contratos, quédate con una por expediente (la relación más reciente: las anuales rellenan importes que el trimestre daba a 0: 256 en 2019, 158 en 2020 y 8 en 2021).
- **Las anuales no son la suma de los trimestres.** La de 2019 está en el formato B y tiene 2.252 expedientes; los trimestres de 2019, 2.641 (los del formato A incluyen expedientes de 2018 adjudicados en 2019). Las de 2020 y 2021 casan con sus trimestres en expedientes (1.847 frente a 1.848; 2.050 frente a 2.050).
- **Importe 0** en 981 filas trimestrales (539 de 2019): sin importe publicado.
- **Una fecha imposible en origen** (`26/08/0119`): se da como viene, `0119-08-26`.
- **El PDF** reparte el final de un nombre largo en la celda de al lado, que a veces cae encima de la fecha: en los 13 periodos cotejados, las filas, importes, NIF y unidades son iguales al XLS, y 9 fechas del PDF no se pueden leer (`fecha_ilegible_pdf` en `raw/_cotejo.json`).
- **Personas físicas, sin nombre ni NIF**: 4.287 filas trimestrales (23 %) tienen por adjudicatario a una persona física (DNI, NIE, pasaporte, «APELLIDO*APELLIDO,NOMBRE»); van con `adjudicatario` y `adjudicatario_nif` nulos y `adjudicatario_persona_fisica` = 'True'. Ante la duda (un identificador que no es el NIF de una persona jurídica) se oculta.
- **Lo que también está en la PLACSP**: casi nada de la Ciudad (1 expediente), pero sí los menores del Patronato de Turismo de 2024-2025, que este publica en la PLACSP con su NIF (Q5200009H): 106 expedientes. En la PLACSP el importe va sin impuesto; aquí, con él.

## Columnas añadidas

- De origen: `_fuente`, `_archivo_origen`, `_documento_id`, `_titulo`, `_publicado`, `_relacion`, `_anio`, `_trimestre`, `_formato_origen`, `_fila` y `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `melilla/melilla_menores.py` | PLACSP, pestaña «Documentos» del perfil de la Ciudad Autónoma de Melilla | Relaciones trimestrales y anuales de contratos menores (2019-), XLS y PDF tal cual con versiones, cotejo XLS-PDF y un Parquet acumulado |

---

[Todas las fuentes](README.md) · [README](../../README.md)
