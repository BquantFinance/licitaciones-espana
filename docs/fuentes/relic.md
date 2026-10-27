# Cataluña: prohibiciones de contratar e inscripciones del RELIC

- **En la release:** todavía no (el script es nuevo).
- **Script:** [`relic/relic_cataluna.py`](../../relic/relic_cataluna.py). Su docstring es la referencia completa.
- **Portal:** [dades obertes de la Generalitat](https://analisi.transparenciacatalunya.cat/d/t3wj-j4pu), conjunto `t3wj-j4pu` (Registre Electrònic d'Empreses Licitadores i Classificades de Catalunya, actualización diaria).
- **Licencia:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (Llicència oberta d'ús d'informació – Catalunya).

## Qué trae

El RELIC es el registro de licitadores de la Generalitat (art. 343 LCSP). Su conjunto abierto lista cada empresa inscrita con sus clasificaciones y, si la tiene, su **prohibición de contratar**: ámbito (todo el sector público o una administración concreta), causa (art. 71 LCSP), fecha de la resolución y periodo (inicio y fin). Es una de las pocas listas públicas de prohibiciones con fechas: el ROLECE estatal no publica la suya.

| Fichero | Contenido | Filas (2026-10-27) |
|---|---|--:|
| `inscripciones.parquet` | Las filas del conjunto de las personas jurídicas (una por empresa y clasificación), las 17 columnas tal cual, como texto | 23.085 (de 27.293) |
| `prohibiciones.parquet` | Las filas con «Prohibició de contractar» = `true`, acumuladas | 133 (de 168) |

**Solo vigentes:** el portal publica las prohibiciones en vigor; al caducar se cancelan (art. 73.2 LCSP) y desaparecen. El script se ejecuta cada semana: el Parquet acumulado conserva la que el portal deja de servir, con `_en_ultima_descarga = False` y su `_ultima_descarga`.

**Sin personas físicas:** el RELIC inscribe también a empresarios individuales con su nombre y no publica el NIF ni el tipo de persona. Solo se conservan las filas cuyo nombre lleva una forma jurídica o una palabra de entidad (S.L., S.A., SCCL, cooperativa, fundación, asociación, club, mancomunidad, ayuntamiento, consorcio, GmbH, Ltd…); las comunidades de bienes, tampoco. El 27-oct-2026 quedaban fuera 4.208 filas (35 prohibiciones); alguna persona jurídica sin forma en el nombre se pierde con ellas. La descarga original no se guarda (su sha256 queda en `raw/_manifiesto.json`).

## Cómo cruzarla

- **Sin NIF:** el RELIC publica el nombre y el número registral. El cruce con adjudicatarios va por nombre normalizado (sin forma jurídica ni puntuación), dentro de Cataluña o del ámbito de la prohibición.
- **Hecho medible:** una adjudicación con fecha entre «Data inici prohibició» y «Data fi prohibició» a una empresa con prohibición de ámbito «tot el sector públic» (o del mismo órgano). Es un hecho publicado, no un juicio: la prohibición puede estar recurrida o suspendida.

## Columnas añadidas

- De origen: `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `relic/relic_cataluna.py` | Socrata de la Generalitat (`t3wj-j4pu`) | Inscripciones y prohibiciones de contratar del RELIC (personas jurídicas): el CSV filtrado con versiones y dos Parquet acumulados |

---

[Todas las fuentes](README.md) · [README](../../README.md)
