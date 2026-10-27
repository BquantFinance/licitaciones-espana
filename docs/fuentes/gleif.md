# GLEIF: entidades españolas con LEI y sus grupos

- **En la release:** todavía no (el script es nuevo).
- **Script:** [`gleif/gleif_espana.py`](../../gleif/gleif_espana.py). Su docstring es la referencia completa.
- **Portal:** [Golden Copy de la Global LEI Foundation](https://www.gleif.org/en/lei-data/gleif-golden-copy) (API en `goldencopy.gleif.org`).
- **Licencia:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#nacional-y-unión-europea) (CC0 1.0).

## Qué trae

El LEI (Legal Entity Identifier, ISO 17442) es el código que identifica a una entidad jurídica en los mercados financieros: lo necesita cualquier sociedad que emita valores, opere con derivados o reciba según qué financiación. La GLEIF publica tres veces al día todos los LEI del mundo con su ficha y, desde 2017, quién consolida las cuentas de quién («nivel 2»). Es el único dato abierto y gratuito de **grupos empresariales con la matriz declarada**: el Registro Mercantil no publica la estructura de los grupos y la titularidad real no es pública.

| Fichero | Contenido | Filas (2026-10-26) |
|---|---|--:|
| `entidades.parquet` | Ficha de cada entidad del ámbito: las 338 columnas del formato LEI-CDF 3.1 (nombre, domicilios, registro e identificador de registro, forma jurídica, categoría, estado, sucesoras, fechas), como texto | 197.808 (195.434 españolas; 105.861 con el registro vigente) |
| `relaciones.parquet` | Relaciones de nivel 2 con un extremo español (RR-CDF 2.1): matriz directa (`IS_DIRECTLY_CONSOLIDATED_BY`), matriz última (`IS_ULTIMATELY_CONSOLIDATED_BY`), sucursal internacional, fondo y gestora, con su periodo y su validación | 22.607 |
| `excepciones.parquet` | Por qué una entidad española no informa de su matriz (*Reporting Exceptions*): no consolida, la controlan personas físicas, no se conoce, no es pública… | 373.624 |

**Ámbito:** entidades con domicilio legal, sede o jurisdicción en España, más las de cualquier país enlazadas con ellas por una relación (matrices extranjeras, filiales en el extranjero). **Nunca entran los empresarios individuales** (personas físicas): ni las entidades de categoría `SOLE_PROPRIETOR` ni las españolas cuyo identificador de registro es un DNI o un NIE, ni las relaciones que las citan. Por eso los ZIP originales, que las traen de todo el mundo, se borran al terminar (con `--conservar-zip` se quedan).

## Cómo cruzarla

- **Por NIF:** 24.091 entidades españolas traen el NIF en `Entity.RegistrationAuthority.RegistrationAuthorityEntityID` (las inscritas con `RA000780` y algunas más). Las inscritas por el Registro Mercantil (`RA000533`, 159.746) traen su número de inscripción, no el NIF.
- **Por nombre:** el nombre legal normalizado (sin forma jurídica ni puntuación) casa con el de un adjudicatario cuando apunta a un solo NIF. Sobre las 3.831 entidades que traen NIF y casan también por nombre, el nombre da el mismo NIF en 3.820 (99,7 %).
- **Grupo:** la matriz última de una entidad (o su matriz directa, o la última de esa) agrupa a sus filiales españolas.

## Avisos

- **Solo están las entidades que tienen LEI**: sobre todo sociedades medianas y grandes, entidades financieras, fondos y administraciones que emiten deuda. Una pyme sin actividad financiera no suele tenerlo.
- **La matriz la declara la propia entidad** y la valida la entidad emisora (LOU); `Registration.ValidationSources` dice cómo (`FULLY_CORROBORATED`, `PARTIALLY_CORROBORATED`, `ENTITY_SUPPLIED_ONLY`).
- **Nivel 2 es consolidación contable**, no titularidad: una participación sin consolidación no aparece.
- **Estados:** un LEI no se borra; cambia de estado (`ISSUED`, `LAPSED` si no se renueva, `RETIRED`, `ANNULLED`, `MERGED`…). Una entidad `LAPSED` sigue existiendo: solo no ha pagado la renovación.

## Columnas añadidas

- De origen: `_publicacion` (fecha de la publicación de la Golden Copy) y `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `gleif/gleif_espana.py` | Golden Copy de GLEIF (API) | Entidades españolas con LEI, las enlazadas con ellas por relaciones de nivel 2 y las excepciones de matriz: las filas del ámbito tal cual, con versiones, y tres Parquet acumulados |

---

[Todas las fuentes](README.md) · [README](../../README.md)
