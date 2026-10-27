# Comunitat Valenciana: grupos de interés (REGIA) y actividades de influencia

- **En la release:** todavía no (el script es nuevo).
- **Script:** [`regia/regia_valencia.py`](../../regia/regia_valencia.py). Su docstring es la referencia completa.
- **Portal:** [dadesobertes.gva.es](https://dadesobertes.gva.es/): «Datos relativos a las entidades inscritas en el Registro de Grupos de Interés» e «Inventario de actividades de influencia».
- **Licencia:** [`DATA_LICENSE.md`](../../DATA_LICENSE.md#comunidades-autónomas) (datos abiertos de la Generalitat Valenciana).

## Qué trae

El REGIA es el registro de grupos de interés de la Generalitat Valenciana (Ley 25/2018). Trae, con **NIF y fecha de inscripción**, cada grupo inscrito, y el inventario de sus **actividades de influencia**: cada reunión o contacto con su fecha, tipo y asuntos, los grupos que estuvieron y los cargos de la Generalitat con los que se reunieron (el cargo, no la persona).

| Fichero | Contenido | Filas (2026-10-27) |
|---|---|--:|
| `grupos.parquet` | Grupo inscrito: nombre, nº REGIA, NIF, fecha de inscripción, categoría, subcategoría, tipo de organización y los ámbitos de actuación e interés (`S`/`N`) | 2.177 (de 2.210) |
| `reuniones.parquet` | Actividad de influencia: `id_reunion`, tipo, asuntos tratados, fecha, huella normativa | 2.550 |
| `reuniones_grupos.parquet` | Grupo de cada reunión (`id_reunion`, nombre, NIF, nº REGIA) | 2.682 (de 2.706) |
| `reuniones_cargos.parquet` | Cargo de la Generalitat de cada reunión (`id_reunion`, cargo) | 2.893 |

**Sin personas físicas ni contactos:** el portal enmascara a los empresarios individuales (nombre «Empresario individual» u «Otros», NIF `*********`) pero publica su dirección y su correo: esas filas no se guardan, ni las filas reunión-grupo que los citan (la reunión sí). La dirección y el correo no se guardan de ningún grupo. En los asuntos, un nombre con tratamiento («don», «D.», «Sra.», «senyor»…) se sustituye por «[persona física]».

**Versiones:** los cuatro CSV se guardan depurados con versiones y los Parquet acumulan: un grupo que se da de baja o una reunión que se retira sigue con `_en_ultima_descarga = False`. Se ejecuta cada mes.

## Cómo cruzarla

- **Por NIF:** el NIF del grupo casa con el del adjudicatario (sin casado por nombre). `reuniones_grupos.parquet` lleva el NIF de cada grupo de cada reunión.
- **Reunión → contratos:** la fecha de la reunión y el cargo (la dirección general o la conselleria) se comparan con las adjudicaciones de ese departamento al mismo grupo antes y después. Reunirse e inscribirse es legal y público: es un descriptor, no un juicio.

## Columnas añadidas

- De origen: `_fecha_descarga`.
- Las 3 de control de `comun/historico.py`.

## Scripts

| Script | Fuente | Descripción |
|--------|--------|-------------|
| `regia/regia_valencia.py` | dadesobertes.gva.es (URL de descarga de cada recurso; el robots.txt prohíbe la API y pide 10 s entre peticiones) | Grupos de interés del REGIA y sus actividades de influencia, sin personas físicas ni datos de contacto: cuatro CSV depurados con versiones y cuatro Parquet acumulados |

---

[Todas las fuentes](README.md) · [README](../../README.md)
