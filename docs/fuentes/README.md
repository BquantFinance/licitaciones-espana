# Fuentes

Una ficha por fuente: qué trae la release, cómo lo descarga su script, sus columnas, cómo usarla sin contar dos veces y los errores conocidos de cada versión. Las columnas y los tipos de cada fichero están en [`docs/esquema/`](../esquema/README.md), y las marcas `_…` comunes, en [`docs/ESQUEMA.md`](../ESQUEMA.md). El código de cada script, con su docstring, es la referencia completa; su estado (cerrado y desde qué commit) está en [`docs/ESTADO_SCRAPERS.md`](../ESTADO_SCRAPERS.md), y cómo se ejecuta, en [`docs/REPRODUCIR.md`](../REPRODUCIR.md).

- **Nacional y Unión Europea:** [PLACSP](placsp.md) · [TED](ted.md) · [BORME](borme.md) · [Calidad](calidad.md)
- **Comunidades autónomas:** [Andalucía](andalucia.md) y [sus menores](andalucia_menores.md) · [Aragón](aragon.md) · [Asturias](asturias.md) · [Canarias](canarias.md) · [Castilla-La Mancha](castilla_la_mancha.md) · [Castilla y León](castilla_leon.md) · [Cataluña](catalunya.md) · [Comunitat Valenciana](valencia.md) y [sus menores](valencia_menores.md) · [Extremadura](extremadura.md) · [Galicia](galicia.md) · [La Rioja](la_rioja.md) · [Comunidad de Madrid](comunidad_madrid.md) · [Región de Murcia](murcia.md) · [País Vasco](euskadi.md)
- **Ayuntamientos:** [Madrid](madrid_ayuntamiento.md) · [8 ayuntamientos grandes, menores](municipios.md)

## Resumen de la release v2026.10

Medido sobre los ficheros de v2026.10. **Registros**: filas de los Parquet de cada ZIP. **Período**: años con datos según la fecha principal de cada fuente, sin contar fechas sueltas fuera de rango (menos del 0,05 % de las filas). **Tamaño**: Parquet, antes de comprimir en ZIP.

| Fuente | ZIP | Registros | Período | Tamaño |
|--------|-----|-----------|---------|--------|
| [Nacional (PLACSP)](placsp.md) | `nacional_*.zip` (9) | 10.9M entradas publicadas de 5.24M licitaciones + 41.3M filas en las tablas de detalle (resultados, adjudicatarios, lotes, criterios…) | 2012-2026 | 7.0 GB |
| [Catalunya (contratación)](catalunya.md) | `catalunya.zip` | 8.05M (14 tablas) | 2014-2026 | 1.17 GB |
| [Catalunya – contratos menores PSCP](catalunya.md#contratos-menores-de-la-pscp) | `catalunya_menores.zip` | 1.43M | 2016-2026 | 120 MB |
| [Valencia](valencia.md) | `valencia.zip` | 303K (contratación 276K + convenios, lobbies y entidades) | 2013-2026 | 49 MB |
| [Valencia – menores fuera del REGCON](valencia_menores.md) | `valencia_menores.zip` | 1.88M | 2015-2026 | 66 MB |
| [Andalucía](andalucia.md) | `andalucia.zip` | 901K | 2008-2026 | 115 MB |
| [Andalucía – menores (CKAN, con el SAS)](andalucia_menores.md) | `andalucia_menores.zip` | 769K | 2018-2026 | 75 MB |
| [Euskadi](euskadi.md) | `euskadi.zip` | 2.75M (9 tablas) | 2011-2026 | 387 MB |
| [Madrid – Comunidad](comunidad_madrid.md) | `comunidad_madrid.zip` | 4.91M | 2015-2026 | 176 MB |
| [Madrid – Ayuntamiento](madrid_ayuntamiento.md) | `madrid_ayuntamiento.zip` | 132K (tabla unificada; la fiel repite las mismas filas) | 2011-2026 | 32 MB |
| [Galicia](galicia.md) | `contratos_galicia.zip` | 1.85M (tabla final; la base repite las mismas filas con 12 columnas) | 2008-2026 | 82 MB |
| [Asturias](asturias.md) | `asturias.zip` | 375K | 2019-2024 | 22 MB |
| [Aragón](aragon.md) | `aragon.zip` | 267K (13 tablas) | 2009-2025 | 16 MB |
| [Castilla y León](castilla_leon.md) | `castilla_leon.zip` | 185K (15 tablas) | 2005-2026 | 13 MB |
| [Castilla-La Mancha](castilla_la_mancha.md) | `castilla_la_mancha.zip` | 6.95M (4.16M son líneas de factura del SESCAM) | 2015-2026 | 200 MB |
| [Región de Murcia](murcia.md) | `murcia.zip` | 1.01M | 2014-2025 | 22 MB |
| [Extremadura](extremadura.md) | `extremadura.zip` | 222K | 2022-2026 | 10 MB |
| [La Rioja](la_rioja.md) | `la_rioja.zip` | 351K | 2018-2026 | 9 MB |
| [Canarias](canarias.md) | `canarias.zip` | 115K | 2016-2026 | 13 MB |
| [8 ayuntamientos (menores)](municipios.md) | `municipios.zip` | 290K | 2015-2026 | 20 MB |
| [TED (España)](ted.md) | `ted.zip` | 857K filas de 355K avisos | 2006-2026 | 156 MB |
| [BORME (Registro Mercantil)](borme.md) | `borme.zip` | 9.61M actos + 17.8M cargos | 2009-2026 | 980 MB |
| [Calidad (indicadores)](calidad.md) | `calidad_licitaciones_resultado.zip` (fichero de v2026.02) | 8.7M filas × 20 indicadores (v2026.02, sin regenerar; ver correcciones PLACSP) | 2012-2026 | 977 MB |
| **TOTAL v2026.10** | **31 ZIP** | **87.8M + BORME (27.4M)** | **2005-2026** | **10.7 GB** |

Los registros y tamaños de cada fuente son los de v2026.10; no cuentan los ficheros de v2026.02 que van tal cual en la carpeta `v2026.02/` de algunos ZIP ni el de calidad.

## Portales

| Portal | URL |
|--------|-----|
| PLACSP | https://contrataciondelsectorpublico.gob.es/ |
| TED | https://ted.europa.eu/ |
| TED API v3 | https://ted.europa.eu/api/docs/ |
| TED CSV Bulk | https://data.europa.eu/data/datasets/ted-csv |
| Andalucía | https://www.juntadeandalucia.es/contratacion/ |
| Euskadi — KontratazioA | https://www.contratacion.euskadi.eus/ |
| Euskadi — Open Data | https://opendata.euskadi.eus/ |
| Euskadi — API REST | https://api.euskadi.eus/procurements/ |
| Madrid – Comunidad | https://contratos-publicos.comunidad.madrid/ |
| Madrid – Ayuntamiento | https://datos.madrid.es/ |
| Catalunya | https://analisi.transparenciacatalunya.cat/ |
| Valencia | https://dadesobertes.gva.es/ |
| Galicia | https://www.contratosdegalicia.gal/ |
| Asturias | https://sede.asturias.es/ |
| Canarias | https://datos.canarias.es/ · https://www.gobiernodecanarias.org/transparencia/ |
| Aragón | https://opendata.aragon.es/ · https://www.zaragoza.es/ |
| Castilla y León | https://datosabiertos.jcyl.es/ |
| Castilla-La Mancha | https://contratacion.castillalamancha.es/ · https://contratos.apps.uclm.es/ |
| Región de Murcia | https://datosabiertos.regiondemurcia.es/ · https://transparencia.carm.es/ |
| Extremadura | https://www.juntaex.es/ |
| La Rioja | https://web.larioja.org/dato-abierto |
| BORME | https://www.boe.es/diario_borme/ |
| BQuant Finance | https://bquantfinance.com |

## Estado de las fuentes

- [x] Nacional (PLACSP), Andalucía, Asturias, Catalunya, Euskadi, Galicia, Madrid, Valencia
- [x] Castilla y León, Región de Murcia y Aragón: `scripts/ccaa_castilla_leon.py`, `ccaa_murcia.py` (incluye los menores del Servicio Murciano de Salud 2019-2025) y `ccaa_aragon.py`, verificados en vivo el 2026-09-27
- [x] Extremadura: `scripts/ccaa_extremadura.py`, el Registro de Contratos de la Junta (menores, mayores e incidencias, 1T 2022-2T 2026; 207.702 filas de menores con NIF), verificado en vivo el 2026-09-28
- [x] La Rioja: `scripts/ccaa_la_rioja.py`, el CSV anual de menores del Gobierno (2018-2026, 350.967 filas, con NIF salvo personas físicas enmascaradas), verificado en vivo el 2026-09-28
- [x] Menores valencianos fuera del REGCON: `scripts/ccaa_valencia_menores.py` (Universitat de València, Ajuntament de València, Diputación de Alicante, UA, UMH y UPV; 1,43 M filas 2015-2026), verificado en vivo el 2026-09-28
- [x] Menores de 8 ayuntamientos grandes con poco o nada en el 1143: `scripts/municipios_menores.py` (Gijón, Vigo, Valladolid, Fuenlabrada, Leganés, Málaga, Córdoba y Santa Cruz de Tenerife; 289.401 filas 2015-2026), verificado en vivo el 2026-09-28
- [x] Andalucía, menores de la Junta con el SAS entero: `scripts/ccaa_andalucia_menores.py`, los CSV «Contratación Menor en {año}» del CKAN de la Junta (2018-2026, 768.647 registros, 544.898 del SAS, NIF válido en el 98,4 %), verificado en la descarga de producción el 2026-09-29. Cubre los ~41K menores del SAS que el buscador no alcanza; las dos fuentes se complementan ([`docs/COBERTURA.md`](../COBERTURA.md) §5.2)
- [x] Castilla-La Mancha: `scripts/ccaa_castilla_la_mancha.py`, los ficheros de transparencia de la Junta (menores, caja pagadora, SESCAM por línea de factura, sector público 2015-2018) y la UCLM 2017-2026, verificado en vivo el 2026-09-28. Los RAR necesitan `rarfile` con `unrar` o `libarchive-c`
- [x] Canarias: `scripts/ccaa_canarias.py` (CSV de contratos del Gobierno, que es su PLACSP; Las Palmas de GC 2016-2026 y Cabildo de Tenerife 2019-2025 por las API de sus portales; resúmenes del SCS y de los departamentos), verificado en vivo el 2026-09-30. El SCS solo publica totales: 89.340 menores en 2025 y ~1.500 contrato a contrato ([`docs/COBERTURA.md`](../COBERTURA.md) §5.2)
- [ ] Cantabria, Illes Balears, Ceuta y Melilla
- Navarra: su ley foral de contratos (art. 102.3) solo obliga a publicar la menor cuantía agregada por empresa y trimestre; no hay fuente contrato a contrato

**Contratos menores:** no hay cobertura del 100 %. Estimamos el 55-65 % de los menores publicados y no existe un denominador oficial completo (el RCSP es una cota inferior). Veredicto por comunidad, inventario de fuentes y plan: [`docs/COBERTURA.md`](../COBERTURA.md) (§0.1 y §5).

---

[README](../../README.md)
