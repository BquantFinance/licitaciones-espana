# Licencias de los datos

Los datos de este repositorio y de sus releases tienen tres capas, cada una con su licencia:

| Qué | Licencia |
|---|---|
| **El código**: scripts, tests y documentación | [MIT](LICENSE) |
| **Lo que añade este proyecto a los datos**: columnas de control y marcas (`_primera_descarga`, `_en_ultima_descarga`, `_origen`, `_duplicado`, `_columnas_corridas`…), versiones de la PLACSP (`n_versiones`, `es_ultima_version`, `entrada_repetida`), valores corregidos (`<campo>_corregido`, `correccion_<campo>`), [`calidad/errores_fuente.csv`](calidad/errores_fuente.csv), indicadores de calidad, cruces entre fuentes y el código seudónimo del BORME (`persona_hash`) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/deed.es) |
| **Los datos de origen**: los valores tal como los publica cada portal | La licencia o las condiciones de reutilización de cada portal ([tabla](#por-fuente)) |

Para atribuir lo que añade este proyecto: «licitaciones-espana, BQuant Finance, https://github.com/BquantFinance/licitaciones-espana (CC BY 4.0)», o la cita de [`CITATION.cff`](CITATION.cff).

> Verificado el **2026-10-07** en la página de condiciones de cada portal, enlazada en cada fila. Los portales pueden cambiarlas: comprueba las vigentes antes de reutilizar los datos. Esto no es asesoramiento jurídico.

## Lo que piden casi todas

En España, el marco general es la [Ley 37/2007](https://www.boe.es/buscar/act.php?id=BOE-A-2007-19814) de reutilización de la información del sector público y sus condiciones generales ([RD 1495/2011](https://www.boe.es/buscar/act.php?id=BOE-A-2011-17560)). Muchos portales usan además Creative Commons Reconocimiento (CC BY). Con una u otra, casi todas piden:

- **citar la fuente**, con la fórmula que indique cada portal (en la tabla, cuando la da);
- **no desnaturalizar** el sentido de la información;
- **indicar la fecha de la última actualización**. La fecha de cada descarga va en el `LEEME.txt` de cada ZIP de la release y, en la mayoría de las tablas, en la columna `_ultima_descarga`;
- **no sugerir** que el organismo patrocina o respalda la reutilización.

## Datos personales

- Algunas fuentes publican el nombre y el NIF de personas físicas: por ejemplo, autónomos adjudicatarios de contratos menores.
- Los cargos del BORME se publican **seudonimizados** (`persona_hash`): no es anonimización. El código es el mismo para el mismo nombre, así que se pueden seguir los cargos de una persona cuyo nombre ya se conoce, y junta a los homónimos.
- Su reutilización está sujeta al RGPD y a la LOPDGDD. El BOE lo exige expresamente en sus condiciones.

## Por fuente

Cada fila dice de dónde descargan los scripts, qué licencia o condiciones publica ese portal y dónde se ha leído. Entre comillas, la fórmula de cita que pide el portal, cuando la da.

### Nacional y Unión Europea

| Fuente (ZIP de la release) | Licencia o condiciones | Verificado en |
|---|---|---|
| **PLACSP**, datos abiertos de la Plataforma de Contratación del Sector Público (`nacional_*.zip`) | Condiciones generales de reutilización del Ministerio de Hacienda (Ley 37/2007; RD 1495/2011, modalidad básica): uso comercial y no comercial. «Origen de los datos: Ministerio de Hacienda» | [Aviso legal de Hacienda](https://www.hacienda.gob.es/es-ES/Paginas/Avisolegal.aspx) (las condiciones de uso que enlaza datos.gob.es) y [aviso legal de la PLACSP](https://contrataciondelestado.es/wps/portal/info/aviso_legal) |
| **TED**, CSV masivo y API v3 (`ted.zip`) | Política de reutilización de la Comisión Europea ([Decisión 2011/833/UE](https://eur-lex.europa.eu/eli/dec/2011/833/oj)): los anuncios se reutilizan libremente, con fines comerciales o no, reconociendo la fuente. En el CSV masivo, 12 de las 48 distribuciones llevan además CC BY 4.0 | [Aviso legal de TED](https://ted.europa.eu/en/legal-notice) y [ficha del CSV en data.europa.eu](https://data.europa.eu/data/datasets/ted-csv) |
| **BORME**, PDF de boe.es (`borme.zip`) | Licencia tipo de la Agencia Estatal BOE (Resolución de 27 de junio de 2024). Hay que citar la fuente con un enlace a [boe.es](https://www.boe.es): «Basado en datos de la Agencia Estatal Boletín Oficial del Estado». Exige respetar el RGPD | [Aviso legal del BOE](https://www.boe.es/informacion/aviso_legal/) |

### Comunidades autónomas

| Fuente (ZIP de la release) | Licencia o condiciones | Verificado en |
|---|---|---|
| **Andalucía**, buscador de contratación de la Junta (`andalucia.zip`) | Sin condiciones propias para los datos. El aviso legal del portal ofrece CC BY 3.0 para sus «textos»: «Información obtenida del Portal de la Junta de Andalucía» | [Aviso legal](https://www.juntadeandalucia.es/informacion/legal.html) |
| **Andalucía**, CKAN de datos abiertos, contratación menor (`andalucia_menores.zip`) | CC BY 4.0 | [Ficha del conjunto](https://www.juntadeandalucia.es/datosabiertos/portal/dataset/contratacion-menor-plataforma-de-contratacion-andalucia-2024) |
| **Aragón**, Gobierno de Aragón en opendata.aragon.es (`aragon.zip`) | CC BY 4.0. «Fuente de los datos: Gobierno de Aragón» | [Términos de uso](https://opendata.aragon.es/informacion/terminos-de-uso-licencias) |
| **Aragón**, Ayuntamiento de Zaragoza, OCDS (`aragon.zip`) | «Condiciones de uso» propias: reutilización con fines comerciales y no comerciales. «Origen de los datos: Ayuntamiento de Zaragoza» | [Aviso legal](https://www.zaragoza.es/sede/portal/aviso-legal#condiciones) |
| **Asturias**, contratación centralizada del Principado (`asturias.zip`) | CC BY 4.0. «Origen de los datos: Administración del Principado de Asturias» | [Licencias y términos de uso](https://www.asturias.es/licencias-y-terminos-de-uso) y [ficha en datos.gob.es](https://datos.gob.es/es/catalogo/a03002951-contratacion-publica-asturias) |
| **Canarias**, Gobierno de Canarias, contratos en datos.canarias.es (`canarias.zip`) | Aviso legal del Gobierno de Canarias, que publica sus conjuntos en CC BY 4.0. «Fuente: Gobierno de Canarias» | [Aviso legal y condiciones de uso](https://datos.canarias.es/portal/aviso-legal-y-condiciones-de-uso) |
| **Canarias**, resúmenes de menores de los departamentos y del SCS (`canarias.zip`) | «Se autoriza su reproducción siempre que se cite la fuente» | [Aviso legal del Gobierno de Canarias](https://www.gobiernodecanarias.org/principal/avisolegal.html) |
| **Canarias**, Ayuntamiento de Las Palmas de Gran Canaria (`canarias.zip`) | Aviso legal restrictivo: reproducción sin propósito comercial y comunicándolo antes al Ayuntamiento | [Aviso legal](https://transparencia.laspalmasgc.es/aviso-legal) |
| **Canarias**, Cabildo de Tenerife (`canarias.zip`) | Contradictorio: el aviso legal dice CC0 y la página de propiedad intelectual, CC BY 4.0 | [Aviso legal](https://www.tenerife.es/aviso-legal) y [propiedad intelectual](https://www.tenerife.es/propiedad-intelectual) |
| **Castilla-La Mancha**, Junta: menores, caja pagadora, SESCAM y sector público (`castilla_la_mancha.zip`) | El portal de contratación no tiene aviso propio. El de la Junta permite reproducir citando la fuente, sin desfigurar el sentido e indicando la fecha de la última publicación | [Aviso legal de la Junta](https://www.castillalamancha.es/avisolegal) |
| **Castilla-La Mancha**, Universidad de Castilla-La Mancha (`castilla_la_mancha.zip`) | Aviso legal con todos los derechos reservados: «prohibida toda reproducción… salvo consentimiento expreso». Leído en una copia de enero de 2026, porque el sitio respondió 403 | [Aviso legal](https://www.uclm.es/es/legal/informacion-legal/aviso-legal) |
| **Castilla y León**, Junta y SACYL (`castilla_leon.zip`) | CC BY 4.0. «Fuente de los datos: Junta de Castilla y León» | [Términos de uso](https://datosabiertos.jcyl.es/web/es/catalogo-datos/terminos.html) y [ficha de contratos menores](https://analisis.datosabiertos.jcyl.es/explore/dataset/contratos-menores/) |
| **Cataluña**, dades obertes de la Generalitat: RPC, PSCP, menores y el resto de conjuntos de la carpeta (`catalunya.zip`, `catalunya_menores.zip`) | Llicència oberta d'ús d'informació – Catalunya (no es CC BY): no alterar el contenido, citar «Generalitat de Catalunya. Departament de …» e indicar la fecha de actualización. No autoriza sublicenciar los datos | [Llicències](https://web.gencat.cat/ca/generalitat/dades-indicadors/dades-obertes/llicencies) |
| **Cataluña**, Plataforma de Serveis de Contractació Pública (`catalunya_menores.zip`) | Avís legal de la Generalitat: Llicència oberta d'ús d'informació – Catalunya o CC0 | [Avís legal](https://web.gencat.cat/ca/avis-legal) |
| **Cataluña**, Ayuntamiento de Barcelona, Open Data BCN (`catalunya.zip`) | CC BY 4.0. «Font de les dades: Ajuntament de Barcelona» | [Condicions d'ús](https://opendata-ajuntament.barcelona.cat/ca/condicions-us) |
| **C. Valenciana**, dades obertes de la Generalitat: REGCON y el resto de conjuntos de la carpeta (`valencia.zip`) | CC BY (sin versión, según su catálogo). El aviso legal del portal no respondió | [Ficha de ejemplo](https://dadesobertes.gva.es/dataset/eco-gvo-contratos-2025) |
| **C. Valenciana**, Universitat de València (`valencia_menores.zip`) | Aviso legal restrictivo: «Se prohíbe la reproducción total o parcial… sin solicitar autorización» | [Aviso legal](https://www.uv.es/uvweb/universidad/es/aviso-legal/aviso-legal-1285919088090.html) |
| **C. Valenciana**, Ajuntament de València (`valencia_menores.zip`) | Condiciones propias: permite reutilizar sin alterar ni desnaturalizar la información y citando la fuente | [Aviso legal](https://www.valencia.es/cas/aviso-legal) |
| **C. Valenciana**, Diputación de Alicante (`valencia_menores.zip`) | Contradictorio: enumera las condiciones de la Ley 37/2007 y luego prohíbe la reproducción de los contenidos | [Aviso legal](https://abierta.diputacionalicante.es/aviso-legal/) |
| **C. Valenciana**, Universidad de Alicante (`valencia_menores.zip`) | Solo uso personal, sin finalidad comercial ni de distribución | [Condiciones de uso](https://si.ua.es/es/web-institucional-ua/normativa/condiciones-de-uso.html) |
| **C. Valenciana**, Universidad Miguel Hernández (`valencia_menores.zip`) | No verificado: su aviso legal no dice nada sobre la reutilización de datos | [Aviso legal](https://sede.umh.es/legal/aviso-legal/) |
| **C. Valenciana**, Universitat Politècnica de València (`valencia_menores.zip`) | CC BY (`contratos-menores`) y ODC-BY (`contratos-menores-ley-1-2022`) | [Ficha](https://upvtransparent.upv.es/dataset/contratos-menores) |
| **Extremadura**, Registro de Contratos de la Junta (`extremadura.zip`) | Contradictorio: el aviso legal limita el uso a la descarga y el uso privado, y a continuación recoge las condiciones de la Ley 37/2007 | [Aviso legal](https://www.juntaex.es/aviso-legal) |
| **Galicia**, Contratos Públicos de Galicia (`contratos_galicia.zip`) | Condiciones propias: reproducción, modificación, distribución y comunicación para usos comerciales y no comerciales, sin desnaturalizar el contenido y citando la fuente | [Aviso legal](https://www.contratosdegalicia.gal/avisoLegal.jsp?lang=es) |
| **La Rioja**, contratos menores del Gobierno (`la_rioja.zip`) | CC BY | [Términos de uso](https://web.larioja.org/dato-abierto/soporte-ayuda) y [ficha opd-979](https://web.larioja.org/dato-abierto/datoabierto?n=opd-979) |
| **Comunidad de Madrid**, portal de contratación (`comunidad_madrid.zip`) | Contradictorio: el aviso propio del portal solo admite el uso personal y privado. El aviso general de comunidad.madrid, que dice cubrir todos sus subdominios, permite reutilizar sin alterar el contenido, citando la fuente e indicando la fecha de extracción | [Aviso del portal](https://contratos-publicos.comunidad.madrid/aviso-legal-0) y [aviso general](https://www.comunidad.madrid/atencion-ciudadano/aviso-legal-privacidad) |
| **Región de Murcia**, CARM en datos abiertos (`murcia.zip`) | Condiciones generales de la Ley 37/2007: uso comercial y no comercial, citando el origen de los datos e indicando la fecha de actualización | [Aviso legal](https://datosabiertos.regiondemurcia.es/avisolegal) |
| **Región de Murcia**, Servicio Murciano de Salud en el portal de transparencia (`murcia.zip`) | CC BY 4.0 | [Términos de uso](https://transparencia.carm.es/web/transparencia/terminos-de-uso-y-privacidad) |
| **País Vasco**, Open Data Euskadi y su API de contratos (`euskadi.zip`) | CC BY 4.0 en las fichas de contratación. La API no declara una licencia propia | [Información legal](https://opendata.euskadi.eus/general/-/informacion-legal-opendata/) |
| **País Vasco**, Ayuntamiento de Bilbao (`euskadi.zip`) | CC BY 3.0 ES, según sus fichas en datos.gob.es (el portal propio respondió 403) | [Ficha en datos.gob.es](https://datos.gob.es/es/catalogo/l01480209-contratos-adjudicados-durante-el-ano-2024) |

### Ayuntamientos

| Fuente (ZIP de la release) | Licencia o condiciones | Verificado en |
|---|---|---|
| **Madrid** (`madrid_ayuntamiento.zip`) | CC BY 4.0 | [Condiciones de uso](https://datos.madrid.es/pages/condiciones-de-uso) |
| **Gijón** (`municipios.zip`) | CC BY 4.0 | [Catálogo de datos abiertos](https://opendata.gijon.es/descargar.php?id=721&tipo=JSON), campo `licencia` del conjunto |
| **Vigo** (`municipios.zip`) | CC BY 4.0 según las condiciones del portal; sus fichas en datos.gob.es dicen ODC-By | [Condiciones de uso](https://datos.vigo.org/es/condiciones-de-uso-de-los-datos/) |
| **Valladolid** (`municipios.zip`) | El perfil del contratante remite a la normativa de la sede, que prohíbe la reproducción con fines comerciales sin autorización. Su portal de datos abiertos usa CC BY 3.0 ES, pero no consta que incluya estos contratos | [Normativa de la sede](https://sede.valladolid.es/opencms/system/modules/gsede/elements/contenido/normativa.jsp) |
| **Fuenlabrada** (`municipios.zip`) | Sin licencia en el portal de transparencia. El aviso del Ayuntamiento exige autorización expresa | [Aviso legal](https://www.ayto-fuenlabrada.es/web/portal/aviso-legal) |
| **Leganés** (`municipios.zip`) | Remite a la Ley 37/2007, pero pide permiso escrito para el uso comercial | [Aviso legal](https://www.leganes.org/w/aviso-legal) |
| **Málaga** (`municipios.zip`) | Contradictorio: el catálogo da CC BY-SA 4.0 (`by-sa-40`) y el título de la licencia dice CC BY 4.0. Ante la duda, CC BY-SA 4.0 | [Ficha](https://datosabiertos.malaga.eu/dataset/contratos-menores-2o-trimestre-2026-ayuntamiento-de-malaga) |
| **Córdoba** (`municipios.zip`) | Licencia no especificada | [Ficha](https://datosabiertos.cordoba.es/ckan/dataset/contratos-menores) |
| **Santa Cruz de Tenerife** (`municipios.zip`) | Aviso legal restrictivo: reproducción solo para uso personal y privado | [Aviso legal](https://www.santacruzdetenerife.es/gobiernoabierto/transparencia/aviso-legal) |

### Lo que calcula el proyecto

| Conjunto | Licencia |
|---|---|
| Indicadores de calidad (`calidad_licitaciones_resultado.zip`) y cruces entre fuentes | CC BY 4.0 para lo calculado. Llevan datos de la PLACSP, el TED y el BORME, con sus condiciones |

## Portales sin licencia para los datos o con avisos restrictivos

- **Sin licencia para los datos, o con avisos legales que restringen la reproducción o el uso comercial:**
  - la Universidad de Castilla-La Mancha, la Universitat de València, la Universidad de Alicante y la Diputación de Alicante;
  - la Junta de Extremadura;
  - el portal de contratación de la Comunidad de Madrid, que contradice el aviso general de la Comunidad;
  - los ayuntamientos de Las Palmas de Gran Canaria, Valladolid, Fuenlabrada, Leganés y Santa Cruz de Tenerife.
- **Sin licencia especificada:** Córdoba.
- **Contradictorios:** el Cabildo de Tenerife y Málaga.

Suelen ser avisos pensados para el contenido de la web más que para los datos. Son datos que estas administraciones publican por obligación de transparencia, y el marco general de su reutilización es la Ley 37/2007. Aun así, antes de reutilizarlos, sobre todo con fines comerciales, revisa sus condiciones o pide autorización al organismo.
