# ccaa_Andalucia/

Carpeta de salida por defecto de [`scripts/ccaa_andalucia.py`](../scripts/ccaa_andalucia.py). Lo que escribe el
script aquí (la capa cruda, `licitaciones_andalucia.parquet`, `_historico/`) es un dato y no va en el repositorio:
los datos se publican en las [releases](https://github.com/BquantFinance/licitaciones-espana/releases). El Parquet de
v2026.02 que estuvo aquí en Git LFS sigue en la etiqueta `datos-lfs-v2026.02`.

La carpeta va en el repositorio porque el script la crea al importarse: si el repositorio está montado en solo
lectura, tiene que existir ya.
