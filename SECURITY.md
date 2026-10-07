# Seguridad

## Qué se avisa en privado

- **Una vulnerabilidad en el código.** Por ejemplo, un fichero descargado que al descomprimirse escribe fuera de su carpeta, o un lector que ejecuta contenido de un portal.
- **Un secreto publicado por error**: un token, una contraseña o una clave.
- **Datos personales que no deberían estar publicados.** Por ejemplo, un nombre de persona física en los cargos del BORME (que se publican seudonimizados) o un dato personal real en un fixture de los tests.

Un error en los datos tal como los publica la fuente no es un problema de seguridad: va en un issue normal, con la plantilla «Error en los datos».

## Cómo avisar

1. **En GitHub**, con un aviso privado: pestaña **Security** → **Report a vulnerability**.
2. Si no puedes usarlo, escribe por el [formulario de contacto de BQuant Finance](https://bquantfinance.com/contacto/), sin detalles, y te diremos cómo enviarlos.

No abras un issue público ni una PR con el detalle hasta que esté corregido.

## Qué se corrige

- **Código:** la rama `main`.
- **Datos:** el repositorio y la siguiente release.

Las copias que ya circulan (forks y descargas) no se pueden retirar.
