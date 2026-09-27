"""Monta la §5 de docs/COBERTURA.md con los inventarios de los agentes (g1-g4).
Baja un nivel los títulos de cada inventario y quita su título '## 5...'.
Uso: python montar_seccion5.py <salida.md> g1 g2 g3 [g4]"""
import re
import sys
from pathlib import Path

S = Path(__file__).resolve().parent
TITULOS = {
    'g1': 'Catalunya, C. Valenciana, Illes Balears y Aragón',
    'g2': 'Andalucía, Murcia, Extremadura, Castilla-La Mancha, Canarias, Ceuta y Melilla',
    'g3': 'Madrid, Castilla y León, Galicia, Asturias y Cantabria',
    'g4': 'País Vasco, Navarra, La Rioja y referencias nacionales',
}
partes = ['## 5. Contratos menores: inventario de fuentes por comunidad (2026-09-27)\n',
          'Inventario hecho con red completa desde la nube el 2026-09-27, en cuatro bloques independientes, cada uno '
          'con su método y su leyenda de confianza (**A** = verificada en vivo; **M** = fuente secundaria o portal '
          'inalcanzable desde la nube; **B** = inferida). "¿En 1143?" es cuántos menores de ese órgano trae el feed '
          '1143 de la PLACSP. Las prioridades de cada bloque están resumidas y ordenadas en `docs/CONTINUACION.md` §3.6.\n']
for n, g in enumerate(sys.argv[2:], 1):
    texto = (S / g / 'inventario.md').read_text(encoding='utf-8').strip('\n').split('\n')
    if texto and texto[0].startswith(('# ', '## ')):
        texto = texto[1:]
    # g4: sin los títulos de su borrador ("## A. Sección para…", "### 5. …"); "## B. X" → "## X"
    texto = [l for l in texto if not l.startswith(('## A. ', '### 5. Contratos'))]
    texto = [('## ' + l[len('## B. '):]) if l.startswith('## B. ') else l for l in texto]
    salida = []
    for linea in texto:
        m = re.match(r'^(#{2,5}) (?:5\.\d+ )?(.*)$', linea)
        if m:
            nivel = min(len(m.group(1)) + 1, 6)
            linea = '#' * max(nivel, 4) + ' ' + m.group(2)
        salida.append(linea)
    partes.append(f'### 5.{n} {TITULOS[g]}\n\n' + '\n'.join(salida).strip('\n') + '\n')
Path(sys.argv[1]).write_text('\n'.join(partes), encoding='utf-8')
print(sys.argv[1], sum(len(p.split()) for p in partes), 'palabras')
