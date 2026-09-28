"""Crea un release en BORRADOR en BquantFinance/licitaciones-espana y sube ficheros.

Uso:
  python publicar_release.py crear <tag> <commit> <titulo> <notas.md>
  python publicar_release.py subir <id_release> <fichero> [<fichero>...]
  python publicar_release.py ver <id_release>

El token lo inyecta el proxy de la sesión (GITHUB_TOKEN). Nunca publica: el
release queda en borrador hasta que el propietario lo publique desde GitHub.
"""
import json
import os
import sys
from pathlib import Path

import requests

API = 'https://api.github.com/repos/BquantFinance/licitaciones-espana'
SUBIDAS = 'https://uploads.github.com/repos/BquantFinance/licitaciones-espana'
CABECERAS = {'Authorization': f"Bearer {os.environ['GITHUB_TOKEN']}", 'Accept': 'application/vnd.github+json',
             'X-GitHub-Api-Version': '2022-11-28'}
LIMITE = 2 * 1024 ** 3   # tamaño máximo de un fichero de release


def crear(tag, commit, titulo, notas):
    r = requests.post(f'{API}/releases', headers=CABECERAS, timeout=60, json={
        'tag_name': tag, 'target_commitish': commit, 'name': titulo,
        'body': Path(notas).read_text(encoding='utf-8'), 'draft': True, 'prerelease': False})
    print(r.status_code, r.json().get('id'), r.json().get('html_url'), r.json().get('message', ''))
    r.raise_for_status()


def subir(id_release, ficheros):
    existentes = {a['name'] for a in requests.get(f'{API}/releases/{id_release}/assets', headers=CABECERAS,
                                                  timeout=60, params={'per_page': 100}).json()}
    for fichero in map(Path, ficheros):
        if fichero.name in existentes:
            print(f'ya está: {fichero.name}')
            continue
        tam = fichero.stat().st_size
        if tam >= LIMITE:
            raise SystemExit(f'{fichero.name}: {tam / 1024 ** 3:.2f} GiB, más que el límite de 2 GiB')
        with open(fichero, 'rb') as f:
            r = requests.post(f'{SUBIDAS}/releases/{id_release}/assets', params={'name': fichero.name},
                              headers={**CABECERAS, 'Content-Type': 'application/octet-stream',
                                       'Content-Length': str(tam)}, data=f, timeout=(60, 7200))
        print(fichero.name, r.status_code, f'{tam / 1024 ** 2:,.0f} MB', r.json().get('state', r.text[:200]))
        r.raise_for_status()


def ver(id_release):
    r = requests.get(f'{API}/releases/{id_release}', headers=CABECERAS, timeout=60).json()
    print(json.dumps({k: r.get(k) for k in ('id', 'tag_name', 'name', 'draft', 'html_url')}, ensure_ascii=False))
    for a in r.get('assets', []):
        print(f"   {a['name']}: {a['size'] / 1024 ** 2:,.0f} MB ({a['state']})")


if __name__ == '__main__':
    orden, *resto = sys.argv[1:]
    {'crear': lambda: crear(*resto), 'subir': lambda: subir(resto[0], resto[1:]), 'ver': lambda: ver(*resto)}[orden]()
