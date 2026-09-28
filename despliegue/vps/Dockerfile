# Imagen para ejecutar los scrapers de licitaciones-espana en el VPS.
# El código NO va dentro: se monta el worktree anclado a un commit en /repo.
# unar: rarfile lo usa para los RAR que libarchive 3.7 no lee (Castilla-La Mancha, SESCAM 2T 2016).
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        libarchive13 libarchive-tools unar unzip ca-certificates procps && \
    rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt pytest && pip freeze > /pip-freeze.txt
RUN useradd -m -u 1000 scraper
USER scraper
WORKDIR /repo
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PY=python
