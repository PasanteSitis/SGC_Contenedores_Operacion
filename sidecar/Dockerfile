FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/sidecar

# Instala dependencias necesarias para pandas/openpyxl/pyyaml
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
      build-essential \
      libpq-dev \
      tini \
      procps \
      util-linux \
    && rm -rf /var/lib/apt/lists/*

# Copia requirements.txt (ver abajo) y código
COPY requirements.txt /opt/sidecar/requirements.txt
RUN pip install --no-cache-dir -r /opt/sidecar/requirements.txt

COPY . /opt/sidecar

# Entrypoint
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/sidecar/entrypoint.sh"]
