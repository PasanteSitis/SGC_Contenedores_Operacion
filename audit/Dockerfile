FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/audit

# Dependencias
COPY requirements.txt /opt/audit/requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && pip install --no-cache-dir -r /opt/audit/requirements.txt \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY . /opt/audit
CMD ["python", "log_audit.py"]