FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git curl ca-certificates gnupg lsb-release unzip && \
    # instalar git-lfs
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash && \
    apt-get update && apt-get install -y git-lfs && \
    git lfs install && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /nextcloud

COPY requirements.txt /nextcloud/requirements.txt
RUN pip install --no-cache-dir -r /nextcloud/requirements.txt

COPY . /nextcloud
RUN chmod +x /nextcloud/sync_daemon.py

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "sync_daemon.py"]
