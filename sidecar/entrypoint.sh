#!/usr/bin/env bash
set -euo pipefail

# Configurables por ENV
ROOT_DIR="${ROOT_DIR:-/repo}"
RUN_INTERVAL="${RUN_INTERVAL:-600}"   # segundos (default 10 min)
LOCKFILE="${LOCKFILE:-/var/lock/sidecar.lock}"
GIT_COMMIT="${GIT_COMMIT:-0}"         # 1 = hacer commit+push desde este contenedor
GIT_USER="${GIT_USER:-sidecar-bot}"
GIT_EMAIL="${GIT_EMAIL:-sidecar@local}"
GIT_BRANCH="${GIT_BRANCH:-master}"
# Opcional: si tu repo requiere credenciales, monta .netrc o usa token

# Logging simple
log() { echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') - $*"; }

# Ensure lock dir exists
mkdir -p "$(dirname "$LOCKFILE")"
touch "$LOCKFILE"

# ensure repo dir exists
if [ ! -d "$ROOT_DIR" ]; then
  log "ERROR: ROOT_DIR $ROOT_DIR no existe. Saliendo."
  exit 2
fi

# helper atomic write: move temp -> final
atomic_write() {
  tmp="$1.tmp.$$"
  cat > "$tmp" && mv -f "$tmp" "$1"
}

# run once function
run_once() {
  log "Attempting to acquire lock..."
  # Use flock on fd 200
  exec 200>"$LOCKFILE"
  if ! flock -n 200; then
    log "Lock busy - another instance running. Skipping this run."
    return 0
  fi
  log "Lock acquired. Running sidecar build..."

  TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  cd "$ROOT_DIR"

  # make sure we run from repo root if .git exists; otherwise still run in root
  if [ -d ".git" ]; then
    REPO_ROOT="$ROOT_DIR"
  else
    REPO_ROOT="$ROOT_DIR"
  fi

  # Ejecutar build_sidecars.py
  log "Running build_sidecars.py on $REPO_ROOT"
  if python3 /opt/sidecar/build_sidecars.py "$REPO_ROOT"; then
    log "build_sidecars.py OK"
  else
    log "build_sidecars.py returned non-zero"
  fi

  # Ejecutar build_catalog.py (usa metadata generada)
  log "Running build_catalog.py on $REPO_ROOT"
  if python3 /opt/sidecar/build_catalog.py "$REPO_ROOT"; then
    log "build_catalog.py OK"
  else
    log "build_catalog.py returned non-zero"
  fi

  # optionally commit changes (careful with races)
  if [ "${GIT_COMMIT}" = "1" ]; then
    if [ -d ".git" ]; then
      # configure minimal identity
      git config user.name "${GIT_USER}" || true
      git config user.email "${GIT_EMAIL}" || true
      # add and commit only metadata files (safer)
      git add -A "*.yml" "catalogo_grouped.xlsx" "catalog_index.json" || true
      if git diff --cached --quiet; then
        log "No changes to commit"
      else
        git commit -m "Automated: update sidecars & catalog (${TS})" || true
        # push - use origin remote already configured by sync process? If not, skip push.
        git push origin "${GIT_BRANCH}" || log "git push failed (maybe remote auth required)"
      fi
    else
      log "No .git in $ROOT_DIR; skipping git commit"
    fi
  fi

  # release flock by closing fd 200
  flock -u 200 || true
  exec 200>&-
  log "Run finished; lock released."
}

# If argument --once provided, run once and exit
if [ "${1:-}" = "--once" ]; then
  run_once
  exit 0
fi

log "Sidecar service starting. ROOT_DIR=$ROOT_DIR; interval=${RUN_INTERVAL}s; GIT_COMMIT=${GIT_COMMIT}"
# loop
while true; do
  run_once
  log "Sleeping ${RUN_INTERVAL}s..."
  sleep "$RUN_INTERVAL"
done
