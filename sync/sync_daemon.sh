#!/bin/sh
# sync_daemon.sh - versión robusta
set -eu

REPO_DIR="${REPO_DIR:-/repo}"
GIT_BRANCH="${GIT_BRANCH:-main}"
GIT_AUTHOR_NAME="${GIT_AUTHOR_NAME:-Nextcloud Sync}"
GIT_AUTHOR_EMAIL="${GIT_AUTHOR_EMAIL:-sync@localhost}"
SYNC_INTERVAL="${SYNC_INTERVAL:-300}"

log() { echo "$(date -u '+%Y-%m-%d %H:%M:%S UTC') - $*"; }

log "DEBUG: REPO_DIR=${REPO_DIR}"
ls -la "$REPO_DIR" || true

if [ ! -d "$REPO_DIR/.git" ]; then
  log "ERROR: No .git en $REPO_DIR — abortando"
  ls -la "$REPO_DIR" || true
  exit 3
fi

cd "$REPO_DIR"

# Required env vars
MISSING=0
for v in GIT_USER GIT_TOKEN GIT_REPO; do
  eval val=\${$v:-}
  if [ -z "$val" ]; then
    log "ERROR: falta variable obligatoria: $v"
    MISSING=1
  fi
done
[ "$MISSING" -eq 0 ] || { log "Corrige variables de entorno"; exit 4; }

GIT_REMOTE="https://$GIT_USER:$GIT_TOKEN@github.com/$GIT_REPO.git"

# configure local identity
git config user.name "$GIT_AUTHOR_NAME"
git config user.email "$GIT_AUTHOR_EMAIL"
git remote set-url origin "$GIT_REMOTE" 2>/dev/null || git remote add origin "$GIT_REMOTE" 2>/dev/null || true

process_once() {
  log "Starting single sync run..."
  git fetch origin || log "Warning: git fetch failed"

  git status --porcelain -z --untracked-files=all | while IFS= read -r -d '' entry; do
    code=$(printf "%s" "$entry" | cut -c1-2)
    path=$(printf "%s" "$entry" | cut -c4-)
    [ -z "$path" ] && path="$entry"
    code=${code:-"  "}
    idx_char=$(printf "%s" "$code" | cut -c1)
    wk_char=$(printf "%s" "$code" | cut -c2)
    log "Detected status '$code' on path '$path'"

    if [ "$code" = "??" ]; then
      git add -- "$path" && git commit -m "Add: $path" -- "$path" || log "Commit add failed for $path"
    elif [ "$wk_char" = "D" ] || [ "$idx_char" = "D" ]; then
      git rm -- "$path" || true
      git commit -m "Delete: $path" -- "$path" || log "Commit delete failed for $path"
    else
      git add -- "$path" && git commit -m "Update: $path" -- "$path" || log "Commit update failed for $path"
    fi
  done

  ahead=$(git rev-list --count origin/"$GIT_BRANCH"..HEAD 2>/dev/null || echo 0)
  if [ "${ahead:-0}" -gt 0 ]; then
    log "Found $ahead local commits to push. Pushing..."
    if git push origin "HEAD:$GIT_BRANCH"; then
      log "Push successful."
    else
      log "Push rejected. Attempting rebase..."
      if git pull --rebase origin "$GIT_BRANCH"; then
        if git push origin "HEAD:$GIT_BRANCH"; then
          log "Push successful after rebase."
        else
          BR="sync-conflict-$(date +%Y%m%d%H%M%S)"
          git branch "$BR"
          git push origin "$BR"
          log "Pushed to $BR. Manual merge required."
        fi
      else
        BR="sync-conflict-$(date +%Y%m%d%H%M%S)"
        git branch "$BR"
        git push origin "$BR"
        log "Rebase failed. Pushed to $BR. Manual merge required."
      fi
    fi
  else
    log "No new local commits to push."
  fi

  log "Single sync run finished."
}

if [ "${1:-}" = "--once" ]; then process_once; exit 0; fi

log "Daemon mode. Interval=${SYNC_INTERVAL}s"
while true; do
  process_once
  log "Sleeping ${SYNC_INTERVAL}s..."
  sleep "$SYNC_INTERVAL"
done
