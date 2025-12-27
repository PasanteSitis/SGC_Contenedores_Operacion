#!/bin/sh
# sync_daemon.sh - DB-only, prioriza fileid -> oc_activity para detectar autor humano
set -eu

REPO_DIR="${REPO_DIR:-/repo}"
GIT_BRANCH="${GIT_BRANCH:-main}"
GIT_AUTHOR_NAME="${GIT_AUTHOR_NAME:-Nextcloud Sync}"
GIT_AUTHOR_EMAIL="${GIT_AUTHOR_EMAIL:-sync@localhost}"
SYNC_INTERVAL="${SYNC_INTERVAL:-300}"

# Lista de cuentas consideradas "sistema" (no humanas)
SYSTEM_USERS="nextcloud system cron updater www-data"

# log -> STDERR (importante: evitar contaminar stdout de funciones que retornan valores)
log() { printf '%s - %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')" "$*" >&2; }

log "DEBUG: REPO_DIR=${REPO_DIR}"
ls -la "$REPO_DIR" 2>/dev/null || true

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

# configure local identity (fallback global identity)
git config user.name "$GIT_AUTHOR_NAME" || true
git config user.email "$GIT_AUTHOR_EMAIL" || true
git remote set-url origin "$GIT_REMOTE" 2>/dev/null || git remote add origin "$GIT_REMOTE" 2>/dev/null || true

# ---------------- helper: lookup_user_meta ----------------
# devuelve "display|email" por stdout (sin logs mezclados)
lookup_user_meta() {
  username="$1"
  display=""
  email=""

  if [ -z "${DB_HOST:-}" ] || [ -z "${DB_NAME:-}" ]; then
    printf '%s|%s' "$display" "$email"
    return 0
  fi

  # oc_users.displayname
  SQL_USERS="SELECT displayname FROM oc_users WHERE uid = '${username}' LIMIT 1;"
  users_out=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_USERS}" 2>/tmp/psql_users_err || true)
  users_err=$(cat /tmp/psql_users_err 2>/dev/null || true)
  [ -n "$users_err" ] && log "DEBUG: psql oc_users stderr: $users_err"
  users_out=$(printf '%s' "$users_out" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
  if [ -n "$users_out" ]; then
    display="$users_out"
  fi

  # oc_preferences for email
  if [ -z "$email" ]; then
    SQL_PREF_EMAIL="SELECT configvalue FROM oc_preferences WHERE userid='${username}' AND (configkey='email' OR configkey='emailAddress' OR configkey='mail') LIMIT 1;"
    pref_out=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_PREF_EMAIL}" 2>/tmp/psql_pref_err || true)
    pref_err=$(cat /tmp/psql_pref_err 2>/dev/null || true)
    [ -n "$pref_err" ] && log "DEBUG: psql oc_preferences stderr: $pref_err"
    pref_out=$(printf '%s' "$pref_out" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    if [ -n "$pref_out" ]; then
      email="$pref_out"
    fi
  fi

  # oc_accounts.data (texto JSON) si hace falta
  if [ -z "$display" ] || [ -z "$email" ]; then
    SQL_ACC="SELECT data::text FROM oc_accounts WHERE uid='${username}' LIMIT 1;"
    acc_out=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_ACC}" 2>/tmp/psql_acc_err || true)
    acc_err=$(cat /tmp/psql_acc_err 2>/dev/null || true)
    [ -n "$acc_err" ] && log "DEBUG: psql oc_accounts stderr: $acc_err"
    if [ -n "$acc_out" ]; then
      if [ -z "$display" ]; then
        d=$(printf '%s' "$acc_out" | sed -n 's/.*"displayname"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' || true)
        [ -n "$d" ] && display="$d"
      fi
      if [ -z "$email" ]; then
        m=$(printf '%s' "$acc_out" | sed -n 's/.*"email"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' || true)
        [ -n "$m" ] && email="$m"
      fi
    fi
  fi

  printf '%s|%s' "$display" "$email"
}
# ---------------- end lookup_user_meta ----------------

# ---------------- helper: find_author_for_path ----------------
# Entrada: ruta completa nc_path (p.e. /TopDir/dir/file.docx)
# Salida: imprime "username" en stdout (solo el nombre)
find_author_for_path() {
  nc_path="$1"
  nc_rel=$(printf '%s' "$nc_path" | sed 's%^/%%' )

  if [ -z "${DB_HOST:-}" ] || [ -z "${DB_NAME:-}" ]; then
    printf ''
    return 0
  fi

  # intentar fileid en oc_filecache
  SQL_FILEID="SELECT fileid FROM oc_filecache WHERE path='${nc_rel}' LIMIT 1;"
  fcid=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_FILEID}" 2>/tmp/psql_fcid_err || true)
  fcid_err=$(cat /tmp/psql_fcid_err 2>/dev/null || true)
  [ -n "$fcid_err" ] && log "DEBUG: psql oc_filecache stderr: $fcid_err"
  fcid=$(printf '%s' "$fcid" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')

  if [ -n "$fcid" ]; then
    SQL_ACTS="SELECT COALESCE(\"user\", affecteduser) AS username, timestamp, activity_id FROM oc_activity WHERE app='files' AND object_id=${fcid} ORDER BY timestamp DESC, activity_id DESC LIMIT 20;"
    acts_out=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_ACTS}" 2>/tmp/psql_acts_err || true)
    acts_err=$(cat /tmp/psql_acts_err 2>/dev/null || true)
    [ -n "$acts_err" ] && log "DEBUG: psql oc_activity(act list) stderr: $acts_err"
    log "DEBUG: oc_activity rows for fileid=${fcid}: [$acts_out]"

    # buscar primera fila cuyo username NO sea SYSTEM_USERS
    if [ -n "$acts_out" ]; then
      # lista de usernames (col1)
      human=$(printf '%s' "$acts_out" | cut -d'|' -f1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | grep -v -E "^(?:$(echo "$SYSTEM_USERS" | sed 's/ /|/g'))$" | head -n1 || true)
      if [ -n "$human" ]; then
        printf '%s' "$human"
        return 0
      fi
      # si no hay humano, devolver la primera username (más reciente aunque sea sistema)
      first_user=$(printf '%s' "$acts_out" | cut -d'|' -f1 | head -n1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
      [ -n "$first_user" ] && printf '%s' "$first_user" && return 0
    fi
  fi

  # fallback por texto en subjectparams/file
  esc_path=$(printf '%s' "$nc_path" | sed "s/'/''/g")
  like_plain="%${esc_path}%"
  SQL_FALL="SELECT COALESCE(\"user\", affecteduser) AS username, object_id, file, subjectparams, timestamp
FROM oc_activity
WHERE app='files'
  AND (
     file = '${esc_path}'
     OR file LIKE '${like_plain}'
     OR subjectparams::text LIKE '${like_plain}'
  )
ORDER BY timestamp DESC
LIMIT 5;"
  fall_out=$(PGPASSWORD="${DB_PASS:-}" psql -h "${DB_HOST:-}" -p "${DB_PORT:-5432}" -U "${DB_USER:-}" -d "${DB_NAME:-}" -t -A -F '|' -c "${SQL_FALL}" 2>/tmp/psql_fall_err || true)
  fall_err=$(cat /tmp/psql_fall_err 2>/dev/null || true)
  [ -n "$fall_err" ] && log "DEBUG: psql oc_activity(fallback) stderr: $fall_err"
  log "DEBUG: fallback oc_activity result raw: [$fall_out] for nc_path=$nc_path"

  if [ -n "$fall_out" ]; then
    # preferir no-sistema:
    human2=$(printf '%s' "$fall_out" | cut -d'|' -f1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' | grep -v -E "^(?:$(echo "$SYSTEM_USERS" | sed 's/ /|/g'))$" | head -n1 || true)
    if [ -n "$human2" ]; then
      printf '%s' "$human2"
      return 0
    fi
    # si no hay humano, devolver la primera
    uname=$(printf '%s' "$fall_out" | cut -d'|' -f1 | head -n1 | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    [ -n "$uname" ] && printf '%s' "$uname" && return 0
  fi

  # nada encontrado
  printf ''
}
# ---------------- end find_author_for_path ----------------

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

    if [ -n "${NEXTCLOUD_START_DIR:-}" ]; then
      base="/${NEXTCLOUD_START_DIR%/}"
    else
      base=""
    fi
    nc_path="${base}/${path}"

    editor_user=""
    editor_display=""
    editor_email=""

    # determinar autor preferente
    editor_user=$(find_author_for_path "$nc_path" 2>/dev/null || true)
    log "DEBUG: find_author_for_path returned: [$editor_user] for nc_path=$nc_path"

    if [ -n "$editor_user" ]; then
      meta=$(lookup_user_meta "$editor_user" 2>/dev/null || true)
      editor_display=$(printf '%s' "$meta" | cut -d'|' -f1)
      editor_email=$(printf '%s' "$meta" | cut -d'|' -f2)
      [ -z "$editor_display" ] && editor_display="$editor_user"
      [ -z "$editor_email" ] && editor_email="${editor_user}@${EMAIL_DOMAIN:-example.com}"
    else
      editor_display="$GIT_AUTHOR_NAME"
      editor_email="$GIT_AUTHOR_EMAIL"
    fi

    if ! git add -- "$path" 2>/dev/null; then
      log "Warning: git add falló para $path"
    fi

    if [ "$code" = "??" ]; then
      msg="Add: $path"
    elif [ "$wk_char" = "D" ] || [ "$idx_char" = "D" ]; then
      msg="Delete: $path"
      git rm -- "$path" >/dev/null 2>&1 || true
    else
      msg="Update: $path"
    fi

    if git -c "user.name=$editor_display" -c "user.email=$editor_email" commit -m "$msg" -- "$path"; then
      log "Committed $msg as $editor_display <$editor_email>"
    else
      log "Commit failed for $path"
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
