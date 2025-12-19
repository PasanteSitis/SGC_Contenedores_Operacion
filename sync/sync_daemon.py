#!/usr/bin/env python3
"""
batch_sync.py
Recorre el espacio accesible por NEXTCLOUD_SYNC_USER via WebDAV (PROPFIND)
y aplica commits individuales por fichero modificado. Evita commitear si no hay cambios.
"""
import os, subprocess, tempfile, shutil, time, hashlib, errno
from urllib.parse import quote, unquote
import xml.etree.ElementTree as ET
import requests

# --- Config (heredadas de env) ---
GIT_CLONE_DIR = os.environ.get("GIT_CLONE_DIR", "/data/sync_daemon/repo_clone")
GIT_WORK_DIR = os.environ.get("GIT_WORK_DIR", "/data/sync_daemon/repo_work")
NEXTCLOUD_URL = os.environ.get("NEXTCLOUD_URL", "http://nextcloud:80")
SYNC_USER = os.environ.get("NEXTCLOUD_SYNC_USER") or os.environ.get("SYNC_USER")
SYNC_PASS = os.environ.get("NEXTCLOUD_SYNC_PASS") or os.environ.get("SYNC_PASS")
GIT_USER = os.environ.get("GIT_USER")
GIT_TOKEN = os.environ.get("GIT_TOKEN")
GIT_REPO = os.environ.get("GIT_REPO")
DEFAULT_AUTHOR_EMAIL_DOMAIN = os.environ.get("DEFAULT_AUTHOR_EMAIL_DOMAIN", "example.com")

# --- Util helpers ---
def remove_stale_index_lock(repo_dir):
    """
    Si existe .git/index.lock y es viejo, lo elimina.
    Siempre llamarlo antes de hacer operaciones git automáticas en batch.
    """
    lock_path = os.path.join(repo_dir, ".git", "index.lock")
    try:
        if os.path.exists(lock_path):
            age = time.time() - os.path.getmtime(lock_path)
            # si el lock tiene más de 10 segundos lo consideramos stale y lo eliminamos
            if age > 10:
                print(f"Stale git index.lock found (age={age:.1f}s) - removing {lock_path}")
                os.remove(lock_path)
            else:
                print(f"Index.lock exists but is recent (age={age:.1f}s) - proceeding (will retry git ops)")
    except Exception as e:
        print("Error while checking/removing index.lock:", e)
        
def run(cmd, cwd=None, check=True):
    # cambio: preferible no usar shell=True para seguridad; dividimos si cmd es list o str
    import shlex
    print("RUN:", cmd)
    if isinstance(cmd, str):
        args = shlex.split(cmd)
    else:
        args = cmd
    res = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    if res.stdout:
        print("OUT:", res.stdout.strip())
    if res.returncode != 0:
        if res.stderr:
            print("ERR:", res.stderr.strip())
        if check:
            raise RuntimeError(f"Command failed: {' '.join(args)}\n{res.stderr}")
    return res

def _sha256_file(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def detect_main_branch():
    candidates = ["main", "master", "trunk"]
    for b in candidates:
        res = run(
            ["git", "--git-dir", GIT_CLONE_DIR, "rev-parse", "--verify", f"refs/heads/{b}"],
            check=False
        )
        if res.returncode == 0:
            return b
    raise RuntimeError("No se pudo detectar rama principal (main/master/trunk)")

def ensure_clone():
    remote = os.environ.get("GIT_REMOTE")
    if not remote:
        if not (GIT_USER and GIT_TOKEN and GIT_REPO):
            raise RuntimeError("Faltan GIT_USER/GIT_TOKEN/GIT_REPO")
        remote = f"https://{GIT_USER}:{GIT_TOKEN}@github.com/{GIT_REPO}.git"

    # asegurar work dir
    os.makedirs(GIT_WORK_DIR, exist_ok=True)

    # --- CLONE DIR ---
    if os.path.exists(GIT_CLONE_DIR):
        # ¿ya es un repo git?
        if os.path.exists(os.path.join(GIT_CLONE_DIR, "HEAD")):
            print("Repo ya existe en clone dir, haciendo fetch")
            remove_stale_index_lock(GIT_CLONE_DIR)
            run(["git", "--git-dir", GIT_CLONE_DIR, "fetch", "--all", "--prune"])
        else:
            print("Clone dir existe pero no es repo, limpiando contenido")
            for entry in os.listdir(GIT_CLONE_DIR):
                p = os.path.join(GIT_CLONE_DIR, entry)
                try:
                    if os.path.isdir(p):
                        shutil.rmtree(p)
                    else:
                        os.remove(p)
                except Exception as e:
                    print("No se pudo borrar", p, e)

            run(["git", "clone", "--mirror", remote, GIT_CLONE_DIR])

    else:
        print("Clonando repo (mirror):", remote)
        run(["git", "clone", "--mirror", remote, GIT_CLONE_DIR])

    # --- CHECKOUT A WORKTREE ---
    branch = detect_main_branch()
    print("Checkout branch:", branch)

    run([
        "git",
        "--git-dir", GIT_CLONE_DIR,
        "--work-tree", GIT_WORK_DIR,
        "checkout",
        "-f",
        branch
    ], check=False)

    print("Checkout branch:", branch)
    run([
        "git",
        "--git-dir", GIT_CLONE_DIR,
        "--work-tree", GIT_WORK_DIR,
        "checkout",
        "-f",
        branch
    ], check=False)

    run(["git", "--git-dir", GIT_CLONE_DIR, "config", "user.name", "Sync Batch"])
    run(["git", "--git-dir", GIT_CLONE_DIR, "config", "user.email",
        f"sync-batch@{DEFAULT_AUTHOR_EMAIL_DOMAIN}"])


def safe_repo_relpath(path):
    p = os.path.normpath(path)
    if p.startswith(os.sep):
        p = p.lstrip(os.sep)
    if p.startswith(".."):
        raise ValueError("Path traversal")
    return p.replace("\\", "/")

# --- WebDAV helpers (PROPFIND + GET) ---
def propfind_list(user, remote_dir):
    url = f"{NEXTCLOUD_URL.rstrip('/')}/remote.php/dav/files/{quote(user, safe='')}/{quote(remote_dir.lstrip('/'), safe='/')}"
    headers = {"Depth": "1"}
    r = requests.request("PROPFIND", url, auth=(user, SYNC_PASS), headers=headers, timeout=30)
    if r.status_code not in (207,200):
        raise RuntimeError(f"PROPFIND {url} -> {r.status_code} {r.text[:200]}")
    ns = {"d": "DAV:"}
    root = ET.fromstring(r.content)
    hrefs = []
    for resp in root.findall("d:response", ns):
        href = resp.find("d:href", ns)
        if href is not None and href.text:
            h = unquote(href.text)
            token = f"/remote.php/dav/files/{user}/"
            if token in h:
                h = h.split(token,1)[1]
            else:
                h = h.lstrip("/")
            hrefs.append(h)
    return hrefs

def dav_download(user_for_url, remote_rel_path):
    dav_path = quote(remote_rel_path.lstrip('/'), safe="/")
    url = f"{NEXTCLOUD_URL.rstrip('/')}/remote.php/dav/files/{quote(user_for_url, safe='')}/{dav_path}"
    r = requests.get(url, auth=(user_for_url, SYNC_PASS), stream=True, timeout=60)
    if r.status_code != 200:
        return None, r.status_code, (r.text[:800] if r.text else None)
    fd, tmp = tempfile.mkstemp(prefix="sync_tmp_", dir="/tmp")
    with os.fdopen(fd, "wb") as tf:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                tf.write(chunk)
    return tmp, 200, None

# --- filesystem walker ---
def walk_remote(user, start_dir=""):
    stack = [start_dir.rstrip("/")]
    files = []
    while stack:
        d = stack.pop()
        try:
            hrefs = propfind_list(user, d)
        except Exception as e:
            print("propfind failed for", d, e)
            continue
        for h in hrefs:
            if h.rstrip("/") == (d.rstrip("/") or ""):
                continue
            if h.endswith("/"):
                stack.append(h.rstrip("/"))
            else:
                files.append(h)
    return files

# --- util para detectar contenido idéntico ---
def find_tracked_file_with_same_content(repo_dir, tmp_path):
    tmp_hash = _sha256_file(tmp_path)
    # usamos el clone como git-dir y work-tree apuntando al work dir
    res = run(["git", "--git-dir", GIT_CLONE_DIR, "--work-tree", GIT_WORK_DIR, "ls-files"], cwd=GIT_CLONE_DIR, check=False)
    if res.returncode != 0:
        return None
    for line in res.stdout.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        candidate_abs = os.path.join(GIT_WORK_DIR, candidate)
        if not os.path.exists(candidate_abs):
            continue
        try:
            if _sha256_file(candidate_abs) == tmp_hash:
                return candidate.replace("\\","/")
        except Exception:
            continue
    return None

def build_commit_message(action, file_path, user=None):
    action = action.lower()
    verb = {"crear":"Crear","actualizar":"Actualizar","eliminar":"Eliminar","renombrar":"Renombrar"}.get(action, action.capitalize())
    area = file_path.split("/",1)[0] if file_path else "root"
    short = os.path.basename(file_path) or file_path
    return f'{verb}({area}): {short}'

def commit_single_file(repo_rel, owner_username):
    author = f'{owner_username} <{owner_username}@{DEFAULT_AUTHOR_EMAIL_DOMAIN}>'
    # ADD usando clone como git-dir y work-tree apuntando al work dir
    run(["git", "--git-dir", GIT_CLONE_DIR, "--work-tree", GIT_WORK_DIR, "add", repo_rel], cwd=GIT_CLONE_DIR)
    staged = run(["git", "--git-dir", GIT_CLONE_DIR, "--work-tree", GIT_WORK_DIR, "diff", "--cached", "--name-only"], cwd=GIT_CLONE_DIR, check=False)
    if not staged.stdout.strip():
        print("No changes staged for", repo_rel, "- skipping commit")
        return False

    exists = run(["git", "--git-dir", GIT_CLONE_DIR, "--work-tree", GIT_WORK_DIR, "ls-files", "--error-unmatch", repo_rel], cwd=GIT_CLONE_DIR, check=False)
    action = "actualizar" if exists.returncode == 0 else "crear"
    msg = build_commit_message(action, repo_rel, owner_username)

    res_commit = run(["git", "--git-dir", GIT_CLONE_DIR, "--work-tree", GIT_WORK_DIR, "commit", "-m", msg, "--author", author], cwd=GIT_CLONE_DIR, check=False)
    if res_commit.returncode != 0:
        print("git commit failed or nothing to commit for", repo_rel)
        return False

    # push: primero fetch/rebase y push desde clone
    branch = detect_main_branch()
    run([
        "git",
        "--git-dir", GIT_CLONE_DIR,
        "push",
        "origin",
        f"HEAD:refs/heads/{branch}"
    ], check=False)
    # rebase local branch (work with refs) - command adjusted for mirror/clone layout
    run(["git", "--git-dir", GIT_CLONE_DIR, "rebase", f"origin/{branch}"], cwd=GIT_CLONE_DIR, check=False)
    res_push = run(["git", "--git-dir", GIT_CLONE_DIR, "push", "origin", f"HEAD:refs/heads/{branch}"], cwd=GIT_CLONE_DIR, check=False)
    if res_push.returncode != 0:
        print("Push failed for", repo_rel)
        return False
    return True

def main():
    if not SYNC_USER or not SYNC_PASS:
        raise RuntimeError("SYNC_USER/SYNC_PASS faltan")
    ensure_clone()
    START_DIR = os.environ.get("NEXTCLOUD_START_DIR", "").strip("/")

    remote_files = walk_remote(SYNC_USER, START_DIR)
    print("Remote files discovered:", len(remote_files))
    for remote_path in remote_files:
        tmp = None

        # ---- filtros ANTES de cualquier normalización ----
        if remote_path.startswith(".git") or "/.git/" in remote_path:
            print("Skipping .git path:", remote_path)
            continue

        skip_prefixes = (
            ".ocdata",
            "appdata_",
            "preview",
            "thumbnails",
            ".cache",
            "files_trashbin"
        )
        low = remote_path.lower()
        if any(low.startswith(pref) or f"/{pref}/" in f"/{low}" for pref in skip_prefixes):
            print("Skipping internal/preview file:", remote_path)
            continue

        # normalizar y mapear la ruta remota a la ruta relativa del repo
        try:
            repo_relpath = safe_repo_relpath(remote_path)
        except Exception as e:
            print("Skipping unsafe path:", remote_path, e)
            continue

        # Si se definió NEXTCLOUD_START_DIR (p.ej. "Sistema_de_Gestion_de_Calidad"),
        # queremos quitar ese prefijo para que los archivos del groupfolder vayan
        # al root del repo (o a su estructura interna), en lugar de crear una
        # carpeta extra "Sistema_de_Gestion_de_Calidad" en el repo.
        START_DIR = os.environ.get("NEXTCLOUD_START_DIR", "").strip("/")
        if START_DIR:
            # si repo_relpath = "Sistema_de_Gestion_de_Calidad/01_Gestion_Humana/archivo"
            # lo convertimos a "01_Gestion_Humana/archivo"
            if repo_relpath == START_DIR:
                # el path apunta exactamente a la carpeta de inicio; en este caso
                # no hay archivo a procesar (es un folder), lo saltamos
                print("Skipping top-level start dir entry:", repo_relpath)
                continue
            if repo_relpath.startswith(START_DIR + "/"):
                repo_relpath = repo_relpath[len(START_DIR)+1:]

        # Opción adicional: si dentro del groupfolder subiste un topdir con el nombre
        # del repo (p.ej. "Sistema_de_Gestion_de_Calidad/Sistema_de_Gestion_de_Calidad/..."),
        # puedes quitar también ese topdir (controlado por env var STRIP_REPO_TOPDIR).
        STRIP_REPO_TOPDIR = os.environ.get("STRIP_REPO_TOPDIR", "1")  # "1" por defecto
        repo_basename = os.path.basename(GIT_REPO).split(".")[0] if GIT_REPO else None
        if STRIP_REPO_TOPDIR in ("1","true","True") and repo_basename:
            if repo_relpath.startswith(repo_basename + "/"):
                repo_relpath = repo_relpath[len(repo_basename)+1:]

        # Finalmente, si la ruta resultante es vacía (ej. se refería solo al topdir),
        # saltamos (no procesamos carpetas sin nombre de archivo)
        if not repo_relpath or repo_relpath.strip("/") == "":
            print("Resulting repo_relpath empty after strip - skipping:", remote_path)
            continue

        # ahora repo_relpath representa la ruta relativa que queremos en el repo
        dest_rel = repo_relpath

        tmp, status, body = dav_download(SYNC_USER, remote_path)
        if not tmp:
            print("Download failed for", remote_path, status, body)
            continue

        try:
            matched = find_tracked_file_with_same_content(GIT_CLONE_DIR, tmp)
            dest_abs = os.path.join(GIT_WORK_DIR, dest_rel)
            os.makedirs(os.path.dirname(dest_abs), exist_ok=True)

            if matched and matched != dest_rel:
                print("Detected rename by content:", matched, "->", dest_rel)
                try:
                    shutil.move(tmp, dest_abs)
                except Exception:
                    shutil.copy2(tmp, dest_abs)
                    os.remove(tmp)

                run(f'git mv "{matched}" "{dest_rel}"', cwd=GIT_CLONE_DIR, check=False)
                commit_single_file(dest_rel, SYNC_USER)

            else:
                if matched == dest_rel:
                    print("No content change for", dest_rel)
                    os.remove(tmp)
                    continue

                try:
                    shutil.move(tmp, dest_abs)
                except Exception:
                    shutil.copy2(tmp, dest_abs)
                    os.remove(tmp)

                commit_single_file(dest_rel, SYNC_USER)

        except Exception as e:
            print("Error processing", remote_path, e)
            if tmp and os.path.exists(tmp):
                os.remove(tmp)

if __name__ == "__main__":
    main()