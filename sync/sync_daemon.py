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
GIT_CLONE_DIR = os.environ.get("GIT_CLONE_DIR", "/data/sync_daemon/repo_work")
NEXTCLOUD_URL = os.environ.get("NEXTCLOUD_URL", "http://nextcloud:80")
SYNC_USER = os.environ.get("NEXTCLOUD_SYNC_USER") or os.environ.get("SYNC_USER")
SYNC_PASS = os.environ.get("NEXTCLOUD_SYNC_PASS") or os.environ.get("SYNC_PASS")
GIT_USER = os.environ.get("GIT_USER")
GIT_TOKEN = os.environ.get("GIT_TOKEN")
GIT_REPO = os.environ.get("GIT_REPO")
DEFAULT_AUTHOR_EMAIL_DOMAIN = os.environ.get("DEFAULT_AUTHOR_EMAIL_DOMAIN", "example.com")

# --- Util helpers ---
def run(cmd, cwd=None, check=True):
    print("RUN:", cmd)
    res = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if res.stdout:
        print("OUT:", res.stdout.strip())
    if res.returncode != 0:
        if res.stderr:
            print("ERR:", res.stderr.strip())
        if check:
            raise RuntimeError(f"Command failed: {cmd}\n{res.stderr}")
    return res

def _sha256_file(path, block_size=65536):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_clone():
    remote = os.environ.get("GIT_REMOTE")
    if not remote:
        if not (GIT_USER and GIT_TOKEN and GIT_REPO):
            raise RuntimeError("Faltan GIT_USER/GIT_TOKEN/GIT_REPO")
        remote = f"https://{GIT_USER}:{GIT_TOKEN}@github.com/{GIT_REPO}.git"
    if not os.path.exists(GIT_CLONE_DIR) or not os.listdir(GIT_CLONE_DIR):
        print("Clonando repo:", remote)
        run(f"git clone {remote} {GIT_CLONE_DIR}")
        run("git lfs install", cwd=GIT_CLONE_DIR, check=False)
        run('git config user.name "Sync Batch"', cwd=GIT_CLONE_DIR)
        run(f'git config user.email "sync-batch@{DEFAULT_AUTHOR_EMAIL_DOMAIN}"', cwd=GIT_CLONE_DIR)
    else:
        try:
            run("git fetch --all", cwd=GIT_CLONE_DIR, check=False)
            branch = "main" if run("git rev-parse --verify origin/main", cwd=GIT_CLONE_DIR, check=False).returncode == 0 else "master"
            run(f"git checkout {branch}", cwd=GIT_CLONE_DIR, check=False)
            run(f"git pull --rebase origin {branch}", cwd=GIT_CLONE_DIR, check=False)
        except Exception as e:
            print("Warning updating repo:", e)

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
    res = run("git ls-files", cwd=repo_dir, check=False)
    if res.returncode != 0:
        return None
    for line in res.stdout.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        candidate_abs = os.path.join(repo_dir, candidate)
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
    run(f'git add "{repo_rel}"', cwd=GIT_CLONE_DIR)
    staged = run('git diff --cached --name-only', cwd=GIT_CLONE_DIR, check=False)
    if not staged.stdout.strip():
        print("No changes staged for", repo_rel, "- skipping commit")
        return False
    action = "actualizar" if run(f'git ls-files --error-unmatch "{repo_rel}"', cwd=GIT_CLONE_DIR, check=False).returncode == 0 else "crear"
    msg = build_commit_message(action, repo_rel, owner_username)
    res_commit = run(f'git commit -m "{msg}" --author="{author}"', cwd=GIT_CLONE_DIR, check=False)
    if res_commit.returncode != 0:
        print("git commit failed or nothing to commit for", repo_rel)
        return False
    # push only if commit succeeded
    branch = "main" if run("git rev-parse --verify origin/main", cwd=GIT_CLONE_DIR, check=False).returncode == 0 else "master"
    run("git fetch origin", cwd=GIT_CLONE_DIR, check=False)
    run(f"git rebase origin/{branch}", cwd=GIT_CLONE_DIR, check=False)
    res_push = run(f"git push origin {branch}", cwd=GIT_CLONE_DIR, check=False)
    if res_push.returncode != 0:
        print("Push failed for", repo_rel)
        return False
    return True

def main():
    if not SYNC_USER or not SYNC_PASS:
        raise RuntimeError("SYNC_USER/SYNC_PASS faltan")
    ensure_clone()
    remote_files = walk_remote(SYNC_USER, "")
    print("Remote files discovered:", len(remote_files))
    for remote_path in remote_files:
        tmp = None
        try:
            repo_relpath = safe_repo_relpath(remote_path)
        except Exception as e:
            print("Skipping unsafe path:", remote_path, e)
            continue
        tmp, status, body = dav_download(SYNC_USER, remote_path)
        if not tmp:
            print("Download failed for", remote_path, status, body)
            continue
        try:
            matched = find_tracked_file_with_same_content(GIT_CLONE_DIR, tmp)
            dest_rel = repo_relpath
            dest_abs = os.path.join(GIT_CLONE_DIR, dest_rel)
            os.makedirs(os.path.dirname(dest_abs), exist_ok=True)
            if matched and matched != dest_rel:
                print("Detected rename by content:", matched, "->", dest_rel)
                try:
                    shutil.move(tmp, dest_abs)
                except Exception:
                    shutil.copy2(tmp, dest_abs)
                    os.remove(tmp)
                run(f'git mv "{matched}" "{dest_rel}"', cwd=GIT_CLONE_DIR, check=False)
                committed = commit_single_file(dest_rel, SYNC_USER)
                if committed:
                    print("Committed rename:", dest_rel)
            else:
                # if matched == dest_rel -> content identical with tracked file at same path => skip
                if matched == dest_rel:
                    print("No content change for", dest_rel, "- skipping")
                    os.remove(tmp)
                    continue
                # replace/create
                try:
                    shutil.move(tmp, dest_abs)
                except Exception:
                    shutil.copy2(tmp, dest_abs)
                    os.remove(tmp)
                committed = commit_single_file(dest_rel, SYNC_USER)
                if committed:
                    print("Committed file:", dest_rel)
        except Exception as e:
            print("Error processing", remote_path, e)
            try:
                if tmp and os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            continue

if __name__ == "__main__":
    main()