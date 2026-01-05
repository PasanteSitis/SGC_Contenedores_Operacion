#!/usr/bin/env python3
# coding: utf-8
"""
build_sidecars.py - versión corregida
- 'responsable' ahora es el autor del último commit (last_author)
- se agrega 'creador' que es el autor del primer commit (creator)
Escribe metadata.yml de forma atómica.
"""
from __future__ import annotations
import os, sys, subprocess, hashlib, re, unicodedata
from datetime import datetime
try:
    import yaml
    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

# ---------- Config ----------
ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
METADATA_FILENAME = "metadata.yml"
IGNORE_DIRS = {'.git', '.github', '__pycache__', '.venv', 'venv', 'node_modules', '.idea', '.vscode'}
MONTHS = {
    "enero","febrero","marzo","abril","mayo","junio","julio","agosto","septiembre","octubre","noviembre","diciembre",
    "january","february","march","april","may","june","july","august","september","october","november","december",
    "01_enero","02_febrero","03_marzo","04_abril","05_mayo","06_junio","07_julio","08_agosto","09_septiembre",
    "10_octubre","11_noviembre","12_diciembre"
}
CONTROLLED_TYPES = {
    "formatos": "Formatos", "formato": "Formatos",
    "guias": "Guias", "guia": "Guias",
    "instructivos": "Instructivos", "instructivo": "Instructivos",
    "indicadores": "Indicadores", "indicador": "Indicadores",
    "procedimiento": "Procedimiento", "procedimientos": "Procedimiento",
    "registros": "Registros", "registro": "Registros"
}

# ---------- Helpers ----------
def log(*a):
    print(datetime.utcnow().isoformat() + "Z -", *a, flush=True)

def normalize_token(s):
    if s is None: return ""
    s = str(s).replace("_", " ").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\s+', ' ', s)
    return s.lower()

def looks_like_year(name):
    if not name: return False
    name = name.strip()
    return bool(re.fullmatch(r"\d{4}", name) or re.match(r"^\d{4}[\s_-]", name))

def looks_like_month(name):
    if not name: return False
    base = re.sub(r'^\d{1,2}[_\-\s]*', '', name).lower()
    base = re.sub(r'[_\-\s].*$', '', base)
    return base in MONTHS

def is_year_or_month(name):
    return looks_like_year(name) or looks_like_month(name)

def run_git_cmd(args, cwd):
    try:
        out = subprocess.check_output(["git"] + list(args), cwd=cwd, stderr=subprocess.DEVNULL)
        return out.decode("utf-8", errors="ignore").strip()
    except Exception:
        return None

def find_git_root(path):
    cur = os.path.abspath(path)
    while cur and cur != os.path.dirname(cur):
        if os.path.isdir(os.path.join(cur, ".git")):
            return cur
        cur = os.path.dirname(cur)
    return None

def get_git_creation_info(repo_root, file_relpath):
    """Devuelve (creator_name, creation_date_iso, last_author, last_date_iso) o (None,...)."""
    if not repo_root or not file_relpath:
        return (None, None, None, None)
    try:
        # primer commit que añade el archivo (creator)
        res = run_git_cmd(["log", "--diff-filter=A", "--follow", "--format=%aN|%aI", "--", file_relpath], cwd=repo_root)
        creator = creation_date = None
        if res:
            lines = [l for l in res.splitlines() if l.strip()]
            if lines:
                first = lines[-1]  # el más antiguo de los resultados
                parts = first.split("|", 1)
                if len(parts) == 2:
                    creator, creation_date = parts[0], parts[1]
        # último autor (last_author)
        last = run_git_cmd(["log", "-1", "--format=%aN|%aI", "--", file_relpath], cwd=repo_root)
        last_author = last_date = None
        if last:
            p = last.split("|",1)
            if len(p) == 2:
                last_author, last_date = p[0], p[1]
        return creator, creation_date, last_author, last_date
    except Exception:
        return (None, None, None, None)

def git_hash_object(repo_root, filepath):
    if not repo_root or not filepath:
        return None
    try:
        rel = os.path.relpath(filepath, repo_root)
        h = run_git_cmd(["hash-object", rel], cwd=repo_root)
        if not h:
            h = run_git_cmd(["hash-object", filepath], cwd=repo_root)
        return h
    except Exception:
        return None

def sha1_of_file(path):
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def first_level_under_root(some_path, root):
    root = os.path.abspath(root)
    some_path = os.path.abspath(some_path)
    if not some_path.startswith(root):
        return os.path.basename(root)
    rel = os.path.relpath(some_path, root)
    parts = rel.split(os.sep)
    if not parts or parts[0] in (".",""):
        return os.path.basename(root)
    return parts[0]

def ancestor_tokens(path, root):
    p = os.path.dirname(os.path.abspath(path))
    tokens = []
    root_abs = os.path.abspath(root)
    while True:
        if p == root_abs or len(p) < len(root_abs):
            break
        name = os.path.basename(p)
        if name:
            tokens.append(name)
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return tokens

def find_first_non_year_month_ancestor(path, root, start_at=None):
    if start_at:
        p = os.path.abspath(start_at)
    else:
        p = os.path.dirname(os.path.abspath(path))
    p = os.path.dirname(p)
    root_abs = os.path.abspath(root)
    while True:
        if p == root_abs or len(p) < len(root_abs):
            break
        name = os.path.basename(p)
        if name and not is_year_or_month(name):
            return name
        parent = os.path.dirname(p)
        if parent == p:
            break
        p = parent
    return None

def determine_tipo_area(file_path, stop_root):
    stop_root = os.path.abspath(stop_root)
    dirpath = os.path.dirname(os.path.abspath(file_path))
    parent = os.path.basename(dirpath)
    general = first_level_under_root(dirpath, stop_root)
    anc = ancestor_tokens(file_path, stop_root)
    controlled_idx = None
    for i, t in enumerate(anc):
        if normalize_token(t) in CONTROLLED_TYPES:
            controlled_idx = i
            break
    if controlled_idx is not None:
        tipo = CONTROLLED_TYPES[normalize_token(anc[controlled_idx])]
        area_candidate = None
        if controlled_idx + 1 < len(anc):
            j = controlled_idx + 1
            while j < len(anc) and is_year_or_month(anc[j]):
                j += 1
            if j < len(anc):
                area_candidate = anc[j]
        area = area_candidate if area_candidate else general
        return tipo, area
    if general.startswith("05"):
        return parent, general
    if general.startswith("06") or general.startswith("12"):
        a = find_first_non_year_month_ancestor(file_path, stop_root, start_at=dirpath)
        return "Registros", (a if a else general)
    if general.startswith("07"):
        return "Formatos", (parent if parent and parent != general else general)
    if general.startswith("10") or general.startswith("11"):
        q = find_first_non_year_month_ancestor(file_path, stop_root, start_at=dirpath)
        return parent, (q if q else general)
    if normalize_token(parent) in CONTROLLED_TYPES:
        tipo = CONTROLLED_TYPES[normalize_token(parent)]
        q = find_first_non_year_month_ancestor(file_path, stop_root, start_at=dirpath)
        return tipo, (q if q else general)
    tipo = parent
    q = find_first_non_year_month_ancestor(file_path, stop_root, start_at=dirpath)
    if q:
        if q == tipo:
            p = os.path.dirname(os.path.dirname(os.path.abspath(file_path)))
            found = None
            root_abs = os.path.abspath(stop_root)
            while True:
                if p == root_abs or len(p) < len(root_abs):
                    break
                name = os.path.basename(p)
                if name and not is_year_or_month(name) and name != tipo:
                    found = name
                    break
                parent_p = os.path.dirname(p)
                if parent_p == p:
                    break
                p = parent_p
            return tipo, (found if found else general)
        else:
            return tipo, q
    return tipo, general

def write_yaml_atomic(path, data):
    tmp = path + ".tmp"
    try:
        if HAVE_YAML:
            with open(tmp, "w", encoding="utf-8") as f:
                yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)
        else:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("# metadata generado automáticamente\n")
                if isinstance(data, list):
                    for item in data:
                        f.write("-\n")
                        for k,v in item.items():
                            f.write(f"  {k}: {v}\n")
                elif isinstance(data, dict):
                    for k,v in data.items():
                        f.write(f"{k}: {v}\n")
                else:
                    f.write(str(data))
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass

# ---------- Operación ----------
def build_sidecars(root):
    root = os.path.abspath(root)
    git_root = find_git_root(root)
    repo_root = git_root if git_root else None
    total_dirs = 0
    total_files = 0
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS and not d.startswith(".")]
        files = [f for f in filenames if f != METADATA_FILENAME and not f.startswith(".")]
        files_full = [os.path.join(dirpath, f) for f in files if os.path.isfile(os.path.join(dirpath,f))]
        if not files_full:
            continue
        entries = []
        for fullp in files_full:
            total_files += 1
            relpath = None
            if repo_root:
                try:
                    relpath = os.path.relpath(fullp, repo_root)
                except Exception:
                    relpath = None
            file_id = None
            if repo_root and relpath:
                try:
                    file_id = git_hash_object(repo_root, relpath) or git_hash_object(repo_root, fullp)
                except Exception:
                    file_id = None
            if not file_id:
                try:
                    file_id = sha1_of_file(fullp)
                except Exception:
                    file_id = None

            tipo, area = determine_tipo_area(fullp, root)

            anc = ancestor_tokens(fullp, root)
            estado = "activo"
            if any(x.lower().startswith("13_documentos_obsoletos") or x.lower() == "13_documentos_obsoletos" for x in anc):
                estado = "obsoleto"

            # ---------- obtener metadatos desde git si es posible ----------
            creador = None
            fecha_creacion = None
            responsable = None
            ultima_revision = None
            if repo_root and relpath:
                try:
                    creator, creation_date, last_author, last_date = get_git_creation_info(repo_root, relpath)
                    # 'creador' = author of first commit (creator)
                    if creator:
                        creador = creator
                        log("Git: creator for", relpath, "=>", creador)
                    # 'responsable' = last commit author (last_author)
                    if last_author:
                        responsable = last_author
                        log("Git: last_author for", relpath, "=>", responsable)
                    # fechas desde git
                    if creation_date:
                        fecha_creacion = creation_date
                    if last_date:
                        ultima_revision = last_date
                except Exception as e:
                    log("Git metadata failed for", relpath, ":", e)

            # fallback a timestamps FS si no hay info git
            try:
                st = os.stat(fullp)
                if not fecha_creacion:
                    fecha_creacion = datetime.fromtimestamp(st.st_ctime).isoformat()
                if not ultima_revision:
                    ultima_revision = datetime.fromtimestamp(st.st_mtime).isoformat()
            except Exception:
                pass

            # si no hay 'responsable' de git, mantener vacío (o usar creador si quieres)
            if not responsable and creador:
                # opcional: si no existe last_author usar creator como fallback
                # responsable = creador
                pass

            entry = {
                "id": file_id or "",
                "nombre": os.path.basename(fullp),
                "area": area or "",
                "tipo": tipo or "",
                "responsable": responsable or "",
                "creador": creador or "",
                "estado": estado,
                "fecha_creacion": fecha_creacion or "",
                "ultima_revision": ultima_revision or ""
            }
            entries.append(entry)
        outpath = os.path.join(dirpath, METADATA_FILENAME)
        try:
            write_yaml_atomic(outpath, entries)
            total_dirs += 1
            log("Creado/actualizado:", outpath, f"({len(entries)} archivos)")
        except Exception as e:
            log("Error escribiendo", outpath, ":", e)
    log("Finished. directorios procesados:", total_dirs, "archivos inspeccionados:", total_files)

if __name__ == "__main__":
    log("Root:", ROOT)
    try:
        build_sidecars(ROOT)
        log("Listo.")
    except Exception as e:
        log("Error general:", e)
        raise
