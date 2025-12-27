#!/usr/bin/env python3
# sync/get_nc_editor.py
# Uso:
#   get_nc_editor.py --url URL --user USER --pass PASS --path "/Sistema_de_Gestion_de_Calidad/...." [--cache-ttl 600] [--no-cache]
# Salida:
#   username|display|email|fileid
# Si no se encuentra -> |||

import sys, os, json, time, argparse, urllib.request, urllib.parse, base64

CACHE_FILE = "/tmp/editor_cache.json"

def load_cache():
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(c):
    try:
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(c, f)
        os.replace(tmp, CACHE_FILE)
    except Exception:
        pass

def ocs_get(url, user, passwd, params=None):
    try:
        if params:
            url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={"OCS-APIRequest":"true"})
        auth = f"{user}:{passwd}".encode("utf-8")
        token = base64.b64encode(auth).decode()
        req.add_header("Authorization", "Basic " + token)
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None

def extract_user_from_item(item, target_path):
    # 1) object_name exact match
    obj = item.get("object_name") or ""
    if obj and (obj == target_path or target_path.endswith(obj)):
        return item.get("user") or item.get("affecteduser") or None, None

    # 2) subject_rich traversal: can contain dicts where keys are fileid -> path (your DB shows that)
    sr = item.get("subject_rich")
    if isinstance(sr, list):
        for s in sr:
            # if entry is dict containing file info as mapping fileid->path
            if isinstance(s, dict):
                # iterate nested values; they might be dicts with 'path' or simple mappings
                for k, v in s.items():
                    # v could be dict (with path) or simple string
                    if isinstance(v, dict):
                        # common shape: {'file1': { 'type': 'file', 'id': '46660', 'name': 'X', 'path': 'Documents/..' } }
                        # or nested mapping where values are path strings
                        # try common keys:
                        p = v.get("path") or v.get("file") or v.get("link") or None
                        if p and (p == target_path or target_path.endswith(p) or p.endswith(target_path)):
                            # return username and try to extract fileid if present
                            fid = v.get("id") or None
                            return item.get("user") or item.get("affecteduser") or None, fid
                    else:
                        # v might be a path string or mapping id->path if s itself is a mapping of fileid->path
                        if isinstance(v, str):
                            if v == target_path or target_path.endswith(v) or v.endswith(target_path):
                                # cannot derive fileid here (k could be fileid) -> attempt to return k if k numeric
                                fid = k if str(k).isdigit() else None
                                return item.get("user") or item.get("affecteduser") or None, fid
                        # if v is list, iterate
                        if isinstance(v, list):
                            for elem in v:
                                if isinstance(elem, str) and (elem == target_path or target_path.endswith(elem)):
                                    return item.get("user") or item.get("affecteduser") or None, None

    # 3) subjectparams / message params (older shapes) - fallback search in raw serialized fields
    try:
        # some servers include subjectparams or subject as json-like strings
        for key in ("subjectparams", "messageparams", "objects"):
            val = item.get(key)
            if isinstance(val, (dict, list)):
                # convert to string search
                s = json.dumps(val)
                if target_path in s:
                    return item.get("user") or item.get("affecteduser") or None, None
    except Exception:
        pass

    return None, None

def get_user_display_email(url, api_user, api_pass, username):
    if not username:
        return None, None
    u = f"{url.rstrip('/')}/ocs/v2.php/cloud/users/{urllib.parse.quote(username)}?format=json"
    j = ocs_get(u, api_user, api_pass)
    if not j:
        return username, f"{username}@{os.environ.get('EMAIL_DOMAIN','example.com')}"
    data = j.get("ocs", {}).get("data", {})
    display = data.get("displayname") or username
    email = data.get("email") or f"{username}@{os.environ.get('EMAIL_DOMAIN','example.com')}"
    return display, email

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--pass", dest="password", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--cache-ttl", type=int, default=600)
    parser.add_argument("--no-cache", action="store_true", default=False)
    args = parser.parse_args()

    target = args.path
    cache = load_cache() if not args.no_cache else {}
    now = int(time.time())

    if not args.no_cache and target in cache:
        entry = cache[target]
        if now - entry.get("ts",0) < args.cache_ttl:
            # return username|display|email|fileid
            print(f"{entry.get('username') or ''}|{entry.get('display') or ''}|{entry.get('email') or ''}|{entry.get('fileid') or ''}")
            return

    base = f"{args.url.rstrip('/')}/ocs/v2.php/apps/activity/api/v2/activity?format=json"
    per_page = 200
    page = 0
    found_user = None
    found_fileid = None

    while True:
        params = {"limit": per_page, "offset": page * per_page}
        j = ocs_get(base, args.user, args.password, params)
        if not j:
            break
        data = j.get("ocs", {}).get("data", [])
        for item in data:
            user, fid = extract_user_from_item(item, target)
            if user:
                found_user = user
                found_fileid = fid
                break
        if found_user:
            break
        if not data or len(data) < per_page:
            break
        page += 1
        if page > 20:
            break

    if found_user:
        display, email = get_user_display_email(args.url, args.user, args.password, found_user)
        cache[target] = {"ts": now, "username": found_user, "display": display or "", "email": email or "", "fileid": found_fileid or ""}
        save_cache(cache)
        print(f"{found_user}|{display or ''}|{email or ''}|{found_fileid or ''}")
    else:
        print("|||")  # username|display|email|fileid all empty

if __name__ == "__main__":
    main()
