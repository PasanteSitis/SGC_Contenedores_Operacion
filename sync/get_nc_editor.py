#!/usr/bin/env python3
# sync/get_nc_editor.py
# Uso: get_nc_editor.py --url URL --user USER --pass PASS --path "/Sistema_de_Gestion_de_Calidad/01/..." --cache-ttl 600
import sys, os, json, time, argparse, urllib.request, urllib.parse

CACHE_FILE = "/tmp/editor_cache.json"

def load_cache():
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(c):
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(c, f)
    os.replace(tmp, CACHE_FILE)

def ocs_get(url, user, passwd, params=None):
    if params:
        url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"OCS-APIRequest":"true"})
    auth = (user + ":" + passwd).encode("utf-8")
    req.add_header("Authorization", "Basic " + urllib.request.base64.b64encode(auth).decode())
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None

def find_editor_from_activity(ocs_json, target_path):
    if not ocs_json:
        return None
    data = ocs_json.get("ocs", {}).get("data", [])
    for item in data:
        # Try object_name first
        obj = item.get("object_name") or ""
        if obj == target_path or target_path.endswith(obj):
            # user or affecteduser
            return item.get("user") or item.get("affecteduser") or None
        # Try subject_rich entries
        sr = item.get("subject_rich")
        if isinstance(sr, list):
            for s in sr:
                # some entries can be dicts with file info
                if isinstance(s, dict):
                    f = s.get("file") or s.get("path") or {}
                    # if file is dict with path
                    if isinstance(f, dict) and f.get("path") == target_path:
                        return item.get("user") or item.get("affecteduser") or None
                    # if f is a string
                    if isinstance(f, str) and f == target_path:
                        return item.get("user") or item.get("affecteduser") or None
    return None

def get_user_display_email(url, api_user, api_pass, username):
    if not username:
        return None, None
    u = f"{url.rstrip('/')}/ocs/v2.php/cloud/users/{urllib.parse.quote(username)}?format=json"
    j = ocs_get(u, api_user, api_pass)
    if not j:
        return username, f"{username}@{os.environ.get('EMAIL_DOMAIN','example.com')}"
    data = j.get("ocs", {}).get("data", {})
    display = data.get("displayname") or username
    # email may be in metadata, fallback to constructed
    email = data.get("email") or f"{username}@{os.environ.get('EMAIL_DOMAIN','example.com')}"
    return display, email

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--pass", dest="password", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--cache-ttl", type=int, default=600)
    args = parser.parse_args()

    target = args.path
    cache = load_cache()
    now = int(time.time())
    # cache key is the path string
    if target in cache:
        entry = cache[target]
        if now - entry.get("ts",0) < args.cache_ttl:
            print(f"{entry.get('username') or ''}|{entry.get('display') or ''}|{entry.get('email') or ''}")
            return

    # call activity API pages (we'll fetch a few pages, stop early when found)
    base = f"{args.url.rstrip('/')}/ocs/v2.php/apps/activity/api/v2/activity?format=json"
    per_page = 200
    page = 0
    found_user = None
    while True:
        params = {"limit": per_page, "offset": page * per_page}
        j = ocs_get(base, args.user, args.password, params)
        if not j:
            break
        found_user = find_editor_from_activity(j, target)
        if found_user:
            break
        data = j.get("ocs", {}).get("data", [])
        if not data or len(data) < per_page:
            break
        page += 1
        if page > 10: # safety cap
            break

    display, email = get_user_display_email(args.url, args.user, args.password, found_user) if found_user else (None, None)
    # store to cache
    cache[target] = {"ts": now, "username": found_user or "", "display": display or "", "email": email or ""}
    try:
        save_cache(cache)
    except Exception:
        pass
    print(f"{found_user or ''}|{display or ''}|{email or ''}")

if __name__ == "__main__":
    main()
