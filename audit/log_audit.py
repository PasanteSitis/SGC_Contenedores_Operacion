#!/usr/bin/env python3
import os, re, time, json, psycopg2, threading, traceback
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dateutil import parser as dtparser
from urllib.parse import unquote, urlparse, parse_qs

# CONFIG
DB_HOST = os.environ.get("DATABASE_HOST")
DB_NAME = os.environ.get("POSTGRES_DB")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")

ACCESS_LOG = os.environ.get("NEXTCLOUD_LOG_PATH", "/var/log/apache2/access.log")
NEXTCLOUD_APP_LOG = os.environ.get("NEXTCLOUD_APP_LOG", "/var/www/html/data/nextcloud/nextcloud.log")

DEDUPE_SECONDS = int(os.environ.get("DEDUPE_SECONDS", "2"))
CORRELATION_WINDOW = int(os.environ.get("CORRELATION_WINDOW", "30"))  # segundos para correlacionar username

# Regex para Apache combined log
APACHE_RE = re.compile(r'(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<time>[^\]]+)\] "(?P<method>GET|POST|PUT|DELETE|PROPFIND) (?P<url>\S+)[^"]*" (?P<status>\d{3}) (?P<size>\S+) "(?P<referrer>[^"]*)" "(?P<ua>[^"]*)"')

INTEREST_PATTERNS = [
    "/remote.php/dav/files/",
    "/apps/files_pdfviewer",
    "/index.php/apps/files/download",
    "/remote.php/webdav/"
]

_FILE_EXT_RE = re.compile(r'\.(pdf|docx?|xlsx?|pptx?|txt|odt|ods|odg|jpg|jpeg|png|zip|rar|7z)(?:$|\?)', re.IGNORECASE)

# In-memory map: normalized_resource -> (username, timestamp)
recent_actions = {}
recent_lock = threading.Lock()

def db_connect():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS audit_events (
        id SERIAL PRIMARY KEY,
        ts timestamptz NOT NULL,
        username text,
        ip text,
        method text,
        resource text,
        status int,
        size bigint,
        user_agent text,
        referrer text,
        raw_line text,
        event_type text
    );
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()

def _normalize_resource(r):
    if not r:
        return r
    r2 = r.split('?',1)[0]
    try:
        r2 = unquote(r2)
    except Exception:
        pass
    return r2.rstrip('/')

def _looks_like_file(path):
    if not path:
        return False
    return bool(_FILE_EXT_RE.search(path))

def detect_event_type(ev):
    res = ev.get("resource") or ""
    method = (ev.get("method") or "").upper()
    status = int(ev.get("status") or 0)
    try:
        urlp = urlparse(res)
        path = urlp.path or res
        qs = parse_qs(urlp.query or "")
    except Exception:
        path = res
        qs = {}
    if 'download' in path.lower() or 'download' in qs:
        return "download"
    if method == "PROPFIND":
        return "list"
    if method == "GET" and status == 200 and _looks_like_file(path):
        return "download"
    if method == "GET" and status == 200:
        return "view"
    return "other"

def insert_event(ev):
    sql = """
    INSERT INTO audit_events (ts, username, ip, method, resource, status, size, user_agent, referrer, raw_line, event_type)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        resource_norm = _normalize_resource(ev.get("resource"))
        ev["resource"] = resource_norm

        # skip PROPFIND listing if not needed
        if ev.get("method","").upper() == "PROPFIND":
            return

        # if username unknown, try to correlate with recent nextcloud.log actions
        if not ev.get("username"):
            guessed = guess_username_for_resource(resource_norm)
            if guessed:
                ev["username"] = guessed

        # dedupe: buscar evento similar en los últimos N segundos
        dedupe_sql = """
        SELECT 1 FROM audit_events
        WHERE resource = %s
          AND coalesce(username,'(unknown)') = coalesce(%s,'(unknown)')
          AND ip = %s
          AND method = %s
          AND abs(extract(epoch from ts - %s::timestamptz)) <= %s
        LIMIT 1
        """
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(dedupe_sql, (resource_norm, ev.get("username"), ev.get("ip"), ev.get("method"), ev.get("ts"), DEDUPE_SECONDS))
                if cur.fetchone():
                    return

                ev['event_type'] = detect_event_type(ev)
                params = (
                    ev.get("ts"),
                    ev.get("username"),
                    ev.get("ip"),
                    ev.get("method"),
                    resource_norm,
                    ev.get("status"),
                    ev.get("size"),
                    ev.get("ua"),
                    ev.get("referrer"),
                    ev.get("raw"),
                    ev.get("event_type")
                )
                cur.execute(sql, params)
                conn.commit()
    except Exception as e:
        print("DB insert failed:", e)
        traceback.print_exc()

def parse_apache_line(line):
    m = APACHE_RE.search(line)
    if not m:
        return None
    if int(m.group("status")) == 207:
        return None
    url = m.group("url")
    if not any(p in url for p in INTEREST_PATTERNS):
        return None
    tstr = m.group("time")
    try:
        ts = dtparser.parse(tstr.replace(":", " ", 1))
    except Exception:
        ts = datetime.utcnow()
    return {
        "ts": ts,
        "username": None if m.group("user") == "-" else m.group("user"),
        "ip": m.group("ip"),
        "method": m.group("method"),
        "resource": url,
        "status": int(m.group("status")),
        "size": None if m.group("size") == "-" else int(m.group("size")),
        "ua": m.group("ua"),
        "referrer": m.group("referrer"),
        "raw": line.strip()
    }

# --- Correlator: parse nextcloud app log (JSON lines) and store recent actions
def parse_nextcloud_app_line(line):
    # nextcloud log is usually JSON lines; try to parse
    try:
        j = json.loads(line)
    except Exception:
        return None
    # Typical Nextcloud JSON has fields like "app", "message", "level", "time", "user"
    user = j.get("user") or j.get("uid") or None
    ts = None
    if j.get("time"):
        try:
            ts = dtparser.parse(j.get("time"))
        except Exception:
            ts = datetime.utcnow()
    else:
        ts = datetime.utcnow()
    # Attempt to extract resource from message if present
    message = j.get("message","")
    resource = None
    # heuristics: look for strings like /remote.php/dav/files/...
    m = re.search(r'(/remote.php[^\s,\\"]+)', message)
    if m:
        resource = m.group(1)
    # also check details object (nextcloud sometimes logs structured data)
    details = j.get("data") or j.get("details") or {}
    if isinstance(details, dict):
        # example: { "path": "/some/path" }
        rp = details.get("path") or details.get("file") or details.get("resource")
        if rp:
            resource = rp
    if not resource:
        # give up if no resource
        return None
    return {"ts": ts, "user": user, "resource": _normalize_resource(resource)}

def add_recent_action(resource, user, ts=None):
    if not resource or not user:
        return
    if ts is None:
        ts = datetime.utcnow()
    with recent_lock:
        recent_actions[resource] = (user, ts)
        # prune older than window
        cutoff = datetime.utcnow() - timedelta(seconds=CORRELATION_WINDOW)
        to_del = [k for k,(u,t) in recent_actions.items() if t < cutoff]
        for k in to_del:
            recent_actions.pop(k, None)

def guess_username_for_resource(resource):
    if not resource:
        return None
    norm = resource
    with recent_lock:
        # direct match
        item = recent_actions.get(norm)
        if item:
            user, ts = item
            if datetime.utcnow() - ts <= timedelta(seconds=CORRELATION_WINDOW):
                return user
        # try prefix matching (resource may be full path, sometimes message logs parent path)
        for k,(user,ts) in recent_actions.items():
            if datetime.utcnow() - ts > timedelta(seconds=CORRELATION_WINDOW):
                continue
            # if recent action resource is contained in the request resource or viceversa
            if k in norm or norm in k:
                return user
    return None

# Tail handlers
class AccessTailHandler(FileSystemEventHandler):
    def __init__(self, filepath):
        self.filepath = filepath
        self._open_file()
    def _open_file(self):
        try:
            self.f = open(self.filepath, "r", encoding="utf-8", errors="ignore")
            self.f.seek(0,2)
        except FileNotFoundError:
            self.f = None
    def on_modified(self, event):
        if event.src_path != self.filepath:
            return
        if self.f is None:
            self._open_file()
            if self.f is None:
                return
        while True:
            line = self.f.readline()
            if not line:
                break
            ev = parse_apache_line(line)
            if ev:
                try:
                    insert_event(ev)
                    print("Inserted", ev["resource"], ev["ip"], ev["ts"])
                except Exception as e:
                    print("DB insert failed:", e)

class AppLogTailHandler(FileSystemEventHandler):
    def __init__(self, filepath):
        self.filepath = filepath
        self._open_file()
    def _open_file(self):
        try:
            self.f = open(self.filepath, "r", encoding="utf-8", errors="ignore")
            self.f.seek(0,2)
        except FileNotFoundError:
            self.f = None
    def on_modified(self, event):
        if event.src_path != self.filepath:
            return
        if self.f is None:
            self._open_file()
            if self.f is None:
                return
        while True:
            line = self.f.readline()
            if not line:
                break
            try:
                parsed = parse_nextcloud_app_line(line)
                if parsed:
                    add_recent_action(parsed["resource"], parsed["user"], parsed["ts"])
                    # debug
                    # print("Correlated:", parsed["resource"], "->", parsed["user"], parsed["ts"])
            except Exception:
                pass

def initial_scan_access(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()[-1000:]
            for line in lines:
                ev = parse_apache_line(line)
                if ev:
                    try:
                        insert_event(ev)
                    except Exception:
                        pass
    except FileNotFoundError:
        pass

def initial_scan_app(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()[-2000:]
            for line in lines:
                parsed = parse_nextcloud_app_line(line)
                if parsed:
                    add_recent_action(parsed["resource"], parsed["user"], parsed["ts"])
    except FileNotFoundError:
        pass

def main():
    print("Audit service starting, watching:", ACCESS_LOG, "and", NEXTCLOUD_APP_LOG)
    ensure_table()
    initial_scan_app(NEXTCLOUD_APP_LOG)
    initial_scan_access(ACCESS_LOG)

    obs = Observer()
    access_handler = AccessTailHandler(ACCESS_LOG)
    app_handler = AppLogTailHandler(NEXTCLOUD_APP_LOG)
    logdir = os.path.dirname(ACCESS_LOG) or "/var/log/apache2"
    appdir = os.path.dirname(NEXTCLOUD_APP_LOG) or "/var/www/html/data/nextcloud"
    obs.schedule(access_handler, path=logdir, recursive=False)
    obs.schedule(app_handler, path=appdir, recursive=False)
    obs.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()

if __name__ == "__main__":
    main()
