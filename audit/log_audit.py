#!/usr/bin/env python3
"""
log_audit.py - audit improved
Guarda original_resource (URL) y resource (ruta humana resuelta desde oc_filecache) + object_id.
Mantiene dedupe por object_id y lógica de resolución de username (RESOLVE_WAIT).
"""
import os, re, time, json, traceback, threading
from datetime import datetime, timedelta

try:
    import psycopg2
except Exception as e:
    print("Missing psycopg2:", e); raise

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
except Exception:
    print("Missing watchdog"); raise

from dateutil import parser as dtparser

# --- ENV / config ---
def getenv_any(*names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

DB_HOST = getenv_any("DB_HOST","DATABASE_HOST","POSTGRES_HOST","DBHOST", default="db")
DB_NAME = getenv_any("POSTGRES_DB","DB_NAME","DATABASE_NAME", default="nextcloud")
DB_USER = getenv_any("POSTGRES_USER","DB_USER", default="nextcloud")
DB_PASS = getenv_any("POSTGRES_PASSWORD","DB_PASS","POSTGRES_PASSWORD", default="nextcloudpass")

ACCESS_LOG = os.getenv("NEXTCLOUD_LOG_PATH", "/var/log/apache2/access.log")
NEXTCLOUD_APP_LOG = os.getenv("NEXTCLOUD_APP_LOG", "/var/www/html/data/nextcloud/nextcloud.log")

DEDUPE_SECONDS = int(os.getenv("DEDUPE_SECONDS","2"))
CORRELATION_WINDOW = int(os.getenv("CORRELATION_WINDOW","30"))

RESOLVE_WAIT = int(os.getenv("RESOLVE_WAIT","10"))
RESOLVE_INTERVAL = int(os.getenv("RESOLVE_INTERVAL","1"))

# regex
APACHE_RE = re.compile(r'(?P<ip>\S+) \S+ (?P<user>\S+) \[(?P<time>[^\]]+)\] "(?P<method>GET|POST|PUT|DELETE|PROPFIND) (?P<url>\S+)[^"]*" (?P<status>\d{3}) (?P<size>\S+) "(?P<referrer>[^"]*)" "(?P<ua>[^"]*)"')
FILE_EXT_RE = re.compile(r'\.(pdf|docx?|xlsx?|pptx?|txt|odt|ods|jpg|jpeg|png|zip|rar|7z)(?:$|\?)', re.IGNORECASE)

INTEREST_PATTERNS = [
    "/remote.php/dav/files/",
    "/apps/files_pdfviewer",
    "/index.php/apps/files/download",
    "/remote.php/webdav/",
    "/index.php/s/",
    "/index.php/apps/richdocuments",
]

IGNORE_PATTERNS = [
    "/index.php/apps/richdocuments/wopi/settings",
    "/index.php/apps/richdocuments/wopi/thumbnail",
]

# --- DB helpers ---
def db_connect():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, connect_timeout=5)

def ensure_table():
    sql_create = """
    CREATE TABLE IF NOT EXISTS audit_events (
        id SERIAL PRIMARY KEY,
        ts timestamptz NOT NULL,
        username text,
        ip text,
        method text,
        original_resource text,
        resource text,
        object_id bigint,
        status int,
        size bigint,
        user_agent text,
        referrer text,
        raw_line text,
        event_type text
    );
    """
    try:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_create)
                # garantizar columnas por compatibilidad
                cur.execute("ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS original_resource text;")
                cur.execute("ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS resource text;")
                cur.execute("ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS object_id bigint;")
                conn.commit()
    except Exception as e:
        print("ensure_table DB error:", e)
        raise

# map path -> fileid and path
def find_fileid_and_path(conn, path):
    if not path:
        return (None, None)
    p = path.lstrip('/')
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT fileid, path FROM oc_filecache WHERE path = %s LIMIT 1;", (p,))
            r = cur.fetchone()
            if r:
                return (r[0], r[1])
            cur.execute("SELECT fileid, path FROM oc_filecache WHERE path = %s LIMIT 1;", (path,))
            r = cur.fetchone()
            if r:
                return (r[0], r[1])
            # fallback por filename
            fname = p.split('/')[-1]
            if fname:
                cur.execute("SELECT fileid, path FROM oc_filecache WHERE path ILIKE %s ORDER BY fileid DESC LIMIT 1;", ('%'+fname+'%',))
                r = cur.fetchone()
                if r:
                    return (r[0], r[1])
    except Exception as e:
        print("find_fileid_and_path error:", e)
    return (None, None)

HUMAN_BLACKLIST = set(['nextcloud','system','www-data','cron','root',''])

def find_username_for_fileid_or_path(conn, fileid=None, path=None):
    try:
        with conn.cursor() as cur:
            if fileid:
                cur.execute("""
                    SELECT COALESCE("user", affecteduser) AS username, timestamp
                    FROM oc_activity
                    WHERE object_id = %s AND app='files'
                    ORDER BY timestamp DESC
                    LIMIT 20;
                """, (fileid,))
                rows = cur.fetchall()
                for r in rows:
                    uname = (r[0] or '').strip()
                    if uname and uname not in HUMAN_BLACKLIST:
                        return uname
                if rows:
                    return (rows[0][0] or None)
            if path:
                cur.execute("""
                    SELECT COALESCE("user", affecteduser) AS username, timestamp
                    FROM oc_activity
                    WHERE app='files' AND (file = %s OR subjectparams::text LIKE %s)
                    ORDER BY timestamp DESC
                    LIMIT 20;
                """, (path, '%' + path + '%'))
                rows = cur.fetchall()
                for r in rows:
                    uname = (r[0] or '').strip()
                    if uname and uname not in HUMAN_BLACKLIST:
                        return uname
                if rows:
                    return (rows[0][0] or None)
    except Exception as e:
        print("find_username_for_fileid_or_path error:", e)
    return None

# utilities
def normalize_resource(r):
    if not r:
        return r
    r2 = r.split('?',1)[0]
    try:
        from urllib.parse import unquote
        r2 = unquote(r2)
    except Exception:
        pass
    return r2.rstrip('/')

def detect_event_type(method, status, resource):
    try:
        from urllib.parse import urlparse, parse_qs
        up = urlparse(resource)
        qs = parse_qs(up.query or "")
        path = up.path or resource
    except Exception:
        path = resource; qs = {}
    m = method.upper()
    if 'download' in (path or "").lower() or 'download' in (",".join(qs.keys()).lower()):
        return "download"
    if m == "PROPFIND":
        return "list"
    if m == "GET" and status == 200 and FILE_EXT_RE.search(path or ""):
        return "download"
    if m == "GET" and status == 200:
        return "view"
    return "other"

def already_similar(conn, ts, username, ip, method, resource, object_id):
    try:
        with conn.cursor() as cur:
            if object_id:
                q = """
                SELECT 1 FROM audit_events
                WHERE object_id = %s
                  AND coalesce(username,'(unknown)') = coalesce(%s,'(unknown)')
                  AND ip = %s
                  AND method = %s
                  AND abs(extract(epoch from ts - %s::timestamptz)) <= %s
                LIMIT 1;
                """
                cur.execute(q, (object_id, username, ip, method, ts, DEDUPE_SECONDS))
                if cur.fetchone():
                    return True
            q2 = """
            SELECT 1 FROM audit_events
            WHERE resource = %s
              AND coalesce(username,'(unknown)') = coalesce(%s,'(unknown)')
              AND ip = %s
              AND method = %s
              AND abs(extract(epoch from ts - %s::timestamptz)) <= %s
            LIMIT 1;
            """
            cur.execute(q2, (resource, username, ip, method, ts, DEDUPE_SECONDS))
            return cur.fetchone() is not None
    except Exception as e:
        print("already_similar DB error:", e)
        return False

def _insert_event_db(ev, object_id=None, resolved_path=None):
    # original_resource = raw URL; resource = resolved human path (if available)
    orig = normalize_resource(ev.get('resource'))
    ev['event_type'] = detect_event_type(ev.get('method','GET'), int(ev.get('status') or 0), orig or '')
    if ev.get('method','').upper() == "PROPFIND":
        return False
    try:
        with db_connect() as conn:
            if already_similar(conn, ev.get('ts'), ev.get('username'), ev.get('ip'), ev.get('method'), resolved_path or orig, object_id):
                return False
            sql = """
            INSERT INTO audit_events (ts, username, ip, method, original_resource, resource, object_id, status, size, user_agent, referrer, raw_line, event_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            with conn.cursor() as cur:
                cur.execute(sql, (
                    ev.get('ts'),
                    ev.get('username'),
                    ev.get('ip'),
                    ev.get('method'),
                    orig,
                    resolved_path or orig,
                    object_id,
                    int(ev.get('status') or 0),
                    int(ev.get('size') or 0) if ev.get('size') not in (None,'-') else None,
                    ev.get('ua'),
                    ev.get('referrer'),
                    ev.get('raw'),
                    ev.get('event_type')
                ))
            conn.commit()
            return True
    except Exception as e:
        print("insert_event DB error:", e)
        traceback.print_exc()
        return False

# correlation memory
recent_actions = {}
recent_lock = threading.Lock()
def add_recent_action(resource, user, ts):
    if not resource or not user:
        return
    with recent_lock:
        recent_actions[resource] = (user, ts)
        cutoff = datetime.utcnow() - timedelta(seconds=CORRELATION_WINDOW)
        for k,(u,t) in list(recent_actions.items()):
            if t < cutoff:
                recent_actions.pop(k,None)

def guess_username(resource):
    if not resource:
        return None
    with recent_lock:
        v = recent_actions.get(resource)
        if v:
            user,ts = v
            if datetime.utcnow() - ts <= timedelta(seconds=CORRELATION_WINDOW):
                return user
        for k,(u,t) in recent_actions.items():
            if datetime.utcnow() - t > timedelta(seconds=CORRELATION_WINDOW):
                continue
            if k in resource or resource in k:
                return u
    return None

# pending resolution keyed by canonical id
pending = {}
pending_lock = threading.Lock()

def canonical_key_for_resource(resource):
    res = normalize_resource(resource or '')
    m = re.search(r'fileId=(\d+)', res)
    if not m:
        m = re.search(r'/download/(\d+)', res)
    if not m:
        m = re.search(r'/wopi/files/(\d+)_', res)
    if m:
        try:
            fid = int(m.group(1))
            return ("fileid:%d" % fid, fid)
        except:
            pass
    return ("resource:%s" % res, None)

def schedule_resolution_and_insert(ev):
    res = normalize_resource(ev.get('resource'))
    for ign in IGNORE_PATTERNS:
        if ign in (res or ''):
            print("Skipping ignore pattern:", res)
            return
    key, maybe_fid = canonical_key_for_resource(res)
    with pending_lock:
        if key in pending:
            pending[key]['last_seen'] = datetime.utcnow()
            return
        pending[key] = {'first_seen': datetime.utcnow(), 'last_seen': datetime.utcnow(), 'thread': None, 'ev': ev, 'object_id': maybe_fid}
    def worker():
        try:
            ev_local = ev.copy()
            object_id = maybe_fid
            resolved_path = None
            deadline = datetime.utcnow() + timedelta(seconds=RESOLVE_WAIT)
            while datetime.utcnow() <= deadline:
                gu = guess_username(res)
                if gu:
                    ev_local['username'] = gu
                try:
                    with db_connect() as conn:
                        if not object_id:
                            fid,path = find_fileid_and_path(conn, res)
                            if fid:
                                object_id = fid
                                resolved_path = '/' + path if path and not path.startswith('/') else path
                        else:
                            # si ya teníamos object_id, obtener path
                            if not resolved_path:
                                cur = conn.cursor()
                                cur.execute("SELECT path FROM oc_filecache WHERE fileid = %s LIMIT 1;", (object_id,))
                                r = cur.fetchone()
                                if r and r[0]:
                                    resolved_path = '/' + r[0] if not r[0].startswith('/') else r[0]
                        # si hay object_id intentar resolver username
                        if object_id:
                            uname = find_username_for_fileid_or_path(conn, object_id, res)
                        else:
                            uname = find_username_for_fileid_or_path(conn, None, res)
                        if uname:
                            ev_local['username'] = uname
                            # insertar con resource resuelto si disponible
                            if _insert_event_db(ev_local, object_id, resolved_path):
                                print("Inserted (resolved-db canonical):", key, uname, "->", resolved_path or res)
                                return
                except Exception as e:
                    print("worker db resolution error:", e)
                # si ya tenemos username por guess > insertar ahora
                if ev_local.get('username'):
                    if _insert_event_db(ev_local, object_id, resolved_path):
                        print("Inserted (resolved-early):", key, ev_local.get('username'), "->", resolved_path or res)
                        return
                time.sleep(RESOLVE_INTERVAL)
            # final attempt: guess username and insert
            final_guess = guess_username(res)
            if final_guess:
                ev_local['username'] = final_guess
            if not resolved_path and object_id:
                # intentar una última vez obtener path
                try:
                    with db_connect() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT path FROM oc_filecache WHERE fileid = %s LIMIT 1;", (object_id,))
                        r = cur.fetchone()
                        if r and r[0]:
                            resolved_path = '/' + r[0] if not r[0].startswith('/') else r[0]
                except Exception as e:
                    pass
            if _insert_event_db(ev_local, object_id, resolved_path):
                print("Inserted (final canonical):", key, ev_local.get('username'), "->", resolved_path or res)
        finally:
            with pending_lock:
                pending.pop(key, None)
    t = threading.Thread(target=worker, daemon=True)
    with pending_lock:
        pending[key]['thread'] = t
    t.start()

# parsers
def parse_apache_line(line):
    m = APACHE_RE.search(line)
    if not m:
        return None
    url = m.group('url')
    if not any(p in url for p in INTEREST_PATTERNS):
        return None
    tstr = m.group('time')
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
        "raw": line.rstrip("\n")
    }

def parse_nextcloud_app_line(line):
    try:
        j = json.loads(line)
    except Exception:
        return None
    user = j.get("user") or j.get("uid") or j.get("user_id") or None
    ts = None
    if j.get("time"):
        try:
            ts = dtparser.parse(j.get("time"))
        except Exception:
            ts = datetime.utcnow()
    else:
        ts = datetime.utcnow()
    message = j.get("message","") or j.get("msg","")
    resource = None
    m = re.search(r'(/remote.php[^\s,\\"]+)', message)
    if m:
        resource = m.group(1)
    data = j.get("data") or j.get("details") or {}
    if isinstance(data, dict):
        if not resource:
            resource = data.get("path") or data.get("file") or data.get("resource")
    if not resource:
        return None
    return {"ts": ts, "user": user, "resource": normalize_resource(resource)}

# file handlers
class AccessHandler(FileSystemEventHandler):
    def __init__(self, path):
        self.path = path
        self._open()
    def _open(self):
        try:
            self.f = open(self.path, "r", encoding="utf-8", errors="ignore")
            self.f.seek(0,2)
        except Exception:
            self.f = None
    def on_modified(self, event):
        if event.src_path != self.path:
            return
        if self.f is None:
            self._open()
            if self.f is None:
                return
        while True:
            line = self.f.readline()
            if not line:
                break
            ev = parse_apache_line(line)
            if ev:
                schedule_resolution_and_insert(ev)

class AppHandler(FileSystemEventHandler):
    def __init__(self, path):
        self.path = path
        self._open()
    def _open(self):
        try:
            self.f = open(self.path, "r", encoding="utf-8", errors="ignore")
            self.f.seek(0,2)
        except Exception:
            self.f = None
    def on_modified(self, event):
        if event.src_path != self.path:
            return
        if self.f is None:
            self._open()
            if self.f is None:
                return
        while True:
            line = self.f.readline()
            if not line:
                break
            parsed = parse_nextcloud_app_line(line)
            if parsed:
                add_recent_action(parsed['resource'], parsed['user'], parsed['ts'])

# initial scan
def initial_scan_file(path, handler_parse, limit_lines=2000):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()[-limit_lines:]
            for L in lines:
                ev = handler_parse(L)
                if ev:
                    schedule_resolution_and_insert(ev)
    except FileNotFoundError:
        pass

# main
def main():
    print("Audit starting. ACCESS_LOG=", ACCESS_LOG, "APP_LOG=", NEXTCLOUD_APP_LOG)
    ensure_table()
    # preload recent actions to help guess usernames quickly
    try:
        initial_scan_file(NEXTCLOUD_APP_LOG, parse_nextcloud_app_line, limit_lines=2000)
    except Exception:
        pass
    initial_scan_file(ACCESS_LOG, parse_apache_line, limit_lines=1000)
    obs = Observer()
    started_any = False
    try:
        if os.path.exists(NEXTCLOUD_APP_LOG):
            ah = AppHandler(NEXTCLOUD_APP_LOG)
            obs.schedule(ah, path=os.path.dirname(NEXTCLOUD_APP_LOG) or ".", recursive=False)
            started_any = True
            print("Watching app log:", NEXTCLOUD_APP_LOG)
    except Exception as e:
        print("Error scheduling app log watch:", e)
    try:
        if os.path.exists(ACCESS_LOG):
            ah2 = AccessHandler(ACCESS_LOG)
            obs.schedule(ah2, path=os.path.dirname(ACCESS_LOG) or ".", recursive=False)
            started_any = True
            print("Watching access log:", ACCESS_LOG)
    except Exception as e:
        print("Error scheduling access log watch:", e)
    if not started_any:
        print("No logs to watch. Sleeping.")
        while True:
            time.sleep(60)
    obs.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()

if __name__ == "__main__":
    main()
