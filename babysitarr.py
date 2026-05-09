#!/usr/bin/env python3
"""
Babysitarr — monitors and fixes the arr + decypharr + RD pipeline.

Runs every CHECK_INTERVAL seconds and:
1. Detects stuck downloads in decypharr blocking workers → clears + restarts
2. Detects looping torrents (submit → delete → retry) → blocklists in arrs
3. Detects stalled arr imports → restarts the arr
4. Monitors download folders for deleted files → logs deletions
5. Checks Real-Debrid for expired/removed torrents → flags affected media
6. Ensures blocklisted items get re-searched by the arrs
7. Auto-imports items that radarr/sonarr can't parse (infers quality from filename)
8. Auto-fixes stale queue entries (already imported but stuck in queue)
9. Auto-fixes dead queue entries (download gone, media missing → re-search)
10. Detects stuck queue items (delay/downloading with no progress) → blocklists + re-searches
11. Auto-fixes indexer/list failures (clears DB status tables)
12. Auto-removes TMDb-removed movies
13. Monitors media library for items that lose their files unexpectedly
14. Sends notifications via email and/or Discord webhooks
15. Provides a web dashboard with live health scanning and one-click fixes
16. Exposes a /health endpoint for container health checks
"""

import os, sys, time, json, logging, hashlib, re, smtplib, subprocess, signal
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
CHECK_INTERVAL   = int(os.getenv("CHECK_INTERVAL", "120"))  # seconds
RD_API_KEY       = os.getenv("RD_API_KEY", "")
DECYPHARR_URL    = os.getenv("DECYPHARR_URL", "http://localhost:8282")
DECYPHARR_STATE  = os.getenv("DECYPHARR_STATE", "/decypharr-config/torrents.json")
DOWNLOAD_DIRS    = os.getenv("DOWNLOAD_DIRS", "/downloads/movies-1080p,/downloads/movies-4k,/downloads/shows-1080p,/downloads/shows-4k").split(",")
DOCKER_SOCKET    = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
DATA_DIR         = os.getenv("DATA_DIR", "/data")

# Email config
SMTP_HOST        = os.getenv("SMTP_HOST", "")
SMTP_PORT        = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER        = os.getenv("SMTP_USER", "")
SMTP_PASS        = os.getenv("SMTP_PASS", "")
EMAIL_FROM       = os.getenv("EMAIL_FROM", "")
EMAIL_TO         = os.getenv("EMAIL_TO", "")
EMAIL_COOLDOWN   = int(os.getenv("EMAIL_COOLDOWN", "3600"))  # 1 hour between emails

# Discord config
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
NOTIFICATION_TYPE   = os.getenv("NOTIFICATION_TYPE", "")  # email, discord, both, or empty for auto-detect

# Arr config directory (for DB reset feature)
# Each arr's config should be mounted at ARR_CONFIG_DIR/<arr_name>/
# e.g. /arr-configs/radarr/, /arr-configs/sonarr4k/, etc.
ARR_CONFIG_DIR = os.getenv("ARR_CONFIG_DIR", "/arr-configs")

# Arr configs: discover from environment variables
# Supported formats:
#   5-part (explicit type): name|type|host|port|apikey
#     e.g. RADARR_ANIME=radarr_anime|radarr|radarr-anime|7878|your-api-key
#   4-part (legacy, type inferred from env var name): name|host|port|apikey
#     e.g. RADARR=radarr|radarr|7878|your-api-key
ARRS = {}
for env_key, val in os.environ.items():
    if not val or "|" not in val:
        continue
    parts = val.split("|")
    if len(parts) == 5:
        name, arr_type, host, port, apikey = parts
        arr_type = arr_type.lower()
        if arr_type in ("radarr", "sonarr"):
            ARRS[name] = {"host": host, "port": int(port), "key": apikey, "type": arr_type}
    elif len(parts) == 4:
        name, host, port, apikey = parts
        # Infer type from the env var name (backward compatible)
        arr_type = "radarr" if "radarr" in env_key.lower() else ("sonarr" if "sonarr" in env_key.lower() else None)
        if arr_type:
            ARRS[name] = {"host": host, "port": int(port), "key": apikey, "type": arr_type}

# Thresholds
STUCK_DOWNLOAD_TIMEOUT = int(os.getenv("STUCK_DOWNLOAD_TIMEOUT", "600"))  # 10 min
LOOP_THRESHOLD         = int(os.getenv("LOOP_THRESHOLD", "5"))            # retries before blocklist
IMPORT_STALL_TIMEOUT   = int(os.getenv("IMPORT_STALL_TIMEOUT", "300"))    # 5 min no imports = stalled
MAX_WORKERS            = int(os.getenv("MAX_WORKERS", "3"))
STUCK_QUEUE_TIMEOUT    = int(os.getenv("STUCK_QUEUE_TIMEOUT", "1800"))    # 30 min for stuck queue items
STUCK_DONE_TIMEOUT     = int(os.getenv("STUCK_DONE_TIMEOUT", "300"))     # 5 min for sizeleft=0 (download done but no import)
STUCK_RETRY_LIMIT      = int(os.getenv("STUCK_RETRY_LIMIT", "5"))
DEAD_RETRY_LIMIT       = int(os.getenv("DEAD_RETRY_LIMIT", "3"))
RD_ORPHAN_STRIKES      = int(os.getenv("RD_ORPHAN_STRIKES", "2"))         # strikes before auto-blocking an orphan RD magnet_error
SYMLINK_LOOP_THRESHOLD = int(os.getenv("SYMLINK_LOOP_THRESHOLD", "10"))    # post-download timeout retries before treating as stuck
SYMLINK_RD_DELETE_THRESHOLD = int(os.getenv("SYMLINK_RD_DELETE_THRESHOLD", "30"))  # higher count needed to also delete from RD
SYMLINK_RD_DELETE_MIN_CYCLES = int(os.getenv("SYMLINK_RD_DELETE_MIN_CYCLES", "3"))  # consecutive cycles seeing the timeout before RD delete
SYMLINK_LOOK_BACK_SEC = int(os.getenv("SYMLINK_LOOK_BACK_SEC", "600"))     # only count timeouts logged within this window
DISABLE_AUTO_INDEXER_RESET = os.getenv("DISABLE_AUTO_INDEXER_RESET", "").lower() in ("1", "true", "yes")  # issue #1 opt-out

# Decypharr v2.x runtime API (v2.x persists state in BoltDB, not torrents.json)
DECYPHARR_API_LIMIT     = int(os.getenv("DECYPHARR_API_LIMIT", "1000"))
ORPHAN_TIMEOUT_SEC      = int(os.getenv("ORPHAN_TIMEOUT_SEC", "1800"))     # decypharr torrent stuck w/o matching arr queue
HOST_LOAD_THRESHOLD     = float(os.getenv("HOST_LOAD_THRESHOLD", "200"))   # 1-min loadavg above this = alert (set high; decypharr/rclone/arrs run loadavg 50-150 normally)
HOST_DSTATE_THRESHOLD   = int(os.getenv("HOST_DSTATE_THRESHOLD", "3"))     # D-state procs above this = alert (the real wedge canary)
HOST_HEALTH_COOLDOWN    = int(os.getenv("HOST_HEALTH_COOLDOWN", "1800"))   # min seconds between host-health alerts
EXPECTED_DECYPHARR_TAG  = os.getenv("EXPECTED_DECYPHARR_TAG", "2.2-stable")  # alert if decypharr image tag drifts from this
DECYPHARR_CPU_WATCHDOG_THRESHOLD = float(os.getenv("DECYPHARR_CPU_WATCHDOG_THRESHOLD", "80"))  # CPU% for decypharr container
DECYPHARR_CPU_WATCHDOG_CYCLES    = int(os.getenv("DECYPHARR_CPU_WATCHDOG_CYCLES", "3"))         # consecutive over-threshold cycles before restart
DECYPHARR_CPU_WATCHDOG_COOLDOWN  = int(os.getenv("DECYPHARR_CPU_WATCHDOG_COOLDOWN", "3600"))    # min seconds between watchdog restarts

# Zurg WebDAV config (for path-mismatch fix)
ZURG_DAV_URL     = os.getenv("ZURG_DAV_URL", "http://172.18.0.1:9999/dav/__all__/")
ZURG_BASE_PATH   = os.getenv("ZURG_BASE_PATH", "/media/zurg/__all__")
PATH_MISMATCH_COOLDOWN = int(os.getenv("PATH_MISMATCH_COOLDOWN", "300"))  # min seconds between runs
_VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".ts", ".wmv"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(DATA_DIR, "babysitarr.log")),
    ]
)
log = logging.getLogger("babysitarr")

# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
STATE_FILE = os.path.join(DATA_DIR, "babysitarr_state.json")
_start_time = time.time()
_last_cycle_time = None

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "loop_counts": {},          # hash -> retry count
        "last_import_times": {},    # arr_name -> iso timestamp
        "known_files": {},          # path -> first_seen timestamp
        "deleted_files": [],        # list of {path, deleted_at, title}
        "rd_removed": [],           # list of {hash, name, detected_at}
        "actions_log": [],          # list of {action, detail, timestamp}
        "library_snapshot": {},     # {arr: {id: {title, has_file/file_count}}}
        "library_deletions": [],    # list of {arr, title, id, detail, detected_at}
        "library_grace": {},        # grace counters for transient API blips
        "rd_orphan_seen": {},       # hash -> strike count for orphan RD magnet_errors
    }

def save_state(state):
    os.makedirs(DATA_DIR, exist_ok=True)
    # Trim logs to last 500 entries
    state["deleted_files"] = state["deleted_files"][-500:]
    state["rd_removed"] = state["rd_removed"][-500:]
    state["actions_log"] = state["actions_log"][-500:]
    state["library_deletions"] = state.get("library_deletions", [])[-500:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def log_action(state, action, detail):
    entry = {"action": action, "detail": detail, "timestamp": datetime.now().isoformat()}
    state["actions_log"].append(entry)
    log.info(f"ACTION: {action} — {detail}")

_last_email_time = 0

def _send_email(subject, body):
    """Send an email alert. Rate-limited to one per EMAIL_COOLDOWN."""
    global _last_email_time
    if not SMTP_USER or not EMAIL_TO or not SMTP_HOST:
        return
    now = time.time()
    if now - _last_email_time < EMAIL_COOLDOWN:
        log.info(f"Email suppressed (cooldown): {subject}")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = f"[Babysitarr] {subject}"
        msg["From"] = EMAIL_FROM or SMTP_USER
        msg["To"] = EMAIL_TO
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(EMAIL_FROM or SMTP_USER, EMAIL_TO, msg.as_string())
        _last_email_time = now
        log.info(f"Alert email sent: {subject}")
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")

def _send_discord(subject, body, color=0xef4444):
    """Send a Discord webhook notification."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        payload = {
            "embeds": [{
                "title": f"[Babysitarr] {subject}",
                "description": body[:4096],
                "color": color,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }]
        }
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if r.ok:
            log.info(f"Discord notification sent: {subject}")
        else:
            log.error(f"Discord webhook failed ({r.status_code}): {r.text[:200]}")
    except Exception as e:
        log.error(f"Failed to send Discord notification: {e}")

def send_notification(subject, body, level="alert"):
    """Send notification via configured channels (email, discord, or both).

    level: "alert" (red #ef4444) or "info" (green #10b981) — affects Discord embed color.
    """
    discord_color = 0xef4444 if level == "alert" else 0x10b981

    # Determine which channels to use
    ntype = NOTIFICATION_TYPE.lower().strip()
    if ntype == "email":
        use_email, use_discord = True, False
    elif ntype == "discord":
        use_email, use_discord = False, True
    elif ntype == "both":
        use_email, use_discord = True, True
    else:
        # Auto-detect based on what's configured
        use_email = bool(SMTP_USER and EMAIL_TO and SMTP_HOST)
        use_discord = bool(DISCORD_WEBHOOK_URL)

    if use_email:
        _send_email(subject, body)
    if use_discord:
        _send_discord(subject, body, color=discord_color)

# Keep backward compatibility — internal callers used send_alert
send_alert = send_notification

# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------
def _docker_api(method, path):
    """Send a request to Docker via unix socket. Returns (status_code, body)."""
    import socket as _socket
    sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect("/var/run/docker.sock")
    sock.sendall(f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    response = sock.recv(4096).decode()
    sock.close()
    status = int(response.split(" ")[1]) if " " in response else 0
    return status, response


def docker_restart(container_name):
    """Restart a container via Docker socket HTTP API."""
    try:
        status, response = _docker_api("POST", f"/containers/{container_name}/restart?t=10")
        if status in (200, 204):
            log.info(f"Restarted container: {container_name}")
            return True
        else:
            log.error(f"Failed to restart {container_name}: {response[:100]}")
            return False
    except Exception as e:
        log.error(f"Failed to restart {container_name}: {e}")
        return False

# ---------------------------------------------------------------------------
# Arr API helpers
# ---------------------------------------------------------------------------
def arr_get(arr, path):
    info = ARRS[arr]
    sep = "&" if "?" in path else "?"
    url = f"http://{info['host']}:{info['port']}/api/v3/{path}{sep}apikey={info['key']}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Failed to query {arr} {path}: {e}")
        return None

def arr_post(arr, path, data=None):
    info = ARRS[arr]
    sep = "&" if "?" in path else "?"
    url = f"http://{info['host']}:{info['port']}/api/v3/{path}{sep}apikey={info['key']}"
    try:
        r = requests.post(url, json=data or {}, timeout=15)
        return r.json() if r.ok else None
    except Exception:
        return None

def arr_delete(arr, path):
    info = ARRS[arr]
    sep = "&" if "?" in path else "?"
    url = f"http://{info['host']}:{info['port']}/api/v3/{path}{sep}apikey={info['key']}"
    try:
        r = requests.delete(url, timeout=15)
        return r.ok
    except Exception:
        return False

def arr_get_queue(arr, page_size=500):
    return arr_get(arr, f"queue?page=1&pageSize={page_size}")

def arr_get_last_import(arr):
    info = ARRS[arr]
    event_type = 1  # grabbed=1 for import in both radarr/sonarr
    data = arr_get(arr, f"history?page=1&pageSize=1&sortKey=date&sortDirection=descending&eventType={event_type}")
    if data and data.get("records"):
        return data["records"][0]["date"]
    return None

def arr_remove_queue_item(arr, item_id, blocklist=True):
    info = ARRS[arr]
    url = f"http://{info['host']}:{info['port']}/api/v3/queue/{item_id}?removeFromClient=true&blocklist={'true' if blocklist else 'false'}&skipRedownload=true&apikey={info['key']}"
    try:
        r = requests.delete(url, timeout=15)
        return r.ok
    except Exception:
        return False

def arr_search_missing(arr):
    info = ARRS[arr]
    cmd_name = "MissingMoviesSearch" if info["type"] == "radarr" else "MissingEpisodeSearch"
    return arr_post(arr, "command", {"name": cmd_name})

RELEASE_PROFILE_NAME = "Babysitarr auto-blocks"

def arr_add_release_profile_ignore(arr, term, profile_name=RELEASE_PROFILE_NAME):
    """Add a term to a named Release Profile's ignored list. Creates the profile if absent.
    Idempotent — repeated calls with the same term are no-ops."""
    info = ARRS[arr]
    profiles = arr_get(arr, "releaseprofile") or []
    existing = next((p for p in profiles if p.get("name") == profile_name), None)
    if existing:
        ignored = list(existing.get("ignored", []))
        if term in ignored:
            return True
        ignored.append(term)
        payload = dict(existing)
        payload["ignored"] = ignored
        url = f"http://{info['host']}:{info['port']}/api/v3/releaseprofile/{existing['id']}?apikey={info['key']}"
        try:
            r = requests.put(url, json=payload, timeout=15)
            return r.ok
        except Exception:
            return False
    payload = {
        "name": profile_name,
        "enabled": True,
        "required": [],
        "ignored": [term],
        "indexerId": 0,
        "tags": [],
    }
    return bool(arr_post(arr, "releaseprofile", payload))

_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')
_LOG_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
DECYPHARR_LOG_FILE = os.getenv("DECYPHARR_LOG_FILE", "/decypharr-config/logs/decypharr.log")


def _read_decypharr_log_tail(window_seconds=1800, max_bytes=500_000, _now=None):
    """Read the last `max_bytes` of the decypharr log file and return ANSI-
    stripped lines whose leading timestamp falls within `window_seconds` of
    `_now` (defaults to current UTC). Returns [] if the log is missing or
    unreadable.

    babysitarr's container has no docker CLI — only the docker socket. We
    can't shell out to `docker logs`. Instead we read decypharr's rolling
    file log, which is mounted at /decypharr-config/logs/decypharr.log via
    the existing config volume.

    Decypharr writes timestamps in UTC (its container has TZ=UTC by default
    while babysitarr's host runs WEST/UTC+1). All comparisons here use naive
    UTC: the parsed log timestamp is naive-UTC and the default `_now` is
    UTC-now-stripped-of-tzinfo. Don't replace this with datetime.now() — the
    naive local-time vs. naive UTC mismatch will silently filter out
    everything from the live log (a 30-min window from local time will
    exclude entries that are actually 3 minutes old in UTC).

    `_now` exists for tests: pin it to a fixture-relative naive-UTC time to
    assert against captured log samples without freezing system time.
    """
    if not os.path.exists(DECYPHARR_LOG_FILE):
        return []
    try:
        with open(DECYPHARR_LOG_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            tail = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning(f"Could not read decypharr log: {e}")
        return []
    now = _now or datetime.now(timezone.utc).replace(tzinfo=None)
    cutoff = now - timedelta(seconds=window_seconds)
    out = []
    for line in tail.split("\n"):
        plain = _ANSI_RE.sub("", line)
        m = _LOG_TS_RE.match(plain)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        # Both bounds: cutoff <= ts <= now. The upper bound is a no-op in
        # production (file timestamps can't be in the future) but lets tests
        # anchor `_now` to a point in the middle of a fixture.
        if cutoff <= ts <= now:
            out.append(plain)
    return out


_PROCESSING_HASH_RE = re.compile(r"Hash=([a-f0-9]+)")


def _count_processing_hashes(lines):
    """Given decypharr log lines, return {hash: count} for 'Processing torrent'
    entries. Hashes are lower-cased hex.

    The log format is decypharr v1.1.6's:
        ... [realdebrid] Processing torrent Action=symlink Arr=movies-1080p
            Debrid=realdebrid Hash=aff544...e5 Name="..."
    A retry of the same release re-emits the same Hash=, so the count of
    Processing-torrent lines per hash is the retry count.

    ANSI-strips defensively: production logs have color escapes between the
    field name and the value (`Hash=\\x1b[0m<hex>`), and we want this helper
    to be safe to call on either pre-stripped or raw lines.
    """
    counts = {}
    for line in lines:
        plain = _ANSI_RE.sub("", line)
        if "Processing torrent" not in plain:
            continue
        m = _PROCESSING_HASH_RE.search(plain)
        if not m:
            continue
        h = m.group(1).lower()
        counts[h] = counts.get(h, 0) + 1
    return counts

def decypharr_lookup_hash(hash_val):
    """Scan the decypharr log file for a hash. Returns (category, release_name) or None.
    Works around decypharr's dirty-write bug where a magnet is submitted to RD
    even when decypharr returns 400 to the arr (so no arr queue entry exists)."""
    if not os.path.exists(DECYPHARR_LOG_FILE):
        log.warning(f"Decypharr log file not found: {DECYPHARR_LOG_FILE}")
        return None
    hash_lower = hash_val.lower()
    try:
        with open(DECYPHARR_LOG_FILE, errors="replace") as f:
            for line in f:
                if hash_lower not in line.lower() or "Processing torrent" not in line:
                    continue
                plain = _ANSI_RE.sub("", line)
                mcat = re.search(r'Arr=(\S+)', plain)
                mname = re.search(r'Name="([^"]+)"', plain)
                if mcat and mname:
                    return mcat.group(1), mname.group(1)
    except Exception as e:
        log.warning(f"Could not read decypharr log: {e}")
    return None

def _wait_container_stopped(cn, timeout=20):
    """Wait until a container is fully stopped (not running)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, resp = _docker_api("GET", f"/containers/{cn}/json")
            if status == 200 and '"Running":false' in resp:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _reset_arr_db(arr_name):
    """Stop arr, clear IndexerStatus + ImportListStatus from DB, restart.

    Always ensures the container is started back up, even if something fails.

    NOTE: This requires each arr's config directory to be mounted into babysitarr.
    Mount them at ARR_CONFIG_DIR/<arr_name>/ (e.g. /arr-configs/radarr/).
    """
    info = ARRS[arr_name]
    cn = arr_name.lower().replace(" ", "")
    db_name = "radarr.db" if info["type"] == "radarr" else "sonarr.db"
    db_path = os.path.join(ARR_CONFIG_DIR, cn, db_name)
    try:
        _docker_api("POST", f"/containers/{cn}/stop?t=15")
        if not _wait_container_stopped(cn, timeout=25):
            log.error(f"Container {cn} did not stop in time, skipping DB reset")
            # Still try to start it back up
            raise Exception("container did not stop")
        subprocess.run(["sqlite3", db_path, "DELETE FROM IndexerStatus; DELETE FROM ImportListStatus;"],
                       capture_output=True, timeout=10, check=True)
    except Exception as e:
        log.error(f"DB reset failed for {arr_name}: {e}")
    # ALWAYS start the container, even if the DB clear failed
    for attempt in range(3):
        try:
            _docker_api("POST", f"/containers/{cn}/start")
            time.sleep(5)
            # Verify it's actually running
            status, resp = _docker_api("GET", f"/containers/{cn}/json")
            if '"Running":true' in resp or '"running":true' in resp:
                return True
            log.warning(f"Container {cn} not running after start attempt {attempt+1}")
        except Exception as e:
            log.error(f"Failed to start {cn} (attempt {attempt+1}): {e}")
        time.sleep(3)
    log.error(f"CRITICAL: Could not restart {cn} after 3 attempts!")
    return False

# ---------------------------------------------------------------------------
# Check 1: Stuck downloads in decypharr
# ---------------------------------------------------------------------------
def _decypharr_get_torrents():
    """Query decypharr's runtime REST API for current torrents.

    Decypharr v2.x persists state in BoltDB (config/db/*.db), NOT in torrents.json
    — that file is a v1.x leftover. The runtime API is the only reliable source
    of truth for v2.x.

    Returns a list of dicts. Useful keys: info_hash, name, state, status,
    added_on (ISO 8601 string), action, category. Empty list on failure.
    """
    try:
        r = requests.get(f"{DECYPHARR_URL}/api/torrents",
                         params={"limit": DECYPHARR_API_LIMIT}, timeout=15)
        if not r.ok:
            log.warning(f"decypharr /api/torrents returned HTTP {r.status_code}")
            return []
        data = r.json()
        # v2.x returns {"torrents": [...], "categories": [...], ...}
        if isinstance(data, dict):
            return data.get("torrents", []) or []
        # v1.x returns a flat list. Different field schema (no info_hash/state/
        # status/action), so v2-style filters in the consuming checks naturally
        # return no candidates — that's correct behavior since v1.x doesn't
        # have the DFS-stack leak those checks were designed to catch.
        if isinstance(data, list):
            return data
    except Exception as e:
        log.warning(f"Could not fetch decypharr torrents: {e}")
    return []


def _decypharr_delete_torrent(hash_val):
    """Delete a torrent from decypharr by info_hash.

    Uses the working endpoint pattern: DELETE /api/torrents?hashes=H (query string,
    not body). The qbittorrent-mock /api/v2/torrents/delete returns 200 but does
    NOT actually persist a delete in v2.x. This is the API that does.
    """
    if not hash_val:
        return False
    try:
        r = requests.delete(f"{DECYPHARR_URL}/api/torrents",
                            params={"hashes": hash_val}, timeout=15)
        return r.ok
    except Exception as e:
        log.warning(f"Decypharr delete failed for {hash_val[:10]}: {e}")
        return False


def _parse_iso_ts(s):
    """Parse decypharr's ISO 8601 timestamps. Returns epoch seconds or None."""
    if not s:
        return None
    try:
        # Handle trailing 'Z' (UTC)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (ValueError, AttributeError, TypeError):
        return None


def check_stuck_downloads(state):
    """Find decypharr torrents that finished downloading on RD but never completed
    their post-download action (symlink) past STUCK_DOWNLOAD_TIMEOUT.

    Indicator: state='downloading' AND status='downloaded' AND added_on age >
    threshold. The state stays 'downloading' until decypharr finishes the
    symlink step, so a stale entry here means the symlink never landed.

    Reads from the v2.x runtime API. The previous v1.x torrents.json path is
    abandoned (not written by decypharr v2.x).
    """
    torrents = _decypharr_get_torrents()
    if not torrents:
        return

    now = time.time()
    stuck = []
    for t in torrents:
        if t.get("state") != "downloading":
            continue
        # Only consider items where RD already has the file — these are
        # post-download stalls, not in-flight downloads.
        if t.get("status") != "downloaded":
            continue
        added = _parse_iso_ts(t.get("added_on"))
        if added is None:
            continue
        age = now - added
        if age <= STUCK_DOWNLOAD_TIMEOUT:
            continue
        stuck.append({
            "hash": t.get("info_hash", ""),
            "name": t.get("name", "unknown"),
            "age_min": int(age / 60),
            "category": t.get("category", ""),
        })

    if not stuck:
        return

    log.warning(f"Found {len(stuck)} stuck downloads in decypharr (>{STUCK_DOWNLOAD_TIMEOUT}s old)")

    cleared = 0
    for s in stuck:
        if _decypharr_delete_torrent(s["hash"]):
            cleared += 1
            log_action(state, "clear_stuck",
                       f"Removed stuck download: {s['name']} (stuck {s['age_min']}min)")
    # No decypharr restart needed — DELETE /api/torrents is persistent in v2.x.

# ---------------------------------------------------------------------------
# Check 2: Looping torrents (submit → delete → retry)
# ---------------------------------------------------------------------------
def check_looping_torrents(state):
    """Detect torrents that keep getting submitted-and-deleted on Real-Debrid
    (the classic uncached-release retry storm) and blocklist them in the
    *arrs once they cross LOOP_THRESHOLD retries.

    Reads decypharr's rolling file log directly. Earlier versions shelled out
    to `docker logs decypharr` via subprocess, which silently no-op'd in
    production: the babysitarr container has the docker socket mounted but
    not the docker CLI binary, so subprocess.run raised FileNotFoundError,
    the bare except swallowed it, and the function returned 0 actions for
    months. Retry storms ran unchecked. The tests in tests/test_log_parsing.py
    guard against future log-format drift.
    """
    try:
        requests.get(f"{DECYPHARR_URL}/api/v2/torrents/info", timeout=10)
    except Exception:
        return

    lines = _read_decypharr_log_tail(window_seconds=1800)
    if not lines:
        return

    # No deletes in this window means there's no storm to act on right now,
    # even if Processing lines are flowing for healthy downloads.
    if not any("deleted from RD" in l for l in lines):
        return

    hash_counts = _count_processing_hashes(lines)

    # Persist counts across cycles. Storms span hours but each cycle only
    # sees the last 30 min of log; without persistence, a hash that retries
    # 4× per cycle for 2 hours would never cross LOOP_THRESHOLD=5 in a
    # single window.
    loop_counts = state.get("loop_counts", {})
    for h, count in hash_counts.items():
        loop_counts[h] = loop_counts.get(h, 0) + count

    to_blocklist = {h: c for h, c in loop_counts.items() if c >= LOOP_THRESHOLD}
    if not to_blocklist:
        state["loop_counts"] = loop_counts
        return

    log.warning(f"Found {len(to_blocklist)} looping torrents (>={LOOP_THRESHOLD} retries)")

    for arr_name in ARRS:
        queue_data = arr_get_queue(arr_name)
        if not queue_data:
            continue
        for record in queue_data.get("records", []):
            did = (record.get("downloadId") or "").lower()
            if did in to_blocklist:
                if arr_remove_queue_item(arr_name, record["id"], blocklist=True):
                    title = record.get("title", "unknown")[:50]
                    log_action(state, "blocklist_loop",
                               f"Blocklisted looping torrent in {arr_name}: "
                               f"{title} ({to_blocklist[did]} retries)")
                    loop_counts.pop(did, None)

    # Cap stored counts at 3× threshold so the dict doesn't grow forever.
    state["loop_counts"] = {h: c for h, c in loop_counts.items() if c < LOOP_THRESHOLD * 3}

# ---------------------------------------------------------------------------
# Check 2b: Post-download symlink retry loops
# ---------------------------------------------------------------------------
_SYMLINK_NAME_RE = re.compile(
    r'timeout waiting for files.*?name=("([^"]+)"|(\S+))',
    re.IGNORECASE,
)
_RD_API_BASE = "https://api.real-debrid.com/rest/1.0"
DECYPHARR_CONFIG_FILE = os.getenv("DECYPHARR_CONFIG_FILE", "/decypharr-config/config.json")


def _decypharr_rd_api_key():
    """Read the Real-Debrid API key out of the mounted decypharr config."""
    try:
        with open(DECYPHARR_CONFIG_FILE) as f:
            cfg = json.load(f)
        for d in cfg.get("debrids", []):
            if d.get("name") == "realdebrid" and d.get("api_key"):
                return d["api_key"]
    except Exception as e:
        log.warning(f"Could not read decypharr config for RD api key: {e}")
    return None


def _rd_find_torrent_id(api_key, target_name):
    """Find a torrent in RD whose filename matches target_name AND is downloaded.
    Returns the torrent id, or None if not found / ambiguous."""
    try:
        r = requests.get(
            f"{_RD_API_BASE}/torrents",
            headers={"Authorization": f"Bearer {api_key}"},
            params={"limit": 200},
            timeout=15,
        )
        if not r.ok:
            log.warning(f"RD list torrents failed: HTTP {r.status_code}")
            return None
        torrents = r.json()
    except Exception as e:
        log.warning(f"RD list torrents error: {e}")
        return None

    target_base = target_name.rsplit(".", 1)[0].strip().lower()
    matches = [
        t for t in torrents
        if t.get("status") == "downloaded"
        and t.get("filename", "").rsplit(".", 1)[0].strip().lower() == target_base
    ]
    if len(matches) != 1:
        # 0 = nothing to delete; >1 = ambiguous, refuse to act
        if len(matches) > 1:
            log.warning(f"RD lookup for {target_name!r} matched {len(matches)} torrents; refusing to delete (ambiguous)")
        return None
    return matches[0]["id"]


def _rd_delete_torrent(api_key, torrent_id):
    """DELETE a torrent from Real-Debrid by id. Returns True on success."""
    try:
        r = requests.delete(
            f"{_RD_API_BASE}/torrents/delete/{torrent_id}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=15,
        )
        return r.status_code in (200, 204)
    except Exception as e:
        log.warning(f"RD delete {torrent_id} error: {e}")
        return False


def check_symlink_loops(state):
    """Detect torrents stuck in a post-download symlink retry loop.

    Decypharr logs `Error running post-download action error="timeout waiting
    for files..."` every ~15s when the file it expects in the rclone/zurg mount
    isn't there. Common cause: the torrent's name on Real-Debrid (which
    decypharr asks for) doesn't match the folder zurg exposes (e.g. UIndex.org
    uploads with prefixed wrapper folders). Left unchecked, the loop pins CPU
    and accumulates thousands of threads.

    Two action tiers, both with strict false-positive guards:
      - SYMLINK_LOOP_THRESHOLD timeouts (default 10): scrub torrents.json,
        blocklist in arr queues, restart decypharr.
      - SYMLINK_RD_DELETE_THRESHOLD timeouts (default 30) AND seen in
        SYMLINK_RD_DELETE_MIN_CYCLES consecutive cycles AND found in RD with
        status="downloaded": also delete from Real-Debrid so decypharr can't
        re-discover it on the next sync.
    """
    if not os.path.exists(DECYPHARR_LOG_FILE):
        return

    cycle_counts = {}
    try:
        # Read tail (~200KB ≈ last 30 min of looping). Avoids loading huge logs.
        with open(DECYPHARR_LOG_FILE, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 200_000))
            tail = f.read().decode("utf-8", errors="replace")
    except Exception as e:
        log.warning(f"Could not read decypharr log for symlink loop check: {e}")
        return

    cutoff = datetime.now() - timedelta(seconds=SYMLINK_LOOK_BACK_SEC)
    for line in tail.split("\n"):
        if "timeout waiting for files" not in line:
            continue
        plain = _ANSI_RE.sub("", line)
        # Filter to entries within the look-back window — guards against
        # counting old timeouts that have already been resolved.
        ts_m = _LOG_TS_RE.match(plain)
        if not ts_m:
            continue
        try:
            ts = datetime.strptime(ts_m.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        if ts < cutoff:
            continue
        m = _SYMLINK_NAME_RE.search(plain)
        if not m:
            continue
        name = (m.group(2) or m.group(3) or "").strip().strip('"')
        if name:
            cycle_counts[name] = cycle_counts.get(name, 0) + 1

    # Drop names that didn't appear in the recent window — if decypharr
    # stopped logging timeouts for a name, the issue resolved itself.
    sym_counts = {n: c for n, c in state.get("symlink_loop_counts", {}).items() if n in cycle_counts}
    sym_streak = {n: c for n, c in state.get("symlink_loop_streak", {}).items() if n in cycle_counts}
    for n, c in cycle_counts.items():
        sym_counts[n] = sym_counts.get(n, 0) + c
        sym_streak[n] = sym_streak.get(n, 0) + 1
    state["symlink_loop_counts"] = sym_counts
    state["symlink_loop_streak"] = sym_streak

    over = {n: c for n, c in sym_counts.items() if c >= SYMLINK_LOOP_THRESHOLD}
    if not over:
        return

    log.warning(f"Found {len(over)} torrents stuck in symlink-retry loop (>={SYMLINK_LOOP_THRESHOLD} timeouts each)")

    handled = set()

    # 1. Remove from decypharr's torrents.json so the loop dies on next start
    if os.path.exists(DECYPHARR_STATE):
        try:
            with open(DECYPHARR_STATE) as f:
                torrents = json.load(f)
            modified = False
            for key in list(torrents.keys()):
                t_name = (torrents[key].get("name") or "").strip()
                t_base = t_name.rsplit(".", 1)[0]
                for stuck_name in list(over.keys()):
                    s_base = stuck_name.rsplit(".", 1)[0]
                    if t_name == stuck_name or (t_base and t_base == s_base):
                        del torrents[key]
                        modified = True
                        log_action(state, "clear_symlink_loop",
                                   f"Removed symlink-loop torrent from decypharr: {stuck_name} ({over[stuck_name]} timeouts)")
                        handled.add(stuck_name)
                        break
            if modified:
                with open(DECYPHARR_STATE, "w") as f:
                    json.dump(torrents, f, indent=2)
        except Exception as e:
            log.error(f"Failed to update decypharr torrents.json: {e}")

    # 2. Blocklist matching items in arr queues so the arr grabs a different release
    for arr_name in ARRS:
        queue_data = arr_get_queue(arr_name)
        if not queue_data:
            continue
        for record in queue_data.get("records", []):
            title = record.get("title", "") or ""
            for stuck_name in list(over.keys()):
                stuck_base = stuck_name.rsplit(".", 1)[0]
                if stuck_base and stuck_base in title:
                    if arr_remove_queue_item(arr_name, record["id"], blocklist=True):
                        log_action(state, "blocklist_symlink_loop",
                                   f"Blocklisted symlink-loop torrent in {arr_name}: {title[:80]}")
                        handled.add(stuck_name)

    # 3. Real-Debrid delete — only for items that have crossed the higher
    # threshold AND been seen in multiple consecutive cycles. This is the
    # durable fix that prevents decypharr from re-discovering them on restart.
    rd_deleted = set()
    rd_candidates = {
        n: c for n, c in over.items()
        if c >= SYMLINK_RD_DELETE_THRESHOLD
        and sym_streak.get(n, 0) >= SYMLINK_RD_DELETE_MIN_CYCLES
    }
    if rd_candidates:
        rd_key = _decypharr_rd_api_key()
        if not rd_key:
            log.warning("Skipping RD delete: no api key available from decypharr config")
        else:
            for stuck_name, count in rd_candidates.items():
                rd_id = _rd_find_torrent_id(rd_key, stuck_name)
                if not rd_id:
                    # Either not in RD (nothing to delete) or ambiguous (refused).
                    # Either way, mark handled so we don't keep trying every cycle.
                    handled.add(stuck_name)
                    continue
                if _rd_delete_torrent(rd_key, rd_id):
                    rd_deleted.add(stuck_name)
                    handled.add(stuck_name)
                    log_action(state, "rd_delete_symlink_loop",
                               f"Deleted from Real-Debrid: {stuck_name} ({count} timeouts across {sym_streak[stuck_name]} cycles, RD id {rd_id})")

    if not handled:
        return

    # 4. Restart decypharr to clear thread accumulation and the retry loop.
    # Best-effort: if decypharr is stopped, this fails silently and cleanup still stands.
    if docker_restart("decypharr"):
        log_action(state, "restart",
                   f"Restarted decypharr after clearing {len(handled)} symlink-loop torrents")
        time.sleep(10)

    # Drop handled names from the persistent counters
    for n in handled:
        sym_counts.pop(n, None)
        sym_streak.pop(n, None)
    state["symlink_loop_counts"] = sym_counts
    state["symlink_loop_streak"] = sym_streak

    body_lines = [f"Cleared {len(handled)} torrent(s) stuck in decypharr symlink-retry loop:"]
    for n in handled:
        suffix = " — also deleted from Real-Debrid" if n in rd_deleted else ""
        body_lines.append(f"  • {n}{suffix}")
    send_notification("Symlink retry loops cleared", "\n".join(body_lines), level="info")

# ---------------------------------------------------------------------------
# Check 3: Stalled arr imports
# ---------------------------------------------------------------------------
def check_stalled_imports(state):
    """Detect when an arr has importPending items but hasn't imported recently."""
    for arr_name, info in ARRS.items():
        queue_data = arr_get_queue(arr_name, page_size=1)
        if not queue_data:
            continue

        total = queue_data.get("totalRecords", 0)
        if total == 0:
            continue

        # Count importPending, excluding items handled by other checks
        full_queue = arr_get_queue(arr_name, page_size=50)
        if not full_queue:
            continue
        pending = 0
        for r in full_queue.get("records", []):
            state_val = r.get("trackedDownloadState", "")
            if state_val not in ("importPending", "importBlocked"):
                continue
            # Skip items that check_unparseable_imports will handle
            status_val = r.get("trackedDownloadStatus", "")
            msgs = []
            for sm in r.get("statusMessages", []):
                msgs.extend(sm.get("messages", []))
                msgs.append(sm.get("title", ""))
            all_msgs = " ".join(msgs)
            if "Unable to parse file" in all_msgs or "Unable to determine if file is a sample" in all_msgs:
                continue
            if "was unexpected" in all_msgs:
                continue  # check_unparseable_imports handles these
            if state_val == "importBlocked" and status_val == "warning":
                continue  # check_unparseable_imports handles these
            pending += 1
        if pending < 5:
            continue

        # Check last import time
        last_import = state.get("last_import_times", {}).get(arr_name)
        last_import_api = arr_get_last_import(arr_name)

        if last_import_api:
            state.setdefault("last_import_times", {})[arr_name] = last_import_api
            if last_import and last_import_api == last_import:
                # No new imports since last check — might be stalled
                try:
                    last_dt = datetime.fromisoformat(last_import.replace("Z", "+00:00"))
                    age = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds() if last_dt.tzinfo else 0
                    if age > IMPORT_STALL_TIMEOUT:
                        container = arr_name.lower().replace(" ", "").replace("_", "").replace("-", "")
                        # Map arr name to container name
                        container_map = {}
                        for a in ARRS:
                            cn = a.lower().replace(" ", "")
                            container_map[a] = cn
                        cn = container_map.get(arr_name, arr_name.lower())
                        log.warning(f"{arr_name}: {pending} importPending, no imports for {int(age)}s — restarting")
                        if docker_restart(cn):
                            log_action(state, "restart_arr", f"Restarted {arr_name}: {pending} items stuck, no imports for {int(age/60)}min")
                except Exception as e:
                    log.debug(f"Could not check stall for {arr_name}: {e}")

# ---------------------------------------------------------------------------
# Check 4: Deleted files monitoring
# ---------------------------------------------------------------------------
def check_deleted_files(state):
    """Monitor download directories for files that disappear unexpectedly."""
    known = state.get("known_files", {})
    current_files = set()
    # Map of download folder names to real movie/show titles from arr queues
    title_map = state.get("download_title_map", {})

    for dir_path in DOWNLOAD_DIRS:
        if not os.path.exists(dir_path):
            continue
        try:
            for entry in os.scandir(dir_path):
                if entry.is_dir():
                    current_files.add(entry.path)
                    if entry.path not in known:
                        known[entry.path] = datetime.now().isoformat()
                    # Look up real title from arr queues if we don't have it yet
                    basename = os.path.basename(entry.path)
                    if basename not in title_map:
                        for arr_name, info in ARRS.items():
                            try:
                                queue = arr_get(arr_name, "queue?page=1&pageSize=200")
                                if not queue:
                                    continue
                                for rec in queue.get("records", []):
                                    q_title = rec.get("title", "")
                                    if basename[:30].lower().replace(".", " ").replace("-", " ") in q_title.lower().replace(".", " ").replace("-", " ") or q_title[:30].lower().replace(".", " ").replace("-", " ") in basename.lower().replace(".", " ").replace("-", " "):
                                        # Found it — now get the real movie/show title
                                        real_title = None
                                        if info["type"] == "radarr" and rec.get("movieId"):
                                            movie = arr_get(arr_name, f"movie/{rec['movieId']}")
                                            if movie:
                                                real_title = f"{movie.get('title', '')} ({movie.get('year', '')})"
                                        elif info["type"] == "sonarr" and rec.get("seriesId"):
                                            series = arr_get(arr_name, f"series/{rec['seriesId']}")
                                            if series:
                                                real_title = series.get("title", "")
                                        if real_title:
                                            title_map[basename] = real_title
                                        break
                            except Exception:
                                continue
        except Exception:
            continue

    state["download_title_map"] = title_map

    # Find deletions
    disappeared = []
    for path, first_seen in list(known.items()):
        if path not in current_files and os.path.dirname(path) in [d.rstrip("/") for d in DOWNLOAD_DIRS]:
            basename = os.path.basename(path)
            disappeared.append({
                "path": path,
                "name": basename,
                "real_title": title_map.pop(basename, None),
                "deleted_at": datetime.now().isoformat(),
                "first_seen": first_seen,
                "category": os.path.basename(os.path.dirname(path)),
            })
            del known[path]

    # Filter out normal post-import cleanups and upgrades by checking arr history
    # and whether the media already has a file (debrid cache expiry after successful import)
    suspicious = []
    upgrades = []
    already_imported = []
    for d in disappeared:
        was_imported = False
        was_upgrade = False
        has_file = False
        for arr_name, info in ARRS.items():
            try:
                # Check history for import/upgrade events
                history = arr_get(arr_name, "history?page=1&pageSize=200&sortKey=date&sortDirection=descending")
                if history:
                    for rec in history.get("records", []):
                        source = rec.get("sourceTitle", "")
                        name_norm = d["name"][:30].lower().replace(".", " ").replace("-", " ")
                        source_norm = source.lower().replace(".", " ").replace("-", " ")
                        if name_norm in source_norm:
                            event_type = rec.get("eventType", "")
                            reason = rec.get("data", {}).get("reason", "")
                            if event_type in ("episodeFileDeleted", "movieFileDeleted") and reason == "Upgrade":
                                was_upgrade = True
                            else:
                                was_imported = True
                            break
                if was_imported or was_upgrade:
                    break

                # If not found in history, check if the media already has a file
                # (debrid cached downloads often expire after a successful import)
                if not was_imported and not was_upgrade:
                    # Normalize: lowercase, replace dots/dashes/underscores with spaces, collapse whitespace
                    name_norm = re.sub(r'\s+', ' ', d["name"].lower().replace(".", " ").replace("-", " ").replace("_", " ")).strip()
                    if info["type"] == "radarr":
                        movies = arr_get(arr_name, "movie")
                        if movies:
                            for m in movies:
                                title_norm = m.get("title", "").lower().strip()
                                if title_norm and (title_norm in name_norm or name_norm.startswith(title_norm)):
                                    if m.get("hasFile"):
                                        has_file = True
                                        break
                    elif info["type"] == "sonarr":
                        series_list = arr_get(arr_name, "series")
                        if series_list:
                            for s in series_list:
                                title_norm = s.get("title", "").lower().strip()
                                if title_norm and (title_norm in name_norm or name_norm.startswith(title_norm)):
                                    stats = s.get("statistics", {})
                                    if stats.get("episodeFileCount", 0) > 0:
                                        has_file = True
                                        break
                if has_file:
                    was_imported = True
                    break
            except Exception:
                continue

        if was_upgrade:
            upgrades.append(d)
        elif has_file:
            already_imported.append(d)
        elif not was_imported:
            suspicious.append(d)

    if already_imported:
        log.info(f"{len(already_imported)} download(s) expired after successful import (normal debrid cache expiry):")
        for d in already_imported:
            label = f"{d['real_title']} — " if d.get("real_title") else ""
            log.info(f"  CACHE EXPIRED: {label}{d['name']}")

    if upgrades:
        log.info(f"{len(upgrades)} file(s) replaced by upgrades (normal):")
        for d in upgrades:
            label = f"{d['real_title']} — " if d.get("real_title") else ""
            log.info(f"  UPGRADED: {label}{d['name']}")
            log_action(state, "upgrade_cleanup", f"Old file replaced by upgrade: {label}{d['name']}")

    if suspicious:
        log.warning(f"Detected {len(suspicious)} suspicious deletion(s) (not found in import history):")
        names = []
        for d in suspicious:
            label = f"{d['real_title']} — " if d.get("real_title") else ""
            display = f"{label}{d['name']}"
            log.warning(f"  MISSING: {display}")
            state.setdefault("deleted_files", []).append(d)
            log_action(state, "suspicious_deletion", f"File deleted without import: {display}")
            names.append(display)
        if len(suspicious) >= 3:
            send_notification(
                f"{len(suspicious)} files deleted without being imported",
                f"These files disappeared from downloads but were NOT found in import history:\n\n"
                + "\n".join(f"  - {n}" for n in names[:20])
                + (f"\n  ... and {len(names)-20} more" if len(names) > 20 else "")
                + "\n\nThese may need to be re-downloaded."
            )
    elif disappeared:
        log.info(f"{len(disappeared)} files cleaned up after import (normal)")

    state["known_files"] = known

# ---------------------------------------------------------------------------
# Check 4b: Library file monitoring
# ---------------------------------------------------------------------------
LIBRARY_GRACE_CYCLES = 2  # ignore items missing for fewer cycles (handles transient API blips)

def check_library_files(state):
    """Detect media that had files but now doesn't (deleted from the library)."""
    prev = state.get("library_snapshot", {})   # {arr: {id_str: {title, had_file}}}
    current = {}
    vanished = []

    for arr_name, info in ARRS.items():
        current[arr_name] = {}
        prev_arr = prev.get(arr_name, {})

        if info["type"] == "radarr":
            movies = arr_get(arr_name, "movie")
            if movies is None:
                # API failed — keep previous snapshot so we don't false-positive
                current[arr_name] = prev_arr
                continue
            for m in movies:
                mid = str(m["id"])
                has_file = m.get("hasFile", False)
                title = m.get("title", "?")
                current[arr_name][mid] = {"title": title, "has_file": has_file}

                if mid in prev_arr and prev_arr[mid].get("has_file") and not has_file:
                    # Was there, now gone — check grace counter
                    grace_key = f"{arr_name}:{mid}"
                    grace = state.get("library_grace", {}).get(grace_key, 0) + 1
                    state.setdefault("library_grace", {})[grace_key] = grace
                    if grace >= LIBRARY_GRACE_CYCLES:
                        vanished.append({"arr": arr_name, "title": title, "id": mid})
                        state.get("library_grace", {}).pop(grace_key, None)
                    continue
                # Clear grace counter if file is back or first seen
                state.get("library_grace", {}).pop(f"{arr_name}:{mid}", None)

        else:  # sonarr
            series_list = arr_get(arr_name, "series")
            if series_list is None:
                current[arr_name] = prev_arr
                continue
            for s in series_list:
                sid = str(s["id"])
                stats = s.get("statistics", {})
                file_count = stats.get("episodeFileCount", 0)
                title = s.get("title", "?")
                current[arr_name][sid] = {"title": title, "file_count": file_count}

                if sid in prev_arr and prev_arr[sid].get("file_count", 0) > 0:
                    prev_count = prev_arr[sid]["file_count"]
                    if file_count == 0 and prev_count >= 3:
                        # Entire series wiped — grace check
                        grace_key = f"{arr_name}:{sid}"
                        grace = state.get("library_grace", {}).get(grace_key, 0) + 1
                        state.setdefault("library_grace", {})[grace_key] = grace
                        if grace >= LIBRARY_GRACE_CYCLES:
                            vanished.append({"arr": arr_name, "title": title, "id": sid,
                                             "detail": f"had {prev_count} episodes, now 0"})
                            state.get("library_grace", {}).pop(grace_key, None)
                        continue
                    elif file_count < prev_count and (prev_count - file_count) >= 3:
                        # Large drop in episode files
                        grace_key = f"{arr_name}:{sid}"
                        grace = state.get("library_grace", {}).get(grace_key, 0) + 1
                        state.setdefault("library_grace", {})[grace_key] = grace
                        if grace >= LIBRARY_GRACE_CYCLES:
                            vanished.append({"arr": arr_name, "title": title, "id": sid,
                                             "detail": f"dropped from {prev_count} to {file_count} episodes"})
                            state.get("library_grace", {}).pop(grace_key, None)
                        continue
                # Clear grace counter
                state.get("library_grace", {}).pop(f"{arr_name}:{sid}", None)

    state["library_snapshot"] = current

    if vanished:
        log.warning(f"LIBRARY ALERT: {len(vanished)} item(s) lost files — triggering re-search:")
        names = []
        searched = 0
        for v in vanished:
            detail = v.get("detail", "file removed")
            log.warning(f"  [{v['arr']}] {v['title']} — {detail}")
            state.setdefault("library_deletions", []).append({
                "arr": v["arr"], "title": v["title"], "id": v["id"],
                "detail": detail, "detected_at": datetime.now().isoformat(),
            })

            # Trigger a search for this specific item to re-download it
            arr_name = v["arr"]
            info = ARRS[arr_name]
            try:
                if info["type"] == "radarr":
                    arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": [int(v["id"])]})
                else:
                    arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": int(v["id"])})
                searched += 1
                log.info(f"  RE-SEARCHING: [{v['arr']}] {v['title']}")
                log_action(state, "library_re_search", f"[{v['arr']}] {v['title']} — lost file, triggered re-search")
            except Exception as e:
                log.error(f"  Failed to trigger re-search for {v['title']}: {e}")

            names.append(f"[{v['arr']}] {v['title']} — {detail}")

        send_notification(
            f"{len(vanished)} library item(s) lost files — {searched} re-searched",
            "The following media had files but they have disappeared from the library.\n"
            "Babysitarr has triggered a re-search for each one:\n\n"
            + "\n".join(f"  - {n}" for n in names[:20])
            + (f"\n  ... and {len(names)-20} more" if len(names) > 20 else "")
        )

# ---------------------------------------------------------------------------
# Check 5: Real-Debrid torrent health
# ---------------------------------------------------------------------------
def check_rd_health(state):
    """Check if RD has problematic torrents and auto-retry them.

    Deletes the bad torrent from RD, removes the matching queue item from
    the arr (without blocklist), and triggers a re-search so a fresh copy
    is grabbed automatically.
    """
    if not RD_API_KEY:
        return

    try:
        headers = {"Authorization": f"Bearer {RD_API_KEY}"}
        r = requests.get("https://api.real-debrid.com/rest/1.0/torrents?limit=100", headers=headers, timeout=15)
        if r.status_code == 401:
            log.error("RD API key is invalid")
            return
        r.raise_for_status()
        rd_torrents = r.json()
    except Exception as e:
        log.warning(f"Could not check RD health: {e}")
        return

    # Check for torrents with bad status
    problem_statuses = {"magnet_error", "error", "virus", "dead"}
    problems = []
    for t in rd_torrents:
        status = t.get("status", "")
        if status in problem_statuses:
            problems.append({
                "rd_id": t.get("id", ""),
                "hash": (t.get("hash") or "").lower(),
                "name": t.get("filename", "unknown"),
                "status": status,
                "detected_at": datetime.now().isoformat(),
            })

    if not problems:
        return

    log.warning(f"Found {len(problems)} problematic torrents on RD — auto-retrying")

    # Delete bad torrents from RD
    for p in problems:
        if p["rd_id"]:
            try:
                requests.delete(
                    f"https://api.real-debrid.com/rest/1.0/torrents/delete/{p['rd_id']}",
                    headers=headers, timeout=15,
                )
                log.info(f"Deleted bad RD torrent: {p['name']} ({p['status']})")
            except Exception as e:
                log.warning(f"Failed to delete RD torrent {p['rd_id']}: {e}")

    # Build set of problem hashes for matching against arr queues
    problem_hashes = {p["hash"] for p in problems if p["hash"]}
    matched_hashes = set()

    # Remove matching queue items from arrs and trigger re-search
    retried = []
    for arr_name, info in ARRS.items():
        queue_data = arr_get_queue(arr_name)
        if not queue_data:
            continue

        movies_to_search = []
        series_to_search = set()

        for record in queue_data.get("records", []):
            did = (record.get("downloadId") or "").lower()
            if did not in problem_hashes:
                continue

            matched_hashes.add(did)
            title = record.get("title", "unknown")[:80]
            if arr_remove_queue_item(arr_name, record["id"], blocklist=False):
                log.info(f"Removed bad torrent from {arr_name} queue: {title}")
                retried.append(title)
                log_action(state, "rd_auto_retry", f"Auto-retried RD {did[:12]}.. in {arr_name}: {title}")

                if info["type"] == "radarr":
                    mid = record.get("movie", {}).get("id") or record.get("movieId")
                    if mid:
                        movies_to_search.append(mid)
                else:
                    sid = record.get("series", {}).get("id") or record.get("seriesId")
                    if sid:
                        series_to_search.add(sid)

        # Trigger re-searches
        if info["type"] == "radarr" and movies_to_search:
            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": movies_to_search})
            log.info(f"Triggered re-search for {len(movies_to_search)} movies in {arr_name}")
        for sid in series_to_search:
            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": sid})
            log.info(f"Triggered re-search for series {sid} in {arr_name}")

    # Orphans: magnet_error torrents with no matching arr queue entry — symptom of
    # decypharr's dirty-write bug (submits to RD even when it returns 400 to the arr,
    # so the arr has no history/blocklist entry and re-grabs the same bad release
    # on the next RSS cycle). Track strikes per hash; after RD_ORPHAN_STRIKES,
    # look up the release name in decypharr logs and add it as an ignored term
    # in every arr's Release Profile. This breaks the loop.
    orphan_problems = [p for p in problems if p["hash"] and p["hash"] not in matched_hashes]
    orphan_seen = state.setdefault("rd_orphan_seen", {})
    auto_blocked = []
    for p in orphan_problems:
        h = p["hash"]
        orphan_seen[h] = orphan_seen.get(h, 0) + 1
        if orphan_seen[h] < RD_ORPHAN_STRIKES:
            continue
        lookup = decypharr_lookup_hash(h)
        if not lookup:
            continue
        category, release_name = lookup
        ok_arrs = [a for a in ARRS if arr_add_release_profile_ignore(a, release_name)]
        if ok_arrs:
            auto_blocked.append((release_name, category, ok_arrs))
            log_action(
                state, "rd_orphan_blocked",
                f"Auto-blocked '{release_name}' (category={category}, hash={h[:12]}..) in {','.join(ok_arrs)} after {orphan_seen[h]} strikes"
            )
            orphan_seen.pop(h, None)
    # Prune strike counter to avoid unbounded growth
    if len(orphan_seen) > 500:
        state["rd_orphan_seen"] = dict(list(orphan_seen.items())[-500:])

    # Track and alert
    for p in problems:
        state.setdefault("rd_removed", []).append(p)
        log_action(state, "rd_problem", f"RD torrent {p['status']}: {p['name']}")

    # Only notify when there's something actionable. Orphans without a match
    # (the "Invalid Magnet / may need to be re-downloaded manually" case) are
    # silently tracked via strike counter — when they hit RD_ORPHAN_STRIKES,
    # auto_blocked fires with the real release name from decypharr logs.
    if retried or auto_blocked:
        lines = []
        if retried:
            lines.append(f"Auto-retried in arr queues ({len(retried)}):")
            lines.extend(f"  - {t}" for t in retried[:20])
        if auto_blocked:
            if lines:
                lines.append("")
            lines.append(f"Auto-blocked via Release Profile ({len(auto_blocked)}):")
            for name, cat, arrs in auto_blocked[:20]:
                lines.append(f"  - '{name}' [{cat}] → {','.join(arrs)}")
            lines.append("")
            lines.append(
                "These releases were being re-grabbed every RSS cycle due to a "
                "decypharr bug (submits to RD even when rejecting the arr's request). "
                "Added as ignored terms so the loop stops."
            )
        send_notification(
            f"RD health: {len(retried)} retried, {len(auto_blocked)} auto-blocked",
            "\n".join(lines),
            level="info" if not retried else "alert",
        )

# ---------------------------------------------------------------------------
# Check 6: Ensure blocklisted items get re-searched
# ---------------------------------------------------------------------------
def check_missing_searched(state):
    """After blocklisting, trigger a re-search only for recently blocklisted items.

    Only runs if babysitarr itself blocklisted something recently (tracked
    via state). Avoids triggering massive full-library searches that burn
    through Prowlarr's grab limit.
    """
    pending = state.pop("pending_research", [])
    if not pending:
        return

    for entry in pending[:10]:  # cap to avoid rate limit floods
        arr_name = entry.get("arr")
        media_type = entry.get("type")
        media_id = entry.get("media_id")
        if not arr_name or not media_id:
            continue
        if media_type == "radarr":
            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": [media_id]})
        else:
            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": media_id})
        log.info(f"{arr_name}: Re-searching after blocklist for media {media_id}")

# ---------------------------------------------------------------------------
# Check 7: Auto-import items stuck with "Unable to parse file"
# ---------------------------------------------------------------------------
# Quality patterns: map filename indicators to radarr/sonarr quality IDs
_QUALITY_PATTERNS = [
    # Remux
    (r'(?i)\b(bd\s?remux|blu\s?ray\s?remux|remux)\b.*2160', {"id": 31, "name": "Remux-2160p", "source": "bluray", "resolution": 2160}),
    (r'(?i)\b(bd\s?remux|blu\s?ray\s?remux|remux)\b', {"id": 30, "name": "Remux-1080p", "source": "bluray", "resolution": 1080}),
    # Bluray (non-remux)
    (r'(?i)\bblu\s?ray\b.*2160', {"id": 19, "name": "Bluray-2160p", "source": "bluray", "resolution": 2160}),
    (r'(?i)\bblu\s?ray\b.*1080', {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080}),
    (r'(?i)\bblu\s?ray\b.*720', {"id": 6, "name": "Bluray-720p", "source": "bluray", "resolution": 720}),
    # WEB-DL
    (r'(?i)\bweb[\.\-\s]?dl\b.*2160', {"id": 18, "name": "WEBDL-2160p", "source": "webdl", "resolution": 2160}),
    (r'(?i)\bweb[\.\-\s]?dl\b.*1080', {"id": 3, "name": "WEBDL-1080p", "source": "webdl", "resolution": 1080}),
    (r'(?i)\bweb[\.\-\s]?dl\b.*720', {"id": 5, "name": "WEBDL-720p", "source": "webdl", "resolution": 720}),
    (r'(?i)\bweb[\.\-\s]?dl\b', {"id": 3, "name": "WEBDL-1080p", "source": "webdl", "resolution": 1080}),
    # WEBRip
    (r'(?i)\bweb\s?rip\b.*2160', {"id": 17, "name": "WEBRip-2160p", "source": "webrip", "resolution": 2160}),
    (r'(?i)\bweb\s?rip\b.*1080', {"id": 15, "name": "WEBRip-1080p", "source": "webrip", "resolution": 1080}),
    (r'(?i)\bweb\s?rip\b.*720', {"id": 14, "name": "WEBRip-720p", "source": "webrip", "resolution": 720}),
    # HDTV
    (r'(?i)\bhdtv\b.*2160', {"id": 16, "name": "HDTV-2160p", "source": "tv", "resolution": 2160}),
    (r'(?i)\bhdtv\b.*1080', {"id": 9, "name": "HDTV-1080p", "source": "tv", "resolution": 1080}),
    (r'(?i)\bhdtv\b.*720', {"id": 4, "name": "HDTV-720p", "source": "tv", "resolution": 720}),
    # Resolution-only fallbacks (assume bluray for high quality files)
    (r'(?i)\b2160p\b', {"id": 19, "name": "Bluray-2160p", "source": "bluray", "resolution": 2160}),
    (r'(?i)\b4k\b', {"id": 19, "name": "Bluray-2160p", "source": "bluray", "resolution": 2160}),
    (r'(?i)\b1080p\b', {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080}),
    (r'(?i)\b720p\b', {"id": 6, "name": "Bluray-720p", "source": "bluray", "resolution": 720}),
    (r'(?i)\b480p\b', {"id": 20, "name": "Bluray-480p", "source": "bluray", "resolution": 480}),
]

# Download dir name → fallback quality
_DIR_QUALITY_MAP = {
    "movies-4k": {"id": 19, "name": "Bluray-2160p", "source": "bluray", "resolution": 2160},
    "shows-4k": {"id": 19, "name": "Bluray-2160p", "source": "bluray", "resolution": 2160},
    "movies-1080p": {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080},
    "shows-1080p": {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080},
}

def _infer_quality(filename, download_dir=""):
    """Infer quality from filename patterns, falling back to download dir name."""
    for pattern, quality in _QUALITY_PATTERNS:
        if re.search(pattern, filename):
            return quality
    # Fallback: use download directory name
    for dir_key, quality in _DIR_QUALITY_MAP.items():
        if dir_key in download_dir:
            return quality
    # Last resort
    return {"id": 7, "name": "Bluray-1080p", "source": "bluray", "resolution": 1080}


def check_unparseable_imports(state):
    """Auto-import items that radarr/sonarr can't parse or that are blocked.

    Handles:
    - importPending with "Unable to parse file" or "Unable to determine if file is a sample"
    - importBlocked with "was unexpected" (season pack episode mapping failures)
    - Any importBlocked with status=warning (files are there but arr won't auto-import)
    """
    for arr_name, info in ARRS.items():
        queue_data = arr_get_queue(arr_name, page_size=100)
        if not queue_data:
            continue

        unparseable = []
        for r in queue_data.get("records", []):
            state_val = r.get("trackedDownloadState", "")
            status_val = r.get("trackedDownloadStatus", "")
            msgs = []
            for sm in r.get("statusMessages", []):
                msgs.extend(sm.get("messages", []))
            title_msgs = [sm.get("title", "") for sm in r.get("statusMessages", [])]
            all_msgs = " ".join(msgs + title_msgs)

            should_handle = False
            if state_val == "importPending":
                if "Unable to parse file" in all_msgs or "Unable to determine if file is a sample" in all_msgs:
                    should_handle = True
                elif "was unexpected" in all_msgs:
                    should_handle = True  # Episode mapping issue (e.g. after restart)
            elif state_val == "importBlocked" and status_val == "warning":
                # Files are there but arr can't auto-import (unexpected episodes, etc.)
                should_handle = True

            if should_handle:
                unparseable.append(r)

        if not unparseable:
            continue

        log.info(f"{arr_name}: {len(unparseable)} unparseable import-pending items, attempting auto-import")

        for item in unparseable:
            download_id = item.get("downloadId", "")
            title = item.get("title", "")

            # Get manual import candidates
            mi_data = arr_get(arr_name, f"manualimport?downloadId={download_id}")
            if not mi_data:
                log.warning(f"  {arr_name}: No manual import data for {title}")
                continue

            files_to_import = []
            for mi in mi_data:
                path = mi.get("path", "")
                # Skip tiny files (< 50MB) — likely samples
                size = mi.get("size", 0)
                if size < 50 * 1024 * 1024:
                    log.info(f"  Skipping small file ({size // (1024*1024)}MB): {os.path.basename(path)}")
                    continue

                # Get movie/series ID
                if info["type"] == "radarr":
                    media_id = mi.get("movie", {}).get("id")
                    id_key = "movieId"
                else:
                    media_id = mi.get("series", {}).get("id")
                    id_key = "seriesId"

                if not media_id:
                    log.warning(f"  {arr_name}: No media match for {os.path.basename(path)}")
                    continue

                # Use quality from the ManualImport API if available (it parses correctly),
                # only fall back to inference if the API didn't provide one
                api_quality = mi.get("quality")
                if api_quality and api_quality.get("quality", {}).get("id"):
                    quality_obj = api_quality
                    quality_name = api_quality.get("quality", {}).get("name", "?")
                else:
                    quality = _infer_quality(path, path)
                    quality_obj = {
                        "quality": quality,
                        "revision": {"version": 1, "real": 0, "isRepack": False}
                    }
                    quality_name = quality["name"]

                # Use languages from the ManualImport API if available, otherwise infer
                api_languages = mi.get("languages")
                if api_languages and len(api_languages) > 0 and api_languages[0].get("id"):
                    languages = api_languages
                else:
                    orig_lang = mi.get("movie", mi.get("series", {})).get("originalLanguage", {})
                    if orig_lang and orig_lang.get("id"):
                        languages = [orig_lang]
                    else:
                        languages = [{"id": 1, "name": "English"}]

                import_file = {
                    "path": path,
                    id_key: media_id,
                    "quality": quality_obj,
                    "languages": languages,
                }

                # Sonarr also needs episodeIds
                if info["type"] == "sonarr":
                    episodes = mi.get("episodes") or []
                    if not episodes:
                        # Try to extract episode number from filename (e.g. "01. Долгий день.mkv")
                        basename = os.path.basename(path)
                        ep_match = re.match(r'^(\d{1,3})\b', basename)
                        if ep_match and media_id:
                            ep_num = int(ep_match.group(1))
                            # Determine season from the queue item or manual import
                            season_num = mi.get("seasonNumber") or item.get("seasonNumber")
                            if not season_num:
                                # Try to get season from the folder name (e.g. "Season 1", "1-й сезон")
                                season_match = re.search(r'(?:season\s*(\d+)|(\d+)[\s\-]*(?:й\s+)?сезон)', path, re.IGNORECASE)
                                if season_match:
                                    season_num = int(season_match.group(1) or season_match.group(2))
                                else:
                                    season_num = 1  # fallback
                            # Look up episode from Sonarr API
                            all_eps = arr_get(arr_name, f"episode?seriesId={media_id}&seasonNumber={season_num}")
                            if all_eps:
                                matched = [e for e in all_eps if e.get("episodeNumber") == ep_num]
                                if matched:
                                    episodes = matched
                                    log.info(f"  Matched {basename} -> S{season_num:02d}E{ep_num:02d} by filename number")
                    if episodes:
                        import_file["episodeIds"] = [ep["id"] for ep in episodes]
                    else:
                        log.warning(f"  {arr_name}: No episode match for {os.path.basename(path)}")
                        continue

                files_to_import.append(import_file)
                log.info(f"  Importing: {os.path.basename(path)} as {quality_name}, lang={languages[0].get('name','?')}")

            if files_to_import:
                result = arr_post(arr_name, "command", {
                    "name": "ManualImport",
                    "files": files_to_import,
                    "importMode": "auto"
                })
                if result:
                    log_action(state, "auto_import", f"Auto-imported {len(files_to_import)} file(s) in {arr_name}: {', '.join(os.path.basename(f['path']) for f in files_to_import)}")
                else:
                    log.error(f"  {arr_name}: ManualImport command failed")


# ---------------------------------------------------------------------------
# Check 8: Auto-fix stale and dead queue entries
# ---------------------------------------------------------------------------
def check_stale_queue(state):
    """Remove queue entries for already-imported media, re-search truly missing items.

    Tracks retry counts for dead entries (download gone, media missing).
    After DEAD_RETRY_LIMIT attempts, blocklists the release so the arr picks a
    different one on the next search.
    """
    dead_retries = state.setdefault("dead_retries", {})  # "arr:mediaId" -> count

    for arr_name, info in ARRS.items():
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if not queue_data:
            continue

        stale_removed = 0
        dead_removed = 0
        dead_blocklisted = 0
        movies_to_search = []
        series_to_search = set()

        for r in queue_data.get("records", []):
            if r.get("trackedDownloadState") != "importPending":
                continue

            # Determine if the media already has a file
            if info["type"] == "radarr":
                has_file = r.get("movie", {}).get("hasFile", False)
                media_id = r.get("movie", {}).get("id")
            else:
                has_file = r.get("episode", {}).get("hasFile", False)
                media_id = r.get("series", {}).get("id") or r.get("seriesId")

            if has_file:
                # Already imported — stale entry, safe to remove
                if arr_remove_queue_item(arr_name, r["id"], blocklist=False):
                    stale_removed += 1
                # Clean up any retry tracking for this media
                retry_key = f"{arr_name}:{media_id}"
                dead_retries.pop(retry_key, None)
                stuck_retries = state.get("stuck_retries", {})
                if info["type"] == "radarr":
                    stuck_retries.pop(f"{arr_name}:movie:{media_id}", None)
                else:
                    ep_id = r.get("episodeId") or r.get("episode", {}).get("id")
                    if ep_id:
                        stuck_retries.pop(f"{arr_name}:ep:{ep_id}", None)
            else:
                # Check if download dir is gone (dead entry)
                msgs = []
                for sm in r.get("statusMessages", []):
                    msgs.extend(sm.get("messages", []))
                no_files = any("No files found are eligible" in m for m in msgs)
                if no_files:
                    retry_key = f"{arr_name}:{media_id}"
                    count = dead_retries.get(retry_key, 0) + 1
                    dead_retries[retry_key] = count

                    if count >= DEAD_RETRY_LIMIT:
                        # Too many retries — blocklist this release so a different one is picked
                        if arr_remove_queue_item(arr_name, r["id"], blocklist=True):
                            dead_blocklisted += 1
                            dead_retries.pop(retry_key, None)
                            # Still trigger a search so it finds a different release
                            if info["type"] == "radarr" and media_id:
                                movies_to_search.append(media_id)
                            elif info["type"] == "sonarr" and media_id:
                                series_to_search.add(media_id)
                    else:
                        # Remove without blocklist and re-search (give it another shot)
                        if arr_remove_queue_item(arr_name, r["id"], blocklist=False):
                            dead_removed += 1
                            if info["type"] == "radarr" and media_id:
                                movies_to_search.append(media_id)
                            elif info["type"] == "sonarr" and media_id:
                                series_to_search.add(media_id)

        # Trigger re-searches for dead entries
        if info["type"] == "radarr" and movies_to_search:
            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": movies_to_search})
        for sid in series_to_search:
            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": sid})

        if stale_removed > 0:
            log_action(state, "auto_clear_stale", f"Removed {stale_removed} stale queue entries from {arr_name} (already imported)")
        if dead_removed > 0:
            searched = len(movies_to_search) + len(series_to_search)
            log_action(state, "auto_clear_dead", f"Removed {dead_removed} dead queue entries from {arr_name}, re-searching (attempt {count}/{DEAD_RETRY_LIMIT})")
        if dead_blocklisted > 0:
            log_action(state, "auto_blocklist_dead", f"Blocklisted {dead_blocklisted} releases in {arr_name} after {DEAD_RETRY_LIMIT} failed attempts, searching for alternatives")

    # Clean up old retry entries (media that hasn't been seen in queue for a while)
    # Keep only entries seen in this cycle
    state["dead_retries"] = dead_retries


# ---------------------------------------------------------------------------
# Check 9: Auto-fix stuck queue items (delay/downloading with no progress)
# ---------------------------------------------------------------------------

def check_stuck_queue_items(state):
    """Detect queue items stuck in delay or downloading state with no progress.

    Items in 'delay' status with torrentDelay=0, or 'downloading' with no size
    change for STUCK_QUEUE_TIMEOUT seconds, get removed and re-searched.

    Uses STUCK_DONE_TIMEOUT (shorter) when sizeleft=0 — the download finished on
    RD's side but Decypharr failed to create symlinks, so there's no point waiting
    the full 30 min.

    Tracks retries per media (movie/episode) to prevent infinite blocklist loops.
    After STUCK_RETRY_LIMIT failures for the same media, stops re-searching.
    Retry counters reset after 24h to allow another attempt.
    """
    stuck_tracking = state.setdefault("stuck_queue", {})  # "arr:queueId" -> {"first_seen": ts, "last_sizeleft": n}
    stuck_retries = state.setdefault("stuck_retries", {})  # "arr:type:mediaId" -> {"count": n, "last_ts": ts}

    seen_keys = set()

    for arr_name, info in ARRS.items():
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if not queue_data:
            continue

        for r in queue_data.get("records", []):
            status = r.get("status", "")
            dl_state = r.get("trackedDownloadState") or ""
            qid = r["id"]
            title = r.get("title", "?")
            sizeleft = r.get("sizeleft", 0)
            size = r.get("size", 0)
            key = f"{arr_name}:{qid}"
            seen_keys.add(key)

            is_delay = status == "delay"
            is_downloading = dl_state == "downloading"
            is_importing = status == "completed" and dl_state == "importing"

            if not is_delay and not is_downloading and not is_importing:
                stuck_tracking.pop(key, None)
                continue

            now_ts = time.time()

            added_ts = now_ts
            if r.get("added"):
                try:
                    added_str = r["added"].replace("Z", "+00:00")
                    added_ts = datetime.fromisoformat(added_str).timestamp()
                except Exception:
                    pass

            # Shorter timeout when download is done (sizeleft=0) but arr can't import
            download_done = is_downloading and sizeleft == 0
            timeout = STUCK_DONE_TIMEOUT if download_done else STUCK_QUEUE_TIMEOUT

            if key not in stuck_tracking:
                stuck_tracking[key] = {"first_seen": min(now_ts, added_ts), "last_sizeleft": sizeleft}
                if (now_ts - min(now_ts, added_ts)) < timeout:
                    continue

            entry = stuck_tracking[key]
            age = now_ts - entry["first_seen"]

            if is_downloading and sizeleft < entry["last_sizeleft"]:
                entry["first_seen"] = now_ts
                entry["last_sizeleft"] = sizeleft
                continue

            if age < timeout:
                continue

            # Resolve media key for per-media retry tracking
            if info["type"] == "radarr":
                media_id = r.get("movieId") or r.get("movie", {}).get("id")
                media_key = f"{arr_name}:movie:{media_id}" if media_id else None
            else:
                media_id = r.get("episodeId") or r.get("episode", {}).get("id")
                media_key = f"{arr_name}:ep:{media_id}" if media_id else None

            retry_info = stuck_retries.get(media_key, {"count": 0, "last_ts": 0}) if media_key else None
            retry_count = (retry_info["count"] + 1) if retry_info else 1
            exhausted = media_key and retry_count >= STUCK_RETRY_LIMIT

            reason = "done-no-import" if download_done else "no-progress"
            log.warning("Stuck queue item in %s: '%s' (status=%s, state=%s, age=%.0fmin, reason=%s, attempt %d/%d).",
                        arr_name, title, status, dl_state or "none", age / 60, reason,
                        retry_count, STUCK_RETRY_LIMIT)

            if arr_remove_queue_item(arr_name, qid, blocklist=True):
                stuck_tracking.pop(key, None)

                if media_key:
                    stuck_retries[media_key] = {"count": retry_count, "last_ts": now_ts}

                if exhausted:
                    log.warning("Media '%s' in %s exhausted %d retries — stopping re-search.", title, arr_name, retry_count)
                    log_action(state, "stuck_exhausted",
                               f"Gave up on {title} in {arr_name} after {retry_count} failed downloads — all grabbed releases fail to import. Manual intervention needed.")
                else:
                    if info["type"] == "radarr":
                        mid = r.get("movieId") or r.get("movie", {}).get("id")
                        if mid:
                            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": [mid]})
                    else:
                        mid = r.get("seriesId") or r.get("series", {}).get("id")
                        if mid:
                            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": mid})

                    log_action(state, "auto_clear_stuck",
                               f"Removed stuck queue item from {arr_name}: {title} (stuck {age/60:.0f}min, attempt {retry_count}/{STUCK_RETRY_LIMIT})")

    # Clean up tracking for items no longer in queue
    for k in list(stuck_tracking):
        if k not in seen_keys:
            del stuck_tracking[k]

    # Reset retry counters after 24h cooldown — allows another round of attempts
    now = time.time()
    for mk in list(stuck_retries):
        if now - stuck_retries[mk]["last_ts"] > 86400:
            del stuck_retries[mk]

    state["stuck_queue"] = stuck_tracking
    state["stuck_retries"] = stuck_retries


# ---------------------------------------------------------------------------
# Check 10: Force-grab delayed queue items
# ---------------------------------------------------------------------------
def check_delayed_queue(state):
    """Force-grab any queue items sitting in 'delay' status."""
    for arr_name, info in ARRS.items():
        queue_data = arr_get_queue(arr_name)
        if not queue_data:
            continue

        forced = 0
        for r in queue_data.get("records", []):
            if r.get("status") != "delay":
                continue
            item_id = r.get("id")
            title = r.get("title", "unknown")[:60]
            result = arr_post(arr_name, f"queue/grab/{item_id}")
            if result is not None:
                forced += 1
                log.info(f"{arr_name}: Force-grabbed delayed item: {title}")

        if forced:
            log_action(state, "force_grab_delayed", f"Force-grabbed {forced} delayed items in {arr_name}")


# ---------------------------------------------------------------------------
# Check 11: Auto-fix indexer and list failures
# ---------------------------------------------------------------------------
INDEXER_RESET_COOLDOWN = 1800  # 30 min cooldown between resets
CONTAINER_RESTART_COOLDOWN = 300  # 5 min cooldown between container restarts

def _reset_prowlarr_db():
    """Stop Prowlarr, clear IndexerStatus, restart. This is the source of truth for indexers.

    NOTE: Requires Prowlarr's config directory to be mounted at ARR_CONFIG_DIR/prowlarr/.
    """
    prowlarr_db_path = os.path.join(ARR_CONFIG_DIR, "prowlarr", "prowlarr.db")
    try:
        _docker_api("POST", "/containers/prowlarr/stop?t=15")
        if not _wait_container_stopped("prowlarr", timeout=25):
            log.error("Prowlarr did not stop in time, skipping DB reset")
            raise Exception("container did not stop")
        subprocess.run(["sqlite3", prowlarr_db_path,
                         "DELETE FROM IndexerStatus;"],
                       capture_output=True, timeout=10, check=True)
    except Exception as e:
        log.error(f"Prowlarr DB reset failed: {e}")
    # Always restart
    for attempt in range(3):
        try:
            _docker_api("POST", "/containers/prowlarr/start")
            time.sleep(5)
            status, resp = _docker_api("GET", "/containers/prowlarr/json")
            if '"Running":true' in resp or '"running":true' in resp:
                return True
            log.warning(f"Prowlarr not running after start attempt {attempt+1}")
        except Exception as e:
            log.error(f"Failed to start prowlarr (attempt {attempt+1}): {e}")
        time.sleep(3)
    log.error("CRITICAL: Could not restart prowlarr after 3 attempts!")
    return False


def check_container_health(state):
    """Auto-restart crashed arr containers.

    Checks all configured arr containers and restarts any that have exited.
    Uses a cooldown to avoid restart loops.
    """
    restart_times = state.setdefault("container_restart_times", {})
    now = time.time()

    # Build list of container names from ARRS config
    containers = {}
    for arr_name in ARRS:
        cn = arr_name.lower().replace(" ", "")
        containers[cn] = arr_name

    for cn, arr_name in containers.items():
        try:
            status, resp = _docker_api("GET", f"/containers/{cn}/json")
        except Exception as e:
            log.warning(f"Could not check container {cn}: {e}")
            continue

        if status != 200:
            continue

        # Parse running state from response
        if '"Running":true' in resp:
            # Container is running, clear any restart tracking
            restart_times.pop(cn, None)
            continue

        # Container is not running
        last_restart = restart_times.get(cn, 0)
        if now - last_restart < CONTAINER_RESTART_COOLDOWN:
            log.info(f"{cn}: still down but cooldown active (restarted {int((now - last_restart) / 60)}min ago)")
            continue

        log.warning(f"{cn}: container is not running — auto-restarting")
        if docker_restart(cn):
            restart_times[cn] = now
            log_action(state, "auto_restart_container", f"Auto-restarted crashed container: {cn}")
            send_notification(
                f"{cn} was down — auto-restarted",
                f"The {arr_name} container ({cn}) was found in a stopped state "
                f"and has been automatically restarted."
            )
        else:
            log.error(f"Failed to auto-restart {cn}")


def check_indexer_health(state):
    """Detect and auto-fix indexer/list failures.

    Indexer failures come from Prowlarr, so we reset Prowlarr first (once),
    then reset each affected arr to clear its cached failure state.
    30-min cooldown per container to avoid restart loops.

    The reset itself is a container restart (the only reliable way to clear
    cached failure state in Sonarr/Radarr/Prowlarr). On debrid setups where
    indexer failures are frequent and transient, the periodic restarts may
    be more disruptive than the failures themselves — set
    DISABLE_AUTO_INDEXER_RESET=true to skip the auto-reset and only handle
    indexer failures via the dashboard's manual button (issue #1).
    """
    if DISABLE_AUTO_INDEXER_RESET:
        return
    reset_times = state.setdefault("indexer_reset_times", {})
    now = time.time()

    # First pass: collect which arrs have issues
    arrs_with_indexer_issues = []
    arrs_with_list_issues = []
    tmdb_cleanup = {}  # arr_name -> set of tmdb_ids

    for arr_name, info in ARRS.items():
        health = arr_get(arr_name, "health")
        if not health:
            continue

        has_indexer_failure = False
        has_list_failure = False
        tmdb_ids = set()

        for h in health:
            msg = h.get("message", "")
            if "indexer" in msg.lower() or "Indexer" in msg:
                if "unavailable due to failures" in msg:
                    has_indexer_failure = True
            if "lists are unavailable due to failures" in msg:
                has_list_failure = True
            if "removed from TMDb" in msg:
                for m in re.findall(r'tmdbid\s+(\d+)', msg):
                    tmdb_ids.add(int(m))

        if has_indexer_failure:
            arrs_with_indexer_issues.append(arr_name)
        if has_list_failure:
            arrs_with_list_issues.append(arr_name)
        if tmdb_ids and info["type"] == "radarr":
            tmdb_cleanup[arr_name] = tmdb_ids

    # Reset Prowlarr if any arr has indexer issues (it's the source)
    if arrs_with_indexer_issues:
        last_prowlarr_reset = reset_times.get("prowlarr", 0)
        if now - last_prowlarr_reset >= INDEXER_RESET_COOLDOWN:
            log.info(f"Resetting Prowlarr (indexer failures in: {', '.join(arrs_with_indexer_issues)})")
            if _reset_prowlarr_db():
                reset_times["prowlarr"] = now
                log_action(state, "auto_reset_prowlarr", f"Reset Prowlarr indexer failures (affected: {', '.join(arrs_with_indexer_issues)})")
                time.sleep(5)
        else:
            log.info(f"Prowlarr indexer failures detected but cooldown active (reset {int((now - last_prowlarr_reset) / 60)}min ago)")

    # Reset each affected arr to clear its cached indexer/list failure state
    for arr_name in set(arrs_with_indexer_issues + arrs_with_list_issues):
        last_reset = reset_times.get(arr_name, 0)
        if now - last_reset < INDEXER_RESET_COOLDOWN:
            log.info(f"{arr_name}: failures detected but cooldown active (reset {int((now - last_reset) / 60)}min ago)")
            continue
        issues = []
        if arr_name in arrs_with_indexer_issues:
            issues.append("indexer failures")
        if arr_name in arrs_with_list_issues:
            issues.append("list failures")
        log.info(f"{arr_name}: clearing {' and '.join(issues)}")
        if _reset_arr_db(arr_name):
            reset_times[arr_name] = now
            log_action(state, "auto_reset_indexers", f"Auto-fixed {' and '.join(issues)} in {arr_name}")

    # Auto-remove TMDb-removed movies
    for arr_name, tmdb_ids in tmdb_cleanup.items():
        if tmdb_ids:
            movies = arr_get(arr_name, "movie")
            if movies:
                deleted = 0
                for m in movies:
                    if m.get("tmdbId") in tmdb_ids:
                        if arr_delete(arr_name, f"movie/{m['id']}?deleteFiles=true&addImportExclusion=true"):
                            deleted += 1
                if deleted > 0:
                    log_action(state, "auto_delete_tmdb", f"Auto-removed {deleted} TMDb-removed movies from {arr_name}")


# ---------------------------------------------------------------------------
# Web UI
# ---------------------------------------------------------------------------
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Cached scan results, updated by background thread
_cached_scan = {"data": {}, "timestamp": None, "scanning": False}
_scan_lock = threading.Lock()

def _html_escape(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _scan_arr_health():
    """Live scan of all arrs: health warnings, queue issues, indexer status."""
    results = {}
    for arr_name, info in ARRS.items():
        arr_result = {"health": [], "queue_stale": [], "queue_missing": [], "queue_blocked": [], "queue_downloading": 0, "queue_total": 0}

        # Health check
        health = arr_get(arr_name, "health")
        if health:
            for h in health:
                arr_result["health"].append({
                    "type": h.get("type", "warning"),
                    "message": h.get("message", ""),
                    "source": h.get("source", ""),
                })

        # Queue check — identify stale vs missing
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if queue_data:
            arr_result["queue_total"] = queue_data.get("totalRecords", 0)
            for r in queue_data.get("records", []):
                state = r.get("trackedDownloadState", "")
                if state == "downloading" or r.get("status") == "delay":
                    arr_result["queue_downloading"] += 1
                    continue
                if state == "importBlocked":
                    # Files are there but arr can't auto-import
                    status_val = r.get("trackedDownloadStatus", "")
                    if status_val == "warning":
                        block_msgs = []
                        for sm in r.get("statusMessages", []):
                            block_msgs.extend(sm.get("messages", []))
                        block_title = r.get("title", "?")
                        arr_result["queue_blocked"].append({
                            "qid": r["id"],
                            "title": block_title,
                            "message": block_msgs[0][:100] if block_msgs else "Import blocked",
                        })
                    continue
                if state != "importPending":
                    continue

                if info["type"] == "radarr":
                    movie = r.get("movie", {})
                    has_file = movie.get("hasFile", False)
                    title = movie.get("title", r.get("title", "?"))
                    media_id = movie.get("id")
                else:
                    ep = r.get("episode", {})
                    has_file = ep.get("hasFile", False)
                    series = r.get("series", {})
                    series_title = series.get("title", "")
                    ep_label = f"S{ep.get('seasonNumber', 0):02d}E{ep.get('episodeNumber', 0):02d}"
                    title = f"{series_title} {ep_label}" if series_title else r.get("title", "?")
                    media_id = series.get("id") or r.get("seriesId")

                entry = {"qid": r["id"], "title": title, "media_id": media_id}

                if has_file:
                    arr_result["queue_stale"].append(entry)
                else:
                    msgs = []
                    for sm in r.get("statusMessages", []):
                        msgs.extend(sm.get("messages", []))
                    no_files = any("No files found are eligible" in m for m in msgs)
                    entry["no_files"] = no_files
                    arr_result["queue_missing"].append(entry)

        results[arr_name] = arr_result
    return results


def _background_scan():
    """Run scan in background and cache results."""
    with _scan_lock:
        if _cached_scan["scanning"]:
            return
        _cached_scan["scanning"] = True
    try:
        data = _scan_arr_health()
        with _scan_lock:
            _cached_scan["data"] = data
            _cached_scan["timestamp"] = datetime.now().isoformat()
    finally:
        with _scan_lock:
            _cached_scan["scanning"] = False


def _handle_action(action, params):
    """Execute a fix action. Returns (success, message)."""
    if action == "clear_stale":
        arr_name = params.get("arr")
        if arr_name not in ARRS:
            return False, "Unknown arr"
        info = ARRS[arr_name]
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if not queue_data:
            return False, "Could not fetch queue"
        removed = 0
        for r in queue_data.get("records", []):
            if r.get("trackedDownloadState") != "importPending":
                continue
            if info["type"] == "radarr":
                has_file = r.get("movie", {}).get("hasFile", False)
            else:
                has_file = r.get("episode", {}).get("hasFile", False)
            if has_file:
                if arr_remove_queue_item(arr_name, r["id"], blocklist=False):
                    removed += 1
        return True, f"Removed {removed} stale queue entries from {arr_name}"

    elif action == "clear_dead_and_research":
        arr_name = params.get("arr")
        if arr_name not in ARRS:
            return False, "Unknown arr"
        info = ARRS[arr_name]
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if not queue_data:
            return False, "Could not fetch queue"
        removed = 0
        series_to_search = set()
        movies_to_search = []
        for r in queue_data.get("records", []):
            if r.get("trackedDownloadState") != "importPending":
                continue
            if info["type"] == "radarr":
                has_file = r.get("movie", {}).get("hasFile", False)
            else:
                has_file = r.get("episode", {}).get("hasFile", False)
            if not has_file:
                if arr_remove_queue_item(arr_name, r["id"], blocklist=False):
                    removed += 1
                    if info["type"] == "radarr":
                        mid = r.get("movie", {}).get("id")
                        if mid:
                            movies_to_search.append(mid)
                    else:
                        sid = r.get("series", {}).get("id") or r.get("seriesId")
                        if sid:
                            series_to_search.add(sid)
        searched = 0
        if info["type"] == "radarr" and movies_to_search:
            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": movies_to_search})
            searched = len(movies_to_search)
        for sid in series_to_search:
            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": sid})
            searched += 1
        return True, f"Removed {removed} dead entries, triggered {searched} searches in {arr_name}"

    elif action == "clear_all_queue":
        arr_name = params.get("arr")
        if arr_name not in ARRS:
            return False, "Unknown arr"
        info = ARRS[arr_name]
        queue_data = arr_get(arr_name, "queue?page=1&pageSize=200&includeEpisode=true&includeSeries=true")
        if not queue_data:
            return False, "Could not fetch queue"
        removed = 0
        series_to_search = set()
        movies_to_search = []
        for r in queue_data.get("records", []):
            if r.get("trackedDownloadState") != "importPending":
                continue
            if info["type"] == "radarr":
                has_file = r.get("movie", {}).get("hasFile", False)
                mid = r.get("movie", {}).get("id")
            else:
                has_file = r.get("episode", {}).get("hasFile", False)
                sid = r.get("series", {}).get("id") or r.get("seriesId")
            if arr_remove_queue_item(arr_name, r["id"], blocklist=False):
                removed += 1
                if not has_file:
                    if info["type"] == "radarr" and mid:
                        movies_to_search.append(mid)
                    elif info["type"] == "sonarr" and sid:
                        series_to_search.add(sid)
        searched = 0
        if info["type"] == "radarr" and movies_to_search:
            arr_post(arr_name, "command", {"name": "MoviesSearch", "movieIds": movies_to_search})
            searched = len(movies_to_search)
        for sid in series_to_search:
            arr_post(arr_name, "command", {"name": "SeriesSearch", "seriesId": sid})
            searched += 1
        return True, f"Cleared {removed} stuck entries, triggered {searched} re-searches in {arr_name}"

    elif action == "reset_indexers":
        arr_name = params.get("arr")
        if arr_name not in ARRS:
            return False, "Unknown arr"
        # Reset Prowlarr first (source of indexer state), then the arr
        msgs = []
        if _reset_prowlarr_db():
            msgs.append("Reset Prowlarr")
        if _reset_arr_db(arr_name):
            msgs.append(f"reset {arr_name}")
        if msgs:
            return True, " and ".join(msgs) + " — indexer/list failures cleared"
        return False, f"Failed to reset indexers"

    elif action == "delete_tmdb_removed":
        arr_name = params.get("arr")
        if arr_name not in ARRS:
            return False, "Unknown arr"
        info = ARRS[arr_name]
        if info["type"] != "radarr":
            return False, "Only applicable to Radarr"
        health = arr_get(arr_name, "health")
        tmdb_ids = set()
        if health:
            for h in health:
                msg = h.get("message", "")
                if "removed from TMDb" in msg:
                    for m in re.findall(r'tmdbid\s+(\d+)', msg):
                        tmdb_ids.add(int(m))
        if not tmdb_ids:
            return True, "No TMDb-removed movies found"
        movies = arr_get(arr_name, "movie")
        if not movies:
            return False, "Could not fetch movies"
        deleted = 0
        for m in movies:
            if m.get("tmdbId") in tmdb_ids:
                if arr_delete(arr_name, f"movie/{m['id']}?deleteFiles=true&addImportExclusion=true"):
                    deleted += 1
        return True, f"Deleted {deleted} TMDb-removed movies from {arr_name}"

    return False, "Unknown action"


def _check_arr_reachability():
    """Check reachability of all configured arrs for the health endpoint."""
    result = {}
    for arr_name, info in ARRS.items():
        try:
            url = f"http://{info['host']}:{info['port']}/api/v3/system/status?apikey={info['key']}"
            r = requests.get(url, timeout=5)
            result[arr_name] = "reachable" if r.ok else "unreachable"
        except Exception:
            result[arr_name] = "unreachable"
    return result


class StatusHandler(BaseHTTPRequestHandler):
    state_ref = None

    def do_POST(self):
        if self.path == "/api/action":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                params = json.loads(body)
            except Exception:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":false,"message":"Bad request"}')
                return

            action = params.pop("action", "")
            success, message = _handle_action(action, params)

            if success and self.state_ref is not None:
                log_action(self.state_ref, f"ui_{action}", message)
                save_state(self.state_ref)

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": success, "message": message}).encode())
            return

        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            uptime = time.time() - _start_time
            health_data = {
                "status": "ok",
                "uptime": round(uptime, 1),
                "last_cycle": _last_cycle_time,
                "arrs": _check_arr_reachability(),
            }
            self.wfile.write(json.dumps(health_data).encode())
            return

        if self.path == "/api/status":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            s = self.state_ref or {}
            self.wfile.write(json.dumps({
                "last_check": s.get("last_check"),
                "cycle_count": s.get("cycle_count", 0),
                "recent_actions": s.get("actions_log", [])[-20:],
                "recent_deletions": s.get("deleted_files", [])[-20:],
                "rd_problems": s.get("rd_removed", [])[-20:],
                "looping_torrents": len(s.get("loop_counts", {})),
            }).encode())
            return

        if self.path == "/api/scan":
            # Return cached scan, trigger refresh in background
            threading.Thread(target=_background_scan, daemon=True).start()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            with _scan_lock:
                data = _cached_scan["data"]
                ts = _cached_scan["timestamp"]
            self.wfile.write(json.dumps({"arrs": data, "scanned_at": ts}).encode())
            return

        # Dashboard HTML
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        s = self.state_ref or {}

        actions = s.get("actions_log", [])[-30:]
        actions.reverse()

        action_labels = {
            "clear_stuck": ("Cleared stuck download", "#f59e0b"),
            "restart": ("Restarted service", "#3b82f6"),
            "restart_arr": ("Restarted arr", "#3b82f6"),
            "blocklist_loop": ("Blocklisted looping torrent", "#ef4444"),
            "suspicious_deletion": ("Suspicious file deletion", "#ef4444"),
            "file_deleted": ("File cleaned up", "#6e6e73"),
            "rd_problem": ("Real-Debrid issue", "#ef4444"),
            "rd_orphan_blocked": ("Auto-blocked orphan RD release", "#10b981"),
            "search_missing": ("Triggered missing search", "#10b981"),
            "auto_import": ("Auto-imported", "#10b981"),
            "auto_clear_stale": ("Auto-cleared stale queue", "#10b981"),
            "auto_clear_dead": ("Auto-cleared dead + re-searched", "#10b981"),
            "auto_reset_indexers": ("Auto-fixed indexer/list failures", "#10b981"),
            "auto_delete_tmdb": ("Auto-removed TMDb junk", "#10b981"),
            "stuck_exhausted": ("Gave up — all releases failing", "#ef4444"),
            "ui_clear_stale": ("Cleared stale queue", "#3b82f6"),
            "ui_clear_dead_and_research": ("Cleared dead + re-searched", "#3b82f6"),
            "ui_clear_all_queue": ("Cleared queue + re-searched", "#3b82f6"),
            "ui_reset_indexers": ("Reset indexers", "#3b82f6"),
            "ui_delete_tmdb_removed": ("Deleted TMDb-removed", "#f59e0b"),
        }

        def action_row(a):
            label, color = action_labels.get(a["action"], (a["action"], "#6e6e73"))
            detail = _html_escape(a["detail"][:120])
            t = a["timestamp"][11:19]
            d = a["timestamp"][:10]
            return f'<tr><td style="white-space:nowrap;color:#999;">{d} {t}</td><td><span style="color:{color};font-weight:500;">{label}</span></td><td style="color:#555;">{detail}</td></tr>'

        actions_html = "".join(action_row(a) for a in actions) or '<tr><td colspan=3 style="color:#999;text-align:center;padding:24px;">No actions taken yet.</td></tr>'

        # Pre-populate with cached scan data if available
        with _scan_lock:
            initial_data = json.dumps({"arrs": _cached_scan["data"], "scanned_at": _cached_scan["timestamp"]})

        html = f"""<!DOCTYPE html>
<html><head><title>Babysitarr</title>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f7;color:#1d1d1f;min-height:100vh;}}
.header{{background:#fff;border-bottom:1px solid #e5e5e5;padding:20px 32px;display:flex;align-items:center;justify-content:space-between;}}
.header h1{{font-size:20px;font-weight:700;}}
.status{{display:flex;align-items:center;gap:8px;font-size:14px;font-weight:500;}}
.status-dot{{width:10px;height:10px;border-radius:50%;}}
.wrap{{max-width:960px;margin:0 auto;padding:24px 32px;}}
.card{{background:#fff;border-radius:12px;padding:20px;margin-bottom:16px;box-shadow:0 1px 3px rgba(0,0,0,.05);}}
.card h2{{font-size:15px;font-weight:600;margin:0 0 14px;color:#1d1d1f;display:flex;align-items:center;justify-content:space-between;}}
.card .subtitle{{font-size:12px;color:#999;margin:-8px 0 14px;}}
table{{width:100%;border-collapse:collapse;font-size:13px;}}
th{{text-align:left;padding:8px 10px;color:#6e6e73;font-weight:500;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid #e5e5e5;}}
td{{padding:8px 10px;border-bottom:1px solid #f5f5f5;}}
tr:last-child td{{border-bottom:none;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;}}
.badge-ok{{background:#dcfce7;color:#166534;}}
.badge-warn{{background:#fef3c7;color:#92400e;}}
.badge-err{{background:#fee2e2;color:#991b1b;}}
.badge-info{{background:#dbeafe;color:#1e40af;}}
.btn{{display:inline-block;padding:6px 14px;border-radius:8px;border:none;font-size:12px;font-weight:600;cursor:pointer;transition:all .15s;}}
.btn:hover{{filter:brightness(.9);}}
.btn:disabled{{opacity:.5;cursor:not-allowed;}}
.btn-blue{{background:#3b82f6;color:#fff;}}
.btn-green{{background:#10b981;color:#fff;}}
.btn-amber{{background:#f59e0b;color:#fff;}}
.btn-red{{background:#ef4444;color:#fff;}}
.btn-sm{{padding:4px 10px;font-size:11px;}}
.arr-section{{border:1px solid #e5e5e5;border-radius:10px;padding:16px;margin-bottom:12px;}}
.arr-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;}}
.arr-header h3{{font-size:14px;font-weight:600;}}
.arr-stats{{display:flex;gap:16px;font-size:12px;color:#6e6e73;}}
.issue-row{{display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid #f0f0f0;font-size:13px;gap:12px;}}
.issue-row:last-child{{border-bottom:none;}}
.issue-row .desc{{flex:1;}}
.toast{{position:fixed;bottom:24px;right:24px;background:#1d1d1f;color:#fff;padding:12px 20px;border-radius:10px;font-size:13px;font-weight:500;z-index:999;display:none;max-width:400px;box-shadow:0 4px 12px rgba(0,0,0,.3);}}
.footer{{text-align:center;color:#999;font-size:12px;padding:20px;}}
.loading{{color:#999;text-align:center;padding:20px;font-size:13px;}}
.scanned-at{{font-size:11px;color:#999;font-weight:400;}}
</style></head><body>
<div class="header">
    <h1>Babysitarr</h1>
    <div class="status"><div class="status-dot" id="statusDot" style="background:#10b981;"></div><span id="statusText">Loading...</span></div>
</div>
<div class="wrap">
    <div class="card">
        <h2>
            <span>Status <span class="scanned-at" id="scannedAt"></span></span>
            <span style="display:flex;gap:8px;">
                <button class="btn btn-green btn-sm" id="fixAllBtn" style="display:none;" onclick="fixAll()">Fix all issues</button>
                <button class="btn btn-blue btn-sm" onclick="refreshScan()" id="refreshBtn">Refresh</button>
            </span>
        </h2>
        <div id="arrHealth"><div class="loading">Loading...</div></div>
    </div>

    <div class="card">
        <h2>Activity Log</h2>
        <p class="subtitle">Actions taken automatically and manually. Auto-fixes are green.</p>
        <table><thead><tr><th>When</th><th>Action</th><th>Details</th></tr></thead>
        <tbody>{actions_html}</tbody></table>
    </div>
</div>
<div class="toast" id="toast"></div>
<div class="footer">Checks every {CHECK_INTERVAL}s &middot; Auto-fixes run each cycle &middot; <a href="https://buymeacoffee.com/Davidalves" target="_blank" style="color:#999;text-decoration:none;">☕ Buy me a coffee</a></div>

<script>
let pendingFixes = [];

function showToast(msg, ms) {{
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.style.display = 'block';
    clearTimeout(t._tid);
    t._tid = setTimeout(() => t.style.display = 'none', ms || 4000);
}}

async function doAction(action, arr, btn) {{
    if (btn) {{ btn.disabled = true; btn.textContent = 'Fixing...'; }}
    try {{
        const r = await fetch('/api/action', {{
            method: 'POST',
            headers: {{'Content-Type': 'application/json'}},
            body: JSON.stringify({{action, arr}})
        }});
        const j = await r.json();
        showToast(j.message, 5000);
        setTimeout(() => {{ refreshScan(); location.reload(); }}, 2000);
    }} catch(e) {{
        showToast('Failed: ' + e.message, 5000);
    }}
    if (btn) {{ btn.disabled = false; btn.textContent = 'Done'; }}
}}

async function fixAll() {{
    const btn = document.getElementById('fixAllBtn');
    btn.disabled = true; btn.textContent = 'Fixing...';
    for (const fix of pendingFixes) {{
        try {{
            const r = await fetch('/api/action', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(fix)
            }});
            const j = await r.json();
            showToast(j.message, 3000);
        }} catch(e) {{}}
        await new Promise(r => setTimeout(r, 500));
    }}
    showToast('All fixes applied. Refreshing...', 3000);
    setTimeout(() => {{ refreshScan(); location.reload(); }}, 3000);
}}

function renderScan(resp) {{
    const data = resp.arrs || {{}};
    const scannedAt = resp.scanned_at;
    const el = document.getElementById('arrHealth');
    pendingFixes = [];
    let totalIssues = 0;
    let html = '';

    // Update scanned time
    if (scannedAt) {{
        const t = scannedAt.substring(11, 19);
        document.getElementById('scannedAt').textContent = 'as of ' + t;
    }}

    for (const [name, arr] of Object.entries(data)) {{
        const issues = arr.health || [];
        const stale = arr.queue_stale || [];
        const missing = arr.queue_missing || [];
        const blocked = arr.queue_blocked || [];
        const downloading = arr.queue_downloading || 0;
        const staleCount = stale.length;
        const missingCount = missing.length;
        const blockedCount = blocked.length;
        const healthIssues = issues.filter(h => !h.message.includes('removed from TMDb'));
        const tmdbIssues = issues.filter(h => h.message.includes('removed from TMDb'));
        const indexerIssues = healthIssues.filter(h => h.message.toLowerCase().includes('indexer') || h.message.toLowerCase().includes('list'));

        const problemCount = indexerIssues.length + (staleCount > 0 ? 1 : 0) + (missingCount > 0 ? 1 : 0) + (tmdbIssues.length > 0 ? 1 : 0) + (blockedCount > 0 ? 1 : 0);
        totalIssues += problemCount;

        const badge = problemCount > 0
            ? '<span class="badge badge-warn">' + problemCount + ' issue' + (problemCount > 1 ? 's' : '') + '</span>'
            : (downloading > 0 ? '<span class="badge badge-info">' + downloading + ' downloading</span>' : '<span class="badge badge-ok">Healthy</span>');

        html += '<div class="arr-section">';
        html += '<div class="arr-header"><h3>' + escHtml(name) + ' ' + badge + '</h3>';
        html += '<div class="arr-stats"><span>Queue: ' + arr.queue_total + '</span></div></div>';

        if (problemCount === 0) {{
            html += '<div style="color:#10b981;font-size:13px;padding:4px 0;">No issues. Everything is working.</div>';
            html += '</div>';
            continue;
        }}

        // Indexer/list failures — plain English
        for (const h of indexerIssues) {{
            let plain = '';
            const msg = h.message;
            if (msg.includes('All lists are unavailable')) {{
                plain = 'List sync is broken — can\\'t pull new movies/shows from your lists';
            }} else if (msg.includes('All rss-capable indexers')) {{
                plain = 'RSS is down — can\\'t check for new releases automatically';
            }} else if (msg.includes('All search-capable indexers')) {{
                plain = 'Search is broken — can\\'t find downloads when you request them';
            }} else if (msg.includes('unavailable due to failures for more than 6 hours')) {{
                const names = msg.match(/: (.+)/);
                plain = (names ? names[1] : 'Some indexers') + ' have been failing for 6+ hours';
            }} else if (msg.includes('unavailable due to failures')) {{
                const names = msg.match(/: (.+)/);
                plain = (names ? names[1] : 'Some indexers') + ' temporarily failing';
            }} else {{
                plain = msg.substring(0, 100);
            }}
            const isErr = h.type === 'error';
            html += '<div class="issue-row"><div class="desc"><span class="badge ' + (isErr ? 'badge-err' : 'badge-warn') + '">' + (isErr ? 'error' : 'warning') + '</span> ' + escHtml(plain) + '</div>';
            html += '<button class="btn btn-amber btn-sm" onclick="doAction(\'reset_indexers\',\'' + name + '\',this)">Fix</button></div>';
            pendingFixes.push({{action: 'reset_indexers', arr: name}});
        }}

        // TMDb removed
        if (tmdbIssues.length > 0) {{
            const count = (tmdbIssues[0].message.match(/tmdbid/g) || []).length;
            html += '<div class="issue-row"><div class="desc"><span class="badge badge-err">cleanup</span> ' + count + ' junk movies were added by lists but removed from TMDb — safe to delete</div>';
            html += '<button class="btn btn-red btn-sm" onclick="doAction(\'delete_tmdb_removed\',\'' + name + '\',this)">Delete junk</button></div>';
            pendingFixes.push({{action: 'delete_tmdb_removed', arr: name}});
        }}

        // Stale queue
        if (staleCount > 0) {{
            html += '<div class="issue-row"><div class="desc"><span class="badge badge-warn">stale</span> ' + staleCount + ' downloads finished and imported, but queue entries are stuck — safe to clear</div>';
            html += '<button class="btn btn-blue btn-sm" onclick="doAction(\'clear_stale\',\'' + name + '\',this)">Clear</button></div>';
            pendingFixes.push({{action: 'clear_stale', arr: name}});
        }}

        // Missing media
        if (missingCount > 0) {{
            const deadCount = missing.filter(m => m.no_files).length;
            let plain = missingCount + ' item' + (missingCount > 1 ? 's' : '') + ' stuck in queue — download files are gone and media is missing';
            if (deadCount > 0 && deadCount < missingCount) plain += ' (' + deadCount + ' confirmed empty)';
            html += '<div class="issue-row"><div class="desc"><span class="badge badge-err">missing</span> ' + escHtml(plain) + '</div>';
            html += '<button class="btn btn-red btn-sm" onclick="doAction(\'clear_dead_and_research\',\'' + name + '\',this)">Clear + re-search</button></div>';
            pendingFixes.push({{action: 'clear_dead_and_research', arr: name}});
        }}

        // Blocked imports (files are there but arr can't auto-import)
        if (blockedCount > 0) {{
            html += '<div class="issue-row"><div class="desc"><span class="badge badge-warn">blocked</span> ' + blockedCount + ' item' + (blockedCount > 1 ? 's' : '') + ' have files ready but can\\'t auto-import — babysitarr will force-import on next cycle</div></div>';
        }}

        html += '</div>';
    }}

    // Empty state
    if (Object.keys(data).length === 0) {{
        html = '<div class="loading">No data yet — first scan is running...</div>';
    }}

    el.innerHTML = html;

    // Update status
    const dot = document.getElementById('statusDot');
    const statusText = document.getElementById('statusText');
    const fixAllBtn = document.getElementById('fixAllBtn');

    if (Object.keys(data).length === 0) {{
        dot.style.background = '#999';
        statusText.textContent = 'Scanning...';
        fixAllBtn.style.display = 'none';
    }} else if (totalIssues === 0) {{
        dot.style.background = '#10b981';
        statusText.textContent = 'All clear';
        fixAllBtn.style.display = 'none';
    }} else {{
        dot.style.background = '#f59e0b';
        statusText.textContent = totalIssues + ' issue' + (totalIssues > 1 ? 's' : '');
        fixAllBtn.style.display = 'inline-block';
        fixAllBtn.textContent = 'Fix all (' + pendingFixes.length + ')';
    }}
}}

function escHtml(s) {{
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}}

async function refreshScan() {{
    const btn = document.getElementById('refreshBtn');
    btn.disabled = true; btn.textContent = 'Scanning...';
    try {{
        const r = await fetch('/api/scan');
        const data = await r.json();
        renderScan(data);
    }} catch(e) {{
        showToast('Scan failed: ' + e.message, 5000);
    }}
    btn.disabled = false; btn.textContent = 'Refresh';
}}

// Render cached data immediately if available, then refresh
const initial = {initial_data};
if (initial.arrs && Object.keys(initial.arrs).length > 0) {{
    renderScan(initial);
}}
refreshScan();
setInterval(refreshScan, 60000);
</script>
</body></html>"""
        self.wfile.write(html.encode())

    def log_message(self, format, *args):
        pass  # Suppress access logs

def start_web_ui(state):
    StatusHandler.state_ref = state
    port = int(os.getenv("WEB_PORT", "8284"))
    server = HTTPServer(("0.0.0.0", port), StatusHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info(f"Status page running on port {port}")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_shutdown_requested = False

def _signal_handler(signum, frame):
    global _shutdown_requested
    sig_name = signal.Signals(signum).name
    log.info(f"Received {sig_name}, shutting down gracefully...")
    _shutdown_requested = True

signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)

# ---------------------------------------------------------------------------
# Decypharr orphan detection (v2.x runtime-API based)
# ---------------------------------------------------------------------------
def check_decypharr_orphans(state):
    """Detect decypharr torrents that have been waiting on a symlink for a long
    time AND are no longer tracked by any *arr — i.e. truly orphaned.

    Pattern: state='downloading', status='downloaded', action='symlink', age >
    ORPHAN_TIMEOUT_SEC, and no matching title in any *arr's queue. The *arr has
    moved on (failed, removed, or never tracked), but decypharr keeps polling
    these forever. On v2.2 this caused the FUSE wedge incident; on v2.1 it
    causes silent background load creep.

    The *arr-queue check is the safety guard: if the *arr is still tracking
    this torrent, we leave it alone so Babysitarr's other checks (looping
    torrents, stuck queue items) handle it through the *arr side instead.
    """
    torrents = _decypharr_get_torrents()
    if not torrents:
        return

    now = time.time()
    candidates = []
    for t in torrents:
        if t.get("state") != "downloading":
            continue
        if t.get("status") != "downloaded":
            continue
        if t.get("action") != "symlink":
            continue
        added = _parse_iso_ts(t.get("added_on"))
        if added is None or now - added <= ORPHAN_TIMEOUT_SEC:
            continue
        candidates.append({
            "hash": t.get("info_hash", ""),
            "name": (t.get("name") or "").strip(),
            "age_min": int((now - added) / 60),
        })

    if not candidates:
        return

    # Build a set of titles currently tracked by any *arr
    arr_titles = set()
    for arr_name in ARRS:
        q = arr_get_queue(arr_name)
        if not q:
            continue
        for r in q.get("records", []):
            title = (r.get("title") or "").strip().lower()
            if title:
                arr_titles.add(title)

    orphans = []
    for c in candidates:
        cname = c["name"].lower()
        cbase = cname.rsplit(".", 1)[0]
        # Forgiving match: orphan only if NO *arr title contains the basename
        in_arr = any(cbase and cbase in t for t in arr_titles) or cname in arr_titles
        if not in_arr:
            orphans.append(c)

    if not orphans:
        return

    log.warning(f"Found {len(orphans)} orphaned decypharr torrents "
                f"(stuck >{ORPHAN_TIMEOUT_SEC // 60}min, no matching arr queue entry)")

    cleared = []
    for o in orphans:
        if _decypharr_delete_torrent(o["hash"]):
            cleared.append(o)
            log_action(state, "clear_decypharr_orphan",
                       f"Removed orphan from decypharr: {o['name']} "
                       f"(stuck {o['age_min']}min, not in any arr queue)")

    if cleared:
        body = [f"Cleared {len(cleared)} orphaned torrent(s) from decypharr:"]
        for o in cleared:
            body.append(f"  • {o['name']} (stuck {o['age_min']}min)")
        body.append("")
        body.append("These were polling silently in decypharr's DB but no longer")
        body.append("tracked by Sonarr/Radarr. Left alone they cause background")
        body.append("load creep (and on decypharr v2.2, FUSE wedge).")
        send_notification("Decypharr orphans cleared", "\n".join(body), level="info")

# ---------------------------------------------------------------------------
# Host health monitoring (alert-only)
# ---------------------------------------------------------------------------
def check_host_health(state):
    """Monitor host load average and D-state process count.

    Linux containers share the host kernel; /proc inside the container reflects
    host-wide stats, so we can read /proc/loadavg and /proc/<pid>/status without
    needing extra mounts.

    Alerts only — recovery from a FUSE wedge requires sudo (fusermount -uz)
    and is too risky to automate. The alert tells the user exactly what to do.
    """
    try:
        with open("/proc/loadavg") as f:
            load_1min = float(f.read().split()[0])
    except Exception as e:
        log.debug(f"Could not read /proc/loadavg: {e}")
        return

    d_count = 0
    try:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                with open(f"/proc/{pid}/status") as f:
                    for line in f:
                        if line.startswith("State:"):
                            # State line: "State:\tD (disk sleep)" or "State:\tR (running)" etc.
                            if "\tD" in line or " D " in line:
                                d_count += 1
                            break
            except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
                continue
    except Exception as e:
        log.debug(f"Could not enumerate /proc for D-state: {e}")
        return

    high_load = load_1min > HOST_LOAD_THRESHOLD
    high_dstate = d_count > HOST_DSTATE_THRESHOLD

    if not (high_load or high_dstate):
        # Healthy — clear any prior alert state so the next problem alerts immediately
        state.pop("host_health_last_alert", None)
        return

    last_alert = state.get("host_health_last_alert", 0)
    now = time.time()
    if now - last_alert < HOST_HEALTH_COOLDOWN:
        return
    state["host_health_last_alert"] = now

    reasons = []
    if high_load:
        reasons.append(f"load_1min={load_1min:.1f} (>{HOST_LOAD_THRESHOLD})")
    if high_dstate:
        reasons.append(f"D-state={d_count} (>{HOST_DSTATE_THRESHOLD})")

    log.error(f"Host health alert: {', '.join(reasons)}")
    log_action(state, "host_health_alert", f"Host elevated: {', '.join(reasons)}")

    body = (
        f"Host health alert on the box running babysitarr.\n\n"
        f"  - 1-min load avg: {load_1min:.2f}\n"
        f"  - D-state (uninterruptible) processes: {d_count}\n\n"
        f"This pattern matches the FUSE wedge failure mode "
        f"(decypharr/rclone-zurg). If load keeps climbing or D-state stays "
        f"high, the rclone mount is likely deadlocked.\n\n"
        f"Manual recovery (in order, stop when load drops):\n"
        f"  1. ssh to the host\n"
        f"  2. sudo fusermount -uz /media/library/zurg   (no password)\n"
        f"  3. docker kill plex && docker start plex\n"
        f"  4. sudo systemctl restart rclone-zurg.service (needs password)\n\n"
        f"If multiple steps fail: full reboot.\n\n"
        f"Cooldown: next host-health alert in {HOST_HEALTH_COOLDOWN // 60}min."
    )
    send_notification("Host health alert (load / D-state)", body, level="alert")

# ---------------------------------------------------------------------------
# Decypharr version drift detection
# ---------------------------------------------------------------------------
def check_decypharr_version(state):
    """Alert if decypharr's running version has drifted from EXPECTED_DECYPHARR_TAG.

    We pin to v2.1 because v2.2+ regressed FUSE handling and wedges the box on
    symlink-loop torrents. The watchtower-skip label should prevent auto-update,
    but this is belt-and-suspenders in case the systemd unit is edited or
    watchtower config changes.

    Reads decypharr's own GET /version endpoint, which returns
    {"version": "2.1", "channel": "stable"} — far cleaner than docker socket
    inspection. Both 'v2.1' and '2.1' formats accepted in EXPECTED_DECYPHARR_TAG.

    Empty EXPECTED_DECYPHARR_TAG disables this check.
    """
    if not EXPECTED_DECYPHARR_TAG:
        return
    try:
        r = requests.get(f"{DECYPHARR_URL}/version", timeout=10)
        if not r.ok:
            return
        running = (r.json().get("version") or "").strip()
    except Exception as e:
        log.debug(f"check_decypharr_version fetch failed: {e}")
        return
    if not running:
        return

    # Normalize: strip leading 'v' from BOTH sides. v1.1.6 returns "v1.1.6",
    # v2.x returns "2.1" — different conventions across major versions, so we
    # have to compare the version-number portion only.
    running_norm = running.lstrip("v").strip()
    expected_norm = EXPECTED_DECYPHARR_TAG.lstrip("v").strip()
    if running_norm == expected_norm:
        state.pop("decypharr_version_last_alert", None)
        return

    last_alert = state.get("decypharr_version_last_alert", 0)
    now = time.time()
    if now - last_alert < HOST_HEALTH_COOLDOWN:
        return
    state["decypharr_version_last_alert"] = now

    log.error(f"Decypharr version drift: running '{running}', expected '{EXPECTED_DECYPHARR_TAG}'")
    log_action(state, "decypharr_version_drift",
               f"Running {running}, expected {EXPECTED_DECYPHARR_TAG}")
    send_notification(
        "Decypharr version drift",
        f"Decypharr is running version '{running}', but EXPECTED_DECYPHARR_TAG="
        f"'{EXPECTED_DECYPHARR_TAG}'.\n\n"
        f"Background: v2.x introduced the DFS stack which leaks CPU via parallel "
        f"FUSE stat probes against rclone-zurg. v2.2 also has a separate "
        f"FUSE-wedge regression. We pin to v1.1.6 (Oct 2024, pre-DFS) until "
        f"upstream offers a way to disable DFS workers.\n\n"
        f"To pin back to {EXPECTED_DECYPHARR_TAG}:\n"
        f"  sudo sed -i 's|decypharr:[^[:space:]]*|decypharr:{EXPECTED_DECYPHARR_TAG}|' /etc/systemd/system/decypharr.service\n"
        f"  sudo systemctl daemon-reload && sudo systemctl restart decypharr",
        level="alert"
    )

# ---------------------------------------------------------------------------
# Decypharr CPU watchdog (auto-restart on sustained high CPU)
# ---------------------------------------------------------------------------
def _docker_container_cpu_percent(name):
    """Single CPU% sample for a container via Docker stats API.

    Returns float (0.0-N00.0 where N is core count) or None on error.
    Reads the unix socket directly with proper full-body reading because the
    /containers/X/stats response is multi-KB and the lightweight _docker_api
    helper truncates at 4096 bytes.
    """
    import socket as _socket
    try:
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        sock.settimeout(15)
        sock.connect("/var/run/docker.sock")
        sock.sendall((f"GET /containers/{name}/stats?stream=false HTTP/1.1\r\n"
                      f"Host: localhost\r\nConnection: close\r\n\r\n").encode())
        chunks = []
        while True:
            c = sock.recv(8192)
            if not c:
                break
            chunks.append(c)
            if sum(len(x) for x in chunks) > 200000:
                break
        sock.close()
    except Exception as e:
        log.debug(f"Container stats fetch failed for {name}: {e}")
        return None

    raw = b"".join(chunks).decode("utf-8", errors="replace")
    body_start = raw.find("\r\n\r\n")
    if body_start < 0:
        return None
    body = raw[body_start + 4:].lstrip()
    json_start = body.find("{")
    if json_start < 0:
        return None
    # Find balanced JSON object (skips chunked-encoding line lengths if present)
    depth = 0
    end = -1
    for i, ch in enumerate(body[json_start:]):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = json_start + i + 1
                break
    if end < 0:
        return None
    try:
        data = json.loads(body[json_start:end])
    except Exception as e:
        log.debug(f"Container stats JSON parse failed for {name}: {e}")
        return None

    cpu = data.get("cpu_stats", {})
    pre = data.get("precpu_stats", {})
    cu = cpu.get("cpu_usage", {}).get("total_usage", 0)
    pu = pre.get("cpu_usage", {}).get("total_usage", 0)
    sc = cpu.get("system_cpu_usage", 0)
    ps = pre.get("system_cpu_usage", 0)
    online = cpu.get("online_cpus") or len(cpu.get("cpu_usage", {}).get("percpu_usage", []) or []) or 1

    cpu_delta = cu - pu
    system_delta = sc - ps
    if system_delta <= 0 or cpu_delta < 0:
        return 0.0
    return (cpu_delta / system_delta) * online * 100.0


def _docker_exec(container, cmd):
    """Run a command inside `container` via the Docker exec API on the unix
    socket. Returns combined stdout+stderr as a string, or None on error.

    Uses Tty=true so the response is raw bytes (no per-frame stream-type
    multiplexing). Suitable for diagnostic captures where we don't need to
    distinguish stdout from stderr.
    """
    import socket as _socket
    try:
        # Step 1: create the exec instance.
        # Connection: close tells docker to close the socket after responding,
        # so our recv loop terminates without waiting for timeout.
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect("/var/run/docker.sock")
        body = json.dumps({
            "Cmd": cmd,
            "AttachStdout": True,
            "AttachStderr": True,
            "Tty": True,
        })
        req = (f"POST /containers/{container}/exec HTTP/1.1\r\n"
               f"Host: localhost\r\nContent-Type: application/json\r\n"
               f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}")
        sock.sendall(req.encode())
        chunks = []
        try:
            while True:
                c = sock.recv(8192)
                if not c:
                    break
                chunks.append(c)
                if sum(len(x) for x in chunks) > 16384:
                    break
        except _socket.timeout:
            pass
        sock.close()
        resp = b"".join(chunks).decode("utf-8", errors="replace")
        m = re.search(r'"Id"\s*:\s*"([0-9a-f]+)"', resp)
        if not m:
            log.debug(f"_docker_exec: no exec id in response: {resp[:200]}")
            return None
        exec_id = m.group(1)

        # Step 2: start the exec instance and read output
        sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
        sock.settimeout(60)
        sock.connect("/var/run/docker.sock")
        body = json.dumps({"Detach": False, "Tty": True})
        req = (f"POST /exec/{exec_id}/start HTTP/1.1\r\n"
               f"Host: localhost\r\nContent-Type: application/json\r\n"
               f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}")
        sock.sendall(req.encode())
        chunks = []
        try:
            while True:
                c = sock.recv(65536)
                if not c:
                    break
                chunks.append(c)
                if sum(len(x) for x in chunks) > 5_000_000:
                    break
        except _socket.timeout:
            pass
        sock.close()
        full = b"".join(chunks).decode("utf-8", errors="replace")
        # Strip HTTP headers
        body_idx = full.find("\r\n\r\n")
        return full[body_idx + 4:] if body_idx >= 0 else full
    except Exception as e:
        log.debug(f"_docker_exec failed for {container}: {e}")
        return None


# Diagnostic script run inside decypharr via /proc/1 (decypharr is PID 1 in its namespace)
_DECYPHARR_DIAG_SCRIPT = r"""
echo '=== thread count ==='
ls /proc/1/task 2>/dev/null | wc -l
echo
echo '=== thread state breakdown ==='
for tid in /proc/1/task/*; do
  cat $tid/status 2>/dev/null | grep '^State:' | awk '{print $2}'
done | sort | uniq -c | sort -rn
echo
echo '=== top wchans (kernel wait points) ==='
for tid in /proc/1/task/*; do
  cat $tid/wchan 2>/dev/null
  echo
done | sort | uniq -c | sort -rn | head -25
echo
echo '=== child processes (ffprobe etc) ==='
ps -ef 2>/dev/null
echo
echo '=== open fd count ==='
ls /proc/1/fd 2>/dev/null | wc -l
echo
echo '=== open fd type distribution (top 15) ==='
for fd in /proc/1/fd/*; do
  readlink $fd 2>/dev/null
done | awk -F: '{print $1}' | sort | uniq -c | sort -rn | head -15
echo
echo '=== memory + threads ==='
cat /proc/1/status 2>/dev/null | grep -E '^(VmRSS|VmSize|VmPeak|Threads):'
echo
echo '=== uptime of decypharr process (from /proc/1/stat field 22 in clock ticks) ==='
awk '{print "starttime_ticks=" $22}' /proc/1/stat 2>/dev/null
cat /proc/uptime 2>/dev/null
"""


def _capture_decypharr_diagnostics(reason):
    """Capture /proc-based diagnostics from decypharr container.

    Saves to {DATA_DIR}/decypharr-dumps/<timestamp>_<reason>.txt.
    Returns the saved path or None on failure.

    Why /proc instead of pprof: decypharr v2.1's --pprof flag is broken
    (advertised in --help but the server doesn't actually start). So we use
    the kernel's view: thread states, wchans (what kernel ops threads block
    on), child processes, open fd counts. Comparing a leak capture against
    a healthy baseline reveals what's accumulating (more threads in particular
    states, hung ffprobes, leaked file descriptors, etc).
    """
    dumps_dir = os.path.join(DATA_DIR, "decypharr-dumps")
    try:
        os.makedirs(dumps_dir, exist_ok=True)
    except Exception as e:
        log.warning(f"Could not create dumps dir: {e}")
        return None

    ts = datetime.now().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(dumps_dir, f"{ts}_{reason}.txt")

    output = _docker_exec("decypharr", ["sh", "-c", _DECYPHARR_DIAG_SCRIPT])
    if output is None:
        return None
    try:
        with open(path, "w") as f:
            f.write(f"# decypharr diagnostic capture\n")
            f.write(f"# timestamp: {ts}\n")
            f.write(f"# reason: {reason}\n\n")
            f.write(output)
        log.info(f"Decypharr diagnostics captured: {path}")
        return path
    except Exception as e:
        log.warning(f"Failed to write diagnostic dump {path}: {e}")
        return None


def check_decypharr_cpu_watchdog(state):
    """Auto-restart decypharr if its CPU stays above DECYPHARR_CPU_WATCHDOG_THRESHOLD
    for DECYPHARR_CPU_WATCHDOG_CYCLES consecutive cycles.

    Decypharr v2.x's DFS-stack background workers and ffprobe subprocesses are
    known to leak goroutines / stick around after errors, accumulating CPU
    usage even when the queue is empty. A simple `docker restart` clears the
    runtime state instantly. Cooldown prevents restart loops if something
    else is broken post-restart.

    Today's incident: 120-150% CPU sustained for ~5.5h with empty DB/no logs;
    restart dropped it to 0.3%.
    """
    cpu = _docker_container_cpu_percent("decypharr")
    if cpu is None:
        return

    if cpu > DECYPHARR_CPU_WATCHDOG_THRESHOLD:
        state["decypharr_cpu_high_streak"] = state.get("decypharr_cpu_high_streak", 0) + 1
    else:
        # CPU dropped below threshold — reset the streak so we only act on
        # SUSTAINED high CPU, not transient spikes during a real workload.
        if state.get("decypharr_cpu_high_streak"):
            state.pop("decypharr_cpu_high_streak", None)
        return

    streak = state["decypharr_cpu_high_streak"]
    if streak < DECYPHARR_CPU_WATCHDOG_CYCLES:
        log.info(f"Decypharr CPU at {cpu:.1f}% (streak {streak}/{DECYPHARR_CPU_WATCHDOG_CYCLES})")
        return

    last_restart = state.get("decypharr_cpu_watchdog_last_restart", 0)
    now = time.time()
    if now - last_restart < DECYPHARR_CPU_WATCHDOG_COOLDOWN:
        log.warning(f"Decypharr CPU still elevated ({cpu:.1f}%) but watchdog "
                    f"in cooldown ({int((now - last_restart) / 60)}min ago); skipping")
        return

    log.warning(f"Decypharr CPU watchdog firing: {cpu:.1f}% sustained for "
                f"{streak} cycles, capturing diagnostics then restarting")

    # Capture /proc-based diagnostics BEFORE restart so we have evidence
    # of what was accumulating (decypharr v2.1's pprof is broken, so we use
    # kernel-level visibility instead).
    dump_path = _capture_decypharr_diagnostics(f"watchdog_{cpu:.0f}pct")

    if docker_restart("decypharr"):
        state["decypharr_cpu_watchdog_last_restart"] = now
        state["decypharr_cpu_high_streak"] = 0
        dump_note = f"\n\nDiagnostic dump saved to: {dump_path}\n" if dump_path else \
                    "\n\n(Diagnostic capture failed — check babysitarr logs)\n"
        log_action(state, "decypharr_cpu_restart",
                   f"Auto-restarted decypharr after {cpu:.1f}% CPU sustained "
                   f"{streak} cycles ({streak * CHECK_INTERVAL // 60}min); "
                   f"dump={dump_path or 'failed'}")
        send_notification(
            "Decypharr CPU watchdog: auto-restarted",
            f"Babysitarr auto-restarted decypharr because it was sustained at "
            f"{cpu:.1f}% CPU across {streak} consecutive check cycles "
            f"({streak * CHECK_INTERVAL // 60}min).\n"
            f"{dump_note}"
            f"The dump has thread state breakdown, kernel wait points (wchan), "
            f"child process tree (ffprobe accumulation?), and fd counts. "
            f"Comparing it to a healthy baseline reveals what was leaking.\n\n"
            f"Likely cause: leaked goroutines and/or stuck ffprobe processes "
            f"(known v2.x quirk — DFS-stack background workers don't always "
            f"clean up after errors).\n\n"
            f"Cooldown: next watchdog restart in {DECYPHARR_CPU_WATCHDOG_COOLDOWN // 60}min.",
            level="info"
        )

# ---------------------------------------------------------------------------
# Check: Decypharr path mismatch (auto-create missing symlinks)
# ---------------------------------------------------------------------------
def _zurg_list_dirs():
    """Fetch top-level directory names from zurg WebDAV."""
    try:
        r = requests.get(ZURG_DAV_URL, timeout=60)
        if not r.ok:
            log.warning(f"Zurg WebDAV returned HTTP {r.status_code}")
            return {}
    except Exception as e:
        log.warning(f"Could not reach zurg WebDAV: {e}")
        return {}
    import urllib.parse
    dirs = set()
    for match in re.finditer(r'<d:href>([^<]+)</d:href>', r.text):
        href = urllib.parse.unquote(match.group(1))
        if href in ('/__all__', '/__all__/'):
            continue
        if href.startswith('/'):
            href = href.lstrip('/')
        if href.startswith('__all__/'):
            href = href[len('__all__/'):]
        href = href.rstrip('/')
        if href:
            dirs.add(href)
    return dirs


def _zurg_find_video(zurg_dir):
    """Find the main video file inside a zurg directory via PROPFIND."""
    import urllib.parse
    encoded = urllib.parse.quote(zurg_dir, safe='')
    url = ZURG_DAV_URL + encoded + '/'
    try:
        r = requests.request('PROPFIND', url, headers={'Depth': '1'}, timeout=15)
        if not r.ok:
            return None
    except Exception:
        return None
    files = []
    for match in re.finditer(r'<d:href>([^<]+)</d:href>', r.text):
        href = urllib.parse.unquote(match.group(1))
        fname = href.split('/')[-1]
        if not fname:
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext in _VIDEO_EXTS:
            if not fname.lower().startswith('rarbg') and 'sample' not in fname.lower():
                files.append(fname)
    if len(files) == 1:
        return files[0]
    elif files:
        return max(files, key=len)
    return None


def _normalize_torrent_name(name):
    """Normalize a torrent name for fuzzy matching against zurg dirs."""
    n = name.lower()
    # Strip tracker site prefixes (e.g. "www.UIndex.org    -    ", "www.Torrenting.com - ")
    n = re.sub(r'^www\.[a-z0-9.-]+\s+-\s+', '', n)
    for ext in _VIDEO_EXTS:
        if n.endswith(ext):
            n = n[:-len(ext)]
    n = re.sub(r'\[eztvx?\.to\]', '', n)
    n = re.sub(r'\[eztv\]', '', n)
    n = re.sub(r'\[rartv\]', '', n)
    n = re.sub(r'[._\s]+', ' ', n).strip()
    return n



def _build_queue_title_map():
    """Build a map of downloadId -> (media title, year) from all arr queues.

    When a torrent has a cryptic internal filename (e.g. "maxhd-mip.1080p.mkv"
    for "Midnight in Paris"), the arr queue knows the actual movie/series title
    via its media ID. We use this as a fallback for name matching.
    """
    title_map = {}
    for arr_name, info in ARRS.items():
        queue = arr_get_queue(arr_name)
        if not queue:
            continue
        for rec in queue.get("records", []):
            dl_id = (rec.get("downloadId") or "").lower()
            if not dl_id:
                continue
            if info["type"] == "radarr":
                movie = rec.get("movie", {})
                title = movie.get("title", "")
                year = movie.get("year", "")
            else:
                series = rec.get("series", {})
                title = series.get("title", "")
                year = series.get("year", "")
            if title:
                title_map[dl_id] = (title, str(year) if year else "")
    return title_map

def check_decypharr_path_mismatch(state):
    """Auto-fix decypharr torrents stuck due to path mismatch.

    Decypharr expects files at __all__/{torrent_name}.{ext} (flat file) but
    zurg creates __all__/{torrent_name}/{file} (directory containing the file).
    This causes torrents to time out in the symlink phase (progress=1, no
    status). We fix them by creating the symlinks that decypharr failed to
    create, then trigger arr scans so the media gets imported.
    """
    last_run = state.get('path_mismatch_last_run', 0)
    now = time.time()
    if now - last_run < PATH_MISMATCH_COOLDOWN:
        return

    # 1. Get stuck torrents from decypharr API + scan download dirs
    api_stuck = []
    try:
        r = requests.get(f'{DECYPHARR_URL}/api/torrents',
                         params={'limit': DECYPHARR_API_LIMIT}, timeout=15)
        if r.ok:
            data = r.json()
            if isinstance(data, list):
                torrents = data
            elif isinstance(data, dict):
                torrents = data.get('torrents', [])
                if not isinstance(torrents, list):
                    torrents = []
            else:
                torrents = []
            api_stuck = [t for t in torrents
                         if isinstance(t, dict) and t.get('progress') == 1
                         and not t.get('status')]
    except Exception as e:
        log.warning(f'path_mismatch: could not fetch decypharr torrents: {e}')

    # Also scan download dirs for empty/broken entries (catches cases where
    # decypharr marks the torrent "downloaded" but the symlink is dangling)
    fs_stuck = []
    for dl_dir in DOWNLOAD_DIRS:
        if not os.path.isdir(dl_dir):
            continue
        cat = os.path.basename(dl_dir)
        try:
            entries = os.listdir(dl_dir)
        except OSError:
            continue
        for entry in entries:
            entry_path = os.path.join(dl_dir, entry)
            if not os.path.isdir(entry_path):
                continue
            has_valid_video = False
            try:
                for f in os.listdir(entry_path):
                    fpath = os.path.join(entry_path, f)
                    ext = os.path.splitext(f)[1].lower()
                    if ext in _VIDEO_EXTS:
                        if os.path.islink(fpath) and os.path.exists(fpath):
                            has_valid_video = True
                            break
                        elif not os.path.islink(fpath) and os.path.isfile(fpath):
                            has_valid_video = True
                            break
            except OSError:
                continue
            if not has_valid_video:
                fs_stuck.append({'name': entry, 'category': cat, '_from_fs': True})

    # Merge: API stuck + filesystem-detected broken dirs (deduplicate by name)
    seen_names = set()
    stuck = []
    for t in api_stuck:
        name = t.get('name', '')
        if name and name not in seen_names:
            seen_names.add(name)
            stuck.append(t)
    for t in fs_stuck:
        name = t.get('name', '')
        if name and name not in seen_names:
            seen_names.add(name)
            stuck.append(t)

    if not stuck:
        state['path_mismatch_last_run'] = now
        return

    # 2. Get zurg directory listing
    zurg_dirs = _zurg_list_dirs()
    if not zurg_dirs:
        log.warning('path_mismatch: could not get zurg directory listing')
        return

    norm_to_dir = {}
    for d in zurg_dirs:
        norm_to_dir[_normalize_torrent_name(d)] = d

    state['path_mismatch_last_run'] = now

    # Build downloadId -> media title map for fallback matching
    queue_titles = _build_queue_title_map()

    fixed = 0
    already_ok = 0
    no_match = 0
    no_video = 0
    errors = 0
    fixed_names = []
    categories_touched = set()

    for t in stuck:
        name = t.get('name', '')
        if not name:
            continue
        cat = t.get('category', '') or 'movies-1080p'
        norm = _normalize_torrent_name(name)

        # Exact match first
        zurg_dir = norm_to_dir.get(norm)

        # Fuzzy: longest common prefix
        if not zurg_dir:
            best = None
            best_len = 0
            for key, val in norm_to_dir.items():
                common = 0
                for a, b in zip(norm, key):
                    if a == b:
                        common += 1
                    else:
                        break
                if common > best_len and common > 20:
                    best_len = common
                    best = val
            if best and best_len > len(norm) * 0.6:
                zurg_dir = best

        if not zurg_dir:
            # Fallback: match via arr queue title (handles cryptic filenames)
            dl_id = (t.get("info_hash") or "").lower()
            media_info = queue_titles.get(dl_id) if dl_id else None
            if media_info:
                media_title, media_year = media_info
                search = _normalize_torrent_name(media_title)
                for key, val in norm_to_dir.items():
                    if search in key or (media_year and search in key and media_year in key):
                        zurg_dir = val
                        break
            if not zurg_dir:
                no_match += 1
                continue

        video_file = _zurg_find_video(zurg_dir)
        if not video_file:
            no_video += 1
            continue

        # Strip video extension from name to get the directory name
        dir_name = name
        for ext in _VIDEO_EXTS:
            if dir_name.lower().endswith(ext):
                dir_name = dir_name[:-len(ext)]
                break

        # Map category to download dir (inside babysitarr container: /downloads/{cat})
        download_dir = os.path.join('/downloads', cat, dir_name)
        zurg_path = os.path.join(ZURG_BASE_PATH, zurg_dir, video_file)
        symlink_path = os.path.join(download_dir, video_file)

        if os.path.islink(symlink_path) or os.path.exists(symlink_path):
            already_ok += 1
            continue

        try:
            os.makedirs(download_dir, exist_ok=True)
            os.symlink(zurg_path, symlink_path)
            # Set ownership to UID 1000 so radarr/sonarr can read
            os.lchown(download_dir, 1000, 1000)
            os.lchown(symlink_path, 1000, 1000)
            fixed += 1
            fixed_names.append(name)
            categories_touched.add(cat)
            log.info(f'path_mismatch: fixed {name[:80]} -> {zurg_dir}/{video_file}')
        except Exception as e:
            log.error(f'path_mismatch: failed to create symlink for {name[:80]}: {e}')
            errors += 1

    if fixed == 0:
        if no_match or no_video:
            log.info(f'path_mismatch: {len(stuck)} stuck, {already_ok} already ok, '
                     f'{no_match} no zurg match, {no_video} no video found')
        return

    # 3. Trigger DownloadedMovieScan / DownloadedEpisodeScan in relevant arrs
    scanned_arrs = []
    for arr_name, info in ARRS.items():
        # Match arr to categories
        arr_cats = []
        if info['type'] == 'radarr':
            arr_cats = ['movies-1080p', 'movies-4k']
        elif info['type'] == 'sonarr':
            arr_cats = ['shows-1080p', 'shows-4k']
        if not categories_touched.intersection(arr_cats):
            continue
        scan_cmd = 'DownloadedMoviesScan' if info['type'] == 'radarr' else 'DownloadedEpisodesScan'
        try:
            arr_post(arr_name, 'command', {'name': scan_cmd})
            scanned_arrs.append(arr_name)
        except Exception:
            pass

    log_action(state, 'path_mismatch_fix',
               f'Fixed {fixed} stuck symlinks ({already_ok} already ok, '
               f'{no_match} no match, {no_video} no video, {errors} errors). '
               "Triggered scan in: " + (", ".join(scanned_arrs) or "none"))

    body_lines = [f'Auto-fixed {fixed} decypharr path-mismatch torrent(s):']
    for n in fixed_names[:20]:
        body_lines.append(f'  \u2022 {n[:80]}')
    if len(fixed_names) > 20:
        body_lines.append(f'  ... and {len(fixed_names) - 20} more')
    if scanned_arrs:
        body_lines.append("\nTriggered DownloadedScan in: " + ", ".join(scanned_arrs))
    if no_match:
        body_lines.append(f'\n{no_match} torrent(s) had no matching zurg directory (still being processed by RD)')
    send_notification('Path mismatch: symlinks fixed', '\n'.join(body_lines), level='info')




# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    global _last_cycle_time, _shutdown_requested

    log.info("=" * 50)
    log.info("Babysitarr starting")
    log.info(f"Monitoring {len(ARRS)} arrs, check interval {CHECK_INTERVAL}s")
    log.info(f"Download dirs: {DOWNLOAD_DIRS}")
    log.info("=" * 50)

    state = load_state()
    start_web_ui(state)

    # Run initial scan so the UI has data immediately
    threading.Thread(target=_background_scan, daemon=True).start()

    while not _shutdown_requested:
        state["cycle_count"] = state.get("cycle_count", 0) + 1
        state["last_check"] = datetime.now().isoformat()
        cycle = state["cycle_count"]

        log.info(f"--- Cycle {cycle} ---")

        # Order matters: fix things before detecting stalls, so we don't
        # restart an arr that's about to be auto-imported.

        try:
            check_container_health(state)
        except Exception as e:
            log.error(f"check_container_health failed: {e}")

        try:
            check_host_health(state)
        except Exception as e:
            log.error(f"check_host_health failed: {e}")

        try:
            check_decypharr_version(state)
        except Exception as e:
            log.error(f"check_decypharr_version failed: {e}")

        try:
            check_decypharr_cpu_watchdog(state)
        except Exception as e:
            log.error(f"check_decypharr_cpu_watchdog failed: {e}")

        try:
            check_stuck_downloads(state)
        except Exception as e:
            log.error(f"check_stuck_downloads failed: {e}")

        try:
            check_decypharr_orphans(state)
        except Exception as e:
            log.error(f"check_decypharr_orphans failed: {e}")

        try:
            check_looping_torrents(state)
        except Exception as e:
            log.error(f"check_looping_torrents failed: {e}")

        try:
            check_symlink_loops(state)
        except Exception as e:
            log.error(f"check_symlink_loops failed: {e}")

        try:
            check_decypharr_path_mismatch(state)
        except Exception as e:
            log.error(f"check_decypharr_path_mismatch failed: {e}")

        # Auto-fix checks run BEFORE stall detection
        try:
            check_stale_queue(state)
        except Exception as e:
            log.error(f"check_stale_queue failed: {e}")

        try:
            check_stuck_queue_items(state)
        except Exception as e:
            log.error(f"check_stuck_queue_items failed: {e}")

        try:
            check_unparseable_imports(state)
        except Exception as e:
            log.error(f"check_unparseable_imports failed: {e}")

        try:
            check_delayed_queue(state)
        except Exception as e:
            log.error(f"check_delayed_queue failed: {e}")

        try:
            check_indexer_health(state)
        except Exception as e:
            log.error(f"check_indexer_health failed: {e}")

        # Stall detection runs AFTER auto-fixes, so items that were just handled
        # are no longer counted as "stuck"
        try:
            check_stalled_imports(state)
        except Exception as e:
            log.error(f"check_stalled_imports failed: {e}")

        try:
            check_deleted_files(state)
        except Exception as e:
            log.error(f"check_deleted_files failed: {e}")

        try:
            check_library_files(state)
        except Exception as e:
            log.error(f"check_library_files failed: {e}")

        try:
            check_rd_health(state)
        except Exception as e:
            log.error(f"check_rd_health failed: {e}")

        try:
            check_missing_searched(state)
        except Exception as e:
            log.error(f"check_missing_searched failed: {e}")

        save_state(state)
        _last_cycle_time = datetime.now().isoformat()

        # Update web UI state reference + refresh cached scan
        StatusHandler.state_ref = state
        threading.Thread(target=_background_scan, daemon=True).start()

        log.info(f"Cycle {cycle} complete. Sleeping {CHECK_INTERVAL}s...")

        # Sleep in small increments to allow graceful shutdown
        sleep_end = time.time() + CHECK_INTERVAL
        while time.time() < sleep_end and not _shutdown_requested:
            time.sleep(1)

    # Graceful shutdown: save state before exiting
    log.info("Saving state before exit...")
    save_state(state)
    log.info("Babysitarr stopped.")

if __name__ == "__main__":
    main()
