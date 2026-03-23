# Babysitarr

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-support-yellow?style=flat&logo=buy-me-a-coffee)](https://buymeacoffee.com/Davidalves)

A self-healing monitoring daemon for the **Radarr/Sonarr + Real-Debrid** pipeline. Babysitarr watches your arr stack around the clock, detects problems, and fixes them automatically — so you don't have to.

Built for setups using **Zurg + Decypharr + Real-Debrid** with Radarr/Sonarr, but the arr monitoring features work with any download client.

## Why?

If you run Radarr/Sonarr with Real-Debrid, you know the pain:

- Downloads get stuck in Decypharr and block all workers
- Torrents loop endlessly (grab → fail → grab → fail)
- Imports stall and nothing gets added to your library
- Debrid-cached files expire and disappear from your library
- Indexers break and nobody searches for anything
- Queue items pile up in broken states

Babysitarr detects all of these and fixes them automatically. When it can't fix something, it alerts you.

## Features

### Auto-healing
- **Stuck downloads** — Detects downloads stuck in Decypharr, clears them, and restarts the service
- **Looping torrents** — Detects torrents caught in grab/fail cycles and blocklists them so the arr moves on to the next release
- **Stalled imports** — Detects when an arr has pending imports but hasn't imported anything recently, restarts it
- **Unparseable imports** — Auto-imports files that Radarr/Sonarr can't parse by inferring quality from the filename
- **Stale queue entries** — Clears items stuck in the queue that were already imported
- **Dead queue entries** — Removes entries where the download is gone and triggers a re-search
- **Stuck queue items** — Detects items with no download progress, removes and re-searches
- **Indexer failures** — Clears stale indexer/import list failure flags from the database
- **TMDb removals** — Auto-removes movies that TMDb has delisted

### Library protection
- **Missing file detection** — Monitors your library for items that lose their files (e.g. RD torrent expires) and automatically triggers a re-search
- **Download folder monitoring** — Tracks files in download directories and distinguishes between:
  - **Upgrades** — old file replaced by better quality (normal, logged only)
  - **Cache expiry** — debrid download expired but media already imported (normal, logged only)
  - **Suspicious deletions** — file disappeared without being imported (alert sent)
- **Real title mapping** — Resolves garbled foreign-language release names to the actual movie/show title from the arr database

### Real-Debrid health
- **Torrent monitoring** — Checks RD for expired or removed torrents that your library depends on

### Notifications
- **Email** — SMTP with rate limiting (configurable cooldown)
- **Discord** — Webhook support with color-coded embeds
- Both can run simultaneously

### Web dashboard
- Live status overview of all monitored arrs
- Action log showing everything Babysitarr has done
- One-click buttons to clear stuck items, re-search, and more
- Health scan showing queue status and indexer health per arr

## Quick start

### 1. Clone

```bash
git clone https://github.com/DAdjadj/Babysitarr.git
cd Babysitarr
```

### 2. Configure

Copy the example env file and fill in your secrets:

```bash
cp .env.example .env
```

Edit `docker-compose.yml` and update:
- Your arr instances (Radarr/Sonarr hostnames, ports, API keys)
- Your Real-Debrid API key
- Volume paths to match your setup
- Notification settings (email and/or Discord)

### 3. Run

```bash
docker compose up -d
```

The dashboard will be available at `http://your-server:8284`.

## Configuration

### Arr instances

Define your arr instances as environment variables. Each value is pipe-separated:

```
name|type|host|port|apikey
```

- **name** — Display name (e.g. `radarr`, `sonarr4k`)
- **type** — `radarr` or `sonarr`
- **host** — Container hostname or IP
- **port** — API port
- **apikey** — Found in arr Settings → General

Example:
```yaml
- RADARR=radarr|radarr|radarr|7878|your-api-key
- RADARR4K=radarr4k|radarr|radarr4k|7878|your-api-key
- SONARR=sonarr|sonarr|sonarr|8989|your-api-key
- SONARR4K=sonarr4k|sonarr|sonarr4k|8989|your-api-key
```

You can add as many instances as you want — the env var name doesn't matter, only the value format.

For backward compatibility, a 4-part format also works (`name|host|port|apikey`), with the type inferred from the name.

### Volumes

| Mount | Purpose | Required |
|-------|---------|----------|
| `/var/run/docker.sock` | Restart containers | Yes |
| `/data` | Persistent state and logs | Yes |
| `/decypharr-config` | Read Decypharr state for stuck download detection | For Decypharr monitoring |
| `/downloads` | Monitor download directories | For download monitoring |
| `/arr-configs/{name}` | Access arr databases for indexer reset | For indexer auto-fix |

### Thresholds

| Variable | Default | Description |
|----------|---------|-------------|
| `CHECK_INTERVAL` | `120` | Seconds between monitoring cycles |
| `STUCK_DOWNLOAD_TIMEOUT` | `600` | Seconds before a download is considered stuck |
| `LOOP_THRESHOLD` | `5` | Retries before a torrent is blocklisted |
| `IMPORT_STALL_TIMEOUT` | `300` | Seconds with no imports before restarting arr |
| `STUCK_QUEUE_TIMEOUT` | `1800` | Seconds before a queue item with no progress is removed |
| `MAX_WORKERS` | `3` | Decypharr max workers (for stuck detection) |
| `DEAD_RETRY_LIMIT` | `3` | Max re-search attempts for dead queue entries |
| `EMAIL_COOLDOWN` | `3600` | Minimum seconds between alert emails |

### Notifications

**Email:**
```yaml
- SMTP_HOST=smtp.gmail.com
- SMTP_PORT=587
- SMTP_USER=you@gmail.com
- SMTP_PASS=your-app-password
- EMAIL_FROM=you@gmail.com
- EMAIL_TO=you@gmail.com
```

**Discord:**
```yaml
- DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

Set both for dual notifications.

### Health endpoint

`GET /health` returns:

```json
{
  "status": "ok",
  "uptime": 86400,
  "last_cycle": "2025-01-01T12:00:00",
  "arrs": {
    "radarr": "reachable",
    "sonarr4k": "unreachable"
  }
}
```

Useful for uptime monitoring (e.g. Uptime Kuma, Healthchecks.io).

## How it works

Babysitarr runs a monitoring cycle every `CHECK_INTERVAL` seconds. Each cycle runs these checks in order:

1. **Stuck downloads** — Reads Decypharr's state file for downloads stuck too long
2. **Looping torrents** — Parses Decypharr logs for repeated grab/delete patterns
3. **Stalled imports** — Compares arr queue state to import history
4. **Download folder** — Scans download dirs for appeared/disappeared files
5. **Library files** — Checks arr APIs for media that lost its files
6. **RD health** — Queries Real-Debrid API for torrent status
7. **Blocklist re-search** — Ensures blocklisted items trigger a new search
8. **Unparseable imports** — Finds queue items arr can't parse and auto-imports them
9. **Stale/dead queue** — Cleans up queue entries that are stuck or orphaned
10. **Stuck queue items** — Removes items with no download progress
11. **Indexer health** — Detects and clears stale indexer failure flags

State is persisted to `/data/babysitarr_state.json` between restarts.

## License

MIT
