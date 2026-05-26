## EP ZTF XMATCH SERVICE

A pipeline that cross-matches [Einstein Probe (EP)](https://ep.bao.ac.cn) X-ray transient events with optical alerts from the [Zwicky Transient Facility (ZTF)](https://www.ztf.caltech.edu/). When the EP satellite detects a new X-ray source, this service queries the Kowalski alert broker for ZTF optical counterpart candidates — both contemporaneous and archival — and pushes matches to the [SkyPortal/Fritz](https://fritz.science) platform for human review.

---

### Table of Contents

1. [Architecture](#architecture)
2. [External Services](#external-services)
3. [Configuration](#configuration)
4. [Deployment](#deployment)
5. [Initial Setup](#initial-setup)
6. [Services In Detail](#services-in-detail)
7. [Web Interface & User Roles](#web-interface--user-roles)
8. [Database Schema](#database-schema)
9. [Monitoring & Logs](#monitoring--logs)
10. [Reprocessing Events](#reprocessing-events)
11. [Troubleshooting](#troubleshooting)

---

### Architecture

The system runs as five concurrent processes managed by [Supervisor](http://supervisord.org/). Four of these are Python services; one is a watchdog that restarts services daily.

```
                        ┌──────────────────────────┐
                        │   Web API (Flask/Gunicorn)│
                        │   http://localhost:4000   │
                        └──────────────────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                       │
  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐
  │   EP Listener     │  │   EP XMatch       │  │   EP Fritz        │
  │                   │  │                   │  │                   │
  │ Polls EP data     │  │ Queries Kowalski  │  │ Pushes xmatches   │
  │ center every 5min │  │ for ZTF alerts    │  │ to SkyPortal      │
  │ → inserts events  │  │ matching EP events│  │ (Fritz)           │
  └───────────────────┘  └───────────────────┘  └───────────────────┘
              │                      │                       │
              └──────────────────────┼──────────────────────┘
                                     ▼
                        ┌──────────────────────────┐
                        │   SQLite Database         │
                        │   ./data/database.db      │
                        └──────────────────────────┘

  ┌───────────────────┐
  │   Watchdog        │
  │ Restarts the 3    │
  │ services every 24h│
  └───────────────────┘
```

**Data flow summary:**

1. **EP Listener** fetches new events from the EP data center every 5 minutes and stores them in the database with status `pending`.
2. **EP XMatch** picks up `pending` events, performs two cone searches against Kowalski (one archival, one contemporaneous/post-event), and stores matched ZTF alerts as xmatches.
3. **EP Fritz** picks up unprocessed xmatches and pushes them to SkyPortal: it posts each ZTF candidate, imports its full photometry from Kowalski, and annotates the source with EP event metadata.
4. **Watchdog** restarts the three processing services every 24 hours to prevent drift from stale connections.
5. **Web API** provides a read-only interface for humans to browse events and xmatches, plus admin endpoints for management.

---

### External Services

This system depends on three external services. You need credentials for all three before the pipeline can do anything useful.

#### Einstein Probe (EP) Data Center

- **What it is:** The official data distribution platform for the EP satellite. Provides a JSON API of unverified X-ray transient candidates.
- **Endpoint:** `https://ep.bao.ac.cn/ep/data_center/api/unverified_candidates`
- **Access:** By invitation only. To request access, contact:
  - Tomas Ahumada — tahumada@astro.umd.edu
  - Lin Yan — lyan@caltech.edu
- **Credentials needed:** `EP_EMAIL` and `EP_PASSWORD` (used to obtain a session token each time the listener starts)

#### Kowalski

- **What it is:** A Caltech-hosted astronomical alert broker that stores and indexes ZTF alert streams, enabling cone searches and temporal queries on millions of alerts.
- **Access:** Request a token from:
  - Thomas Culino — tculino@caltech.edu
  - Théophile du Laz — theophile.dulaz@gmail.com
- **Credentials needed:** `KOWALSKI_TOKEN`
- **Notes:** Kowalski queries are performed via the [`penquins`](https://github.com/dmitryduev/penquins) Python library. The service sends batched cone searches using up to 4 parallel threads. Kowalski can occasionally be slow or time out under load — the service handles this gracefully by requeueing events (see [Troubleshooting](#troubleshooting)).

#### SkyPortal / Fritz

- **What it is:** A web-based platform for storing, annotating, and reviewing astronomical transients. This service uses it as the destination for ZTF-EP cross-match results.
- **Endpoint:** Configured via `FRITZ_HOST` (typically `https://fritz.science`)
- **Access:** Request credentials from:
  - Thomas Culino — tculino@caltech.edu
  - Théophile du Laz — theophile.dulaz@gmail.com
- **Credentials needed:**
  - `FRITZ_TOKEN` — API bearer token
  - `FRITZ_FILTER_ID` — integer ID of the ZTF+EP filter in Fritz (must already exist)
  - `FRITZ_IMPORT_GROUP_ID` — integer ID of the group used to import ZTF photometry from Kowalski

---

### Configuration

All configuration is via environment variables. Set these in `docker-compose.yaml` (see [Deployment](#deployment)).

#### Required

| Variable | Description |
|---|---|
| `EP_EMAIL` | Email used to authenticate with the EP data center API |
| `EP_PASSWORD` | Password for EP data center |
| `KOWALSKI_TOKEN` | API token for Kowalski |
| `FRITZ_HOST` | SkyPortal base URL, e.g. `https://fritz.science` |
| `FRITZ_TOKEN` | SkyPortal API token |
| `FRITZ_FILTER_ID` | Integer ID of the ZTF+EP filter in SkyPortal |
| `FRITZ_IMPORT_GROUP_ID` | Integer ID of the Kowalski import group in SkyPortal |

#### Optional (with defaults)

| Variable | Default | Description |
|---|---|---|
| `DELTA_T` | `1.0` | Days **before** the EP event to start the ZTF cone search. Widening this increases sensitivity to fast fading transients but also false positives. |
| `DELTA_T_ARCHIVAL` | `31.0` | Days used for both the archival search (how far back before `DELTA_T`) and the post-event search window. Controls how many historical ZTF alerts are included. |
| `RADIUS_MULTIPLIER` | `1.0` | Scale factor applied to the EP position error to set the cone search radius. `radius = pos_err_degrees × 3600 × RADIUS_MULTIPLIER` arcsec. Increase if you suspect EP astrometry is underestimated. |
| `MAX_DT_XMATCH_NONADMIN` | `60.0` | Minutes. Non-admin users (`external`, `partner`) can only see xmatches where the ZTF alert occurred within this window of the EP event. Admins see all xmatches. |
| `MAX_EVENT_AGE` | `31.0` | Days. Only push xmatches to Fritz if the associated EP event is younger than this. Prevents flooding Fritz with ancient events on reprocess. |
| `MAX_CREATED_AFTER` | `1.0` | Days. Only push xmatches to Fritz if they were created (in the local DB) within this window. Combined with `MAX_EVENT_AGE` this controls Fritz throughput. |

---

### Deployment

#### With Docker (recommended)

```bash
# 1. Clone the repository
git clone https://github.com/Theodlz/ep-ztf-xmatch.git
cd ep-ztf-xmatch

# 2. Create your docker-compose.yaml from the template
cp docker-compose.default.yaml docker-compose.yaml

# 3. Fill in all required environment variables in docker-compose.yaml
#    (EP credentials, Kowalski token, Fritz token + IDs)
nano docker-compose.yaml

# 4. Start the service
docker compose up -d

# 5. Create the initial admin user (one-time step — see Initial Setup below)
docker exec ep-ztf-xmatch uv run python db.py --init \
    --adminusername YOUR_USERNAME \
    --adminpassword YOUR_PASSWORD

# 6. Check the web interface
open http://localhost:4000
```

The `./data/` directory is mounted as a volume so the SQLite database survives container restarts.

> **Note:** The `docker-compose.default.yaml` template includes `API_USERNAME` and `API_PASSWORD` environment variables. These are **not read by any service** — they are leftover placeholders. The admin account is created via `db.py --init` (step 5 above). You can ignore or remove those two lines from your `docker-compose.yaml`.

#### Without Docker (local development)

Prerequisites: Python 3.13+, [uv](https://docs.astral.sh/uv/) package manager.

```bash
# Install uv if needed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Create the data directory
mkdir -p data

# Run database migrations
uv run python migrate.py

# Create the initial admin user
uv run python db.py --init --adminusername admin --adminpassword admin

# Set environment variables (or use a .env file / export them in your shell)
export KOWALSKI_TOKEN=...
export EP_EMAIL=...
# ... (all variables from the Configuration section)

# Start all services via supervisord
uv run supervisord -c supervisord.conf

# OR start each service individually in separate terminals:
uv run python api.py          # Web UI on port 4000
uv run python ep_listener.py  # EP event ingestion
uv run python ep_xmatch.py    # Kowalski cross-matching
uv run python ep_fritz.py     # SkyPortal push
```

---

### Initial Setup

After the first deployment, the database tables exist but there are no users. You must create the first admin account manually:

**Docker:**
```bash
docker exec ep-ztf-xmatch uv run python db.py --init \
    --adminusername YOUR_USERNAME \
    --adminpassword YOUR_PASSWORD
```

**Local:**
```bash
uv run python db.py --init --adminusername YOUR_USERNAME --adminpassword YOUR_PASSWORD
```

This creates a `caltech`-type admin user. Without this step, the web interface will show the login page but no credentials will work.

To add more users after that, use the web API (admin credentials required):

```bash
curl -u YOUR_USERNAME:YOUR_PASSWORD \
     -X POST http://localhost:4000/api/users \
     -H "Content-Type: application/json" \
     -d '{"username": "alice", "password": "secret", "email": "alice@example.com", "type": "external"}'
```

User types: `caltech` (admin), `external`, `partner`. Non-admin users have a restricted view (see [Web Interface & User Roles](#web-interface--user-roles)).

---

### Services In Detail

#### EP Listener (`ep_listener.py`)

Polls the EP data center every **5 minutes** for new unverified X-ray candidates. Each event has a `name` and `version` — the same event can be updated by EP (e.g., refined position), and each version is stored separately. New events enter the database with `query_status = 'pending'`.

Required fields per EP event: `name`, `ra`, `dec`, `pos_err`, `obs_start`, `exp_time`, `flux`, `src_id`, `src_significance`, `bkg_counts`, `net_counts`, `net_rate`, `version`. Events missing any of these are skipped with a warning.

#### EP XMatch (`ep_xmatch.py`)

Processes events from the database in a tight loop (checks every 5 seconds). For each event it runs two Kowalski cone searches:

**Archival search** — looks for ZTF alerts detected *before* the EP event. Time window: `[event_jd − DELTA_T − DELTA_T_ARCHIVAL, event_jd − DELTA_T]`. These are marked `archival = 1` in the database and are only shown to `caltech` users in the web UI.

**Standard (contemporaneous) search** — looks for ZTF alerts *around and after* the EP event. Time window: `[event_jd − DELTA_T, event_jd + DELTA_T_ARCHIVAL]`.

Cone search radius: `pos_err_degrees × 3600 × RADIUS_MULTIPLIER` arcsec.

**Alert quality filters applied by the xmatch service:**

| Filter | Threshold | Purpose |
|---|---|---|
| `rb` (random forest real/bogus) | > 0.3 | Remove obvious artifacts |
| `drb` (deep learning real/bogus) | > 0.5 | Remove subtle artifacts |
| `isdiffpos` | true | Only difference-image detections |
| `ssdistnr` / `ssmagnr` | < 0 or ≥ 12 / ≥ 21 | Remove solar system objects |
| `sgscore` + `distpsnr` | sgscore > 0.7 AND distpsnr ≤ 2″ | Remove stars |
| Red star color cut | based on PS1 colors | Remove red stellar contaminants |

Events are automatically re-queried as long as they are < 31 days old and were last queried > 10 minutes ago — this catches ZTF alerts that arrive late in the stream.

#### EP Fritz (`ep_fritz.py`)

Runs every **60 seconds** and processes xmatches that have not yet been sent to SkyPortal (`to_skyportal = 0`). For each xmatch it:

1. **Posts the candidate** to SkyPortal (`POST /api/candidates`) with the ZTF object ID, coordinates, `drb` score, and the EP filter ID.
2. **Imports full photometry + cutouts** from Kowalski into SkyPortal (`POST /api/alerts/{object_id}`).
3. **Annotates the source** with EP event metadata (`POST` or `PUT /api/sources/{object_id}/annotations`):
   - `delta_t` — time difference between EP event and ZTF detection (JD)
   - `distance_arcmin` — angular separation
   - `drb`, `age`, `sgscore`, `distpsnr`, `ssdistnr`, `ssmagnr`, `ndethist`
   - `ep_mjd` — EP event time in MJD

The service handles SkyPortal rate limiting automatically (backs off on HTTP 429 / 503) and retries on timeout.

#### Watchdog

A simple bash loop in supervisord that sleeps 24 hours, then sends a graceful restart to the three processing services. This prevents any long-running state from accumulating (stale Kowalski connections, memory growth, etc.). All services handle `SIGTERM` cleanly by finishing their current operation before exiting.

---

### Web Interface & User Roles

The service is a full browser-based web application, not just an API. It is available at `http://localhost:4000` (or wherever you expose port 4000 — in production it runs behind a reverse proxy that handles HTTPS).

The first page you see is a login form. After authenticating, you land on the Events list.

**Pages:**

| Route | Description |
|---|---|
| `/events` | Paginated, filterable table of all EP events. Shows each event's coordinates, position error, observation time, and how many ZTF xmatches were found. |
| `/events/<event_name>` | Detail page for one event. Shows a table of all associated ZTF xmatches with magnitude, filter, time, coordinates, `drb`, age, `sgscore`, solar-system proximity, and angular separation. `caltech` users also see an archival matches section below. |
| `/candidates` | Consolidated table of all xmatches across all events, ordered by detection time. A quick way to scan recent activity across the whole sky. |

**Row color coding** on event and candidates pages:
- **Grey** — the ZTF alert is close to a known solar system object (`ssdistnr ≥ 0`)
- **Cyan** — the ZTF alert is likely a star (`sgscore > 0.7` AND `distpsnr ≤ 2″`)
- **White** — likely an extragalactic transient (nominal candidate)

**User roles and what they see:**

| Role | Archival xmatches | Time window filter | ZTF object links | Event-list filters |
|---|---|---|---|---|
| `caltech` | Yes | None — full DELTA_T range | → Fritz | All filter controls |
| `partner` | No | Limited to `MAX_DT_XMATCH_NONADMIN` (default 60 min) | → Fritz | matchesOnly + ignoreArchival controls |
| `external` | No | Same as `partner` | → ALeRCE | No filter controls (forced to show only events with matches) |

`partner` is intended for ZTF collaborators who have Fritz access. `external` is intended for the EP team, who are directed to [ALeRCE](https://alerce.online) instead of Fritz since they may not have Fritz accounts.

**Admin API endpoints** (require `caltech` credentials via HTTP Basic Auth):

| Method | Route | Description |
|---|---|---|
| `GET` | `/api/ping` | Health check |
| `POST` | `/api/users` | Create a new user |
| `GET` | `/api/users` | List all users |
| `GET` | `/api/events/<name>` | Get event + xmatches as JSON |
| `POST` | `/api/reprocess` | Drop all xmatches and requeue all events (see [Reprocessing](#reprocessing-events)) |

---

### Database Schema

SQLite database at `./data/database.db`. Migrations run automatically on startup (`migrate.py`).

#### `users`

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER | Primary key |
| `username` | TEXT | Unique |
| `password` | TEXT | Stored in plaintext — known limitation |
| `email` | TEXT | Unique |
| `type` | TEXT | `caltech`, `external`, or `partner` |

#### `events`

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER | Primary key |
| `name` | TEXT | EP event name |
| `ra`, `dec` | REAL | Degrees (ICRS) |
| `pos_err` | REAL | Position uncertainty in degrees |
| `obs_start` | TIMESTAMP | EP observation start time |
| `exp_time` | REAL | Exposure time (seconds) |
| `flux`, `src_id`, `src_significance`, `bkg_counts`, `net_counts`, `net_rate` | REAL/INT | EP source properties |
| `version` | TEXT | Event version from EP |
| `query_status` | TEXT | `pending` / `processing` / `done` / `reprocess` / `failed: <msg>` |
| `last_queried` | TIMESTAMP | When the xmatch service last ran a cone search |

Unique constraint: `(name, version)`.

#### `xmatches`

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER | Primary key |
| `event_id` | INTEGER | FK → events |
| `candid` | INTEGER | ZTF candidate ID |
| `object_id` | TEXT | ZTF object ID (e.g., `ZTF21abcdefg`) |
| `jd` | REAL | Julian Date of the ZTF detection |
| `ra`, `dec` | REAL | ZTF alert coordinates |
| `fid` | INTEGER | ZTF filter: 1=g, 2=r, 3=i |
| `magpsf`, `sigmapsf` | REAL | PSF magnitude and error |
| `drb` | REAL | Deep learning real/bogus score (0–1, higher = more real) |
| `delta_t` | REAL | ZTF JD − EP event JD (days) |
| `distance_arcmin` | REAL | Angular separation from EP position |
| `distance_ratio` | REAL | `distance_arcmin / (pos_err_deg × 60)` |
| `age` | REAL | JD since first ZTF detection of this object |
| `sgscore`, `distpsnr` | REAL | Star/galaxy classifier and distance to nearest Pan-STARRS source |
| `ssdistnr`, `ssmagnr` | REAL | Solar system object proximity/mag |
| `ndethist` | INTEGER | Number of historical ZTF detections |
| `archival` | INTEGER | 0 = contemporaneous/post-event, 1 = archival (pre-event) |
| `to_skyportal` | INTEGER | 0 = not yet pushed to Fritz, 1 = done |

Unique constraint: `(event_id, candid)`.

---

### Monitoring & Logs

When running via supervisord (Docker or local), logs are written to the `log/` directory:

| File | Service |
|---|---|
| `log/supervisord.log` | Supervisor daemon itself |
| `log/api.log` | Web API / Gunicorn |
| `log/ep_listener.log` | EP event ingestion |
| `log/ep_xmatch.log` | Kowalski cross-matching |
| `log/ep_fritz.log` | SkyPortal push |

**Docker:**
```bash
# Tail all logs
docker compose logs -f

# Tail a specific service
docker exec ep-ztf-xmatch tail -f log/ep_xmatch.log
```

**Check which supervisord-managed processes are running:**
```bash
docker exec ep-ztf-xmatch /app/.venv/bin/supervisorctl -c /app/supervisord.conf status
```

Expected output when healthy:
```
api                              RUNNING   pid 12, uptime 0:04:23
ep-fritz                         RUNNING   pid 18, uptime 0:04:23
ep-listener                      RUNNING   pid 15, uptime 0:04:23
ep-xmatch                        RUNNING   pid 16, uptime 0:04:23
watchdog                         RUNNING   pid 17, uptime 0:04:23
```

**Health check:**
```bash
curl http://localhost:4000/api/ping
# → {"status": "ok"}
```

**Check event processing status (admin):**
```bash
curl -u admin:password http://localhost:4000/api/events/EP_241001a
```

**What healthy logs look like:**

`ep_listener.log` — every 5 minutes you should see either new events being inserted or "no new events":
```
Fetching events from EP API...
No new events found.
```
or:
```
Inserting event EP_250101a v1 ...
Done. Inserted 1 new events.
```

`ep_xmatch.log` — you should see cone searches being run and xmatches (or empty results) logged:
```
Processing EP_250101a | ra=123.456; dec=-12.345; pos_err=0.05; archival=False; dt=32.00 days; radius=180.00"
Found 0 xmatches for EP_250101a
```
Getting 0 xmatches is completely normal — ZTF only covers parts of the sky each night, and not every EP event will have a coincident ZTF alert.

`ep_fritz.log` — every 60 seconds you should see either matches being pushed or a quiet cycle:
```
No xmatches to process.
```
or:
```
Posting ZTF21abcdefg to SkyPortal for event EP_250101a...
Successfully posted ZTF21abcdefg
```

**Expected end-to-end latency:**
From EP data release → visible in Fritz: roughly **6–8 minutes** in normal conditions.
- EP Listener: up to 5 min (polling interval)
- EP XMatch: ~seconds (Kowalski is fast when healthy)
- EP Fritz: up to 60 sec (polling interval)

**Updating credentials without data loss:**

When you need to rotate a token (e.g., `FRITZ_TOKEN` or `KOWALSKI_TOKEN`), update `docker-compose.yaml` and then run:
```bash
docker compose up -d --force-recreate
```
This restarts the container with new env vars while keeping the `./data/` volume intact. The database is not affected.

**Backing up the database:**

The database is a single file. Copy it out while the service is idle (or at least not mid-write):
```bash
docker exec ep-ztf-xmatch sqlite3 data/database.db ".backup /tmp/backup.db"
docker cp ep-ztf-xmatch:/tmp/backup.db ./backup_$(date +%Y%m%d).db
```

**Updating the service (new code version):**
```bash
git pull
docker compose build
docker compose up -d
```
Migrations run automatically on container start — they are safe to re-run (idempotent).

---

### Reprocessing Events

If you need to re-run the cross-matching from scratch (e.g., after changing `DELTA_T` or `RADIUS_MULTIPLIER`, or after a Kowalski outage), use the reprocess endpoint:

```bash
curl -u YOUR_USERNAME:YOUR_PASSWORD \
     -X POST http://localhost:4000/api/reprocess
```

This will:
1. Delete **all** existing xmatches from the database.
2. Set all events back to `reprocess` status.
3. The EP XMatch service will pick them up and re-run cone searches within minutes.

> **Warning:** This also clears the `to_skyportal` flag, so previously-pushed candidates may be re-sent to Fritz.

**Reprocessing a single event** (without triggering a full reprocess) — reset it directly in the database:
```bash
docker exec ep-ztf-xmatch sqlite3 data/database.db \
    "UPDATE events SET query_status='pending', last_queried=NULL WHERE name='EP_250101a';"
```
The xmatch service will re-run cone searches on the next cycle. Existing xmatches are not deleted — duplicates are silently skipped. Only newly found matches (e.g., late-arriving ZTF alerts) will be added. This does not affect any other events.

---

### Troubleshooting

#### EP data center is unreachable or slow

The EP listener will log an error and retry on the next 5-minute cycle. This is normal during EP data center maintenance windows, which can be unannounced. No action needed — events will be ingested when the API becomes available again.

If the listener seems stuck (no new events for >30 minutes and the EP site is up), restart it:
```bash
docker exec ep-ztf-xmatch supervisorctl restart ep-listener
```

#### Kowalski timeouts or rate limiting

The EP XMatch service logs Kowalski errors and resets affected events back to `pending` so they will be retried automatically. You will see messages like:

```
Kowalski query failed for EP_241001a: timeout
Event EP_241001a reset to pending
```

If many events are stuck in `failed` status, this may indicate a Kowalski outage or expired token. Check with the Kowalski team (see [External Services](#external-services)) and update `KOWALSKI_TOKEN` in `docker-compose.yaml` if needed, then restart the container.

#### Fritz push failing

Check `log/ep_fritz.log`. Common causes:
- **401 Unauthorized** — `FRITZ_TOKEN` has expired. Generate a new one from the Fritz web UI and update the env var.
- **404 on filter/group** — `FRITZ_FILTER_ID` or `FRITZ_IMPORT_GROUP_ID` is wrong. Verify these IDs in the Fritz web UI.
- **429 / 503** — Fritz is rate-limiting the service. The service backs off automatically; no action needed.

#### No users / can't log in after fresh deploy

The initial admin user was not created. Run:
```bash
docker exec ep-ztf-xmatch uv run python db.py --init \
    --adminusername YOUR_USERNAME \
    --adminpassword YOUR_PASSWORD
```

#### Events stuck in `processing` status

This happens if a service crashed mid-query. These events will not be automatically retried (the service only picks up `pending`, `reprocess`, or `done`-but-stale events). Fix by manually resetting them:
```bash
docker exec ep-ztf-xmatch sqlite3 data/database.db \
    "UPDATE events SET query_status='pending' WHERE query_status='processing';"
```
