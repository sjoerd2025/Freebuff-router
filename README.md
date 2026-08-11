# Freebuff-router

OpenAI-compatible API gateway for the Freebuff provider. Built with FastAPI,
uvicorn, httpx, and SQLite, it provides streaming chat completions, account
rotation, quota-aware routing, API key authentication, proxy management,
request history, and a browser dashboard.

| | |
|---|---|
| Version | 1.0.0 |
| Python | 3.11+ |
| License | [MIT](#license) |
| Default port | 20130 |

## Features

- OpenAI-compatible `POST /v1/chat/completions` endpoint
- Native Server-Sent Events streaming
- Freebuff model catalog under the `fb/` prefix
- Multiple Freebuff accounts with quota-aware round-robin selection
- Session and agent-run caching
- Automatic session recovery and account cooldown handling
- Device/login URL authentication flow
- Single-token and bulk account import
- Automatic quota synchronization
- API keys with per-key model whitelist, RPM limit, burst limit, and token quota
- Global HTTP and SOCKS5 proxy pool
- Proxy testing, round-robin selection, and automatic disable after five failures
- Bulk proxy import and bulk proxy deletion
- SQLite request history with full request/response bodies
- Lightweight vanilla JavaScript dashboard with no frontend build step
- Circuit breaker for upstream failures

## Supported Models

| Gateway model | Freebuff model | Access mode |
|---|---|---|
| `fb/deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | Limited |
| `fb/mimo-v2.5` | `mimo/mimo-v2.5` | Limited |
| `fb/deepseek-v4-pro` | `deepseek/deepseek-v4-pro` | Full |
| `fb/minimax-m3` | `minimax/minimax-m3` | Full |
| `fb/gpt-5.6-luna` | `openai/gpt-5.6-luna` | Full |
| `fb/glm-5.2` | `z-ai/glm-5.2` | Full |

## Requirements

- Python 3.11 or newer
- Network access to the Freebuff service
- Freebuff authentication token(s)

## Quick Start

```bash
cd Freebuff-router
cp .env.example .env
pip install -r requirements.txt
python -m freebuffrouter
```

The default address is `http://127.0.0.1:20130`.

Sign in at `http://127.0.0.1:20130/dashboard` with the default password
`123456`, import your Freebuff tokens, and start proxying:

```bash
curl http://127.0.0.1:20130/v1/chat/completions \
  -H "Authorization: Bearer gw-..." \
  -H "Content-Type: application/json" \
  -d '{
    "model": "fb/deepseek-v4-flash",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": true
  }'
```

## Installation

### Windows

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start

```bash
python -m freebuffrouter
```

The default address is `http://127.0.0.1:20130`.

The dashboard and `/login` use a dashboard password stored in the database.
On first startup the default password is:

```text
123456
```

Change it after logging in via `POST /api/dashboard/password` (see below).

On first startup, an admin API key is generated and written to
`bootstrap-key.txt`. Use that key as `Authorization: Bearer <key>` for API
requests. API keys remain separate from the dashboard password.

## Configuration

The service reads configuration from environment variables and from a `.env`
file in the working directory. To get started with a local configuration file:

```bash
cp .env.example .env
```

Then edit `.env` with your values. Every variable is optional — defaults are
used when a variable is absent.

| Variable | Default | Description |
|---|---:|---|
| `HOST` | `127.0.0.1` | Listen address |
| `PORT` | `20130` | Listen port |
| `SQLITE_PATH` | `freebuffrouter.db` | SQLite database path |
| `GATEWAY_KEY_FILE` | — | API key file path |
| `GATEWAY_API_KEYS` | — | Comma/newline-separated initial API keys |
| `GATEWAY_AUTH_DISABLE` | — | Set to `1` to disable authentication for development |
| `GATEWAY_NO_AUTOBOOTSTRAP` | — | Set to `1` to disable automatic admin-key generation |
| `COOKIE_SECURE` | `1` | Set to `0` when using cookie sessions over local HTTP |
| `RATE_LIMIT_RPM` | `60` | Global requests per minute per client |
| `RATE_LIMIT_BURST` | `10` | Global burst size |
| `RATE_LIMIT_WINDOW_SEC` | `60` | Sliding window duration in seconds |
| `WORKERS_DISABLED` | — | Set to `1` to disable background quota synchronization |
| `HEALTH_PROBES_DISABLED` | — | Set to `1` to disable upstream health probes |
| `FREEBUFF_DISABLED` | — | Set to `1` to disable Freebuff requests |
| `FREEBUFF_API_BASE` | `https://www.codebuff.com` | Upstream API base URL |
| `FREEBUFF_USER_AGENT` | `Freebuff-CLI/0.0.142` | User-Agent sent upstream |
| `FREEBUFF_UPSTREAM_TIMEOUT` | `25` | Upstream request timeout (seconds) |
| `FREEBUFF_SESSION_TIMEOUT` | `12` | Session endpoint timeout (seconds) |
| `FREEBUFF_NONSTREAM_TIMEOUT` | `50` | Non-streaming request timeout (seconds) |
| `CB_OPEN_THRESHOLD` | `5` | Consecutive errors before circuit opens |
| `CB_OPEN_DURATION` | `60` | Seconds circuit stays open |
| `FB_QUOTA_SYNC_TICK` | `300` | Seconds between quota syncs |
| `MAX_REQUEST_BODY` | `10485760` | Max request body in bytes |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warning`, or `error` |

See `.env.example` for the full documented list with examples.

Example development configuration:

```powershell
$env:COOKIE_SECURE = "0"
$env:GATEWAY_AUTH_DISABLE = "1"
$env:PORT = "20130"
python -m freebuffrouter
```

## API Endpoints

### Chat and Models

| Method | Endpoint | Authentication |
|---|---|---|
| `POST` | `/v1/chat/completions` | API key |
| `GET` | `/v1/models` | API key |
| `GET` | `/health` | Public |

### Freebuff Accounts

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/fb/import` | Import one token |
| `POST` | `/fb/import/bulk` | Import multiple tokens |
| `GET` | `/fb/accounts` | List accounts and quota state |
| `DELETE` | `/fb/accounts/{token}` | Remove an account |
| `POST` | `/fb/quota/sync` | Synchronize quota for one or all accounts |
| `POST` | `/fb/oauth/device/start` | Generate a login URL |
| `GET` | `/fb/oauth/device/poll` | Poll login completion |

Single account import:

```json
{
  "token": "freebuff-auth-token"
}
```

Bulk account import supports either a JSON array or newline-separated raw
values. Each raw line may use:

```text
token|email|userid
```

### API Keys

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/keys` | List API keys |
| `POST` | `/api/keys` | Create an API key |
| `PUT` | `/api/keys/{key}` | Update an API key |
| `DELETE` | `/api/keys/{key}` | Delete an API key |

All API key endpoints require an admin key.

### Dashboard Password

The dashboard password is stored hashed in the `dashboard_credentials` table
and is independent of API keys. The default is `123456`.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/dashboard/password` | Change the dashboard password (admin) |

Request:

```json
{
  "password": "new-secret-password"
}
```

### Proxy Pool

Proxies are global Freebuff upstream proxies, not assigned to individual
accounts. The pool supports HTTP and SOCKS5 proxies with optional
authentication.

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/proxies` | List proxies |
| `POST` | `/api/proxies` | Add one proxy |
| `POST` | `/api/proxies/bulk` | Import multiple proxies |
| `PUT` | `/api/proxies/{id}` | Update a proxy |
| `DELETE` | `/api/proxies/{id}` | Delete one proxy |
| `POST` | `/api/proxies/bulk-delete` | Delete multiple proxies |
| `POST` | `/api/proxies/{id}/toggle` | Enable or disable a proxy |
| `POST` | `/api/proxies/{id}/test` | Test proxy connectivity |

Bulk proxy import accepts one URL per line:

```text
http://user:password@proxy.example.com:8080
socks5://user:password@proxy.example.com:1080
http://proxy.example.com:3128
```

Request for bulk deletion:

```json
{
  "ids": ["proxy-id-1", "proxy-id-2"]
}
```

### Request History

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/history` | Request statistics and model breakdown |
| `GET` | `/history/recent` | Recent request previews |
| `GET` | `/history/detail/{id}` | Full request and response bodies |

## Dashboard

Open `http://127.0.0.1:20130/dashboard` after starting the service. The
dashboard includes:

- Overview with account count, quota summary, and circuit state
- Freebuff account management and quota synchronization
- API key management
- Proxy list with selection, bulk import, test, toggle, and bulk delete
- Request history with full JSON request/response details

For local HTTP development, set `COOKIE_SECURE=0` before starting the service.

## SQLite Storage

The application uses one SQLite database in WAL mode. It stores:

- Freebuff account credentials and quota state
- API key metadata and usage counters
- Global proxy entries and health counters
- Request history with full request/response JSON

Rate-limit buckets and browser sessions are intentionally kept in memory and
are reset when the process restarts.

## Project Layout

```text
freebuffrouter/
├── app.py                 # FastAPI application and routes
├── auth.py                # API key authentication and access control
├── config.py              # Environment configuration
├── health.py              # Circuit breaker and health state
├── httpclient.py          # Shared httpx clients
├── models.py              # Freebuff model catalog
├── proxy_pool.py          # Global proxy pool
├── ratelimit.py           # Rate limiter and browser sessions
├── store.py               # SQLite persistence and request logs
├── dashboard.html         # Browser dashboard
└── freebuff/
    ├── account.py         # Account pool and quota synchronization
    ├── device.py          # Login URL device flow
    ├── proxy.py           # Freebuff request proxy
    ├── session.py         # Session and run caches
    └── transform.py       # Request transformation
```

## License

This project is licensed under the [MIT License](LICENSE).
