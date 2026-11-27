# Synq Meet server

Django serves the meeting HTTP API, signed gateway authority, authenticated snapshots and SSE.
Run Django ASGI with `conf.asgi:application`. The separate JRTC signalling gateway owns Janus sessions, media, SDP and ICE.
Celery handles invitations, waiting-room expiry and scheduled meeting endings. The
`run_gateway_commands` and `run_gateway_events` workers handle the signed Broka streams.

This branch requires explicit gateway configuration before a meeting can start.
The retirement report gives the matching client configuration, checks and rollback procedure.
[Legacy signalling retirement](docs/legacy-signalling-cleanup-report.md)

```sh
uv sync --frozen
uv run --frozen python src/manage.py check
uv run --frozen python src/manage.py makemigrations --check --dry-run
uv run --frozen python src/manage.py test apps.meetings --settings=conf.gateway_postgres_test_settings
```

## Development commands

From the repository root, install the locked development dependencies and list the tasks:

```sh
uv sync --locked
uv run --locked poe --help
```

Use `uv run poe <task> [arguments]` for day-to-day development. Add `--locked`
before `poe` to require the committed lockfile. All tasks are declared in
[pyproject.toml](pyproject.toml); Poe is a development dependency, not a production service.
The simple executor inherits the environment prepared by the outer `uv run`,
so parallel tasks do not repeatedly sync the same virtual environment.

| Task | Purpose |
| --- | --- |
| `server` | Existing Django ASGI development server, with reload and static-file handling |
| `serve` | Standalone Uvicorn ASGI server, bound to all interfaces, without reload |
| `worker` / `worker-solo` | Celery worker (prefork / single-process development) |
| `beat` | Celery scheduler; run only one instance per environment |
| `gateway-commands` / `gateway-events` | Separate Django-side signed gateway stream workers |
| `dev-core` | Django, solo Celery worker and beat in parallel |
| `dev` | All five Django-side processes in parallel |
| `manage` / `shell` / `check` | Generic Django management, shell and checks |
| `migrate` / `makemigrations` / `showmigrations` / `migration-check` | Database migration operations |
| `superuser` / `collectstatic` / `setup` | Local administration and setup |
| `test` / `test-meetings` / `verify` | Tests and sequential checks |
| `gateway-drain` / `reconcile-schedule` | Legacy work inventory and read-only schedule preview |
| `gateway-cutover` / `prune-gateway-tickets` | Explicit state-changing maintenance; review arguments before running |

Examples:

```sh
uv run poe server 127.0.0.1:8080
uv run poe worker --concurrency=4
uv run poe gateway-commands --once
uv run poe manage showmigrations
uv run poe test apps.meetings --settings=conf.gateway_postgres_test_settings
uv run poe --dry-run dev
uv run poe dev
```

The full development task requires Linux/WSL (gateway workers use POSIX asyncio
signal handlers), working PostgreSQL and Redis, configured gateway credentials,
enabled gateway commands/events, and a separately running JRTC signalling gateway
and Janus. It does not provision external services, run migrations automatically,
or start any retired Socket.IO/Janus process. Stop the task with Ctrl+C. Do not run
multiple beat schedulers or duplicate the grouped processes in other terminals.
On native Windows, use the individual Django tasks and the solo worker for local
development; Celery itself does not officially support Windows.

For production/Docker/Render, install with `uv sync --locked --no-dev` and
start each service independently using the underlying commands, not the development
group. For example:

```sh
uv run --locked --no-dev python -m uvicorn conf.asgi:application --app-dir src --host 0.0.0.0 --port 8000
uv run --locked --no-dev python src/manage.py run_gateway_commands
uv run --locked --no-dev python src/manage.py run_gateway_events
```

Use the platform's assigned port and lifecycle configuration. Existing frozen
gateway-integration commands remain unchanged and require the separate
`gateway-integration` dependency group.
