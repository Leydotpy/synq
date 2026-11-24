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
