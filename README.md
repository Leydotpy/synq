# Synq Meet server

The meeting service is a Django/Socket.IO application whose Janus integration
uses `jrtc>=3.1,<4` and the independently packaged `jrtc-video>=3,<4`.

See [docs/jrtc-operations.md](docs/jrtc-operations.md) for the command/event
architecture, runtime ownership rules, broker configuration, deployment units,
failure recovery, and security guidance.

Copy [`.env.example`](.env.example) to an untracked `.env` for local
development and supply the `DATABASE_*` PostgreSQL values there. Process
environment variables take precedence in deployed services; database secrets
must not be placed in Django settings or committed files.

Common validation commands:

```powershell
uv sync --frozen
uv run python src/manage.py check
uv run python src/manage.py makemigrations --check --dry-run
uv run python src/manage.py test apps.meetings.tests
```
