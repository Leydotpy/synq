# Synq Meet backend

## Meeting join lifecycle

Room/session setup and participant admission are separate on purpose:

1. `POST /api/v1/meetings/sessions/` creates a room, starts or reuses a live session, and returns a join URL. It does not create or join a participant.
2. `POST /api/v1/meetings/rooms/<slug>/sessions/` starts or reuses a session for an existing room. It does not create or join a participant.
3. `GET /api/v1/meetings/sessions/<session_id>/state/` opens/views the meeting state. It does not join the user.
4. `POST /api/v1/meetings/sessions/<session_id>/admission/` is the Join-button endpoint. It returns `action: "enter"` for hosts, co-hosts, room members, creators, or policy-based direct entry, and `action: "wait"` when approval is required.
5. `POST /api/v1/meetings/sessions/<session_id>/join-requests/` remains the explicit waiting-room request endpoint and should not be used by direct-entry clients.

The frontend should route only from the admission response:

- `action: "enter"` means connect to the live meeting and use the returned participant.
- `action: "wait"` means show the waiting room until a coordinator approves the request.

## Celery

Celery is loaded by `src/conf/celery.py` and imported from `src/conf/__init__.py`, so Django startup registers the Celery app. Meeting tasks live in `src/apps/meetings/tasks.py` and are discovered by `app.autodiscover_tasks()`.

The default broker is Redis:

```powershell
$env:REDIS_URL = "redis://127.0.0.1:6379/1"
$env:CELERY_BROKER_URL = $env:REDIS_URL
```

Start Redis locally, for example:

```powershell
docker run --rm -p 6379:6379 redis:7
```

From `meet/src`, start a worker:

```powershell
..\.venv\Scripts\celery.exe -A conf worker -l info --pool=solo
```

Start beat when periodic tasks such as stale-connection cleanup should run:

```powershell
..\.venv\Scripts\celery.exe -A conf beat -l info
```

Verify task registration:

```powershell
..\.venv\Scripts\celery.exe -A conf inspect registered
```

Trigger a diagnostic task:

```powershell
..\.venv\Scripts\celery.exe -A conf call conf.celery.debug_task
```

For synchronous local debugging without a worker:

```powershell
$env:CELERY_TASK_ALWAYS_EAGER = "true"
$env:CELERY_TASK_EAGER_PROPAGATES = "true"
```

Meeting service code queues Celery work with `transaction.on_commit()` where database records must exist before the worker can load them. If Redis or the configured broker is down, the request path continues, but `dispatch_task()` logs the task name and traceback so the failure is visible in Django logs.

## Invitation email delivery

Meeting invitation email is delivered by Celery, one recipient per task, so SMTP latency does not block the API response. Scheduled meetings first receive a button-free notice. Celery beat checks for meetings whose start time has arrived and queues a second email with a fresh signed join link.

Apply migrations, then run both the worker and beat processes shown above:

```powershell
uv run python src/manage.py migrate
```

Local development uses Django's console email backend by default. To send real email, copy the values from `.env.example` into an untracked `.env` and configure your SMTP provider. The standard Django settings supported here are:

- `EMAIL_BACKEND`
- `EMAIL_HOST` and `EMAIL_PORT`
- `EMAIL_HOST_USER` and `EMAIL_HOST_PASSWORD`
- `EMAIL_USE_TLS` or `EMAIL_USE_SSL` (never both)
- `EMAIL_TIMEOUT`
- `DEFAULT_FROM_EMAIL` and `SERVER_EMAIL`
- `MEETING_EMAIL_REPLY_TO` (optional comma-separated addresses)
- `MEETING_FRONTEND_BASE_URL` (the public frontend origin used in join links)

The project startup scripts load `server/.env` for the Django web process, Celery worker, and Celery beat. Never commit SMTP credentials.
