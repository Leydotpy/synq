"""Django settings for the meeting platform bootstrap."""

from __future__ import annotations

import json
import os
from pathlib import Path

from decouple import config, Csv


def env_value(*names: str, default: str | None = None) -> str | None:
    """Return the first defined environment variable from the supplied names."""

    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable with a sensible default."""

    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def env_list(name: str, default: list[str] | None = None) -> list[str]:
    """Parse a comma-separated environment variable into a list of strings."""

    raw_value = os.getenv(name)
    if raw_value is None:
        return default or []
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def env_list_any(*names: str, default: list[str] | None = None) -> list[str]:
    """Return a normalized list from the first populated environment variable."""

    raw_value = env_value(*names)
    if raw_value is None:
        return list(default or [])
    return [item.strip().strip("'").strip('"') for item in raw_value.split(",") if item.strip()]


def _config_list(name: str, default: list[str] | None = None) -> list[str]:
    """Return a normalized list setting from env using JSON or comma-separated input."""

    raw_value = config(name, default=None)
    if raw_value in (None, ""):
        return list(default or [])

    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError:
        parsed = raw_value.split(",")

    if isinstance(parsed, str):
        parsed = [parsed]

    if isinstance(parsed, (list, tuple)):
        return [str(item).strip() for item in parsed if str(item).strip()]

    return list(default or [])


BASE_DIR = Path(__file__).resolve().parents[2]

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "django-insecure-bootstrap-meet-secret-key")
DEBUG = env_bool("DJANGO_DEBUG", default=env_bool("DJANGO_DEBUG", default=True))
ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS", default=["127.0.0.1", "localhost"])

AUTH_USER_MODEL = config("DJANGO_AUTH_USER_MODEL", default="users.ClerkUser", cast=str)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_clerk_sdk",
    "django_clerk_sdk.users",
    "corsheaders",
    "guardian",
    "rest_framework",
    "django_celery_results",
    "django_celery_beat",
    "apps.config.Config",
    "apps.meetings.apps.MeetingsConfig",
    "apps.profiles.apps.ProfilesConfig",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    # "django_clerk_sdk.core.auth.clerk.middleware.ClerkMiddleware",
    "core.middleware.janus.JanusSessionMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "conf.urls"
WSGI_APPLICATION = "conf.wsgi.application"
ASGI_APPLICATION = "conf.asgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / os.getenv("DJANGO_SQLITE_NAME", "meet.sqlite3"),
    },
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

AUTHENTICATION_BACKENDS = [
    "django.contrib.auth.backends.ModelBackend",
    "guardian.backends.ObjectPermissionBackend",
]

ANONYMOUS_USER_NAME = "anonymous-meet"

LANGUAGE_CODE = "en-us"
TIME_ZONE = os.getenv("TIME_ZONE", "Africa/Lagos")
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/1")

if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django_redis.cache.RedisCache",
            "LOCATION": REDIS_URL,
            "OPTIONS": {
                "CLIENT_CLASS": "django_redis.client.DefaultClient",
            },
            "KEY_PREFIX": "meet",
        },
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "meet-local-cache",
        },
    }

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "core.api.authentication.SessionOrClerkAuthentication",
    ],
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.IsAuthenticated",
    ],
    "DEFAULT_PAGINATION_CLASS": "core.api.api.StandardResultsSetPagination",
    "PAGE_SIZE": 20,
}

CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "django-db")
CELERY_CACHE_BACKEND = "default"
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_RESULT_SERIALIZER = "json"
CELERY_TIMEZONE = TIME_ZONE
CELERY_TASK_TRACK_STARTED = True
CELERY_TASK_TIME_LIMIT = 60 * 15
CELERY_BEAT_SCHEDULER = "django_celery_beat.schedulers:DatabaseScheduler"

SOCKET_IO_PATH = env_value("SOCKET_IO_PATH", "SOCKETIO_PATH", default="socket.io")
SOCKET_IO_REDIS_URL = env_value("SOCKET_IO_REDIS_URL", "SOCKETIO_REDIS_URL", default=REDIS_URL)
SOCKET_IO_CORS_ALLOWED_ORIGINS = env_list_any(
    "SOCKET_IO_CORS_ALLOWED_ORIGINS",
    "SOCKETIO_CORS_ALLOWED_ORIGINS",
    default=["http://localhost:3000", "http://127.0.0.1:3000"],
)

JANUS_SESSION_URL = os.getenv("JANUS_SESSION_URL", "ws://127.0.0.1:8188")
JANUS_DEFAULT_ROOM_CONFIGURATION = {
    "publishers": int(os.getenv("JANUS_ROOM_PUBLISHERS", "100")),
    "bitrate": int(os.getenv("JANUS_ROOM_BITRATE", "1024000")),
    "audiocodec": os.getenv("JANUS_ROOM_AUDIO_CODEC", "opus"),
    "videocodec": os.getenv("JANUS_ROOM_VIDEO_CODEC", "vp8"),
    "notify_joining": True,
    "audiolevel_ext": True,
    "audiolevel_event": True,
    "audio_active_packets": 100,
    "audio_level_average": 25,
    "videoorient_ext": True,
    "transport_wide_cc_ext": True,
}

MEETING_CONNECTION_STALE_SECONDS = int(os.getenv("MEETING_CONNECTION_STALE_SECONDS", "90"))
MEETING_FRONTEND_BASE_URL = os.getenv("MEETING_FRONTEND_BASE_URL", "http://localhost:3000")
MEETING_FRONTEND_JOIN_PATH = os.getenv("MEETING_FRONTEND_JOIN_PATH", "/meetings/{session_id}")
MEETING_INVITE_MAX_AGE_SECONDS = int(os.getenv("MEETING_INVITE_MAX_AGE_SECONDS", str(60 * 60 * 24 * 7)))
MEET_SERVICE_TOKEN = os.getenv("MEET_SERVICE_TOKEN", "")
MEET_SERVICE_USERNAME = os.getenv("MEET_SERVICE_USERNAME", "law-workspace-service")
MEET_SERVICE_EMAIL = os.getenv("MEET_SERVICE_EMAIL", "law-workspace-service@synq.local")

EMAIL_BACKEND = os.getenv("EMAIL_BACKEND", "django.core.mail.backends.console.EmailBackend")
DEFAULT_FROM_EMAIL = os.getenv("DEFAULT_FROM_EMAIL", "Synq Meet <no-reply@synq.local>")

# Required in production, but allowed to fall back locally so imports and tooling stay usable.
CLERK_SECRET_KEY = config("CLERK_SECRET_KEY", default="clerk-dev-placeholder", cast=str)

# Optional
CLERK_AUTH_PARTIES = _config_list("CLERK_AUTH_PARTIES", default=["http://localhost:3000"])
CLERK_AUDIENCE = _config_list("CLERK_AUDIENCE") or None
# CLERK_JWT_KEY = "..."  # optional Clerk JWT verification key
CLERK_API_URL = config("CLERK_API_URL", default="https://api.clerk.com", cast=str)
CLERK_TIMEOUT_MS = 5000
CLERK_CLOCK_SKEW_IN_MS = 5000
CLERK_CACHE_TIMEOUT = 300

# CORS defaults are intentionally conservative at the shared/base layer.
# Environment-specific modules can opt into allow-all behavior for local
# development or provide an explicit origin allow-list in production.
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOWED_ORIGINS = config("CORS_ALLOWED_ORIGINS", default="", cast=Csv())

CORS_ALLOW_HEADERS = [
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "dnt",
    "origin",
    "user-agent",
    "x-csrftoken",
    "x-requested-with",
]

CORS_ALLOW_METHODS = (
    "DELETE",
    "GET",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
)

SESSION_COOKIE_AGE = 60 * 60 * 24 * 30
SESSION_COOKIE_SAMESITE = "lax"
SESSION_SAVE_EVERY_REQUEST = True
SESSION_COOKIE_SECURE = not DEBUG

EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"