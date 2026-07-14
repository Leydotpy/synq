"""Production overrides.

Select these settings explicitly with
``DJANGO_SETTINGS_MODULE=conf.settings.production``.
"""

from urllib.parse import parse_qs, unquote, urlparse

from django.core.exceptions import ImproperlyConfigured

from .base import *  # noqa: F403


def require_production_env(name: str) -> str:
    value = os.getenv(name, "").strip()  # noqa: F405
    if not value:
        raise ImproperlyConfigured(f"{name} must be configured in production.")
    return value


SECRET_KEY = require_production_env("DJANGO_SECRET_KEY")
CLERK_SECRET_KEY = require_production_env("CLERK_SECRET_KEY")
ALLOWED_HOSTS = env_list("DJANGO_ALLOWED_HOSTS")  # noqa: F405
if not ALLOWED_HOSTS:
    raise ImproperlyConfigured(
        "DJANGO_ALLOWED_HOSTS must list at least one production host."
    )

database_url = urlparse(require_production_env("DATABASE_URL"))
if database_url.scheme not in {"postgres", "postgresql"}:
    raise ImproperlyConfigured("DATABASE_URL must use PostgreSQL in production.")
database_query = parse_qs(database_url.query)
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": unquote(database_url.path.lstrip("/")),
        "USER": unquote(database_url.username or ""),
        "PASSWORD": unquote(database_url.password or ""),
        "HOST": database_url.hostname or "",
        "PORT": str(database_url.port or 5432),
        "CONN_MAX_AGE": int(os.getenv("DJANGO_DB_CONN_MAX_AGE", "60")),  # noqa: F405
        "CONN_HEALTH_CHECKS": True,
        "OPTIONS": {
            "sslmode": database_query.get("sslmode", ["require"])[0],
        },
    }
}

DEBUG = False
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True
SECURE_SSL_REDIRECT = env_bool("DJANGO_SECURE_SSL_REDIRECT", default=True)  # noqa: F405
SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
SECURE_HSTS_SECONDS = int(os.getenv("DJANGO_SECURE_HSTS_SECONDS", "31536000"))  # noqa: F405
SECURE_HSTS_INCLUDE_SUBDOMAINS = env_bool(  # noqa: F405
    "DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS",
    default=True,
)
SECURE_HSTS_PRELOAD = env_bool("DJANGO_SECURE_HSTS_PRELOAD", default=False)  # noqa: F405
