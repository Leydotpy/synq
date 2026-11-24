"""Local contract-test configuration; no production services or credentials.

Use PostgreSQL separately for locking and concurrency acceptance.
"""
import os

os.environ.setdefault("DJANGO_EMAIL_USER", "tests@example.invalid")
os.environ.setdefault("DJANGO_EMAIL_PASSWORD", "unused-test-value")

from .settings.base import *  # noqa: F403,E402

DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]

# Meeting fixtures exercise the gateway-only backend. Individual tests disable
# specific components to check failure behavior. Signing keys are test-scoped.
GATEWAY_TICKETS_ENABLED = GATEWAY_ADMISSION_ENABLED = GATEWAY_COMMANDS_ENABLED = True
GATEWAY_RUNTIME_ENABLED = GATEWAY_EVENTS_ENABLED = GATEWAY_STATE_ENABLED = True
