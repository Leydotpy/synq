"""Local contract-test configuration; no production services or credentials.

The audited checkout refers to conf.logging, which is absent from its tree.
Disable that deployment hook for tests; this does not certify production startup.
Use PostgreSQL separately for locking/concurrency acceptance.
"""
import os

os.environ.setdefault("DJANGO_EMAIL_USER", "tests@example.invalid")
os.environ.setdefault("DJANGO_EMAIL_PASSWORD", "unused-test-value")

from .settings.base import *  # noqa: F403,E402

LOGGING_CONFIG = None
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
EMAIL_BACKEND = "django.core.mail.backends.locmem.EmailBackend"
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
