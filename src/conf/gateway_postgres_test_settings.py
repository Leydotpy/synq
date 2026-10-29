"""Opt-in real PostgreSQL authority/transaction verification; no live Janus."""

import os

from .test_settings import *  # noqa: F403

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("GATEWAY_TEST_DB_NAME", "synq_gateway_test"),
        "USER": os.environ.get("GATEWAY_TEST_DB_USER", "postgres"),
        "PASSWORD": os.environ["GATEWAY_TEST_DB_PASSWORD"],
        "HOST": os.environ.get("GATEWAY_TEST_DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("GATEWAY_TEST_DB_PORT", "5432"),
        "CONN_MAX_AGE": 0,
    }
}
