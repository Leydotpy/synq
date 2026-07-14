"""Default settings entrypoint for local development and test tooling.

Production processes must opt in explicitly with
``DJANGO_SETTINGS_MODULE=conf.settings.production``.  Importing production
settings unconditionally here made ``DJANGO_DEBUG`` ineffective and disabled
the ASGI development server's reload/static behavior.
"""

from .base import *  # noqa: F403

try:
    from .local import *  # type: ignore[import-not-found]  # noqa: F403
except ImportError:
    pass
