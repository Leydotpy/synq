"""
A module to extend Django's `runserver` command with Uvicorn-based ASGI server support.

This module replaces the default WSGI-based development server with an ASGI-compatible
server powered by Uvicorn, enabling features like WebSockets and better performance for
asynchronous operations. It provides custom argument handling and static file serving
logic while retaining compatibility with Django conventions.

Classes:
    Command: Custom implementation of Django's `runserver` command to use Uvicorn.

Functions:
    get_default_application: Utility function to fetch the ASGI application specified
    in Django's `ASGI_APPLICATION` setting.
"""

import datetime
import errno
import importlib
import logging
import os
import sys

# Import Django-native modules
from django.apps import apps
from django.conf import settings
from django.contrib.staticfiles.handlers import ASGIStaticFilesHandler
from django.core.exceptions import ImproperlyConfigured
from django.core.management import CommandError
from django.core.management.commands.runserver import Command as RunserverCommand
from django.db import connections
from django.utils import autoreload
from django.utils.version import get_version
# Import Uvicorn
from uvicorn.config import Config
from uvicorn.server import Server

# Uvicorn does not have a public equivalent of daphne.endpoints.build_endpoint_description_strings
# but we don't need it because uvicorn.run takes host/port directly.
# We also do not need to import Server, we use uvicorn.run for simplicity.

logger = logging.getLogger(__name__)


def get_default_application():
    """
    Gets the default application, set in the ASGI_APPLICATION setting.
    """
    try:
        # Django 5.0+ officially recommends using settings.ASGI_APPLICATION
        path, name = settings.ASGI_APPLICATION.rsplit(".", 1)
    except (ValueError, AttributeError):
        raise ImproperlyConfigured("Cannot find ASGI_APPLICATION setting.")
    try:
        module = importlib.import_module(path)
    except ImportError:
        raise ImproperlyConfigured("Cannot import ASGI_APPLICATION module %r" % path)
    try:
        value = getattr(module, name)
    except AttributeError:
        raise ImproperlyConfigured(
            f"Cannot find {name!r} in ASGI_APPLICATION module {path}"
        )
    return value


class Command(RunserverCommand):
    """
    Extends the RunserverCommand to add support for running an ASGI-based development server with Uvicorn.

    This class customizes the Django development server command to use an ASGI server
    instead of the traditional WSGI server. It integrates Uvicorn for serving ASGI
    applications and provides additional command-line arguments tailored for an ASGI
    environment, such as selecting ASGI or WSGI, setting the number of worker
    processes, and enabling or disabling static file serving.

    Attributes:
        protocol (str): The protocol used by the development server, default is "http".
    """
    protocol = "http"
    help = (
        "Starts a lightweight ASGI development server powered by Uvicorn. "
        "Replaces Django's default WSGI server to enable WebSocket support, "
        "asynchronous request handling, and ASGI application lifespan events. "
        "Includes automatic reloading, static file serving, and Django system checks. "
        "Use --noasgi to fall back to the standard WSGI-based runserver."
    )

    # We override the server_cls in inner_run, but inherit RunserverCommand features

    def add_arguments(self, parser):
        """Extend Django's ``runserver`` arguments with ASGI/Uvicorn options."""

        # Add all standard runserver arguments
        super().add_arguments(parser)

        # Add Uvicorn-specific arguments relevant to development
        # We drop daphne-specific ones like http_timeout

        parser.add_argument(
            "--noasgi",
            action="store_false",
            dest="use_asgi",
            default=True,
            help="Run the old WSGI-based runserver rather than the ASGI-based one",
        )

        # Adding workers argument for robustness, though ignored if --reload is active
        parser.add_argument(
            "--workers",
            action="store",
            dest="workers",
            type=int,
            default=1,
            help="The number of worker processes to use (ignored when DEBUG=True).",
        )

        if apps.is_installed("django.contrib.staticfiles"):
            parser.add_argument(
                "--nostatic",
                action="store_false",
                dest="use_static_handler",
                help="Tells Django to NOT automatically serve static files at STATIC_URL.",
            )
            parser.add_argument(
                "--insecure",
                action="store_true",
                dest="insecure_serving",
                help="Allows serving static files even if DEBUG is False.",
            )

    def handle(self, *args, **options):
        """Validate ASGI settings and defer to Django's normal command flow."""

        # Check Channels/ASGI is installed right
        if options["use_asgi"] and not hasattr(settings, "ASGI_APPLICATION"):
            raise CommandError(
                "You have not set ASGI_APPLICATION, which is needed to run the server."
            )

        # Dispatch upward (this calls inner_run and handles the autoreloader)
        super().handle(*args, **options)

    def inner_run(self, *args, **options):
        """Boot the Uvicorn development server or fall back to Django's WSGI server."""

        # Fallback to standard WSGI runserver if requested
        if not options.get("use_asgi", True):
            return RunserverCommand.inner_run(self, *args, **options)

        # --- Uvicorn Server Setup ---

        # If an exception was silenced in ManagementUtility.execute in order
        # to be raised in the child process, raise it now.
        autoreload.raise_last_exception()

        # 1. Run checks
        if not options["skip_checks"]:
            self.stdout.write("Performing system checks...\n\n")
            check_kwargs = super().get_check_kwargs(options)
            check_kwargs["display_num_errors"] = True
            self.check(**check_kwargs)
        self.check_migrations()
        # Close all connections opened during migration checking.
        for conn in connections.all(initialized_only=True):
            conn.close()

        # 2. Print helpful text
        quit_command = "CTRL-BREAK" if sys.platform == "win32" else "CONTROL-C"
        now = datetime.datetime.now().strftime("%B %d, %Y - %X")
        self.stdout.write(now)
        self.stdout.write(
            (
                "Django version %(version)s, using settings %(settings)r\n"
                "Starting ASGI/Uvicorn development server at %(protocol)s://%(addr)s:%(port)s.../\n"
                "Quit the server with %(quit_command)s.\n"
            )
            % {
                "version": get_version(),
                "settings": settings.SETTINGS_MODULE,
                "protocol": self.protocol,
                "addr": "[%s]" % self.addr if self._raw_ipv6 else self.addr,
                "port": self.port,
                "quit_command": quit_command,
            }
        )
        # 3. Get the application (with static files handler wrapped)
        application = self.get_application(options)

        # 4. Configure Uvicorn programmatically
        is_reloader_active = options["use_reloader"] and settings.DEBUG

        uvicorn_config = Config(
            # Uvicorn works best when passed the application object directly
            app=application,
            host=self.addr,
            port=int(self.port),
            # Use reload only in DEBUG mode when requested
            reload=is_reloader_active,
            # Set workers if not in reload mode
            workers=options["workers"] if not is_reloader_active else 1,
            # Crucial: explicitly set lifespan to ensure it runs
            lifespan="on",
            # Uvicorn handles its own internal logging, but we can set the level
            log_level="info" if settings.DEBUG else "warning",
            log_config=None
        )

        try:
            # We use uvicorn.Server directly to handle the keyboard interrupt cleanly
            # and to align with the Daphne pattern.
            Server(config=uvicorn_config).run()
            logger.debug("uvicorn exited")
        except OSError as e:
            # Use helpful error messages instead of ugly tracebacks.
            ERRORS = {
                errno.EACCES: "You don't have permission to access that port.",
                errno.EADDRINUSE: "That port is already in use.",
                errno.EADDRNOTAVAIL: "That IP address can't be assigned to.",
            }
            try:
                error_text = ERRORS[e.errno] #type:ignore
            except KeyError:
                error_text = e
            self.stderr.write("Error: %s" % error_text)
            # Need to use an OS exit because sys.exit doesn't work in a thread
            os._exit(1)
        except KeyboardInterrupt:
            shutdown_message = options.get("shutdown_message", "")
            if shutdown_message:
                self.stdout.write(shutdown_message)
            sys.exit(0)
        except Exception as e:
            self.stderr.write("Error: %s" % e)
            sys.exit(1)

    @staticmethod
    def get_application(options):
        """
        Returns the static files serving application wrapping the default application.
        This logic is copied exactly from Daphne/Channels.
        """
        staticfiles_installed = apps.is_installed("django.contrib.staticfiles")
        use_static_handler = options.get("use_static_handler", staticfiles_installed)
        insecure_serving = options.get("insecure_serving", False)

        default_app = get_default_application()

        if use_static_handler and (settings.DEBUG or insecure_serving):
            # ASGIStaticFilesHandler wraps the main ASGI app to serve static files
            return ASGIStaticFilesHandler(default_app)
        else:
            return default_app

    # NOTE: We skip implementing log_action as Uvicorn's default logging is
    # standardized and excellent (e.g., "GET /path 200 OK"). Customizing Uvicorn's
    # access logs to Django's style is complex and often unnecessary.
