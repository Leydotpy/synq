"""Root Django app configuration for the local ``apps`` package.

This lightweight config gives Django a stable import path and label for the
project's first-party applications namespace.
"""

from django.apps import AppConfig


class Config(AppConfig):
    """Register the top-level ``apps`` package as a Django application."""

    #: Python import path for the package that contains all project apps.
    name = "apps"
    #: Short label Django uses in the app registry.
    label = "apps"
