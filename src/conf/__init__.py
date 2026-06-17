"""Project package bootstrap for Celery-aware Django startup."""

from conf.celery import app as celery_app

__all__ = ("celery_app",)
