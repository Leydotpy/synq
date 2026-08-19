"""Abstract Django model building blocks used across the project."""

from __future__ import annotations

import uuid

from django.db import models


class UUIDPrimaryKeyModel(models.Model):
    """Provide a UUID primary key for models that need globally unique identifiers."""

    # Stable primary key used across HTTP APIs, realtime payloads, and background tasks.
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        """Mark the model as abstract so it can be inherited safely."""

        abstract = True


class TimestampedModel(models.Model):
    """Track creation and update timestamps for derived models."""

    # Timestamp recording when the row was first persisted to the database.
    created_at = models.DateTimeField(auto_now_add=True)
    # Timestamp recording when the row was last modified by application code.
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        """Mark the model as abstract so it can be inherited safely."""

        abstract = True


class UUIDTimestampedModel(UUIDPrimaryKeyModel, TimestampedModel):
    """Combine UUID primary keys with creation and update timestamps."""

    class Meta:
        """Mark the model as abstract so it can be inherited safely."""

        abstract = True
