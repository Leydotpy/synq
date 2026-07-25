"""General-purpose helper functions used by the meeting domain."""

from __future__ import annotations

import secrets
import string
from typing import Any
from uuid import uuid4

from django.db import models
from django.utils.text import slugify


def generate_short_code(length: int = 10) -> str:
    """Return a collision-resistant uppercase token suitable for meeting credentials."""

    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def build_unique_slug_seed(value: str, fallback: str = "meeting") -> str:
    """Normalize user-facing text into a slug seed while preserving a safe fallback."""

    return slugify(value).strip("-") or fallback


def first_non_empty(*values: str | None) -> str:
    """Return the first non-blank string after trimming surrounding whitespace.

    The helper is used when deriving human-friendly defaults such as display
    names and profile handles from several possible sources.
    """

    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def generate_unique_slug(
    model_class: type[models.Model],
    value: str,
    *,
    slug_field: str = "slug",
    instance: models.Model | None = None,
    max_length: int = 160,
) -> str:
    """Build a unique slug value for any model that exposes a slug-like field.

    Args:
        model_class: Model class whose default manager is searched for conflicts.
        value: Human-readable source string to slugify.
        slug_field: Field name that stores the slug on the model.
        instance: Existing model instance being updated, excluded from conflict checks.
        max_length: Maximum allowed length for the generated slug.

    Returns:
        A unique slug limited to ``max_length`` characters, with numeric suffixes
        added as needed when collisions exist.
    """

    base_slug = slugify(value) or uuid4().hex[:8]
    slug = base_slug[:max_length]
    queryset = model_class._default_manager.all()

    if instance is not None and instance.pk:
        queryset = queryset.exclude(pk=instance.pk)

    counter = 2
    while queryset.filter(**{slug_field: slug}).exists():
        suffix = f"-{counter}"
        slug = f"{base_slug[: max_length - len(suffix)]}{suffix}"
        counter += 1

    return slug

def log_to_terminal(name: str, value: Any) -> None:
    print(
        f"==============================================================={name}========================================================",
        value,
        sep="\n",
    )