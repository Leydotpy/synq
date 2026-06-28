"""Centralized Django admin registrations for all first-party domain models.

This module intentionally keeps admin registration in one place so the project
can enforce consistent, scalable behavior across apps:

1. Every first-party model is registered automatically.
2. Forward relations (FK/O2O/M2M) shown on list pages are rendered as clickable
   links to the related model's admin change page.
3. Reverse FK/O2O relations are inlined where applicable to make relationship
   management fast from a single change form.
4. Search, filtering, list optimization, and autocomplete defaults are derived
   from model metadata to reduce repetitive boilerplate.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from django.apps import apps as django_apps
from django.contrib import admin
from django.contrib.admin.sites import AlreadyRegistered
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.urls import NoReverseMatch, reverse
from django.utils.html import format_html, format_html_join
from django.utils.text import capfirst

# App labels owned and maintained in this repository.
FIRST_PARTY_APP_LABELS: set[str] = {
    "profiles",
    "meetings"
}

# Maximum number of related objects previewed in one list-display M2M column.
M2M_LINK_PREVIEW_LIMIT = 6

admin.site.site_header = "Synq Administration"
admin.site.site_title = "Synq Admin"
admin.site.index_title = "Operations Console"


def _dedupe(items: Iterable[str]) -> list[str]:
    """Return items in insertion order with duplicates removed."""

    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _model_has_field(model: type[models.Model], field_name: str) -> bool:
    """Return whether a model exposes a concrete field with ``field_name``."""

    try:
        model._meta.get_field(field_name)
    except FieldDoesNotExist:
        return False
    return True


def _is_first_party_model(model: type[models.Model] | None) -> bool:
    """Return whether ``model`` belongs to one of the project's domain apps."""

    return bool(model and model._meta.app_label in FIRST_PARTY_APP_LABELS)


def _admin_change_url(obj: models.Model) -> str | None:
    """Build an admin change URL for ``obj`` when available."""

    opts = obj._meta
    try:
        return reverse(f"admin:{opts.app_label}_{opts.model_name}_change", args=(obj.pk,))
    except NoReverseMatch:
        return None


def _admin_object_link(obj: models.Model | None, *, label: str | None = None) -> str:
    """Render a related-object link to the object's admin change page."""

    if obj is None:
        return "-"

    text = label or str(obj)
    url = _admin_change_url(obj)
    if not url:
        return text
    return format_html('<a href="{}">{}</a>', url, text)


def _editable_relation_fields(model: type[models.Model], *, exclude: set[str] | None = None) -> tuple[str, ...]:
    """Return editable relation fields suitable for autocomplete widgets."""

    excluded_names = exclude or set()
    relation_names: list[str] = []

    for field in model._meta.get_fields():
        if field.auto_created or not field.concrete or field.name in excluded_names:
            continue
        if not getattr(field, "editable", False):
            continue

        if isinstance(field, (models.ForeignKey, models.OneToOneField)):
            related_model = getattr(field.remote_field, "model", None)
            if _is_first_party_model(related_model):
                relation_names.append(field.name)
            continue

        if isinstance(field, models.ManyToManyField):
            related_model = getattr(field.remote_field, "model", None)
            uses_auto_through = field.remote_field.through._meta.auto_created
            if _is_first_party_model(related_model) and uses_auto_through:
                relation_names.append(field.name)

    return tuple(relation_names)


def _readonly_audit_fields(model: type[models.Model]) -> tuple[str, ...]:
    """Return common immutable fields that should remain read-only in admin."""

    return tuple(name for name in ("id", "created_at", "updated_at") if _model_has_field(model, name))


def _changelist_scalar_fields(model: type[models.Model], *, limit: int = 4) -> list[str]:
    """Pick concise scalar fields that make changelist rows informative."""

    preferred: list[str] = []
    for field in model._meta.concrete_fields:
        if field.primary_key:
            continue
        if isinstance(field, (models.ForeignKey, models.OneToOneField)):
            continue
        if isinstance(field, models.TextField):
            continue

        if isinstance(
            field,
            (
                models.CharField,
                models.SlugField,
                models.BooleanField,
                models.IntegerField,
                models.PositiveIntegerField,
                models.PositiveSmallIntegerField,
                models.DecimalField,
                models.DateField,
                models.DateTimeField,
                models.UUIDField,
            ),
        ):
            preferred.append(field.name)

        if len(preferred) >= limit:
            break

    return preferred


def _search_fields(model: type[models.Model]) -> tuple[str, ...]:
    """Derive pragmatic search fields from string-like columns."""

    fields: list[str] = []
    if _model_has_field(model, "id"):
        fields.append("id__exact")

    for field in model._meta.concrete_fields:
        if isinstance(field, (models.CharField, models.EmailField, models.SlugField, models.UUIDField)):
            fields.append(field.name)

    return tuple(_dedupe(fields))


def _list_filters(model: type[models.Model]) -> tuple[str, ...]:
    """Derive useful list filters from choice, boolean, relation, and date fields."""

    filters: list[str] = []
    for field in model._meta.concrete_fields:
        if isinstance(field, (models.ForeignKey, models.OneToOneField)):
            filters.append(field.name)
            continue
        if getattr(field, "choices", None):
            filters.append(field.name)
            continue
        if isinstance(field, models.BooleanField):
            filters.append(field.name)
            continue
        if field.name in {"created_at", "updated_at"}:
            filters.append(field.name)

    return tuple(_dedupe(filters)[:8])


def _date_hierarchy_field(model: type[models.Model]) -> str | None:
    """Pick the best date/datetime field for drill-down navigation."""

    candidates = (
        "created_at",
        "updated_at",
        "started_at",
        "kickoff_at",
        "published_at",
        "paid_at",
    )
    for name in candidates:
        if _model_has_field(model, name):
            return name
    return None


def _forward_relation_fields(model: type[models.Model]) -> tuple[list[Any], list[Any]]:
    """Return direct FK/O2O and M2M fields declared on ``model``."""

    fk_o2o_fields: list[Any] = []
    m2m_fields: list[Any] = []

    for field in model._meta.get_fields():
        if field.auto_created or not field.concrete:
            continue
        if isinstance(field, (models.ForeignKey, models.OneToOneField)):
            fk_o2o_fields.append(field)
            continue
        if isinstance(field, models.ManyToManyField):
            m2m_fields.append(field)

    return fk_o2o_fields, m2m_fields


def _build_fk_link_column(field: Any) -> tuple[str, Any]:
    """Build a list-display method that links to the related FK/O2O object."""

    method_name = f"{field.name}_admin_link"

    @admin.display(description=capfirst(field.verbose_name), ordering=field.name)
    def relation_link(self, obj: models.Model):
        related_obj = getattr(obj, field.name, None)
        return _admin_object_link(related_obj)

    relation_link.__name__ = method_name
    return method_name, relation_link


def _build_m2m_link_column(field: Any) -> tuple[str, Any]:
    """Build a list-display method that renders related M2M objects as links."""

    method_name = f"{field.name}_admin_links"

    @admin.display(description=capfirst(field.verbose_name))
    def relation_links(self, obj: models.Model):
        relation_manager = getattr(obj, field.name)
        prefetched = getattr(obj, "_prefetched_objects_cache", {})
        if field.name in prefetched:
            preview = list(prefetched[field.name][: M2M_LINK_PREVIEW_LIMIT + 1])
        else:
            preview = list(relation_manager.all()[: M2M_LINK_PREVIEW_LIMIT + 1])
        has_more = len(preview) > M2M_LINK_PREVIEW_LIMIT
        preview = preview[:M2M_LINK_PREVIEW_LIMIT]

        if not preview:
            return "-"

        rendered = format_html_join(", ", "{}", ((_admin_object_link(item),) for item in preview))
        if not has_more:
            return rendered
        return format_html("{}{}", rendered, " ...")

    relation_links.__name__ = method_name
    return method_name, relation_links


def _build_inlines(model: type[models.Model]) -> tuple[type[admin.options.InlineModelAdmin], ...]:
    """Generate reverse-relation inlines for all applicable child models."""

    inline_classes: list[type[admin.options.InlineModelAdmin]] = []

    for relation in model._meta.get_fields():
        if not relation.auto_created or relation.concrete:
            continue
        if not (relation.one_to_many or relation.one_to_one):
            continue

        related_model = relation.related_model
        if not _is_first_party_model(related_model):
            continue

        # Skip inheritance parent-links and synthetic reverse relations.
        if getattr(relation.field, "parent_link", False):
            continue

        inline_name = f"{model.__name__}{related_model.__name__}{relation.field.name}Inline"
        relation_autocomplete_fields = _editable_relation_fields(
            related_model,
            exclude={relation.field.name},
        )

        inline_attrs: dict[str, Any] = {
            "model": related_model,
            "fk_name": relation.field.name,
            "extra": 0,
            "show_change_link": True,
            "autocomplete_fields": relation_autocomplete_fields,
            "readonly_fields": _readonly_audit_fields(related_model),
            "classes": ("collapse",),
        }

        if relation.one_to_one:
            inline_attrs["max_num"] = 1

        # Stacked layout keeps large child records readable.
        base_inline = admin.StackedInline if len(related_model._meta.fields) > 12 else admin.TabularInline
        inline_classes.append(type(inline_name, (base_inline,), inline_attrs))

    return tuple(inline_classes)


class FirstPartyModelAdmin(admin.ModelAdmin):
    """Shared admin defaults for first-party models.

    Concrete admin classes are generated per model so every model receives the
    same quality baseline without repeating hand-written configuration.
    """

    save_on_top = True
    list_per_page = 50
    _prefetch_for_changelist: tuple[str, ...] = ()

    @admin.display(description="Record")
    def record(self, obj: models.Model) -> str:
        """Render the model's string representation in changelist rows."""

        return str(obj)

    def get_queryset(self, request):
        """Apply prefetch optimizations for relation-heavy changelist columns."""

        queryset = super().get_queryset(request)
        if self._prefetch_for_changelist:
            queryset = queryset.prefetch_related(*self._prefetch_for_changelist)
        return queryset


def _build_model_admin_class(model: type[models.Model]) -> type[FirstPartyModelAdmin]:
    """Create a tailored ``ModelAdmin`` subclass for a specific model."""

    fk_o2o_fields, m2m_fields = _forward_relation_fields(model)
    dynamic_methods: dict[str, Any] = {}

    relation_columns: list[str] = []
    for field in fk_o2o_fields:
        method_name, method = _build_fk_link_column(field)
        dynamic_methods[method_name] = method
        relation_columns.append(method_name)

    for field in m2m_fields:
        method_name, method = _build_m2m_link_column(field)
        dynamic_methods[method_name] = method
        relation_columns.append(method_name)

    scalar_columns = _changelist_scalar_fields(model)
    audit_columns = [name for name in ("created_at", "updated_at") if _model_has_field(model, name)]
    changelist_columns = tuple(_dedupe(["record", *scalar_columns, *relation_columns, *audit_columns]))

    list_select_related = tuple(field.name for field in fk_o2o_fields)
    prefetch_related = tuple(field.name for field in m2m_fields)
    autocomplete_fields = _editable_relation_fields(model)

    ordering: Sequence[str]
    if model._meta.ordering:
        ordering = model._meta.ordering
    elif _model_has_field(model, "created_at"):
        ordering = ("-created_at",)
    else:
        ordering = ("id",)

    admin_attrs: dict[str, Any] = {
        "__doc__": f"Admin configuration for ``{model._meta.label}``.",
        "list_display": changelist_columns,
        "list_display_links": ("record",),
        "list_filter": _list_filters(model),
        "search_fields": _search_fields(model),
        "ordering": ordering,
        "readonly_fields": _readonly_audit_fields(model),
        "autocomplete_fields": autocomplete_fields,
        "list_select_related": list_select_related,
        "_prefetch_for_changelist": prefetch_related,
        "inlines": _build_inlines(model),
    }

    date_hierarchy = _date_hierarchy_field(model)
    if date_hierarchy:
        admin_attrs["date_hierarchy"] = date_hierarchy

    admin_attrs.update(dynamic_methods)
    return type(f"{model.__name__}Admin", (FirstPartyModelAdmin,), admin_attrs)


def _register_first_party_models() -> None:
    """Register each first-party model with a generated admin class."""

    models_to_register = sorted(
        (
            model
            for model in django_apps.get_models()
            if model._meta.app_label in FIRST_PARTY_APP_LABELS
        ),
        key=lambda item: (item._meta.app_label, item._meta.model_name),
    )

    for model in models_to_register:
        try:
            admin.site.register(model, _build_model_admin_class(model))
        except AlreadyRegistered:
            # Safe guard for future app-local registrations that may coexist.
            continue


_register_first_party_models()
