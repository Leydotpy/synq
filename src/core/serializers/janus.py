from __future__ import annotations

from typing import Any

from rest_framework import serializers

from core.models import BoundPluginHandle, JanusPluginField


class JanusPluginSerializerField(serializers.Field):
    """
    DRF field that always represents a JanusPluginField as its raw stored id.

    Python side:
        model_instance.plugin_id -> BoundPluginHandle | None

    API side:
        "plugin_id": "<raw-string-id>" | null
    """

    default_error_messages = {
        "invalid": "Expected a Janus plugin id string or null.",
    }

    def to_representation(self, value: Any) -> str | None:
        if value is None:
            return None

        if isinstance(value, BoundPluginHandle):
            return value.raw_id

        if isinstance(value, str):
            return value

        for attr_name in ("plugin_id", "id"):
            attr = getattr(value, attr_name, None)
            if isinstance(attr, str):
                return attr

        return str(value)

    def to_internal_value(self, data: Any) -> str | None:
        if data in (None, ""):
            return None

        if not isinstance(data, str):
            self.fail("invalid")

        return data


class JanusPluginModelSerializer(serializers.ModelSerializer):
    """
    Optional ModelSerializer base class that auto-maps JanusPluginField
    to JanusPluginSerializerField.
    """

    serializer_field_mapping = dict(serializers.ModelSerializer.serializer_field_mapping)
    serializer_field_mapping[JanusPluginField] = JanusPluginSerializerField