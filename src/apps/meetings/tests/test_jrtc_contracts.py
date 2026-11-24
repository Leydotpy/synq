"""Historical migration field behavior after the Django media runtime was retired."""

from types import SimpleNamespace
from django.test import SimpleTestCase
from django.utils.module_loading import import_string
from core.models.fields.janus import BoundPluginHandle, JanusPluginField


class HistoricalFieldCompatibilityTests(SimpleTestCase):
    def test_historical_field_round_trip_and_dotted_names(self):
        field = JanusPluginField(
            identifier="publisher",
            null=True,
            plugin_class="apps.meetings.services.janus.NativeJanusIdVideoRoomPlugin",
            callback_factory="core.hooks.janus.plugin_callback_factory",
            janus_getter="apps.meetings.services.janus.resolve_janus_session",
            plugin_kwargs_factory="apps.meetings.services.janus.participant_media_plugin_kwargs",
            identifier_getter="apps.meetings.services.janus.participant_media_handle_identifier",
        )
        name, path, args, kwargs = field.deconstruct()
        self.assertEqual(
            import_string(path)(*args, **kwargs).deconstruct(),
            (name, path, args, kwargs),
        )
        for key in (
            "plugin_class",
            "callback_factory",
            "janus_getter",
            "plugin_kwargs_factory",
            "identifier_getter",
        ):
            self.assertTrue(callable(import_string(kwargs[key])))
        self.assertEqual(field.from_db_value(123, None, None), 123)
        wrapper = BoundPluginHandle(instance=SimpleNamespace(), field=field, raw_id=123)
        self.assertEqual(wrapper.id, 123)
        with self.assertRaises(RuntimeError):
            _ = wrapper.plugin
