"""Callback factory referenced by historical JanusPluginField migrations."""


def plugin_callback_factory(instance, field, raw_id):
    def ignore_historical_event(event):
        pass

    return ignore_historical_event
