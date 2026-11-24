"""Inert dotted names serialized by meeting migrations 0001, 0005 and 0009."""


class NativeJanusIdVideoRoomPlugin:
    """Historical field metadata."""


def resolve_janus_session(*args, **kwargs):
    raise RuntimeError("The historical Janus field cannot acquire a session.")


def meeting_session_control_plugin_kwargs(*args, **kwargs):
    return {}


def participant_media_plugin_kwargs(*args, **kwargs):
    return {}


def participant_media_handle_identifier(instance, field):
    return field.identifier
