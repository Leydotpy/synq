"""Compatibility facade over Synq's process-local JRTC integration.

New integration code belongs in ``apps.meetings.jrtc``.  Domain call sites keep
these stable helpers while the migration preserves existing Socket.IO command
and Celery task contracts.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import uuid
from collections.abc import Awaitable, Iterable, Mapping
from typing import Any

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from jrtc_video import VideoRoomPlugin, VideoRoomReply

from apps.meetings.exceptions import JanusGatewayError
from apps.meetings.jrtc.config import configure_jrtc_core
from apps.meetings.jrtc.errors import (
    JrtcHandleOwnershipError,
    JrtcHandleUnavailable,
)
from apps.meetings.jrtc.handles import (
    BoundVideoRoomHandle,
    HandleBindingSpec,
)
from apps.meetings.jrtc.ids import janus_event_to_wire, require_janus_id, thaw_json
from apps.meetings.jrtc.runtime import JanusProcessRuntime, janus_runtime

logger = logging.getLogger(__name__)

# Historical migrations reference this dotted path.  It is compatibility-only:
# active runtime code constructs VideoRoomPlugin without a persisted plugin_id.
NativeJanusIdVideoRoomPlugin = VideoRoomPlugin

# Preserve the former public spelling for deployment code and older imports.
configure_janus_core = configure_jrtc_core


def _session_key(instance: Any | None) -> str | None:
    """Derive a stable pool-selection key from a meeting-domain object."""

    if instance is None:
        return None
    participant = getattr(instance, "participant", None)
    if participant is not None and getattr(participant, "session_id", None) is not None:
        return str(participant.session_id)
    value = getattr(instance, "session_id", None)
    if value is not None:
        return str(value)
    related_session = getattr(instance, "session", None)
    if related_session is not None and getattr(related_session, "pk", None) is not None:
        return str(related_session.pk)
    value = getattr(instance, "pk", None)
    return None if value is None else str(value)


def serialize_janus_response(response: Any) -> dict[str, Any]:
    """Return a browser-safe envelope while retaining typed JRTC internally."""

    if isinstance(response, VideoRoomReply):
        raw = response.raw
        if hasattr(raw, "model_dump"):
            payload = raw.model_dump(mode="json", by_alias=True, exclude_none=True)
        elif isinstance(raw, Mapping) and raw:
            payload = thaw_json(raw)
        else:
            data = response.data.model_dump(mode="json", by_alias=True, exclude_none=True)
            payload = {
                "plugindata": {
                    "plugin": "janus.plugin.videoroom",
                    "data": data,
                }
            }
            if response.jsep is not None:
                payload["jsep"] = response.jsep.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                )
            if response.transaction:
                payload["transaction"] = response.transaction
    elif hasattr(response, "model_dump"):
        payload = response.model_dump(mode="json", by_alias=True, exclude_none=True)
    elif isinstance(response, Mapping):
        payload = thaw_json(response)
    elif hasattr(response, "__dict__"):
        payload = {
            key: value
            for key, value in vars(response).items()
            if not key.startswith("_")
        }
    else:
        return {"value": str(response)}
    if not isinstance(payload, Mapping):
        return {"value": payload}
    return janus_event_to_wire(payload)


def video_room_reply_data(response: Any) -> Any | None:
    """Return the typed VideoRoom data object from a command reply."""

    if isinstance(response, VideoRoomReply):
        return response.data
    return getattr(getattr(response, "plugindata", None), "data", None)


def resolve_maybe_awaitable(result: Any) -> Any:
    """Resolve a JRTC awaitable on its process-owned loop."""

    if not inspect.isawaitable(result):
        return result
    return janus_runtime.run(result)


def resolve_janus_session(instance: Any | None = None, *_args: Any, **_kwargs: Any) -> Any:
    """Historical dotted-path helper returning a ready local JRTC session."""

    return janus_runtime.session(key=_session_key(instance))


def resolve_owned_janus_session(instance: Any | None = None) -> Any | None:
    """Return an already-running local session without creating worker ownership."""

    janus_runtime.reset_after_fork()
    if janus_runtime.state != janus_runtime.RUNNING:
        return None
    manager = janus_runtime.manager
    return None if manager is None else manager.get_session(key=_session_key(instance))


def coerce_janus_id(value: Any, *, kind: str = "Janus ID") -> int:
    """Enforce JRTC's strict positive-integer identifier contract."""

    try:
        return require_janus_id(value, name=kind)
    except TypeError as exc:
        raise JanusGatewayError(f"A {kind} must be a positive integer.") from exc


def coerce_janus_room_id(value: Any) -> int:
    return coerce_janus_id(value, kind="Janus room ID")


def janus_room_id_for_session(session: Any) -> int:
    """Return the persisted room ID or a stable positive signed-63-bit ID."""

    if session.janus_room_id is not None:
        return coerce_janus_room_id(session.janus_room_id)
    digest = hashlib.blake2b(str(session.pk).encode("utf-8"), digest_size=8).digest()
    return (int.from_bytes(digest, byteorder="big") % ((1 << 63) - 1)) or 1


def build_room_payload(session: Any) -> dict[str, Any]:
    """Build a strict JRTC VideoRoom creation payload for a meeting session."""

    room_defaults = dict(getattr(settings, "JANUS_DEFAULT_ROOM_CONFIGURATION", {}))
    raw_configuration = session.room.janus_room_configuration
    room_configuration = {} if raw_configuration is None else raw_configuration
    if not isinstance(room_configuration, dict):
        raise JanusGatewayError("Janus room configuration must be a JSON object.")
    room_defaults.update(room_configuration)
    room_defaults["room"] = (
        coerce_janus_room_id(room_defaults["room"])
        if "room" in room_defaults
        else janus_room_id_for_session(session)
    )
    room_defaults.setdefault("description", session.room.title)
    room_defaults.setdefault("publishers", session.room.max_participants)
    room_defaults.setdefault("bitrate", 1_024_000)
    room_defaults.setdefault("audiocodec", "opus")
    room_defaults.setdefault("videocodec", "vp8")
    room_defaults.setdefault("notify_joining", True)
    if session.janus_room_secret:
        room_defaults.setdefault("secret", session.janus_room_secret)
    if session.janus_room_pin:
        room_defaults.setdefault("pin", session.janus_room_pin)
    return room_defaults


def meeting_session_control_plugin_kwargs(
    instance: Any,
    field: Any,
    raw_id: int | None,
) -> Mapping[str, Any]:
    """Migration compatibility only; management handles are always temporary."""

    del instance, field, raw_id
    return {}


def participant_media_handle_identifier(instance: Any, field: Any) -> str:
    """Migration compatibility for the retired custom field."""

    del instance, field
    return "videoroom"


def participant_media_plugin_kwargs(
    instance: Any,
    field: Any,
    raw_id: int | None,
) -> Mapping[str, Any]:
    """Migration compatibility for the retired custom field."""

    del field, raw_id
    return {"meeting_handle_type": str(instance.handle_type)}


def ensure_bound_plugin_attached(
    bound_handle: Any,
    *,
    persist: bool = False,
    update_fields: Iterable[str] | None = None,
    opaque_id: str | None = None,
) -> BoundVideoRoomHandle:
    """Reject ORM materialization while tolerating an already-verified binding."""

    del persist, update_fields, opaque_id
    if isinstance(bound_handle, BoundVideoRoomHandle):
        return bound_handle
    raise JanusGatewayError(
        "ORM plugin materialization is retired; resolve the media handle through "
        "the process-local JRTC registry."
    )


def ensure_session_control_handle(session: Any) -> None:
    """Management plugin handles are short-lived and are never persisted."""

    session.control_handle_id = None
    raise JanusGatewayError(
        "Persistent VideoRoom control handles are retired; use a management command."
    )


def participant_media_plugin_is_locally_owned(media_handle: Any) -> bool:
    """Return whether this running process owns the persisted handle claim."""

    return bool(
        media_handle.runtime_owner_id == janus_runtime.owner_id
        and janus_runtime.state == janus_runtime.RUNNING
    )


def release_local_participant_media_plugin(
    media_handle: Any,
    *,
    expected_owner_id: str | None = None,
    expected_session_id: int | None = None,
    expected_handle_id: int | None = None,
) -> bool:
    """Invalidate a locally owned binding during an explicit affinity handoff.

    Foreign runtimes are never contacted or reconstructed here. Their stale
    ownership may be released only by the connection-lease policy at the
    persistence boundary.
    """

    owner_id = expected_owner_id or media_handle.runtime_owner_id
    if owner_id != janus_runtime.owner_id:
        return False
    if janus_runtime.state != janus_runtime.RUNNING:
        return False
    binding = janus_runtime.run(
        janus_runtime.registry.invalidate(
            str(media_handle.pk),
            close_local=True,
            expected_session_id=expected_session_id,
            expected_handle_id=expected_handle_id,
        )
    )
    return binding is not None


def release_unclaimed_local_participant_media_plugin(media_handle: Any) -> bool:
    """Clear any local binding after a durable ownerless handoff sentinel."""

    if janus_runtime.state != janus_runtime.RUNNING:
        return False
    binding = janus_runtime.run(
        janus_runtime.registry.invalidate(str(media_handle.pk), close_local=True)
    )
    return binding is not None


def release_disconnected_participant_media_plugins(
    media_handles: Iterable[Any],
) -> int:
    """Detach exact local bindings and clear their disconnected DB generation.

    The caller supplies row-locked snapshots captured when the owning socket is
    marked disconnected.  Every persistence update is fenced by the original
    connection/owner/session/handle tuple, so cleanup cannot erase a reconnect
    that has already installed a newer generation.
    """

    from apps.meetings.models import (
        JanusHandleLifecycleState,
        JanusHandleType,
        Participant,
        ParticipantMediaHandle,
        ParticipantStream,
    )

    released = 0
    runtime_owner_id = janus_runtime.owner_id
    runtime_running = janus_runtime.state == janus_runtime.RUNNING
    for snapshot in media_handles:
        if snapshot.runtime_owner_id != runtime_owner_id:
            continue

        session_id = snapshot.janus_session_id
        handle_id = snapshot.janus_handle_id
        if runtime_running and session_id is not None and handle_id is not None:
            try:
                release_local_participant_media_plugin(
                    snapshot,
                    expected_owner_id=runtime_owner_id,
                    expected_session_id=session_id,
                    expected_handle_id=handle_id,
                )
            except JrtcHandleOwnershipError:
                # A different local binding won the registry race.  The DB CAS
                # below still cancels this disconnected generation, while the
                # winning resolver will observe its lost claim and compensate
                # only its exact binding.
                logger.info(
                    "Skipped non-matching local JRTC binding during disconnect cleanup",
                    extra={"media_handle_id": str(snapshot.pk)},
                )
            except Exception:
                logger.exception(
                    "Could not detach a local JRTC binding after socket disconnect",
                    extra={"media_handle_id": str(snapshot.pk)},
                )

        observed_at = timezone.now()
        with transaction.atomic():
            updated = ParticipantMediaHandle.objects.filter(
                pk=snapshot.pk,
                connection_id=snapshot.connection_id,
                runtime_owner_id=runtime_owner_id,
                janus_session_id=session_id,
                janus_handle_id=handle_id,
            ).update(
                janus_session_id=None,
                janus_handle_id=None,
                runtime_owner_id=None,
                runtime_claim_id=None,
                lifecycle_state=JanusHandleLifecycleState.DETACHED,
                selected_streams=[],
                janus_state={},
                last_event_at=observed_at,
                updated_at=observed_at,
            )
            if not updated:
                continue
            ParticipantStream.objects.filter(media_handle_id=snapshot.pk).delete()
            if str(snapshot.handle_type) == JanusHandleType.PUBLISHER:
                Participant.objects.filter(pk=snapshot.participant_id).update(
                    janus_publisher_id=None,
                    janus_private_id=None,
                    updated_at=observed_at,
                )
        released += 1
    return released


def release_local_media_plugins_for_connection(connection_id: Any) -> int:
    """Detach local bindings by their original socket generation metadata."""

    if connection_id is None or janus_runtime.state != janus_runtime.RUNNING:
        return 0
    bindings = janus_runtime.run(
        janus_runtime.registry.invalidate_connection(str(connection_id))
    )
    return len(bindings)


def ensure_participant_media_plugin(
    media_handle: Any,
    *,
    recreate: bool = True,
) -> BoundVideoRoomHandle:
    """Claim, resolve, and persist a handle without network I/O in DB locks.

    ``runtime_claim_id`` is a short durable claim marker, independent from the
    event-derived lifecycle state. The connection generation and runtime owner
    are verified again when the live binding is finalized. Failed or lost
    claims are conditionally rolled back, and only the exact plugin created by
    this attempt may be detached.
    """

    janus_runtime.reset_after_fork()
    if janus_runtime.state != janus_runtime.RUNNING:
        janus_runtime.ensure_background()
    runtime_owner_id = janus_runtime.owner_id
    expected_connection_id = media_handle.connection_id
    claim_id = uuid.uuid4()
    previous_lifecycle_state: str | None = None
    owner_claimed = False
    lost_claim = False
    resolution = None
    binding: BoundVideoRoomHandle | None = None
    stale = False

    try:
        # Phase 1: make a short durable process/generation claim.
        with transaction.atomic():
            locked_handle = (
                media_handle.__class__.objects.select_for_update(of=("self",))
                .select_related(
                    "participant__profile",
                    "participant__session",
                    "connection",
                )
                .get(pk=media_handle.pk)
            )
            if locked_handle.connection_id != expected_connection_id:
                raise JrtcHandleOwnershipError(
                    "The media connection generation changed before handle resolution."
                )
            if locked_handle.lifecycle_state == "detaching":
                raise JrtcHandleUnavailable(
                    "The prior connection generation is still being released."
                )
            if expected_connection_id is not None and (
                locked_handle.connection is None
                or str(locked_handle.connection.status)
                not in {"connected", "subscribed", "active"}
            ):
                raise JrtcHandleOwnershipError(
                    "The media connection generation is no longer active."
                )
            persisted_owner_id = locked_handle.runtime_owner_id or None
            if persisted_owner_id not in (None, runtime_owner_id):
                raise JrtcHandleOwnershipError(
                    f"The media handle belongs to runtime {persisted_owner_id!r}."
                )
            if locked_handle.runtime_claim_id is not None:
                raise JrtcHandleUnavailable(
                    "Another JRTC handle resolution is already in progress."
                )

            owner_claimed = persisted_owner_id is None
            previous_lifecycle_state = str(locked_handle.lifecycle_state)
            locked_handle.runtime_owner_id = runtime_owner_id
            locked_handle.runtime_claim_id = claim_id
            locked_handle.save(
                update_fields=["runtime_owner_id", "runtime_claim_id", "updated_at"]
            )
            spec = HandleBindingSpec(
                model_id=str(locked_handle.pk),
                session_key=_session_key(locked_handle),
                persisted_session_id=locked_handle.janus_session_id,
                persisted_handle_id=locked_handle.janus_handle_id,
                persisted_owner_id=runtime_owner_id,
                connection_id=(
                    None
                    if expected_connection_id is None
                    else str(expected_connection_id)
                ),
                opaque_id=locked_handle.opaque_id or None,
            )

        # Phase 2: attach/validate on the process-owned event loop, with no DB
        # transaction held open during Janus network I/O.
        resolution = janus_runtime.run(
            janus_runtime.adapter.resolve_handle(spec, recreate=recreate)
        )
        binding = resolution.binding
        stale = resolution.replaced_stale
        observed_at = timezone.now()

        # Phase 3: finalize only if this owner and connection still hold claim.
        with transaction.atomic():
            locked_handle = (
                media_handle.__class__.objects.select_for_update(of=("self",))
                .select_related(
                    "participant__profile",
                    "participant__session",
                    "connection",
                )
                .get(pk=media_handle.pk)
            )
            if (
                locked_handle.connection_id != expected_connection_id
                or locked_handle.runtime_owner_id != runtime_owner_id
                or locked_handle.runtime_claim_id != claim_id
                or (
                    expected_connection_id is not None
                    and (
                        locked_handle.connection is None
                        or str(locked_handle.connection.status)
                        not in {"connected", "subscribed", "active"}
                    )
                )
            ):
                lost_claim = True
                raise JrtcHandleOwnershipError(
                    "The JRTC handle claim changed before persistence completed."
                )

            locked_handle.janus_session_id = binding.session_id
            locked_handle.janus_handle_id = binding.handle_id
            locked_handle.runtime_owner_id = binding.owner_id
            locked_handle.runtime_claim_id = None
            final_lifecycle_state = (
                "attached"
                if resolution.recreated
                or stale
                or previous_lifecycle_state
                in {None, "attaching", "detaching", "detached", "failed"}
                else locked_handle.lifecycle_state
            )
            locked_handle.lifecycle_state = final_lifecycle_state
            locked_handle.last_event_at = observed_at
            update_fields = [
                "janus_session_id",
                "janus_handle_id",
                "runtime_owner_id",
                "runtime_claim_id",
                "lifecycle_state",
                "last_event_at",
                "updated_at",
            ]
            if stale:
                locked_handle.selected_streams = []
                locked_handle.janus_state = {}
                update_fields.extend(["selected_streams", "janus_state"])
            locked_handle.save(update_fields=update_fields)

            if stale:
                locked_handle.streams.all().delete()
                if str(locked_handle.handle_type) == "publisher":
                    participant = locked_handle.participant
                    participant.janus_publisher_id = None
                    participant.janus_private_id = None
                    participant.save(
                        update_fields=[
                            "janus_publisher_id",
                            "janus_private_id",
                            "updated_at",
                        ]
                    )

            if resolution.recreated:
                from apps.meetings.models import MeetingEventType
                from apps.meetings.services.lifecycle import record_session_event

                record_session_event(
                    session=locked_handle.participant.session,
                    event_type=MeetingEventType.JANUS_HANDLE_ATTACHED,
                    actor_profile=locked_handle.participant.profile,
                    actor_participant=locked_handle.participant,
                    payload={
                        "handle_id": str(locked_handle.pk),
                        "handle_type": str(locked_handle.handle_type),
                        "janus_session_id": str(binding.session_id),
                        "janus_handle_id": str(binding.handle_id),
                        "runtime_owner_id": binding.owner_id,
                    },
                )
    except BaseException:
        if resolution is not None and (
            resolution.recreated or owner_claimed or lost_claim
        ):
            try:
                janus_runtime.run(
                    janus_runtime.registry.detach(
                        str(media_handle.pk),
                        expected=resolution.binding,
                    )
                )
            except Exception:
                logger.exception(
                    "Could not compensate a JRTC handle after persistence failed"
                )

        if previous_lifecycle_state is not None:
            try:
                cleanup_values: dict[str, Any] = {
                    "runtime_claim_id": None,
                    "updated_at": timezone.now(),
                }
                if owner_claimed:
                    cleanup_values["runtime_owner_id"] = None
                with transaction.atomic():
                    media_handle.__class__.objects.select_for_update().filter(
                        pk=media_handle.pk,
                        connection_id=expected_connection_id,
                        runtime_owner_id=runtime_owner_id,
                        runtime_claim_id=claim_id,
                    ).update(**cleanup_values)
            except Exception:
                logger.exception("Could not release a failed JRTC handle claim")
        raise

    assert binding is not None
    assert previous_lifecycle_state is not None
    media_handle.janus_session_id = binding.session_id
    media_handle.janus_handle_id = binding.handle_id
    media_handle.runtime_owner_id = binding.owner_id
    media_handle.lifecycle_state = final_lifecycle_state
    media_handle.last_event_at = observed_at
    if stale:
        media_handle.selected_streams = []
        media_handle.janus_state = {}
        if str(media_handle.handle_type) == "publisher":
            media_handle.participant.janus_publisher_id = None
            media_handle.participant.janus_private_id = None
    return binding


def call_video_room_management_method(
    instance: Any,
    method_name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run one direct command with a short-lived VideoRoom plugin."""

    try:
        return janus_runtime.run(
            janus_runtime.adapter.management_command(
                session_key=_session_key(instance),
                method_name=method_name,
                args=args,
                kwargs=kwargs,
            )
        )
    except JanusGatewayError:
        raise
    except Exception as exc:
        raise JanusGatewayError(
            f"Unable to execute Janus VideoRoom management method {method_name!r}."
        ) from exc


def call_plugin_method(
    bound_handle: BoundVideoRoomHandle,
    method_name: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Invoke a command directly on a verified process-local VideoRoom handle."""

    if not isinstance(bound_handle, BoundVideoRoomHandle):
        raise JanusGatewayError("The supplied VideoRoom handle is not registry-owned.")
    try:
        return janus_runtime.run(
            janus_runtime.adapter.invoke(bound_handle, method_name, *args, **kwargs)
        )
    except JanusGatewayError:
        raise
    except Exception as exc:
        raise JanusGatewayError(
            f"Unable to execute Janus VideoRoom method {method_name!r}."
        ) from exc


__all__ = [
    "JanusProcessRuntime",
    "NativeJanusIdVideoRoomPlugin",
    "build_room_payload",
    "call_plugin_method",
    "call_video_room_management_method",
    "coerce_janus_id",
    "coerce_janus_room_id",
    "configure_janus_core",
    "ensure_bound_plugin_attached",
    "ensure_participant_media_plugin",
    "ensure_session_control_handle",
    "janus_room_id_for_session",
    "janus_runtime",
    "meeting_session_control_plugin_kwargs",
    "participant_media_handle_identifier",
    "participant_media_plugin_is_locally_owned",
    "participant_media_plugin_kwargs",
    "release_local_participant_media_plugin",
    "release_disconnected_participant_media_plugins",
    "release_local_media_plugins_for_connection",
    "release_unclaimed_local_participant_media_plugin",
    "resolve_janus_session",
    "resolve_maybe_awaitable",
    "resolve_owned_janus_session",
    "serialize_janus_response",
    "video_room_reply_data",
]
