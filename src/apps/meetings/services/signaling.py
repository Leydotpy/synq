"""Janus signaling helpers that bridge Socket.IO meeting clients to backend-owned handles."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, Iterable, Optional, NotRequired, Sequence, TypedDict

from asgiref.sync import sync_to_async
from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from logvista import get_logger
from jrtc.models.base import Jsep
from jrtc.models.request import TrickleCandidate
from jrtc_video import (
    PublisherConfigureRequest,
    PublisherJoinAndConfigureRequest,
    PublisherPublishRequest,
    StreamDescription,
    SubscribeTarget,
    SubscriberJoinRequest,
    SubscriberUpdateRequest,
    UnsubscribeTarget,
)

from apps.meetings.exceptions import JanusGatewayError, MeetingDomainError
from apps.meetings.jrtc.ice import normalize_ice_request
from apps.meetings.jrtc.ids import (
    janus_event_to_wire,
    janus_id_from_wire,
    optional_janus_id_to_wire,
    require_janus_id,
)
from apps.meetings.jrtc.errors import JrtcHandleOwnershipError, JrtcError, VideoRoomProtocolError
from apps.meetings.models import (
    JanusHandleLifecycleState,
    JanusHandleType,
    MediaDirection,
    MediaKind,
    MeetingSession,
    Participant,
    ParticipantConnection,
    ParticipantMediaHandle,
    ParticipantStream,
    ParticipantStatus,
    RealtimeConnectionStatus,
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.janus import (
    acall_plugin_method,
    acall_video_room_management_method,
    aensure_participant_media_plugin,
    janus_runtime,
    janus_room_id_for_session,
    participant_media_plugin_is_locally_owned,
    release_local_participant_media_plugin,
    release_unclaimed_local_participant_media_plugin,
    serialize_janus_response,
    video_room_reply_data,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.permissions import MeetingPermissionService

logger = get_logger(__name__)


class IceCandidate(TypedDict):
    candidate: str
    sdpMid: NotRequired[str | None]
    sdpMLineIndex: NotRequired[int | None]
    usernameFragment: NotRequired[str | None]


IceCandidates = Sequence[IceCandidate]


@dataclass(slots=True)
class _MediaCommandClaim:
    """Durable generation fence spanning one Janus command and its DB writes."""

    claim_id: uuid.UUID
    model_id: Any
    connection_id: Any
    runtime_owner_id: str | None
    janus_session_id: int | None
    janus_handle_id: int | None
    handle_type: str
    command_applied: bool = False


def _claim_media_command(
    media_handle: ParticipantMediaHandle,
    bound_handle: Any | None = None,
) -> _MediaCommandClaim:
    """Claim a stable handle generation before performing Janus network I/O."""

    expected_connection_id = media_handle.connection_id
    expected_owner_id = (
        media_handle.runtime_owner_id
        if bound_handle is None
        else str(bound_handle.owner_id)
    )
    expected_session_id = (
        media_handle.janus_session_id
        if bound_handle is None
        else int(bound_handle.session_id)
    )
    expected_handle_id = (
        media_handle.janus_handle_id
        if bound_handle is None
        else int(bound_handle.handle_id)
    )
    claim_id = uuid.uuid4()
    active_statuses = {
        RealtimeConnectionStatus.CONNECTED,
        RealtimeConnectionStatus.SUBSCRIBED,
        RealtimeConnectionStatus.ACTIVE,
    }

    with transaction.atomic():
        locked_handle = ParticipantMediaHandle.objects.select_for_update().get(
            pk=media_handle.pk
        )
        if (
            locked_handle.connection_id != expected_connection_id
            or locked_handle.runtime_owner_id != expected_owner_id
            or locked_handle.janus_session_id != expected_session_id
            or locked_handle.janus_handle_id != expected_handle_id
        ):
            raise JrtcHandleOwnershipError(
                "The media handle generation changed before the command started."
            )
        if locked_handle.runtime_claim_id is not None:
            raise JrtcHandleOwnershipError(
                "Another operation already owns the media handle generation."
            )
        if expected_connection_id is not None:
            connection_status = (
                ParticipantConnection.objects.select_for_update()
                .filter(pk=expected_connection_id)
                .values_list("status", flat=True)
                .first()
            )
            if connection_status not in active_statuses:
                raise JrtcHandleOwnershipError(
                    "The media connection generation is no longer active."
                )
        locked_handle.runtime_claim_id = claim_id
        locked_handle.save(update_fields=["runtime_claim_id", "updated_at"])

    media_handle.runtime_claim_id = claim_id
    return _MediaCommandClaim(
        claim_id=claim_id,
        model_id=media_handle.pk,
        connection_id=expected_connection_id,
        runtime_owner_id=expected_owner_id,
        janus_session_id=expected_session_id,
        janus_handle_id=expected_handle_id,
        handle_type=str(media_handle.handle_type),
    )


def _lock_media_command_result(
    claim: _MediaCommandClaim,
) -> ParticipantMediaHandle:
    """Lock and verify the same command claim immediately before persistence."""

    locked_handle = (
        ParticipantMediaHandle.objects.select_for_update()
        .select_related("participant__session", "participant__profile")
        .get(pk=claim.model_id)
    )
    if (
        locked_handle.connection_id != claim.connection_id
        or locked_handle.runtime_owner_id != claim.runtime_owner_id
        or locked_handle.janus_session_id != claim.janus_session_id
        or locked_handle.janus_handle_id != claim.janus_handle_id
        or locked_handle.runtime_claim_id != claim.claim_id
    ):
        raise JrtcHandleOwnershipError(
            "The media handle generation changed while the command was running."
        )
    if claim.connection_id is not None:
        active_statuses = {
            RealtimeConnectionStatus.CONNECTED,
            RealtimeConnectionStatus.SUBSCRIBED,
            RealtimeConnectionStatus.ACTIVE,
        }
        connection_status = (
            ParticipantConnection.objects.select_for_update()
            .filter(pk=claim.connection_id)
            .values_list("status", flat=True)
            .first()
        )
        if connection_status not in active_statuses:
            raise JrtcHandleOwnershipError(
                "The media connection generation ended while the command was running."
            )
    return locked_handle


def _release_media_command_claim(claim: _MediaCommandClaim) -> None:
    """Conditionally release only this command's still-current claim."""

    ParticipantMediaHandle.objects.filter(
        pk=claim.model_id,
        runtime_claim_id=claim.claim_id,
    ).update(runtime_claim_id=None, updated_at=timezone.now())


def _rollback_media_command(claim, *, handle_type):
    observed_at = timezone.now()
    try:
        with transaction.atomic():
            media_handle = (
                ParticipantMediaHandle.objects.select_for_update()
                .filter(
                    pk=claim.model_id,
                    connection_id=claim.connection_id,
                    runtime_owner_id=claim.runtime_owner_id,
                    janus_session_id=claim.janus_session_id,
                    janus_handle_id=claim.janus_handle_id,
                    runtime_claim_id=claim.claim_id,
                )
                .first()
            )
            if media_handle is None:
                return
            ParticipantStream.objects.filter(media_handle=media_handle).delete()
            media_handle.janus_session_id = None
            media_handle.janus_handle_id = None
            media_handle.runtime_owner_id = None
            media_handle.runtime_claim_id = None
            media_handle.lifecycle_state = JanusHandleLifecycleState.DETACHED
            media_handle.selected_streams = []
            media_handle.janus_state = {}
            media_handle.last_event_at = observed_at
            media_handle.save(
                update_fields=[
                    "janus_session_id",
                    "janus_handle_id",
                    "runtime_owner_id",
                    "runtime_claim_id",
                    "lifecycle_state",
                    "selected_streams",
                    "janus_state",
                    "last_event_at",
                    "updated_at",
                ]
            )
            if str(handle_type) == JanusHandleType.PUBLISHER:
                Participant.objects.filter(pk=media_handle.participant_id).update(
                    janus_publisher_id=None,
                    janus_private_id=None,
                    updated_at=observed_at,
                )
    except Exception:
        logger.exception(
            "JRTC handle error!",
            "Could not clear a JRTC handle after command persistence failed",
            extra={"media_handle_id": str(claim.model_id)},
        )


async def _abort_stateful_media_command(claim, *, handle_type):
    try:
        if claim.runtime_owner_id == janus_runtime.owner_id:
            await janus_runtime.registry.invalidate(str(claim.model_id), close_local=True,
                expected_session_id=claim.janus_session_id, expected_handle_id=claim.janus_handle_id)
    except Exception:
        logger.exception("Could not detach after media persistence failed", extra={"media_handle_id": str(claim.model_id)})
    await sync_to_async(_rollback_media_command)(claim, handle_type=handle_type)




@asynccontextmanager
async def _media_command_claim(
    media_handle: ParticipantMediaHandle,
    bound_handle: Any | None = None,
    *,
    compensate_on_error: bool = True,
):
    """Always release a command claim unless its successful write cleared it."""

    claim = await sync_to_async(_claim_media_command)(media_handle, bound_handle)
    try:
        yield claim
    except BaseException:
        if compensate_on_error and claim.command_applied:
            await _abort_stateful_media_command(
                claim,
                handle_type=claim.handle_type,
            )
        raise
    finally:
        await sync_to_async(_release_media_command_claim)(claim)


def _require_internal_janus_id(value: Any, *, kind: str) -> int:
    """Validate one DB/JRTC identifier without accepting strings or booleans."""

    try:
        return require_janus_id(value, name=kind)
    except TypeError as exc:
        raise JanusGatewayError(f"A {kind} must be a positive integer.") from exc


def _janus_id_from_persisted_json(value: Any, *, kind: str) -> int:
    """Parse one canonical decimal ID from app-owned persisted JSON."""

    try:
        return janus_id_from_wire(value, name=kind)
    except TypeError as exc:
        raise JanusGatewayError(
            f"A persisted {kind} must be a positive canonical decimal string."
        ) from exc


def _serialize_model(value: Any) -> dict[str, Any]:
    """Return a JSON-safe dictionary for SDK models or plain mappings."""

    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "__dict__"):
        return {
            key: item
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return {"value": value}


def _serialize_jsep(value: Any) -> dict[str, Any] | None:
    """Serialize a JSEP model or dictionary."""

    if value is None:
        return None
    if hasattr(value, "model_dump"):
        serialized = value.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    else:
        serialized = _serialize_model(value)
    return serialized or None


def _with_subscriber_joined_state(
    payload: dict[str, Any],
    *,
    joined: bool,
) -> dict[str, Any]:
    """Persist app-owned subscriber role state beside the raw Janus envelope."""

    app_state = payload.get("_synq") if isinstance(payload.get("_synq"), dict) else {}
    return {
        **payload,
        "_synq": {
            **app_state,
            "subscriber_joined": joined,
        },
    }


def _subscriber_is_joined(
    media_handle: ParticipantMediaHandle,
    current_targets: set[tuple[int, str]],
) -> bool:
    """Distinguish an attached plugin from one already joined as subscriber."""

    janus_state = media_handle.janus_state if isinstance(media_handle.janus_state, dict) else {}
    app_state = janus_state.get("_synq") if isinstance(janus_state.get("_synq"), dict) else {}
    marker = app_state.get("subscriber_joined")
    if isinstance(marker, bool):
        return marker
    plugin_data = ((janus_state.get("plugindata") or {}).get("data") or {})
    return (
        bool(current_targets)
        or media_handle.lifecycle_state
        in {
            JanusHandleLifecycleState.JOINING,
            JanusHandleLifecycleState.READY,
        }
        or plugin_data.get("videoroom") in {"attached", "updated", "started"}
    )


def _build_jsep(payload: dict[str, Any], *, jsep_type: str) -> Jsep:
    """Validate a browser session description with the Janus Core model."""

    return Jsep.model_validate({**payload, "type": jsep_type})


def _janus_room_id(session: MeetingSession) -> int:
    """Return a strict integer persisted or stable fallback room ID."""

    return janus_room_id_for_session(session)


def _reply_items(reply_data: Any, field_name: str) -> list[Any]:
    """Read a repeated field from typed VideoRoom data or a compatibility mapping."""

    if reply_data is None:
        return []
    if isinstance(reply_data, dict):
        value = reply_data.get(field_name)
    else:
        value = getattr(reply_data, field_name, None)
    return list(value or [])


def _field_was_provided(value: Any, field_name: str) -> bool:
    """Distinguish wire fields from defaults synthesized by response models."""

    if isinstance(value, dict):
        return field_name in value
    fields_set = getattr(value, "model_fields_set", None)
    if fields_set is not None:
        return field_name in fields_set
    return hasattr(value, field_name)


def _merge_publisher_payloads(
    session: MeetingSession,
    publisher_payloads: Sequence[Any],
    *,
    authoritative: bool,
) -> list[dict[str, Any]]:
    """Merge publisher presence with any cached stream-rich publisher details.

    ``listparticipants`` reports whether a participant is publishing but does
    not include the publisher's stream list.  Retaining a previously observed
    ``streams`` field keeps multistream subscriptions and local projections
    stable while still allowing an authoritative presence snapshot to remove
    publishers that have left or stopped publishing.
    """

    session_state = session.janus_state if isinstance(session.janus_state, dict) else {}
    cached_payloads = session_state.get("participants") or []
    cached_by_id: dict[int, dict[str, Any]] = {}
    cached_order: list[int] = []
    for cached_publisher in cached_payloads:
        cached_payload = _serialize_model(cached_publisher)
        raw_publisher_id = cached_payload.get("id")
        if raw_publisher_id is None:
            continue
        publisher_id = _janus_id_from_persisted_json(
            raw_publisher_id,
            kind="Janus publisher ID",
        )
        cached_payload["id"] = publisher_id
        cached_by_id[publisher_id] = cached_payload
        cached_order.append(publisher_id)

    merged_payloads: list[dict[str, Any]] = []
    observed_ids: set[int] = set()
    for publisher in publisher_payloads:
        has_stream_topology = _field_was_provided(publisher, "streams")
        payload = _serialize_model(publisher)
        raw_publisher_id = payload.get("id")
        if raw_publisher_id is None:
            continue
        publisher_id = _require_internal_janus_id(
            raw_publisher_id,
            kind="Janus publisher ID",
        )
        payload["id"] = publisher_id
        observed_ids.add(publisher_id)

        # The management response includes joined non-publishers as well. They
        # are not valid subscription targets and supersede any stale cache row.
        if payload.get("publisher") is False:
            continue

        cached_payload = cached_by_id.get(publisher_id, {})
        merged_payload = {**cached_payload, **payload}
        if not has_stream_topology or payload.get("streams") is None:
            if "streams" in cached_payload:
                merged_payload["streams"] = cached_payload["streams"]
            else:
                merged_payload.pop("streams", None)
        merged_payloads.append(merged_payload)

    if not authoritative:
        merged_payloads.extend(
            cached_by_id[publisher_id]
            for publisher_id in cached_order
            if publisher_id not in observed_ids
        )

    return merged_payloads


async def _serialize_handle_streams(media_handle: ParticipantMediaHandle) -> list[dict[str, Any]]:
    """Serialize current inbound or outbound stream rows for signaling acknowledgements."""

    return [
        {
            "id": str(stream.pk),
            "direction": stream.direction,
            "media_kind": stream.media_kind,
            "janus_mid": stream.janus_mid,
            "janus_feed_id": optional_janus_id_to_wire(
                stream.janus_feed_id,
                name="Janus feed ID",
            ),
            "janus_feed_mid": stream.janus_feed_mid,
            "codec": stream.codec,
            "is_active": stream.is_active,
            "is_ready": stream.is_ready,
            "is_moderated": stream.is_moderated,
            "metadata": stream.metadata,
            "source_participant_id": str(stream.source_participant_id) if stream.source_participant_id else None,
        }
        async for stream in media_handle.streams.order_by("direction", "media_kind", "janus_mid")
    ]


def _track_source(track: dict[str, Any]) -> str:
    """Normalize a client track descriptor into a stable source label."""

    return str(track.get("source") or track.get("description") or track.get("kind") or "").strip().lower()


def _resolve_media_kind(media_type: str | None, description: str | None = None) -> str:
    """Map Janus and client stream labels into the local meeting media taxonomy."""

    normalized_type = str(media_type or "").strip().lower()
    normalized_description = str(description or "").strip().lower()
    if "screen" in normalized_description:
        return MediaKind.SCREEN
    if normalized_type == "audio":
        return MediaKind.AUDIO
    if normalized_type == "data":
        return MediaKind.DATA
    return MediaKind.VIDEO


def _prepare_media_handle(
    *,
    participant: Participant,
    handle_type: str,
    connection: ParticipantConnection | None = None,
    allow_ownership_handoff: bool = False,
) -> ParticipantMediaHandle:
    """Claim one handle for a connection generation under a bounded lease.

    A different active socket cannot silently steal a live process-owned
    plugin. Recovery is explicit when the prior connection is inactive, its
    heartbeat lease expired, or the new socket carries the same non-empty
    logical client-session key. A successful handoff always clears correlation
    IDs and forces a fresh attach; persisted IDs are never adopted.
    """

    active_statuses = {
        RealtimeConnectionStatus.CONNECTED,
        RealtimeConnectionStatus.SUBSCRIBED,
        RealtimeConnectionStatus.ACTIVE,
    }
    stale_seconds = max(1, int(settings.MEETING_CONNECTION_STALE_SECONDS))
    stale_before = timezone.now() - timedelta(seconds=stale_seconds)

    handoff_performed = False
    recover_detaching = False
    with transaction.atomic():
        media_handle, created = ParticipantMediaHandle.objects.get_or_create(
            participant=participant,
            handle_type=handle_type,
            defaults={
                "connection": connection
                or participant.connections.order_by("-connected_at").first(),
                "opaque_id": f"{participant.pk}:{handle_type}",
            },
        )
        if not created:
            media_handle = (
                # ``connection`` is nullable, so keep PostgreSQL's row lock
                # on the media-handle table while still preloading the join.
                ParticipantMediaHandle.objects.select_for_update(of=("self",))
                .select_related("connection", "participant")
                .get(pk=media_handle.pk)
            )

        if connection is not None:
            expected_connection = (
                ParticipantConnection.objects.select_for_update()
                .filter(pk=connection.pk)
                .first()
            )
            if (
                expected_connection is None
                or expected_connection.status not in active_statuses
                or expected_connection.participant_id != participant.pk
                or expected_connection.session_id != participant.session_id
            ):
                raise JrtcHandleOwnershipError(
                    "The requesting media connection is no longer active."
                )
        else:
            expected_connection = (
                media_handle.connection
                or participant.connections.order_by("-connected_at").first()
            )
        updates: list[str] = []
        connection_changed = bool(
            expected_connection
            and media_handle.connection_id != expected_connection.pk
        )
        has_persisted_binding = any(
            value is not None
            for value in (
                media_handle.janus_session_id,
                media_handle.janus_handle_id,
                media_handle.runtime_owner_id,
            )
        )
        recover_detaching = bool(
            not connection_changed
            and not has_persisted_binding
            and media_handle.lifecycle_state == JanusHandleLifecycleState.DETACHING
        )

        if (
            connection_changed
            and media_handle.lifecycle_state == JanusHandleLifecycleState.DETACHING
        ):
            raise JrtcHandleOwnershipError(
                "A connection handoff is already being finalized."
            )

        previous_connection = (
            ParticipantConnection.objects.select_for_update().get(
                pk=media_handle.connection_id
            )
            if connection_changed and media_handle.connection_id is not None
            else None
        )
        if (
            connection_changed
            and previous_connection is not None
            and expected_connection.connected_at <= previous_connection.connected_at
        ):
            raise JrtcHandleOwnershipError(
                "An older media connection cannot supersede a newer generation."
            )

        if connection_changed and has_persisted_binding:
            if not allow_ownership_handoff:
                raise JrtcHandleOwnershipError(
                    "A continuity command cannot replace another connection's media handle."
                )
            previous_is_active = bool(
                previous_connection
                and previous_connection.status in active_statuses
            )
            previous_lease_expired = bool(
                previous_connection
                and previous_connection.last_heartbeat_at <= stale_before
            )
            same_client_generation = bool(
                previous_connection
                and previous_connection.client_session_key
                and previous_connection.client_session_key
                == expected_connection.client_session_key
                and participant_media_plugin_is_locally_owned(media_handle)
            )
            if previous_is_active and not (
                previous_lease_expired or same_client_generation
            ):
                raise JrtcHandleOwnershipError(
                    "The media handle belongs to another active connection."
                )

            # Commit the durable generation handoff before any network cleanup.
            # The old process can no longer claim this row after its connection
            # is superseded; a local binding is invalidated after commit.
            handoff_performed = True
            if previous_connection is not None:
                previous_connection.status = RealtimeConnectionStatus.DISCONNECTED
                previous_connection.disconnected_at = timezone.now()
                previous_connection.save(
                    update_fields=["status", "disconnected_at", "updated_at"]
                )

            media_handle.janus_session_id = None
            media_handle.janus_handle_id = None
            media_handle.runtime_owner_id = None
            media_handle.runtime_claim_id = None
            # Prevent a new resolver from racing the post-commit local cleanup.
            media_handle.lifecycle_state = JanusHandleLifecycleState.DETACHING
            media_handle.selected_streams = []
            media_handle.janus_state = {}
            media_handle.jsep_offer = {}
            media_handle.jsep_answer = {}
            updates.extend(
                [
                    "janus_session_id",
                    "janus_handle_id",
                    "runtime_owner_id",
                    "runtime_claim_id",
                    "lifecycle_state",
                    "selected_streams",
                    "janus_state",
                    "jsep_offer",
                    "jsep_answer",
                ]
            )
            media_handle.streams.all().delete()
            if str(handle_type) == JanusHandleType.PUBLISHER:
                Participant.objects.filter(pk=participant.pk).update(
                    janus_publisher_id=None,
                    janus_private_id=None,
                    updated_at=timezone.now(),
                )
                participant.janus_publisher_id = None
                participant.janus_private_id = None

        if connection_changed:
            media_handle.connection = expected_connection
            updates.append("connection")
        if media_handle.lifecycle_state == JanusHandleLifecycleState.DETACHED:
            media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
            updates.append("lifecycle_state")
        if updates:
            media_handle.save(update_fields=[*dict.fromkeys(updates), "updated_at"])

    return media_handle, expected_connection, handoff_performed or recover_detaching

def _finish_media_handle_handoff(media_handle, expected_connection_id):
    with transaction.atomic():
        media_handle = ParticipantMediaHandle.objects.select_for_update().get(
            pk=media_handle.pk
        )
        if (
            media_handle.connection_id != expected_connection_id
            or media_handle.runtime_owner_id is not None
            or media_handle.lifecycle_state != JanusHandleLifecycleState.DETACHING
        ):
            raise JrtcHandleOwnershipError(
                "The media handle changed during connection handoff cleanup."
            )
        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
        media_handle.save(update_fields=["lifecycle_state", "updated_at"])
    return media_handle

def _get_or_create_media_handle(**kwargs):
    """Synchronous handoff helper retained for worker callers."""
    media_handle, expected_connection, cleanup_required = _prepare_media_handle(**kwargs)
    if cleanup_required:
        release_unclaimed_local_participant_media_plugin(media_handle)
        media_handle = _finish_media_handle_handoff(media_handle, None if expected_connection is None else expected_connection.pk)
    return media_handle


async def _aget_or_create_media_handle(**kwargs):
    janus_runtime.require_owner_loop()
    media_handle, expected_connection, cleanup_required = await sync_to_async(_prepare_media_handle)(**kwargs)
    if cleanup_required:
        await janus_runtime.registry.invalidate(str(media_handle.pk), close_local=True)
        media_handle = await sync_to_async(_finish_media_handle_handoff)(media_handle, None if expected_connection is None else expected_connection.pk)
    return media_handle


def _build_stream_descriptions(tracks: Sequence[dict[str, Any]]) -> list[StreamDescription]:
    """Convert client track descriptors into Janus stream descriptions."""

    descriptions: list[StreamDescription] = []
    seen: set[str] = set()
    for track in tracks:
        mid = str(track.get("mid") or "").strip()
        if not mid or mid in seen:
            continue
        source = _track_source(track).replace("_", "-")
        descriptions.append(StreamDescription(mid=mid, description=source or str(track.get("kind") or "media")))
        seen.add(mid)
    return descriptions


def _build_metadata(participant: Participant) -> dict[str, str]:
    """Attach lightweight correlation metadata to Janus publisher joins."""

    return {
        "participant_id": str(participant.pk),
        "profile_id": str(participant.profile_id),
        "room_id": str(participant.room_id),
        "session_id": str(participant.session_id),
    }


def _match_participant_for_publisher(
    *,
    session: MeetingSession,
    publisher_id: int,
    display_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> Participant | None:
    """Resolve a present participant from trusted Janus correlation data."""

    present_participants = session.participants.present()
    participant = present_participants.filter(janus_publisher_id=publisher_id).first()
    if participant is not None:
        return participant

    if isinstance(metadata, dict) and metadata.get("participant_id"):
        expected_metadata = {
            "session_id": session.pk,
            "room_id": session.room_id,
        }
        if any(
            metadata.get(key) is not None
            and str(metadata[key]) != str(expected_value)
            for key, expected_value in expected_metadata.items()
        ):
            return None
        try:
            return present_participants.filter(pk=metadata["participant_id"]).first()
        except (TypeError, ValueError, ValidationError):
            return None

    if display_name:
        matches = list(present_participants.filter(display_name=display_name)[:2])
        if len(matches) == 1:
            return matches[0]
    return None


def _ensure_publish_permissions(participant: Participant, tracks: Sequence[dict[str, Any]]) -> None:
    """Validate that the participant is allowed to publish the requested local tracks."""

    wants_audio = any(track.get("kind") == "audio" for track in tracks)
    wants_video = any(track.get("kind") == "video" and _track_source(track) != "screen_share" for track in tracks)
    wants_screen = any(_track_source(track) == "screen_share" for track in tracks)
    if wants_audio:
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_publish_audio")
    if wants_video:
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_publish_video")
    if wants_screen:
        MeetingPermissionService.require_participant_capability(participant=participant, capability_field="can_share_screen")


def _serialize_selected_streams(
    streams: Iterable[SubscribeTarget | UnsubscribeTarget],
) -> list[dict[str, Any]]:
    """Serialize Janus subscriber stream selections for persistence and acknowledgements."""

    serialized_streams: list[dict[str, Any]] = []
    for stream in streams:
        payload = _serialize_model(stream)
        feed = payload.get("feed")
        serialized_streams.append(
            {
                "feed": optional_janus_id_to_wire(
                    feed,
                    name="Janus feed ID",
                ),
                "mid": payload.get("mid"),
                "crossrefid": payload.get("crossrefid"),
                "sub_mid": payload.get("sub_mid"),
            }
        )
    return serialized_streams


def _persisted_subscriber_target_key(
    payload: dict[str, Any],
) -> tuple[int, str] | None:
    """Read one app-owned subscriber selection back into the integer domain."""

    raw_feed = payload.get("feed")
    if raw_feed is None:
        return None
    return (
        _janus_id_from_persisted_json(
            raw_feed,
            kind="Janus feed ID",
        ),
        str(payload.get("mid") or ""),
    )


def _reconcile_publisher_payloads(
    session: MeetingSession,
    publisher_payloads: Sequence[Any],
    *,
    prune_missing: bool = True,
) -> list[dict[str, Any]]:
    """Project Janus publisher state into participants and outbound stream rows."""

    now = timezone.now()
    serialized_publishers: list[dict[str, Any]] = []
    active_publisher_ids: set[int] = set()

    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        if payload.get("publisher") is False:
            continue
        raw_publisher_id = payload.get("id")
        if raw_publisher_id is None:
            continue
        publisher_id = _require_internal_janus_id(
            raw_publisher_id,
            kind="Janus publisher ID",
        )
        payload["id"] = publisher_id
        wire_payload = janus_event_to_wire(payload)
        serialized_publishers.append(wire_payload)
        participant = _match_participant_for_publisher(
            session=session,
            publisher_id=publisher_id,
            display_name=payload.get("display"),
            metadata=payload.get("metadata"),
        )
        if participant is None:
            continue

        active_publisher_ids.add(publisher_id)
        participant.janus_publisher_id = publisher_id
        participant.janus_state = wire_payload
        participant.last_seen_at = now
        participant.save(update_fields=["janus_publisher_id", "janus_state", "last_seen_at", "updated_at"])

        publisher_handle = participant.publisher_mediahandle_record
        if publisher_handle is None:
            continue

        stream_rows = payload.get("streams") or []
        has_complete_stream_topology = "streams" in payload and payload.get("streams") is not None
        seen_mids: set[str] = set()
        for stream_payload in stream_rows:
            stream_payload = _serialize_model(stream_payload)
            mid = str(stream_payload.get("mid") or "")
            if not mid:
                has_complete_stream_topology = False
                continue
            seen_mids.add(mid)
            ParticipantStream.objects.update_or_create(
                media_handle=publisher_handle,
                janus_mid=mid,
                defaults={
                    "participant": participant,
                    "source_participant": participant,
                    "direction": MediaDirection.OUTBOUND,
                    "media_kind": _resolve_media_kind(
                        stream_payload.get("type"),
                        stream_payload.get("description"),
                    ),
                    "janus_feed_id": publisher_id,
                    "janus_feed_mid": mid,
                    "codec": str(stream_payload.get("codec") or ""),
                    "is_active": not bool(stream_payload.get("disabled", False)),
                    "is_ready": not bool(stream_payload.get("disabled", False)),
                    "is_moderated": bool(stream_payload.get("moderated", False)),
                    "metadata": janus_event_to_wire(stream_payload),
                    "last_synced_at": now,
                },
            )

        if has_complete_stream_topology:
            publisher_handle.streams.filter(direction=MediaDirection.OUTBOUND).exclude(janus_mid__in=seen_mids).delete()
        if publisher_handle.lifecycle_state in {JanusHandleLifecycleState.JOINING, JanusHandleLifecycleState.ATTACHED} and seen_mids:
            publisher_handle.lifecycle_state = JanusHandleLifecycleState.READY
            publisher_handle.janus_state = wire_payload
            publisher_handle.last_event_at = now
            publisher_handle.save(update_fields=["lifecycle_state", "janus_state", "last_event_at", "updated_at"])

    if prune_missing:
        publisher_handles = ParticipantMediaHandle.objects.filter(
            participant__session=session,
            handle_type=JanusHandleType.PUBLISHER,
        ).select_related("participant")
        for media_handle in publisher_handles:
            raw_publisher_id = media_handle.participant.janus_publisher_id
            if raw_publisher_id is None:
                continue
            publisher_id = _require_internal_janus_id(
                raw_publisher_id,
                kind="Janus publisher ID",
            )
            if publisher_id not in active_publisher_ids:
                media_handle.streams.filter(direction=MediaDirection.OUTBOUND).delete()
                if media_handle.lifecycle_state == JanusHandleLifecycleState.READY:
                    media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHED
                    media_handle.last_event_at = now
                    media_handle.save(update_fields=["lifecycle_state", "last_event_at", "updated_at"])

    return serialized_publishers


def _reconcile_subscriber_streams(
    media_handle: ParticipantMediaHandle,
    stream_payloads: Sequence[dict[str, Any]] | None,
) -> None:
    """Project Janus subscriber state into inbound stream rows."""

    if stream_payloads is None:
        return

    participant = media_handle.participant
    session = participant.session
    now = timezone.now()
    seen_mids: set[str] = set()
    has_complete_stream_topology = True

    for payload in stream_payloads:
        payload = _serialize_model(payload)
        janus_mid = str(payload.get("mid") or "")
        if not janus_mid:
            has_complete_stream_topology = False
            continue
        seen_mids.add(janus_mid)
        raw_feed_id = payload.get("feed_id")
        if raw_feed_id is None:
            has_complete_stream_topology = False
            continue
        janus_feed_id = _require_internal_janus_id(
            raw_feed_id,
            kind="Janus feed ID",
        )
        payload["feed_id"] = janus_feed_id
        janus_feed_mid = str(payload.get("feed_mid") or "")
        source_participant = _match_participant_for_publisher(
            session=session,
            publisher_id=janus_feed_id,
            display_name=payload.get("feed_display"),
        )
        source_description = ""
        if source_participant is not None and janus_feed_mid:
            outbound_stream = source_participant.streams.filter(
                direction=MediaDirection.OUTBOUND,
                janus_mid=janus_feed_mid,
            ).first()
            source_description = str((outbound_stream.metadata or {}).get("description") or "")

        ParticipantStream.objects.update_or_create(
            media_handle=media_handle,
            janus_mid=janus_mid,
            defaults={
                "participant": participant,
                "source_participant": source_participant,
                "direction": MediaDirection.INBOUND,
                "media_kind": _resolve_media_kind(payload.get("type"), source_description),
                "janus_feed_id": janus_feed_id,
                "janus_feed_mid": janus_feed_mid,
                "codec": str(payload.get("codec") or ""),
                "is_active": bool(payload.get("active", False)),
                "is_ready": bool(payload.get("ready", False)),
                "is_moderated": not bool(payload.get("send", True)),
                "metadata": janus_event_to_wire(payload),
                "last_synced_at": now,
            },
        )

    if has_complete_stream_topology:
        media_handle.streams.filter(direction=MediaDirection.INBOUND).exclude(janus_mid__in=seen_mids).delete()


def _build_subscriber_targets(*, participant: Participant, publisher_payloads: Sequence[Any]) -> list[SubscribeTarget]:
    """Build the target publisher streams for the participant's multistream subscriber handle."""

    targets: list[SubscribeTarget] = []
    local_feed_id = (
        None
        if participant.janus_publisher_id is None
        else _require_internal_janus_id(
            participant.janus_publisher_id,
            kind="Janus publisher ID",
        )
    )
    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        raw_publisher_id = payload.get("id")
        if raw_publisher_id is None or payload.get("publisher") is False:
            continue
        publisher_id = _require_internal_janus_id(
            raw_publisher_id,
            kind="Janus publisher ID",
        )
        if publisher_id == local_feed_id:
            continue
        if "streams" not in payload or payload.get("streams") is None:
            targets.append(SubscribeTarget(feed=publisher_id))
            continue
        for stream_payload in payload.get("streams") or []:
            stream_payload = _serialize_model(stream_payload)
            media_type = str(stream_payload.get("type") or "")
            mid = str(stream_payload.get("mid") or "")
            if media_type not in {"audio", "video"} or not mid or bool(stream_payload.get("disabled", False)):
                continue
            targets.append(
                SubscribeTarget(
                    feed=publisher_id,
                    mid=mid,
                    crossrefid=f"{publisher_id}:{mid}",
                )
            )
    return targets


def _preserve_feed_wide_targets(
    targets: Sequence[SubscribeTarget],
    selected_streams: Sequence[dict[str, Any]],
) -> list[SubscribeTarget]:
    """Keep an existing feed-wide subscription when richer topology appears.

    Replacing ``feed`` with per-MID targets in one update would ask Janus to
    subscribe individual streams and unsubscribe the whole feed at once.  A
    retained feed-wide target already covers those streams (and future ones),
    so no renegotiation is necessary until the publisher itself disappears.
    """

    feed_wide_ids = {
        _janus_id_from_persisted_json(
            item["feed"],
            kind="Janus feed ID",
        )
        for item in selected_streams
        if item.get("feed") is not None and not item.get("mid")
    }
    if not feed_wide_ids:
        return list(targets)

    normalized_targets: list[SubscribeTarget] = []
    emitted_feed_wide_ids: set[int] = set()
    for target in targets:
        feed_id = _require_internal_janus_id(
            target.feed,
            kind="Janus feed ID",
        )
        if feed_id not in feed_wide_ids:
            normalized_targets.append(target)
            continue
        if feed_id not in emitted_feed_wide_ids:
            normalized_targets.append(SubscribeTarget(feed=feed_id))
            emitted_feed_wide_ids.add(feed_id)
    return normalized_targets


def _serialize_trickle_candidate(payload: IceCandidate) -> TrickleCandidate:
    return normalize_ice_request({"candidate": payload}).candidates[0]


def _serialize_trickle_candidates(candidates: IceCandidates) -> list[TrickleCandidate]:
    return list(normalize_ice_request({"candidates": list(candidates)}).candidates)


class MeetingMediaSignalService:
    """Own the Janus-backed signaling flows used by the browser client."""

    @staticmethod
    def sync_publishers(*, session: MeetingSession, emit_state: bool = True):
        """Celery compatibility. ASGI uses async_sync_publishers directly."""
        if janus_runtime.state != janus_runtime.RUNNING:
            janus_runtime.ensure_background()
        return janus_runtime.run(MeetingMediaSignalService.async_sync_publishers(session=session, emit_state=emit_state))

    @staticmethod
    async def async_sync_publishers(*, session: MeetingSession, emit_state: bool = True) -> dict[str, Any]:
        """Synchronize Janus publisher state into participants and outbound stream rows."""

        response = await acall_video_room_management_method(
            session,
            "list_participants",
            _janus_room_id(session),
        )
        publisher_payloads = _reply_items(video_room_reply_data(response), "participants")
        merged_publishers = _merge_publisher_payloads(
            session,
            publisher_payloads,
            authoritative=True,
        )
        serialized_publishers = await sync_to_async(transaction.atomic(_reconcile_publisher_payloads))(
            session,
            merged_publishers,
        )
        session_state = session.janus_state if isinstance(session.janus_state, dict) else {}
        session.janus_state = {**session_state, "participants": serialized_publishers}
        session.last_synced_at = timezone.now()
        await session.asave(update_fields=["janus_state", "last_synced_at", "updated_at"])
        await sync_to_async(transaction.atomic(MeetingLifecycleService.refresh_session_metrics))(session=session)
        if emit_state:
            await MeetingSocketEmitter.aemit_session_state(session=session)
        return {"publishers": serialized_publishers}

    @staticmethod
    async def publish_offer(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
        offer: dict[str, Any],
        tracks: Sequence[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Join, publish, or reconfigure the participant publisher handle with a browser SDP offer."""

        if not offer.get("sdp"):
            raise MeetingDomainError("A publisher SDP offer is required.")

        track_descriptors = list(tracks or [])
        _ensure_publish_permissions(participant, track_descriptors)
        media_handle = await _aget_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.PUBLISHER,
            connection=connection,
            allow_ownership_handoff=True,
        )
        bound_handle = await aensure_participant_media_plugin(media_handle)
        await participant.arefresh_from_db(
            fields=["janus_publisher_id", "janus_private_id"],
        )
        publisher_id = (
            None
            if participant.janus_publisher_id is None
            else _require_internal_janus_id(
                participant.janus_publisher_id,
                kind="Janus publisher ID",
            )
        )
        descriptions = _build_stream_descriptions(track_descriptors)
        offer_jsep = _build_jsep(offer, jsep_type="offer")

        claim = await sync_to_async(_claim_media_command)(media_handle, bound_handle)
        command_completed = False
        try:
            method_name = "configure"
            # Once invocation begins Janus may apply the request even if its
            # final event times out or response parsing fails. Compensate such
            # ambiguity by exact-detaching this binding.
            command_completed = True
            if publisher_id is None:
                method_name = "join_and_configure"
                response = await acall_plugin_method(
                    bound_handle,
                    "join_and_configure",
                    PublisherJoinAndConfigureRequest(
                        room=_janus_room_id(participant.session),
                        display=participant.display_name or None,
                        pin=participant.session.janus_room_pin or None,
                        metadata=_build_metadata(participant),
                        descriptions=descriptions,
                    ),
                    offer_jsep,
                )
            elif media_handle.lifecycle_state == JanusHandleLifecycleState.ATTACHED:
                method_name = "publish"
                response = await acall_plugin_method(
                    bound_handle,
                    "publish",
                    offer_jsep,
                    body=PublisherPublishRequest(descriptions=descriptions),
                )
            else:
                response = await acall_plugin_method(
                    bound_handle,
                    "configure_publisher",
                    PublisherConfigureRequest(descriptions=descriptions),
                    offer=offer_jsep,
                )
            plugin_data = video_room_reply_data(response)
            reply_publishers = _reply_items(plugin_data, "publishers")
            serialized_response = serialize_janus_response(response)
            serialized_answer = _serialize_jsep(getattr(response, "jsep", None))
            now = timezone.now()

            def persist_result():
                nonlocal media_handle, participant
                with transaction.atomic():
                    media_handle = _lock_media_command_result(claim)
                    participant = (
                        Participant.objects.select_for_update()
                        .select_related("session")
                        .get(pk=participant.pk)
                    )
                    media_handle.lifecycle_state = JanusHandleLifecycleState.JOINING
                    media_handle.jsep_offer = offer
                    media_handle.jsep_answer = (
                        serialized_answer or media_handle.jsep_answer
                    )
                    media_handle.selected_streams = track_descriptors
                    media_handle.janus_state = serialized_response
                    media_handle.last_event_at = now
                    media_handle.runtime_claim_id = None
                    media_handle.save(
                        update_fields=[
                            "lifecycle_state",
                            "jsep_offer",
                            "jsep_answer",
                            "selected_streams",
                            "janus_state",
                            "last_event_at",
                            "runtime_claim_id",
                            "updated_at",
                        ]
                    )
                    if plugin_data is not None:
                        plugin_payload = _serialize_model(plugin_data)
                        raw_publisher_id = plugin_payload.get("id")
                        if raw_publisher_id is not None:
                            participant.janus_publisher_id = _require_internal_janus_id(
                                raw_publisher_id,
                                kind="Janus publisher ID",
                            )
                        raw_private_id = plugin_payload.get("private_id")
                        if raw_private_id is not None:
                            participant.janus_private_id = _require_internal_janus_id(
                                raw_private_id,
                                kind="Janus private ID",
                            )
                        participant.janus_state = janus_event_to_wire(plugin_payload)
                    participant.last_seen_at = now
                    participant.save(
                        update_fields=[
                            "janus_publisher_id",
                            "janus_private_id",
                            "janus_state",
                            "last_seen_at",
                            "updated_at",
                        ]
                    )

                    if reply_publishers:
                        merged_publishers = _merge_publisher_payloads(
                            participant.session,
                            reply_publishers,
                            authoritative=False,
                        )
                        serialized_publishers = _reconcile_publisher_payloads(
                            participant.session,
                            merged_publishers,
                            prune_missing=False,
                        )
                        participant.session.janus_state = {
                            **(
                                participant.session.janus_state
                                if isinstance(participant.session.janus_state, dict)
                                else {}
                            ),
                            "participants": serialized_publishers,
                        }
                        participant.session.last_synced_at = now
                        participant.session.save(
                            update_fields=["janus_state", "last_synced_at", "updated_at"],
                        )
            await sync_to_async(persist_result)()
        except BaseException:
            if command_completed:
                await _abort_stateful_media_command(
                    claim,
                    handle_type=JanusHandleType.PUBLISHER,
                )
            await sync_to_async(_release_media_command_claim)(claim)
            raise

        try:
            await MeetingMediaSignalService.async_sync_publishers(
                session=participant.session,
                emit_state=False,
            )
        except JanusGatewayError:
            # The publisher command and SDP answer already succeeded. A
            # secondary participant-list refresh must not make the browser
            # repeat the state-changing publish request.
            logger.warning(
                "Publisher error",
                "Publisher %s negotiated successfully but the follow-up Janus state sync failed",
                context={
                  "participant_id":participant.pk,
                },
                exc_info=True,
            )
        await MeetingSocketEmitter.aemit_session_state(session=participant.session)
        await media_handle.arefresh_from_db()
        return {
            "action": method_name,
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.PUBLISHER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": serialized_answer,
            "streams": await _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    async def unpublish(*, participant: Participant, connection: ParticipantConnection | None = None) -> dict[str, Any]:
        """Stop the participant publisher handle without detaching it from the room."""

        media_handle = await _aget_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.PUBLISHER,
            connection=connection,
        )
        bound_handle = None
        if media_handle.janus_handle_id is not None:
            _require_internal_janus_id(
                media_handle.janus_handle_id,
                kind="Janus handle ID",
            )
            bound_handle = await aensure_participant_media_plugin(
                media_handle,
                recreate=False,
            )
        claim = await sync_to_async(_claim_media_command)(media_handle, bound_handle)
        command_completed = False
        try:
            if bound_handle is not None:
                command_completed = True
                await acall_plugin_method(bound_handle, "unpublish")
            def persist_result():
                nonlocal media_handle
                with transaction.atomic():
                    media_handle = _lock_media_command_result(claim)
                    media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHED
                    media_handle.selected_streams = []
                    media_handle.last_event_at = timezone.now()
                    media_handle.runtime_claim_id = None
                    media_handle.save(
                        update_fields=[
                            "lifecycle_state",
                            "selected_streams",
                            "last_event_at",
                            "runtime_claim_id",
                            "updated_at",
                        ]
                    )
                    media_handle.streams.filter(direction=MediaDirection.OUTBOUND).delete()
            await sync_to_async(persist_result)()
        except BaseException:
            if command_completed:
                await _abort_stateful_media_command(
                    claim,
                    handle_type=JanusHandleType.PUBLISHER,
                )
            await sync_to_async(_release_media_command_claim)(claim)
            raise
        await sync_to_async(transaction.atomic(MeetingLifecycleService.refresh_session_metrics))(session=participant.session)
        await MeetingSocketEmitter.aemit_session_state(session=participant.session)
        return {
            "action": "unpublish",
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.PUBLISHER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": None,
            "streams": await _serialize_handle_streams(media_handle),
            "selected_streams": [],
        }

    @staticmethod
    async def sync_subscriptions(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
    ) -> dict[str, Any]:
        """Join or update the participant subscriber handle so it mirrors all remote publishers."""

        media_handle = await _aget_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.SUBSCRIBER,
            connection=connection,
            allow_ownership_handoff=True,
        )
        bound_handle = await aensure_participant_media_plugin(
            media_handle,
            recreate=True,
        )
        await participant.arefresh_from_db(
            fields=["janus_publisher_id", "janus_private_id"],
        )
        publisher_response = await acall_video_room_management_method(
            participant.session,
            "list_participants",
            _janus_room_id(participant.session),
        )
        publisher_payloads = _reply_items(
            video_room_reply_data(publisher_response),
            "participants",
        )
        merged_publishers = _merge_publisher_payloads(
            participant.session,
            publisher_payloads,
            authoritative=True,
        )
        serialized_publishers = await sync_to_async(transaction.atomic(_reconcile_publisher_payloads))(
            participant.session,
            merged_publishers,
        )
        targets = _build_subscriber_targets(
            participant=participant,
            publisher_payloads=merged_publishers,
        )
        targets = _preserve_feed_wide_targets(
            targets,
            media_handle.selected_streams,
        )
        serialized_targets = _serialize_selected_streams(targets)
        current_targets = {
            target_key
            for item in media_handle.selected_streams
            if (target_key := _persisted_subscriber_target_key(item)) is not None
        }
        next_targets = {
            (
                _require_internal_janus_id(
                    item.feed,
                    kind="Janus feed ID",
                ),
                str(item.mid or ""),
            )
            for item in targets
        }
        subscriber_joined = _subscriber_is_joined(media_handle, current_targets)

        async with _media_command_claim(media_handle, bound_handle) as claim:
            action = "noop"
            response = None
            jsep_payload = None
            stream_payloads: Sequence[dict[str, Any]] | None = None
            next_janus_state = media_handle.janus_state
            next_jsep_offer = media_handle.jsep_offer
            next_lifecycle_state = media_handle.lifecycle_state
            next_last_event_at = media_handle.last_event_at

            if not serialized_targets and not current_targets:
                pass
            elif not subscriber_joined:
                claim.command_applied = True
                response = await acall_plugin_method(
                    bound_handle,
                    "join_subscriber",
                    SubscriberJoinRequest(
                        room=_janus_room_id(participant.session),
                        pin=participant.session.janus_room_pin or None,
                        private_id=(
                            None
                            if participant.janus_private_id is None
                            else _require_internal_janus_id(
                                participant.janus_private_id,
                                kind="Janus private ID",
                            )
                        ),
                        streams=targets,
                        use_msid=True,
                        autoupdate=True,
                    ),
                )
                action = "join"
            elif current_targets != next_targets:
                subscribe_targets = [
                    item
                    for item in targets
                    if (
                        (
                            _require_internal_janus_id(
                                item.feed,
                                kind="Janus feed ID",
                            ),
                            str(item.mid or ""),
                        )
                        not in current_targets
                    )
                ]
                unsubscribe_targets: list[UnsubscribeTarget] = []
                for item in media_handle.selected_streams:
                    target_key = _persisted_subscriber_target_key(item)
                    if target_key is not None and target_key in next_targets:
                        continue
                    raw_feed = item.get("feed")
                    feed = (
                        None
                        if raw_feed is None
                        else _janus_id_from_persisted_json(
                            raw_feed,
                            kind="Janus feed ID",
                        )
                    )
                    mid = str(item.get("mid") or "") or None
                    sub_mid = str(item.get("sub_mid") or "") or None
                    if feed is None and sub_mid is None:
                        continue
                    unsubscribe_targets.append(
                        UnsubscribeTarget(
                            feed=feed,
                            mid=mid if feed is not None else None,
                            sub_mid=sub_mid,
                        )
                    )
                if subscribe_targets or unsubscribe_targets:
                    claim.command_applied = True
                    response = await acall_plugin_method(
                        bound_handle,
                        "update_subscription",
                        SubscriberUpdateRequest(
                            subscribe=subscribe_targets or None,
                            unsubscribe=unsubscribe_targets or None,
                        ),
                    )
                    action = "update"

            if response is not None:
                next_janus_state = _with_subscriber_joined_state(
                    serialize_janus_response(response),
                    joined=True,
                )
                next_last_event_at = timezone.now()
                plugin_data = video_room_reply_data(response)
                plugin_payload = _serialize_model(plugin_data)
                if plugin_data is not None and plugin_payload.get("streams") is not None:
                    stream_payloads = list(plugin_payload["streams"])
                jsep_payload = _serialize_jsep(getattr(response, "jsep", None))
                if jsep_payload and jsep_payload.get("type") == "offer":
                    next_jsep_offer = jsep_payload
                if jsep_payload or action == "join":
                    next_lifecycle_state = JanusHandleLifecycleState.JOINING

            def persist_result():
                nonlocal media_handle
                with transaction.atomic():
                    media_handle = _lock_media_command_result(claim)
                    media_handle.selected_streams = serialized_targets
                    media_handle.janus_state = next_janus_state
                    media_handle.jsep_offer = next_jsep_offer
                    media_handle.lifecycle_state = next_lifecycle_state
                    media_handle.last_event_at = next_last_event_at
                    media_handle.runtime_claim_id = None
                    media_handle.save(
                        update_fields=[
                            "selected_streams",
                            "janus_state",
                            "jsep_offer",
                            "lifecycle_state",
                            "last_event_at",
                            "runtime_claim_id",
                            "updated_at",
                        ]
                    )
                    _reconcile_subscriber_streams(media_handle, stream_payloads)
            await sync_to_async(persist_result)()

        session_state = (
            participant.session.janus_state
            if isinstance(participant.session.janus_state, dict)
            else {}
        )
        participant.session.janus_state = {
            **session_state,
            "participants": serialized_publishers,
        }
        participant.session.last_synced_at = timezone.now()
        await participant.session.asave(update_fields=["janus_state", "last_synced_at", "updated_at"])
        await sync_to_async(transaction.atomic(MeetingLifecycleService.refresh_session_metrics))(session=participant.session)
        await MeetingSocketEmitter.aemit_session_state(session=participant.session)
        await media_handle.arefresh_from_db()
        return {
            "action": action,
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.SUBSCRIBER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": jsep_payload,
            "streams": await _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    async def start_subscriber(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
        answer: dict[str, Any],
    ) -> dict[str, Any]:
        """Start media flow on a subscriber handle with the browser's SDP answer."""

        if not answer.get("sdp"):
            raise MeetingDomainError("A subscriber SDP answer is required.")

        media_handle = await _aget_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.SUBSCRIBER,
            connection=connection,
        )
        bound_handle = await aensure_participant_media_plugin(
            media_handle,
            recreate=False,
        )
        async with _media_command_claim(media_handle, bound_handle) as claim:
            claim.command_applied = True
            response = await acall_plugin_method(
                bound_handle,
                "start",
                answer=_build_jsep(answer, jsep_type="answer"),
            )
            next_janus_state = _with_subscriber_joined_state(
                serialize_janus_response(response),
                joined=True,
            )
            def persist_result():
                nonlocal media_handle
                with transaction.atomic():
                    media_handle = _lock_media_command_result(claim)
                    media_handle.jsep_answer = answer
                    media_handle.janus_state = next_janus_state
                    media_handle.lifecycle_state = JanusHandleLifecycleState.READY
                    media_handle.last_event_at = timezone.now()
                    media_handle.runtime_claim_id = None
                    media_handle.save(
                        update_fields=[
                            "jsep_answer",
                            "janus_state",
                            "lifecycle_state",
                            "last_event_at",
                            "runtime_claim_id",
                            "updated_at",
                        ]
                    )
            await sync_to_async(persist_result)()
        await MeetingSocketEmitter.aemit_session_state(session=participant.session)
        return {
            "action": "start",
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.SUBSCRIBER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": None,
            "streams": await _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    async def trickle(*, participant: Participant, connection: ParticipantConnection | None,
        handle_type: str, candidates=None, completed: bool = False) -> dict[str, Any]:
        """Authorize a healthy live binding without a durable ICE claim or write."""
        if handle_type not in {JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER}:
            raise MeetingDomainError("Unsupported Janus handle type for ICE trickle.")
        batch = normalize_ice_request({"candidates": candidates if candidates is not None else [], "completed": completed})
        janus_runtime.require_owner_loop()
        if connection is None:
            raise JrtcHandleOwnershipError("ICE requires an active socket connection.")
        media_handle = await ParticipantMediaHandle.objects.select_related("participant__session", "connection").filter(
            participant_id=participant.pk, handle_type=handle_type, connection_id=connection.pk,
        ).afirst()
        binding = None if media_handle is None else await janus_runtime.registry.get(str(media_handle.pk))
        if binding is None:
            # Misses keep the durable owner/connection reconciliation. Continuity
            # ICE never reconstructs a lost plugin from persisted Janus IDs.
            media_handle = await _aget_or_create_media_handle(participant=participant,
                handle_type=handle_type, connection=connection)
            binding = await aensure_participant_media_plugin(media_handle, recreate=False)
        if binding.owner_id != janus_runtime.owner_id or binding.connection_id != str(connection.pk):
            raise JrtcHandleOwnershipError("The ICE binding belongs to another connection generation.")

        async def authorize():
            # This read is inside the registry invocation fence, including on
            # completion after a candidate ACK. Revocation is never inferred
            # from a local cache hit. Existing disconnect/handoff invalidation
            # uses the same fence; state commands retain their durable claims.
            allowed = await ParticipantMediaHandle.objects.filter(pk=media_handle.pk,
                participant_id=participant.pk, participant__status__in=[ParticipantStatus.ADMITTED, ParticipantStatus.ACTIVE],
                participant__session_id=participant.session_id,
                connection_id=connection.pk, connection__socket_id=connection.socket_id,
                connection__participant_id=participant.pk, connection__session_id=participant.session_id,
                connection__status__in=[RealtimeConnectionStatus.CONNECTED, RealtimeConnectionStatus.SUBSCRIBED, RealtimeConnectionStatus.ACTIVE],
                runtime_owner_id=binding.owner_id, runtime_claim_id__isnull=True,
                janus_session_id=binding.session_id, janus_handle_id=binding.handle_id,
            ).exclude(lifecycle_state__in=[JanusHandleLifecycleState.DETACHING, JanusHandleLifecycleState.DETACHED, JanusHandleLifecycleState.FAILED]).aexists()
            if not allowed or not janus_runtime.registry._binding_is_live(binding):
                raise JrtcHandleOwnershipError("The ICE connection or handle generation is no longer authorized.")

        await janus_runtime.adapter.trickle_batch(binding, batch.candidates,
            completed=batch.completed, authorize=authorize)
        # Connection heartbeat already coalesces durable activity. ICE itself
        # performs no per-batch last_event_at or last_seen_at persistence.
        return {"action": "trickle", "participant_id": str(participant.pk), "handle_type": handle_type,
            "lifecycle_state": media_handle.lifecycle_state, "jsep": None,
            "streams": await _serialize_handle_streams(media_handle), "selected_streams": media_handle.selected_streams}

    @staticmethod
    def handle_callback_snapshot(instance: Any, normalized_event: dict[str, Any]) -> None:
        """Apply lightweight lifecycle and JSEP state changes from Janus callbacks."""

        if not isinstance(instance, ParticipantMediaHandle):
            return

        update_fields: dict[str, Any] = {
            "last_event_at": timezone.now(),
            "updated_at": timezone.now(),
        }
        jsep_payload = normalized_event.get("jsep")
        if isinstance(jsep_payload, dict):
            if jsep_payload.get("type") == "offer":
                update_fields["jsep_offer"] = jsep_payload
            elif jsep_payload.get("type") == "answer":
                update_fields["jsep_answer"] = jsep_payload

        janus_type = normalized_event.get("janus")
        if janus_type == "webrtcup":
            update_fields["lifecycle_state"] = JanusHandleLifecycleState.READY
        elif janus_type == "hangup":
            update_fields["lifecycle_state"] = JanusHandleLifecycleState.ATTACHED
        elif janus_type == "timeout":
            update_fields["lifecycle_state"] = JanusHandleLifecycleState.FAILED
        elif janus_type == "detached":
            update_fields["lifecycle_state"] = JanusHandleLifecycleState.DETACHED
            update_fields["janus_handle_id"] = None
            update_fields["janus_session_id"] = None
            update_fields["runtime_owner_id"] = None
            update_fields["runtime_claim_id"] = None
            update_fields["selected_streams"] = []
            update_fields["janus_state"] = _with_subscriber_joined_state(
                normalized_event,
                joined=False,
            )
            instance.streams.all().delete()
            if instance.handle_type == JanusHandleType.PUBLISHER:
                instance.participant.__class__.objects.filter(
                    pk=instance.participant_id,
                ).update(
                    janus_publisher_id=None,
                    janus_private_id=None,
                    updated_at=timezone.now(),
                )

        plugin_payload = ((normalized_event.get("plugindata") or {}).get("data") or {})
        if plugin_payload.get("videoroom") in {"joined", "attached"}:
            update_fields.setdefault("lifecycle_state", JanusHandleLifecycleState.JOINING)
        if (
            instance.handle_type == JanusHandleType.SUBSCRIBER
            and plugin_payload.get("videoroom") in {"attached", "updated", "started"}
        ):
            update_fields["janus_state"] = _with_subscriber_joined_state(
                normalized_event,
                joined=True,
            )

        publisher_payloads = plugin_payload.get("publishers") or []
        if publisher_payloads:
            session = instance.participant.session
            merged_publishers = _merge_publisher_payloads(
                session,
                publisher_payloads,
                authoritative=False,
            )
            serialized_publishers = _reconcile_publisher_payloads(
                session,
                merged_publishers,
                prune_missing=False,
            )
            session.janus_state = {
                **(session.janus_state if isinstance(session.janus_state, dict) else {}),
                "participants": serialized_publishers,
            }
            session.last_synced_at = timezone.now()
            session.save(
                update_fields=["janus_state", "last_synced_at", "updated_at"],
            )

        if len(update_fields) > 2:
            instance.__class__.objects.filter(pk=instance.pk).update(**update_fields)
