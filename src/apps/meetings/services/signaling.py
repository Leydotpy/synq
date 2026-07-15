"""Janus signaling helpers that bridge Socket.IO meeting clients to backend-owned handles."""

from __future__ import annotations

import logging
from typing import Any, Iterable, Sequence

from django.core.exceptions import ValidationError
from django.db import transaction
from django.utils import timezone
from janus_api.models.base import Jsep
from janus_api.models.request import TrickleCandidate
from janus_videoroom_plugin import (
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
)
from apps.meetings.realtime.emitter import MeetingSocketEmitter
from apps.meetings.services.janus import (
    call_plugin_method,
    call_video_room_management_method,
    ensure_participant_media_plugin,
    serialize_janus_response,
    video_room_reply_data,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.permissions import MeetingPermissionService

logger = logging.getLogger(__name__)


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
    current_targets: set[tuple[str, str]],
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


def _janus_room_id(session: MeetingSession) -> str:
    """Return the configured Janus room identifier with the historic UUID fallback."""

    return session.janus_room_id or str(session.pk)


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
    cached_by_id: dict[str, dict[str, Any]] = {}
    cached_order: list[str] = []
    for cached_publisher in cached_payloads:
        cached_payload = _serialize_model(cached_publisher)
        publisher_id = str(cached_payload.get("id") or "")
        if not publisher_id:
            continue
        cached_by_id[publisher_id] = cached_payload
        cached_order.append(publisher_id)

    merged_payloads: list[dict[str, Any]] = []
    observed_ids: set[str] = set()
    for publisher in publisher_payloads:
        has_stream_topology = _field_was_provided(publisher, "streams")
        payload = _serialize_model(publisher)
        publisher_id = str(payload.get("id") or "")
        if not publisher_id:
            continue
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


def _serialize_handle_streams(media_handle: ParticipantMediaHandle) -> list[dict[str, Any]]:
    """Serialize current inbound or outbound stream rows for signaling acknowledgements."""

    return [
        {
            "id": str(stream.pk),
            "direction": stream.direction,
            "media_kind": stream.media_kind,
            "janus_mid": stream.janus_mid,
            "janus_feed_id": stream.janus_feed_id,
            "janus_feed_mid": stream.janus_feed_mid,
            "codec": stream.codec,
            "is_active": stream.is_active,
            "is_ready": stream.is_ready,
            "is_moderated": stream.is_moderated,
            "metadata": stream.metadata,
            "source_participant_id": str(stream.source_participant_id) if stream.source_participant_id else None,
        }
        for stream in media_handle.streams.order_by("direction", "media_kind", "janus_mid")
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


def _get_or_create_media_handle(
    *,
    participant: Participant,
    handle_type: str,
    connection: ParticipantConnection | None = None,
) -> ParticipantMediaHandle:
    """Return the persisted participant media handle, creating it on demand."""

    media_handle, _ = ParticipantMediaHandle.objects.get_or_create(
        participant=participant,
        handle_type=handle_type,
        defaults={
            "connection": connection or participant.connections.order_by("-connected_at").first(),
            "opaque_id": f"{participant.pk}:{handle_type}",
        },
    )
    expected_connection = connection or media_handle.connection or participant.connections.order_by("-connected_at").first()
    updates: list[str] = []
    if expected_connection and media_handle.connection_id != expected_connection.pk:
        media_handle.connection = expected_connection
        updates.append("connection")
    if media_handle.lifecycle_state == JanusHandleLifecycleState.DETACHED:
        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHING
        updates.append("lifecycle_state")
    if updates:
        media_handle.save(update_fields=[*updates, "updated_at"])
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
    publisher_id: str,
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
                "feed": str(feed) if feed is not None else None,
                "mid": payload.get("mid"),
                "crossrefid": payload.get("crossrefid"),
                "sub_mid": payload.get("sub_mid"),
            }
        )
    return serialized_streams


def _reconcile_publisher_payloads(
    session: MeetingSession,
    publisher_payloads: Sequence[Any],
    *,
    prune_missing: bool = True,
) -> list[dict[str, Any]]:
    """Project Janus publisher state into participants and outbound stream rows."""

    now = timezone.now()
    serialized_publishers: list[dict[str, Any]] = []
    active_publisher_ids: set[str] = set()

    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        if payload.get("publisher") is False:
            continue
        serialized_publishers.append(payload)
        publisher_id = str(payload.get("id") or "")
        if not publisher_id:
            continue
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
        participant.janus_state = payload
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
                    "metadata": stream_payload,
                    "last_synced_at": now,
                },
            )

        if has_complete_stream_topology:
            publisher_handle.streams.filter(direction=MediaDirection.OUTBOUND).exclude(janus_mid__in=seen_mids).delete()
        if publisher_handle.lifecycle_state in {JanusHandleLifecycleState.JOINING, JanusHandleLifecycleState.ATTACHED} and seen_mids:
            publisher_handle.lifecycle_state = JanusHandleLifecycleState.READY
            publisher_handle.janus_state = payload
            publisher_handle.last_event_at = now
            publisher_handle.save(update_fields=["lifecycle_state", "janus_state", "last_event_at", "updated_at"])

    if prune_missing:
        publisher_handles = ParticipantMediaHandle.objects.filter(
            participant__session=session,
            handle_type=JanusHandleType.PUBLISHER,
        ).select_related("participant")
        for media_handle in publisher_handles:
            if media_handle.participant.janus_publisher_id and media_handle.participant.janus_publisher_id not in active_publisher_ids:
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
        janus_feed_id = str(payload.get("feed_id") or "")
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
                "metadata": payload,
                "last_synced_at": now,
            },
        )

    if has_complete_stream_topology:
        media_handle.streams.filter(direction=MediaDirection.INBOUND).exclude(janus_mid__in=seen_mids).delete()


def _build_subscriber_targets(*, participant: Participant, publisher_payloads: Sequence[Any]) -> list[SubscribeTarget]:
    """Build the target publisher streams for the participant's multistream subscriber handle."""

    targets: list[SubscribeTarget] = []
    local_feed_id = str(participant.janus_publisher_id or "")
    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        publisher_id = str(payload.get("id") or "")
        if not publisher_id or publisher_id == local_feed_id or payload.get("publisher") is False:
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
        str(item.get("feed") or "")
        for item in selected_streams
        if item.get("feed") and not item.get("mid")
    }
    if not feed_wide_ids:
        return list(targets)

    normalized_targets: list[SubscribeTarget] = []
    emitted_feed_wide_ids: set[str] = set()
    for target in targets:
        feed_id = str(target.feed)
        if feed_id not in feed_wide_ids:
            normalized_targets.append(target)
            continue
        if feed_id not in emitted_feed_wide_ids:
            normalized_targets.append(SubscribeTarget(feed=feed_id))
            emitted_feed_wide_ids.add(feed_id)
    return normalized_targets


def _serialize_trickle_candidates(candidates: Sequence[dict[str, Any]]) -> list[TrickleCandidate]:
    """Validate and coerce trickle candidate payloads from the frontend."""

    serialized: list[TrickleCandidate] = []
    for payload in candidates:
        candidate_value = payload.get("candidate")
        if not candidate_value:
            continue
        serialized.append(
            TrickleCandidate(
                candidate=str(candidate_value),
                sdpMid=payload.get("sdpMid"),
                sdpMLineIndex=payload.get("sdpMLineIndex"),
            )
        )
    return serialized


class MeetingMediaSignalService:
    """Own the Janus-backed signaling flows used by the browser client."""

    @staticmethod
    def sync_publishers(*, session: MeetingSession, emit_state: bool = True) -> dict[str, Any]:
        """Synchronize Janus publisher state into participants and outbound stream rows."""

        response = call_video_room_management_method(
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
        serialized_publishers = _reconcile_publisher_payloads(
            session,
            merged_publishers,
        )
        session.janus_state = {**session.janus_state, "participants": serialized_publishers}
        session.last_synced_at = timezone.now()
        session.save(update_fields=["janus_state", "last_synced_at", "updated_at"])
        MeetingLifecycleService.refresh_session_metrics(session=session)
        if emit_state:
            MeetingSocketEmitter.emit_session_state(session=session)
        return {"publishers": serialized_publishers}

    @staticmethod
    def publish_offer(
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
        media_handle = _get_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.PUBLISHER,
            connection=connection,
        )
        bound_handle = ensure_participant_media_plugin(media_handle)
        participant.refresh_from_db(
            fields=["janus_publisher_id", "janus_private_id"],
        )
        descriptions = _build_stream_descriptions(track_descriptors)
        offer_jsep = _build_jsep(offer, jsep_type="offer")

        method_name = "configure"
        if not participant.janus_publisher_id:
            method_name = "join_and_configure"
            response = call_plugin_method(
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
            response = call_plugin_method(
                bound_handle,
                "publish",
                offer_jsep,
                body=PublisherPublishRequest(descriptions=descriptions),
            )
        else:
            response = call_plugin_method(
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

        with transaction.atomic():
            media_handle.connection = connection or media_handle.connection
            media_handle.lifecycle_state = JanusHandleLifecycleState.JOINING
            media_handle.jsep_offer = offer
            media_handle.jsep_answer = serialized_answer or media_handle.jsep_answer
            media_handle.selected_streams = track_descriptors
            media_handle.janus_state = serialized_response
            media_handle.last_event_at = now
            media_handle.save(
                update_fields=[
                    "connection",
                    "lifecycle_state",
                    "jsep_offer",
                    "jsep_answer",
                    "selected_streams",
                    "janus_state",
                    "last_event_at",
                    "updated_at",
                ]
            )
            if plugin_data is not None:
                plugin_payload = _serialize_model(plugin_data)
                participant.janus_publisher_id = str(plugin_payload.get("id") or participant.janus_publisher_id or "")
                participant.janus_private_id = str(plugin_payload.get("private_id") or participant.janus_private_id or "")
                participant.janus_state = plugin_payload
            participant.last_seen_at = now
            participant.save(update_fields=["janus_publisher_id", "janus_private_id", "janus_state", "last_seen_at", "updated_at"])

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
                    **participant.session.janus_state,
                    "participants": serialized_publishers,
                }
                participant.session.last_synced_at = now
                participant.session.save(
                    update_fields=["janus_state", "last_synced_at", "updated_at"],
                )

        try:
            MeetingMediaSignalService.sync_publishers(
                session=participant.session,
                emit_state=False,
            )
        except JanusGatewayError:
            # The publisher command and SDP answer already succeeded. A
            # secondary participant-list refresh must not make the browser
            # repeat the state-changing publish request.
            logger.warning(
                "Publisher %s negotiated successfully but the follow-up Janus state sync failed",
                participant.pk,
                exc_info=True,
            )
        MeetingSocketEmitter.emit_session_state(session=participant.session)
        media_handle.refresh_from_db()
        return {
            "action": method_name,
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.PUBLISHER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": serialized_answer,
            "streams": _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    def unpublish(*, participant: Participant, connection: ParticipantConnection | None = None) -> dict[str, Any]:
        """Stop the participant publisher handle without detaching it from the room."""

        media_handle = _get_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.PUBLISHER,
            connection=connection,
        )
        if media_handle.janus_handle_id:
            bound_handle = ensure_participant_media_plugin(media_handle)
            call_plugin_method(bound_handle, "unpublish")

        media_handle.lifecycle_state = JanusHandleLifecycleState.ATTACHED
        media_handle.selected_streams = []
        media_handle.last_event_at = timezone.now()
        media_handle.save(update_fields=["lifecycle_state", "selected_streams", "last_event_at", "updated_at"])
        media_handle.streams.filter(direction=MediaDirection.OUTBOUND).delete()
        MeetingLifecycleService.refresh_session_metrics(session=participant.session)
        MeetingSocketEmitter.emit_session_state(session=participant.session)
        return {
            "action": "unpublish",
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.PUBLISHER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": None,
            "streams": _serialize_handle_streams(media_handle),
            "selected_streams": [],
        }

    @staticmethod
    def sync_subscriptions(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
    ) -> dict[str, Any]:
        """Join or update the participant subscriber handle so it mirrors all remote publishers."""

        media_handle = _get_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.SUBSCRIBER,
            connection=connection,
        )
        bound_handle = ensure_participant_media_plugin(media_handle)
        participant.refresh_from_db(
            fields=["janus_publisher_id", "janus_private_id"],
        )
        publisher_response = call_video_room_management_method(
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
        serialized_publishers = _reconcile_publisher_payloads(
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
            (str(item.get("feed") or ""), str(item.get("mid") or ""))
            for item in media_handle.selected_streams
        }
        next_targets = {
            (str(item.get("feed") or ""), str(item.get("mid") or ""))
            for item in serialized_targets
        }
        subscriber_joined = _subscriber_is_joined(media_handle, current_targets)

        action = "noop"
        response = None
        jsep_payload = None
        stream_payloads: Sequence[dict[str, Any]] | None = None

        if not serialized_targets and not current_targets:
            pass
        elif not subscriber_joined:
            response = call_plugin_method(
                bound_handle,
                "join_subscriber",
                SubscriberJoinRequest(
                    room=_janus_room_id(participant.session),
                    pin=participant.session.janus_room_pin or None,
                    private_id=participant.janus_private_id or None,
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
                if (str(item.feed), str(item.mid or "")) not in current_targets
            ]
            unsubscribe_targets: list[UnsubscribeTarget] = []
            for item in media_handle.selected_streams:
                target_key = (
                    str(item.get("feed") or ""),
                    str(item.get("mid") or ""),
                )
                if target_key in next_targets:
                    continue
                feed = str(item.get("feed") or "") or None
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
                response = call_plugin_method(
                    bound_handle,
                    "update_subscription",
                    SubscriberUpdateRequest(
                        subscribe=subscribe_targets or None,
                        unsubscribe=unsubscribe_targets or None,
                    ),
                )
                action = "update"

        if response is not None:
            serialized_response = _with_subscriber_joined_state(
                serialize_janus_response(response),
                joined=True,
            )
            media_handle.janus_state = serialized_response
            media_handle.last_event_at = timezone.now()
            plugin_data = video_room_reply_data(response)
            plugin_payload = _serialize_model(plugin_data)
            if plugin_data is not None and plugin_payload.get("streams") is not None:
                stream_payloads = list(plugin_payload["streams"])
            jsep_payload = _serialize_jsep(getattr(response, "jsep", None))
            if jsep_payload and jsep_payload.get("type") == "offer":
                media_handle.jsep_offer = jsep_payload
            if jsep_payload:
                media_handle.lifecycle_state = JanusHandleLifecycleState.JOINING
            elif action == "join":
                media_handle.lifecycle_state = JanusHandleLifecycleState.JOINING

        media_handle.connection = connection or media_handle.connection
        media_handle.selected_streams = serialized_targets
        media_handle.save(
            update_fields=[
                "connection",
                "selected_streams",
                "janus_state",
                "jsep_offer",
                "lifecycle_state",
                "last_event_at",
                "updated_at",
            ]
        )
        _reconcile_subscriber_streams(media_handle, stream_payloads)

        participant.session.janus_state = {**participant.session.janus_state, "participants": serialized_publishers}
        participant.session.last_synced_at = timezone.now()
        participant.session.save(update_fields=["janus_state", "last_synced_at", "updated_at"])
        MeetingLifecycleService.refresh_session_metrics(session=participant.session)
        MeetingSocketEmitter.emit_session_state(session=participant.session)
        media_handle.refresh_from_db()
        return {
            "action": action,
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.SUBSCRIBER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": jsep_payload,
            "streams": _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    def start_subscriber(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
        answer: dict[str, Any],
    ) -> dict[str, Any]:
        """Start media flow on a subscriber handle with the browser's SDP answer."""

        if not answer.get("sdp"):
            raise MeetingDomainError("A subscriber SDP answer is required.")

        media_handle = _get_or_create_media_handle(
            participant=participant,
            handle_type=JanusHandleType.SUBSCRIBER,
            connection=connection,
        )
        bound_handle = ensure_participant_media_plugin(media_handle)
        response = call_plugin_method(
            bound_handle,
            "start",
            answer=_build_jsep(answer, jsep_type="answer"),
        )

        media_handle.connection = connection or media_handle.connection
        media_handle.jsep_answer = answer
        media_handle.janus_state = _with_subscriber_joined_state(
            serialize_janus_response(response),
            joined=True,
        )
        media_handle.lifecycle_state = JanusHandleLifecycleState.READY
        media_handle.last_event_at = timezone.now()
        media_handle.save(
            update_fields=[
                "connection",
                "jsep_answer",
                "janus_state",
                "lifecycle_state",
                "last_event_at",
                "updated_at",
            ]
        )
        MeetingSocketEmitter.emit_session_state(session=participant.session)
        return {
            "action": "start",
            "participant_id": str(participant.pk),
            "handle_type": JanusHandleType.SUBSCRIBER,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": None,
            "streams": _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

    @staticmethod
    def trickle(
        *,
        participant: Participant,
        connection: ParticipantConnection | None,
        handle_type: str,
        candidates: Sequence[dict[str, Any]] | None = None,
        completed: bool = False,
    ) -> dict[str, Any]:
        """Forward browser ICE candidates to the Janus publisher or subscriber handle."""

        if handle_type not in {JanusHandleType.PUBLISHER, JanusHandleType.SUBSCRIBER}:
            raise MeetingDomainError("Unsupported Janus handle type for ICE trickle.")

        media_handle = _get_or_create_media_handle(
            participant=participant,
            handle_type=handle_type,
            connection=connection,
        )
        bound_handle = ensure_participant_media_plugin(media_handle)
        serialized_candidates = _serialize_trickle_candidates(list(candidates or []))
        if completed or not serialized_candidates:
            call_plugin_method(bound_handle, "complete_trickle")
        else:
            call_plugin_method(bound_handle, "trickle", serialized_candidates)
        media_handle.connection = connection or media_handle.connection
        media_handle.last_event_at = timezone.now()
        media_handle.save(update_fields=["connection", "last_event_at", "updated_at"])
        return {
            "action": "trickle",
            "participant_id": str(participant.pk),
            "handle_type": handle_type,
            "lifecycle_state": media_handle.lifecycle_state,
            "jsep": None,
            "streams": _serialize_handle_streams(media_handle),
            "selected_streams": media_handle.selected_streams,
        }

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
            update_fields["janus_session_id"] = ""
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
                    janus_publisher_id="",
                    janus_private_id="",
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
                **session.janus_state,
                "participants": serialized_publishers,
            }
            session.last_synced_at = timezone.now()
            session.save(
                update_fields=["janus_state", "last_synced_at", "updated_at"],
            )

        if len(update_fields) > 2:
            instance.__class__.objects.filter(pk=instance.pk).update(**update_fields)
