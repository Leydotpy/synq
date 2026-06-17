"""Janus signaling helpers that bridge Socket.IO meeting clients to backend-owned handles."""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from django.db import transaction
from django.utils import timezone
from janus_api.models.request import TrickleCandidate
from janus_api.models.videoroom import ParticipantSubscribeJoinRequest, StreamDescription, SubscriberStreams

from apps.meetings.exceptions import MeetingDomainError
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
    ensure_participant_media_plugin,
    ensure_session_control_handle,
    serialize_janus_response,
)
from apps.meetings.services.lifecycle import MeetingLifecycleService
from apps.meetings.services.permissions import MeetingPermissionService


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
    serialized = _serialize_model(value)
    return serialized or None


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
) -> Participant | None:
    """Resolve a local participant from a Janus publisher identifier or display name."""

    participant = session.participants.filter(janus_publisher_id=publisher_id).first()
    if participant is not None:
        return participant
    if display_name:
        return session.participants.filter(display_name=display_name).first()
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


def _serialize_selected_streams(streams: Iterable[SubscriberStreams]) -> list[dict[str, Any]]:
    """Serialize Janus subscriber stream selections for persistence and acknowledgements."""

    return [
        {
            "feed": str(stream.feed),
            "mid": stream.mid,
            "crossrefid": stream.crossrefid,
            "sub_mid": stream.sub_mid,
        }
        for stream in streams
    ]


def _reconcile_publisher_payloads(session: MeetingSession, publisher_payloads: Sequence[Any]) -> list[dict[str, Any]]:
    """Project Janus publisher state into participants and outbound stream rows."""

    now = timezone.now()
    serialized_publishers: list[dict[str, Any]] = []
    active_publisher_ids: set[str] = set()

    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        serialized_publishers.append(payload)
        publisher_id = str(payload.get("id") or "")
        if not publisher_id:
            continue
        participant = _match_participant_for_publisher(
            session=session,
            publisher_id=publisher_id,
            display_name=payload.get("display"),
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
        seen_mids: set[str] = set()
        for stream_payload in stream_rows:
            mid = str(stream_payload.get("mid") or "")
            if not mid:
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

        publisher_handle.streams.filter(direction=MediaDirection.OUTBOUND).exclude(janus_mid__in=seen_mids).delete()
        if publisher_handle.lifecycle_state in {JanusHandleLifecycleState.JOINING, JanusHandleLifecycleState.ATTACHED} and seen_mids:
            publisher_handle.lifecycle_state = JanusHandleLifecycleState.READY
            publisher_handle.janus_state = payload
            publisher_handle.last_event_at = now
            publisher_handle.save(update_fields=["lifecycle_state", "janus_state", "last_event_at", "updated_at"])

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


def _reconcile_subscriber_streams(media_handle: ParticipantMediaHandle, stream_payloads: Sequence[dict[str, Any]]) -> None:
    """Project Janus subscriber state into inbound stream rows."""

    participant = media_handle.participant
    session = participant.session
    now = timezone.now()
    seen_mids: set[str] = set()

    for payload in stream_payloads:
        janus_mid = str(payload.get("mid") or "")
        if not janus_mid:
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

    media_handle.streams.filter(direction=MediaDirection.INBOUND).exclude(janus_mid__in=seen_mids).delete()


def _build_subscriber_targets(*, participant: Participant, publisher_payloads: Sequence[Any]) -> list[SubscriberStreams]:
    """Build the target publisher streams for the participant's multistream subscriber handle."""

    targets: list[SubscriberStreams] = []
    local_feed_id = str(participant.janus_publisher_id or "")
    for publisher in publisher_payloads:
        payload = _serialize_model(publisher)
        publisher_id = str(payload.get("id") or "")
        if not publisher_id or publisher_id == local_feed_id:
            continue
        for stream_payload in payload.get("streams") or []:
            media_type = str(stream_payload.get("type") or "")
            mid = str(stream_payload.get("mid") or "")
            if media_type not in {"audio", "video"} or not mid or bool(stream_payload.get("disabled", False)):
                continue
            targets.append(
                SubscriberStreams(
                    feed=publisher_id,
                    mid=mid,
                    crossrefid=f"{publisher_id}:{mid}",
                )
            )
    return targets


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

        control_handle = ensure_session_control_handle(session)
        publisher_payloads = call_plugin_method(control_handle, "participants")
        serialized_publishers = _reconcile_publisher_payloads(session, publisher_payloads)
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
        descriptions = _build_stream_descriptions(track_descriptors)

        method_name = "configure"
        method_kwargs: dict[str, Any] = {
            "sdp": offer["sdp"],
            "sdp_type": "offer",
            "descriptions": descriptions,
        }
        if not participant.janus_publisher_id:
            method_name = "join_and_configure"
            method_kwargs["pin"] = participant.session.janus_room_pin or None
            method_kwargs["metadata"] = _build_metadata(participant)
        elif media_handle.lifecycle_state == JanusHandleLifecycleState.ATTACHED:
            method_name = "publish"
        response = call_plugin_method(bound_handle, method_name, **method_kwargs)

        plugin_data = getattr(getattr(response, "plugindata", None), "data", None)
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

        MeetingMediaSignalService.sync_publishers(session=participant.session, emit_state=False)
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
        publishers = call_plugin_method(ensure_session_control_handle(participant.session), "participants")
        serialized_publishers = _reconcile_publisher_payloads(participant.session, publishers)
        targets = _build_subscriber_targets(participant=participant, publisher_payloads=publishers)
        serialized_targets = _serialize_selected_streams(targets)
        current_targets = {
            (str(item.get("feed") or ""), str(item.get("mid") or ""))
            for item in media_handle.selected_streams
        }
        next_targets = {(str(item["feed"]), str(item["mid"])) for item in serialized_targets}

        action = "noop"
        response = None
        jsep_payload = None
        stream_payloads: Sequence[dict[str, Any]] = []

        if not serialized_targets and not current_targets:
            pass
        elif not current_targets:
            response = call_plugin_method(
                bound_handle,
                "send",
                ParticipantSubscribeJoinRequest(
                    request="join",
                    ptype="subscriber",
                    room=participant.session.janus_room_id or str(participant.session.pk),
                    pin=participant.session.janus_room_pin or None,
                    private_id=participant.janus_private_id or None,
                    streams=targets,
                    use_msid=True,
                    autoupdate=True,
                ),
            )
            action = "join"
        elif current_targets != next_targets:
            add = [item for item in targets if (str(item.feed), item.mid) not in current_targets]
            drop = [
                SubscriberStreams(
                    feed=str(item.get("feed")),
                    mid=str(item.get("mid")),
                    crossrefid=str(item.get("crossrefid") or f"{item.get('feed')}:{item.get('mid')}"),
                    sub_mid=item.get("sub_mid"),
                )
                for item in media_handle.selected_streams
                if (str(item.get("feed") or ""), str(item.get("mid") or "")) not in next_targets
            ]
            response = call_plugin_method(
                bound_handle,
                "update",
                add=add or None,
                drop=drop or None,
            )
            action = "update"

        if response is not None:
            serialized_response = serialize_janus_response(response)
            media_handle.janus_state = serialized_response
            media_handle.last_event_at = timezone.now()
            plugin_data = getattr(getattr(response, "plugindata", None), "data", None)
            stream_payloads = list(_serialize_model(plugin_data).get("streams") or []) if plugin_data else []
            jsep_payload = _serialize_jsep(getattr(response, "jsep", None))
            if jsep_payload and jsep_payload.get("type") == "offer":
                media_handle.jsep_offer = jsep_payload
            media_handle.lifecycle_state = JanusHandleLifecycleState.JOINING if serialized_targets else JanusHandleLifecycleState.ATTACHED

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
        response = call_plugin_method(bound_handle, "watch", sdp=answer["sdp"], sdp_type="answer")

        media_handle.connection = connection or media_handle.connection
        media_handle.jsep_answer = answer
        media_handle.janus_state = serialize_janus_response(response)
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
            call_plugin_method(bound_handle, "trickle", candidates=serialized_candidates)
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

        plugin_payload = ((normalized_event.get("plugindata") or {}).get("data") or {})
        if plugin_payload.get("videoroom") in {"joined", "attached"}:
            update_fields.setdefault("lifecycle_state", JanusHandleLifecycleState.JOINING)

        if len(update_fields) > 2:
            instance.__class__.objects.filter(pk=instance.pk).update(**update_fields)
