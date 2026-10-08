"""Semantic event metadata, independent of sound or presentation policy."""

def event_context(event_type, identity, session_id, occurred_at, *, actor_profile_id=None, actor_participant_id=None, kind="live"):
    return {
        "kind": kind, "event_id": f"{event_type}:{identity}", "session_id": str(session_id),
        "actor_profile_id": None if actor_profile_id is None else str(actor_profile_id),
        "actor_participant_id": None if actor_participant_id is None else str(actor_participant_id),
        "occurred_at": None if occurred_at is None else occurred_at.isoformat(),
    }
