"""Event name constants shared by Socket.IO namespaces and emitters."""


class MeetingSocketEvents:
    """Group stable event names so the backend can evolve without string duplication."""

    SESSION_STATE = "session_state"
    SESSION_ENDED = "session_ended"
    JANUS_EVENT = "janus_event"
    JOIN_REQUEST_CREATED = "join_request_created"
    JOIN_REQUEST_REVIEWED = "join_request_reviewed"
    PARTICIPANT_REMOVED = "participant_removed"
    CHAT_MESSAGE_CREATED = "chat_message_created"
    REACTION_CREATED = "reaction_created"
    ERROR = "meeting_error"
