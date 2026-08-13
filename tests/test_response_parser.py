from __future__ import annotations

from jrtc.models.response import EventResponse, parse_janus_response


def test_event_response_retains_jsep_in_the_original_envelope() -> None:
    payload = {
        "janus": "event",
        "transaction": "transaction-1",
        "session_id": 1234,
        "sender": 5678,
        "plugindata": {
            "plugin": "janus.plugin.videoroom",
            "data": {"videoroom": "attached", "room": 42},
        },
        "jsep": {
            "type": "offer",
            "sdp": "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\n",
            "trickle": True,
            "vendor_extension": "retained",
        },
    }

    response = parse_janus_response(payload)

    assert isinstance(response, EventResponse)
    assert response.jsep is not None
    assert response.jsep.type == "offer"
    assert response.jsep.sdp == payload["jsep"]["sdp"]
    assert response.jsep.model_extra == {"vendor_extension": "retained"}
    assert (
        response.model_dump(mode="json", by_alias=True, exclude_none=True)["jsep"]
        == payload["jsep"]
    )
