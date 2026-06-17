from __future__ import annotations

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


def scheduled_at(hour: int, minute: int = 0) -> str:
    now = datetime.now().astimezone()
    return now.replace(hour=hour, minute=minute, second=0, microsecond=0).isoformat()


ROOMS = [
    {
        "id": "mock-sales",
        "title": "Sales and Marketing Meeting",
        "slug": "sales-and-marketing-meeting",
        "description": "",
        "access_policy": "approval_required",
        "is_waiting_room_enabled": True,
        "scheduled_start_at": scheduled_at(17),
        "scheduled_end_at": scheduled_at(18),
        "max_participants": 100,
        "metadata": {},
    },
    {
        "id": "mock-suite-tips",
        "title": "G Suite Tips Meeting",
        "slug": "g-suite-tips-meeting",
        "description": "",
        "access_policy": "approval_required",
        "is_waiting_room_enabled": True,
        "scheduled_start_at": scheduled_at(18, 30),
        "scheduled_end_at": scheduled_at(19, 30),
        "max_participants": 100,
        "metadata": {},
    },
    {
        "id": "mock-seo",
        "title": "SEO Meeting",
        "slug": "seo-meeting",
        "description": "",
        "access_policy": "approval_required",
        "is_waiting_room_enabled": True,
        "scheduled_start_at": scheduled_at(20, 30),
        "scheduled_end_at": scheduled_at(21, 30),
        "max_participants": 100,
        "metadata": {},
    },
]


class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self) -> None:
        self.send_empty()

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/v1/meetings/rooms/":
            self.send_json({"results": ROOMS})
            return
        if path.startswith("/api/v1/meetings/sessions/") and path.endswith("/state/"):
            session_id = path.removeprefix("/api/v1/meetings/sessions/").removesuffix("/state/")
            self.send_json(
                {
                    "room": {
                        "id": "mock-created-room",
                        "title": "Scheduled Synq Meeting",
                        "slug": "mock-created-room",
                        "description": "",
                        "access_policy": "approval_required",
                        "is_waiting_room_enabled": True,
                        "scheduled_start_at": None,
                        "scheduled_end_at": None,
                        "max_participants": 100,
                        "feature_flags": {},
                        "created_by_profile": {
                            "id": "mock-host-profile",
                            "handle": "host",
                            "display_name": "Host",
                            "avatar_url": "",
                        },
                    },
                    "session": {
                        "id": session_id,
                        "started_by_profile": {
                            "id": "mock-host-profile",
                            "handle": "host",
                            "display_name": "Host",
                            "avatar_url": "",
                        },
                        "lifecycle_state": "active",
                        "janus_room_id": None,
                        "control_handle_id": None,
                        "state_version": 1,
                        "started_at": datetime.now().astimezone().isoformat(),
                        "ended_at": None,
                        "last_synced_at": datetime.now().astimezone().isoformat(),
                        "metadata": {},
                    },
                    "counts": {
                        "participants": 0,
                        "publishers": 0,
                        "pending_join_requests": 0,
                    },
                    "coordinator_permissions": None,
                    "local_participant": None,
                    "remote_participants": [],
                    "pending_join_requests": [],
                    "messages": [],
                    "recent_reactions": [],
                    "janus": {},
                }
            )
            return
        self.send_json({"detail": "Not found."}, status=404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/v1/meetings/sessions/":
            self.send_json(
                {
                    "provider": "synq_meet",
                    "room_id": "mock-created-room",
                    "room_slug": "mock-created-room",
                    "session_id": "00000000-0000-4000-8000-000000000001",
                    "join_url": "http://127.0.0.1:3000/meetings/00000000-0000-4000-8000-000000000001?invite=mock",
                    "invite_token": "mock",
                    "shared_invites": None,
                    "room": {
                        **ROOMS[0],
                        "id": "mock-created-room",
                        "title": "Scheduled Synq Meeting",
                        "slug": "mock-created-room",
                    },
                },
                status=201,
            )
            return
        if path.endswith("/sessions/"):
            self.send_json(
                {
                    "room": {
                        "id": "mock-sales",
                        "title": "Sales and Marketing Meeting",
                        "slug": "sales-and-marketing-meeting",
                        "scheduled_start_at": ROOMS[0]["scheduled_start_at"],
                        "scheduled_end_at": ROOMS[0]["scheduled_end_at"],
                    },
                    "session": {"id": "00000000-0000-4000-8000-000000000002"},
                },
                status=201,
            )
            return
        self.send_json({"detail": "Not found."}, status=404)

    def send_empty(self) -> None:
        self.send_response(204)
        self.send_cors()
        self.end_headers()

    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_cors(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "http://127.0.0.1:3000")
        self.send_header("Access-Control-Allow-Credentials", "true")
        self.send_header("Access-Control-Allow-Headers", "authorization, content-type, x-csrftoken")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

    def log_message(self, format: str, *args) -> None:
        return


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 8010), Handler).serve_forever()
