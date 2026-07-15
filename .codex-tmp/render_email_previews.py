"""Render temporary browser previews for invitation email design QA."""

from __future__ import annotations

import base64
import os
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "conf.settings")

import django

django.setup()

from django.template.loader import render_to_string
from django.utils import timezone

from apps.meetings.services.invitation_email import (
    EMAIL_ASSET_DIRECTORY,
    build_invitation_template_context,
)


OUTPUT_DIRECTORY = Path(__file__).resolve().parent / "email-preview"
OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)


def data_url(filename: str) -> str:
    encoded = base64.b64encode(
        (EMAIL_ASSET_DIRECTORY / filename).read_bytes(),
    ).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def render(*, ready: bool) -> None:
    scheduled_start = timezone.now() + (
        timedelta(minutes=-5) if ready else timedelta(days=1, hours=2)
    )
    room = SimpleNamespace(
        title="Quarterly product review",
        description="A focused review of the next Synq release and launch plan.",
        scheduled_start_at=scheduled_start,
        scheduled_end_at=scheduled_start + timedelta(hours=1),
    )
    invitation = SimpleNamespace(
        session=SimpleNamespace(room=room),
        issuer_name="Amara Okafor",
        message="Bring your final prototype and any launch blockers.",
    )
    context = build_invitation_template_context(
        invitation=invitation,
        join_url=(
            "https://meet.example.com/meetings/preview?invite=sample-token"
            if ready
            else ""
        ),
        show_join_button=ready,
        logo_url=data_url("synq-logo.png"),
        illustration_url=data_url("synq-human-meeting.png"),
    )
    html = render_to_string("emails/meetings/invitation.html", context)
    state = "ready" if ready else "scheduled"
    (OUTPUT_DIRECTORY / f"invitation-{state}.html").write_text(
        html,
        encoding="utf-8",
    )
    overview_html = html.replace(
        "</head>",
        "<style>html { zoom: 0.66; }</style></head>",
        1,
    )
    (OUTPUT_DIRECTORY / f"invitation-{state}-overview.html").write_text(
        overview_html,
        encoding="utf-8",
    )


render(ready=False)
render(ready=True)
