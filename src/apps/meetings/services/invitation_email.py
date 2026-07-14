"""Render accessible HTML and plain-text meeting invitation emails."""

from __future__ import annotations

from email.message import MIMEPart
from email.utils import make_msgid
from pathlib import Path

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.formats import date_format, time_format

from apps.meetings.models import MeetingInvitation


EMAIL_ASSET_DIRECTORY = (
    Path(__file__).resolve().parents[1]
    / "static"
    / "apps"
    / "meetings"
    / "email"
)


def invitation_is_ready(
    invitation: MeetingInvitation,
    *,
    at=None,
) -> bool:
    """Return whether an invitation should include its join action."""

    scheduled_start_at = invitation.session.room.scheduled_start_at
    return scheduled_start_at is None or scheduled_start_at <= (at or timezone.now())


def build_invitation_template_context(
    *,
    invitation: MeetingInvitation,
    join_url: str,
    show_join_button: bool,
    logo_url: str,
    illustration_url: str,
) -> dict:
    """Build the shared context used by the HTML and text alternatives."""

    room = invitation.session.room
    scheduled_start = room.scheduled_start_at
    scheduled_end = room.scheduled_end_at
    if scheduled_start is not None:
        local_start = timezone.localtime(scheduled_start)
        date_label = date_format(local_start, "l, F j, Y")
        time_label = time_format(local_start, "g:i A")
        if scheduled_end is not None:
            local_end = timezone.localtime(scheduled_end)
            if local_end.date() == local_start.date():
                time_label = f"{time_label} – {time_format(local_end, 'g:i A')}"
        timezone_label = timezone.get_current_timezone_name()
    else:
        date_label = "Available now"
        time_label = "Join when you are ready"
        timezone_label = ""

    product_name = getattr(settings, "MEETING_EMAIL_PRODUCT_NAME", "Synq")
    return {
        "product_name": product_name,
        "meeting_title": room.title,
        "meeting_description": room.description,
        "issuer_name": invitation.issuer_name,
        "custom_message": invitation.message,
        "date_label": date_label,
        "time_label": time_label,
        "timezone_label": timezone_label,
        "show_join_button": show_join_button,
        "join_url": join_url if show_join_button else "",
        "logo_url": logo_url,
        "illustration_url": illustration_url,
        "status_label": "Ready now" if show_join_button else "Scheduled",
        "headline": "Your meeting is ready" if show_join_button else "You’re invited",
        "preheader": (
            f"Join {room.title} now."
            if show_join_button
            else f"{invitation.issuer_name} invited you to {room.title}."
        ),
    }


def _inline_png(*, filename: str, content_id: str) -> MIMEPart:
    """Return a CID-addressable PNG attachment for broad email-client support."""

    image = MIMEPart()
    image.set_content(
        (EMAIL_ASSET_DIRECTORY / filename).read_bytes(),
        maintype="image",
        subtype="png",
        disposition="inline",
        filename=filename,
        cid=content_id,
    )
    return image


def build_meeting_invitation_email(
    *,
    invitation: MeetingInvitation,
    join_url: str,
    show_join_button: bool,
) -> EmailMultiAlternatives:
    """Build one multipart invitation email with inline Synq artwork."""

    logo_cid = make_msgid(domain="synq.email")
    illustration_cid = make_msgid(domain="synq.email")
    context = build_invitation_template_context(
        invitation=invitation,
        join_url=join_url,
        show_join_button=show_join_button,
        logo_url=f"cid:{logo_cid[1:-1]}",
        illustration_url=f"cid:{illustration_cid[1:-1]}",
    )
    subject = (
        f"Your meeting is ready: {invitation.session.room.title}"
        if show_join_button
        else f"You’re invited: {invitation.session.room.title}"
    )
    text_body = render_to_string("emails/meetings/invitation.txt", context).strip()
    html_body = render_to_string("emails/meetings/invitation.html", context)
    email = EmailMultiAlternatives(
        subject=subject,
        body=text_body,
        from_email=settings.DEFAULT_FROM_EMAIL,
        to=[invitation.recipient_email],
        reply_to=getattr(settings, "MEETING_EMAIL_REPLY_TO", []),
    )
    email.attach_alternative(html_body, "text/html")
    email.attach(
        _inline_png(filename="synq-logo.png", content_id=logo_cid),
    )
    email.attach(
        _inline_png(
            filename="synq-human-meeting.png",
            content_id=illustration_cid,
        ),
    )
    return email
