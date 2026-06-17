"""Signed meeting invitation links and email delivery helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.parse import urlencode

from django.conf import settings
from django.core import signing
from django.core.mail import send_mail
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import MeetingSession


class MeetingInvitationService:
    """Create and verify frontend-facing meeting invitation links."""

    token_salt = "apps.meetings.invitation"

    @staticmethod
    def create_invite_token(*, session: MeetingSession, issuer_profile, expires_in_seconds: int | None = None) -> str:
        """Return a signed invite token that grants passcode-less join-request creation."""

        max_age = expires_in_seconds or settings.MEETING_INVITE_MAX_AGE_SECONDS
        expires_at = timezone.now() + timedelta(seconds=max_age)
        payload = {
            "version": 1,
            "scope": "meeting_join",
            "session_id": str(session.pk),
            "room_id": str(session.room_id),
            "issuer_profile_id": str(issuer_profile.pk),
            "auth_grants": ["passcode"],
            "expires_at": expires_at.isoformat(),
        }
        return signing.dumps(payload, salt=MeetingInvitationService.token_salt, compress=True)

    @staticmethod
    def validate_invite_token(*, session: MeetingSession, token: str) -> dict:
        """Validate that a signed invite token belongs to the supplied session."""

        try:
            payload = signing.loads(
                token,
                salt=MeetingInvitationService.token_salt,
                max_age=settings.MEETING_INVITE_MAX_AGE_SECONDS,
            )
        except signing.SignatureExpired as exc:
            raise MeetingDomainError("Meeting invite link has expired.") from exc
        except signing.BadSignature as exc:
            raise MeetingDomainError("Meeting invite link is invalid.") from exc

        if payload.get("scope") != "meeting_join" or payload.get("version") != 1:
            raise MeetingDomainError("Meeting invite link is invalid.")
        if payload.get("session_id") != str(session.pk) or payload.get("room_id") != str(session.room_id):
            raise MeetingDomainError("Meeting invite link does not match this meeting.")

        expires_at = payload.get("expires_at")
        if expires_at:
            parsed_expires_at = datetime.fromisoformat(expires_at)
            if parsed_expires_at < timezone.now():
                raise MeetingDomainError("Meeting invite link has expired.")

        return payload

    @staticmethod
    def build_frontend_join_url(*, session: MeetingSession, invite_token: str) -> str:
        """Build the frontend URL that opens the meeting waiting room."""

        base_url = settings.MEETING_FRONTEND_BASE_URL.rstrip("/")
        path = settings.MEETING_FRONTEND_JOIN_PATH.format(
            session_id=session.pk,
            room_slug=session.room.slug,
        )
        normalized_path = path if path.startswith("/") else f"/{path}"
        separator = "&" if "?" in normalized_path else "?"
        return f"{base_url}{normalized_path}{separator}{urlencode({'invite': invite_token})}"

    @staticmethod
    def share_session(
        *,
        session: MeetingSession,
        issuer_profile,
        emails: list[str],
        message: str = "",
        expires_in_seconds: int | None = None,
    ) -> dict:
        """Create a join link and send it to the supplied email recipients."""

        invite_token = MeetingInvitationService.create_invite_token(
            session=session,
            issuer_profile=issuer_profile,
            expires_in_seconds=expires_in_seconds,
        )
        join_url = MeetingInvitationService.build_frontend_join_url(
            session=session,
            invite_token=invite_token,
        )

        sent_count = 0
        subject = f"Invitation to join {session.room.title}"
        body_parts = [
            f"{issuer_profile.display_name or issuer_profile.handle} invited you to join {session.room.title}.",
        ]
        if message:
            body_parts.extend(["", message])
        body_parts.extend(["", f"Join the meeting: {join_url}"])
        body = "\n".join(body_parts)

        try:
            for email in emails:
                sent_count += send_mail(
                    subject,
                    body,
                    settings.DEFAULT_FROM_EMAIL,
                    [email],
                    fail_silently=False,
                )
        except Exception as exc:
            raise MeetingDomainError("Unable to send meeting invitation email.") from exc

        return {
            "join_url": join_url,
            "invite_token": invite_token,
            "emails": emails,
            "sent_count": sent_count,
            "expires_in_seconds": expires_in_seconds or settings.MEETING_INVITE_MAX_AGE_SECONDS,
        }
