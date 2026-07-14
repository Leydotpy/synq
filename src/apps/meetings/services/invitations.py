"""Signed meeting invitation links and email delivery helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.parse import urlencode

from django.conf import settings
from django.core import signing
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import MeetingInvitation, MeetingSession


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
            )
        except signing.BadSignature as exc:
            raise MeetingDomainError("Meeting invite link is invalid.") from exc

        if payload.get("scope") != "meeting_join" or payload.get("version") != 1:
            raise MeetingDomainError("Meeting invite link is invalid.")
        if payload.get("session_id") != str(session.pk) or payload.get("room_id") != str(session.room_id):
            raise MeetingDomainError("Meeting invite link does not match this meeting.")

        expires_at = payload.get("expires_at")
        if not isinstance(expires_at, str):
            raise MeetingDomainError("Meeting invite link is invalid.")
        try:
            parsed_expires_at = datetime.fromisoformat(expires_at)
        except ValueError as exc:
            raise MeetingDomainError("Meeting invite link is invalid.") from exc
        if timezone.is_naive(parsed_expires_at):
            parsed_expires_at = timezone.make_aware(parsed_expires_at)
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
        """Create a join link, persist recipients, and queue email delivery."""

        invite_token = MeetingInvitationService.create_invite_token(
            session=session,
            issuer_profile=issuer_profile,
            expires_in_seconds=expires_in_seconds,
        )
        join_url = MeetingInvitationService.build_frontend_join_url(
            session=session,
            invite_token=invite_token,
        )

        normalized_emails = list(
            dict.fromkeys(
                email.strip().lower()
                for email in emails
                if email and email.strip()
            )
        )
        expires_after = expires_in_seconds or settings.MEETING_INVITE_MAX_AGE_SECONDS
        issuer_name = issuer_profile.display_name or issuer_profile.handle
        invitations = [
            MeetingInvitation.objects.update_or_create(
                session=session,
                recipient_email=email,
                defaults={
                    "issuer_profile": issuer_profile,
                    "issuer_name": issuer_name,
                    "message": message,
                    "expires_in_seconds": expires_after,
                    "last_delivery_error": "",
                },
            )[0]
            for email in normalized_emails
        ]

        # Import lazily to avoid the tasks -> lifecycle -> invitations import cycle.
        from apps.meetings.services.lifecycle import dispatch_task
        from apps.meetings.tasks import send_meeting_invitation_email

        queued_count = sum(
            dispatch_task(
                send_meeting_invitation_email,
                str(invitation.pk),
                join_url,
                True,
            )
            is not None
            for invitation in invitations
        )
        if queued_count == len(invitations):
            delivery_status = "queued"
        elif queued_count:
            delivery_status = "partially_queued"
        else:
            delivery_status = "queue_unavailable"

        return {
            "join_url": join_url,
            "invite_token": invite_token,
            "emails": normalized_emails,
            "sent_count": 0,
            "queued_count": queued_count,
            "delivery_status": delivery_status,
            "expires_in_seconds": expires_after,
        }
