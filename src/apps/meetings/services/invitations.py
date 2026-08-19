"""Signed meeting invitation links and email delivery helpers."""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.parse import urlencode

from django.conf import settings
from django.core import signing
from django.core.mail import send_mail
from django.utils import timezone

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.models import MeetingInvitation, MeetingLifecycleState, MeetingSession


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
        """Persist recipients and deliver or durably queue their initial invite."""

        if session.lifecycle_state in {
            MeetingLifecycleState.ENDING,
            MeetingLifecycleState.ENDED,
            MeetingLifecycleState.FAILED,
        }:
            raise MeetingDomainError("Invitations cannot be sent for a meeting that has ended.")

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
            ),
        )
        expires_after = expires_in_seconds or settings.MEETING_INVITE_MAX_AGE_SECONDS
        issuer_name = issuer_profile.display_name or issuer_profile.handle
        ready_now = (
            session.room.scheduled_start_at is None
            or session.room.scheduled_start_at <= timezone.now()
        )
        backend = settings.EMAIL_BACKEND
        inline_backend = backend in {
            "django.core.mail.backends.console.EmailBackend",
            "django.core.mail.backends.locmem.EmailBackend",
        }
        console_backend = backend == "django.core.mail.backends.console.EmailBackend"
        sent_count = 0
        queued_count = 0
        pending_count = 0
        failed_count = 0
        for email in normalized_emails:
            invitation, created = MeetingInvitation.objects.update_or_create(
                session=session,
                recipient_email=email,
                defaults={
                    "issuer_profile": issuer_profile,
                    "issuer_name": issuer_name,
                    "message": message,
                    "expires_in_seconds": expires_after,
                    "last_delivery_error": "",
                },
            )
            # Retrying the surrounding create/share request must not duplicate
            # email that was already accepted by the backend.
            if not created and invitation.initial_email_sent_at is not None:
                continue

            attempted_at = timezone.now()
            if not inline_backend:
                # External email is performed by Celery so an SMTP stall or a
                # partial recipient failure cannot hold or invalidate the HTTP
                # operation that created the meeting.  Claim a short enqueue
                # lease so client retries do not publish duplicate tasks.
                lease_cutoff = attempted_at - timedelta(minutes=5)
                claim = MeetingInvitation.objects.filter(
                    pk=invitation.pk,
                    initial_email_sent_at__isnull=True,
                ).filter(
                    last_delivery_attempt_at__isnull=True
                )
                if invitation.last_delivery_attempt_at is not None:
                    if invitation.last_delivery_attempt_at >= lease_cutoff:
                        pending_count += 1
                        continue
                    claim = MeetingInvitation.objects.filter(
                        pk=invitation.pk,
                        initial_email_sent_at__isnull=True,
                        last_delivery_attempt_at=invitation.last_delivery_attempt_at,
                    )
                previous_attempt_at = invitation.last_delivery_attempt_at
                if not claim.update(last_delivery_attempt_at=attempted_at):
                    pending_count += 1
                    continue

                from apps.meetings.services.lifecycle import dispatch_task
                from apps.meetings.tasks import send_meeting_invitation_email

                if dispatch_task(
                    send_meeting_invitation_email,
                    str(invitation.pk),
                    join_url,
                    True,
                ) is None:
                    MeetingInvitation.objects.filter(
                        pk=invitation.pk,
                        initial_email_sent_at__isnull=True,
                        last_delivery_attempt_at=attempted_at,
                    ).update(last_delivery_attempt_at=previous_attempt_at)
                    pending_count += 1
                else:
                    queued_count += 1
                continue

            try:
                sent_count += MeetingInvitationService.send_invitation_email(
                    invitation=invitation,
                    join_url=join_url,
                    ready=ready_now,
                )
            except Exception as exc:
                invitation.delivery_attempts += 1
                invitation.last_delivery_attempt_at = attempted_at
                invitation.last_delivery_error = str(exc)[:2000]
                invitation.save(
                    update_fields=[
                        "delivery_attempts",
                        "last_delivery_attempt_at",
                        "last_delivery_error",
                        "updated_at",
                    ],
                )
                failed_count += 1
                continue
            invitation.delivery_attempts += 1
            invitation.initial_email_sent_at = attempted_at
            invitation.last_delivery_attempt_at = attempted_at
            invitation.last_delivery_error = ""
            update_fields = [
                "delivery_attempts",
                "initial_email_sent_at",
                "last_delivery_attempt_at",
                "last_delivery_error",
                "updated_at",
            ]
            if ready_now:
                invitation.ready_email_sent_at = attempted_at
                update_fields.append("ready_email_sent_at")
            invitation.save(update_fields=update_fields)

        if inline_backend:
            if failed_count and sent_count:
                delivery_status = "partial"
            elif failed_count:
                delivery_status = "failed"
            elif console_backend and sent_count:
                delivery_status = "previewed"
            else:
                delivery_status = "delivered"
        elif queued_count and pending_count:
            delivery_status = "partial"
        elif queued_count:
            delivery_status = "queued"
        else:
            delivery_status = "pending"

        return {
            "join_url": join_url,
            "invite_token": invite_token,
            "emails": normalized_emails,
            "sent_count": sent_count,
            "queued_count": queued_count,
            "pending_count": pending_count,
            "failed_count": failed_count,
            "delivery_status": delivery_status,
            "expires_in_seconds": expires_after,
        }

    @staticmethod
    def send_invitation_email(
        *,
        invitation: MeetingInvitation,
        join_url: str,
        ready: bool,
    ) -> int:
        """Send one plain-text invitation or due reminder through Django email."""

        subject = (
            f"Your meeting is ready: {invitation.session.room.title}"
            if ready
            else f"Invitation to join {invitation.session.room.title}"
        )
        body_parts = [
            f"{invitation.issuer_name} invited you to join {invitation.session.room.title}.",
        ]
        if invitation.message:
            body_parts.extend(["", invitation.message])
        if ready:
            body_parts.extend(["", "The meeting is ready now."])
        body_parts.extend(["", f"Join the meeting: {join_url}"])
        return send_mail(
            subject,
            "\n".join(body_parts),
            settings.DEFAULT_FROM_EMAIL,
            [invitation.recipient_email],
            fail_silently=False,
        )
