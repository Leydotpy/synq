"""Database-backed idempotency for at-least-once JRTC deliveries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

from django.db import transaction
from django.db.models import F
from django.utils import timezone

from apps.meetings.models import JrtcEventReceipt, JrtcEventReceiptStatus
from apps.meetings.jrtc.events.schemas import JanusBrokerEvent


T = TypeVar("T")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IdempotentResult(Generic[T]):
    """Result of processing either a new event or a harmless duplicate."""

    duplicate: bool
    value: T | None = None


class DjangoEventReceiptStore:
    """Apply domain mutations in the same transaction as the unique receipt."""

    def process_once(
        self,
        event: JanusBrokerEvent,
        operation: Callable[[], T],
        after_operation: Callable[[JrtcEventReceipt, T], None] | None = None,
    ) -> IdempotentResult[T]:
        """Run ``operation`` once for an envelope UUID.

        A failed operation rolls back both the receipt transition and all
        domain mutations.  A compact FAILED receipt is then retained for
        diagnostics and may be claimed by a later redelivery.
        """

        try:
            with transaction.atomic():
                receipt, created = JrtcEventReceipt.objects.get_or_create(
                    event_id=event.event_id,
                    defaults={
                        "event_type": event.event_type,
                        "status": JrtcEventReceiptStatus.RECEIVED,
                        "delivery_attempts": event.delivery_attempt,
                        "metadata": self._metadata(event),
                    },
                )
                if not created:
                    receipt = JrtcEventReceipt.objects.select_for_update().get(
                        pk=receipt.pk
                    )
                if receipt.event_type != event.event_type:
                    raise ValueError(
                        "A broker envelope UUID was reused for a different event type."
                    )
                if not created and receipt.status == JrtcEventReceiptStatus.PROCESSED:
                    JrtcEventReceipt.objects.filter(pk=receipt.pk).update(
                        duplicate_count=F("duplicate_count") + 1,
                        delivery_attempts=F("delivery_attempts") + 1,
                        updated_at=timezone.now(),
                    )
                    return IdempotentResult(duplicate=True)

                if not created:
                    receipt.delivery_attempts += 1
                    receipt.status = JrtcEventReceiptStatus.RECEIVED
                    receipt.last_error = ""
                    receipt.metadata = self._metadata(event)

                value = operation()
                if after_operation is not None:
                    after_operation(receipt, value)
                receipt.status = JrtcEventReceiptStatus.PROCESSED
                receipt.processed_at = timezone.now()
                receipt.last_error = ""
                receipt.save(
                    update_fields=[
                        "status",
                        "processed_at",
                        "delivery_attempts",
                        "last_error",
                        "metadata",
                        "updated_at",
                    ]
                )
                return IdempotentResult(duplicate=False, value=value)
        except Exception as exc:
            self._record_failure(event, exc)
            raise

    @staticmethod
    def _metadata(event: JanusBrokerEvent) -> dict[str, object]:
        return {
            "janus_type": event.janus_type,
            "has_sender": event.sender is not None,
        }

    def _record_failure(self, event: JanusBrokerEvent, exc: Exception) -> None:
        """Retain non-sensitive retry diagnostics after the failed transaction."""

        error_name = type(exc).__name__
        try:
            with transaction.atomic():
                receipt, created = JrtcEventReceipt.objects.get_or_create(
                    event_id=event.event_id,
                    defaults={
                        "event_type": event.event_type,
                        "status": JrtcEventReceiptStatus.FAILED,
                        "delivery_attempts": event.delivery_attempt,
                        "last_error": error_name,
                        "metadata": self._metadata(event),
                    },
                )
                if not created:
                    receipt = JrtcEventReceipt.objects.select_for_update().get(
                        pk=receipt.pk
                    )
                if created or receipt.status == JrtcEventReceiptStatus.PROCESSED:
                    return
                receipt.status = JrtcEventReceiptStatus.FAILED
                receipt.delivery_attempts += 1
                receipt.last_error = error_name
                receipt.metadata = self._metadata(event)
                receipt.save(
                    update_fields=[
                        "status",
                        "delivery_attempts",
                        "last_error",
                        "metadata",
                        "updated_at",
                    ]
                )
        except Exception:
            # Preserve the original domain failure if even diagnostic storage
            # is unavailable; Broka will retain its own failure/dead-letter data.
            logger.exception(
                "Unable to persist JRTC event failure diagnostics",
                extra={"janus_event_type": event.event_type},
            )


__all__ = ["DjangoEventReceiptStore", "IdempotentResult"]
