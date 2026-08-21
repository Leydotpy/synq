"""Logical event dispatch with durable receipt-backed side effects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from asgiref.sync import sync_to_async
from django.db.models import F
from django.utils import timezone

from apps.meetings.jrtc.events.handlers import (
    DjangoJanusEventReconciler,
    SocketIoJanusEventEmitter,
)
from apps.meetings.jrtc.events.idempotency import DjangoEventReceiptStore
from apps.meetings.jrtc.events.schemas import JanusBrokerEvent
from apps.meetings.models import (
    JrtcBrowserEventOutbox,
    JrtcBrowserOutboxStatus,
    JrtcEventReceipt,
)


@dataclass(frozen=True, slots=True)
class DispatchOutcome:
    """Operational summary without high-cardinality broker identifiers."""

    duplicate: bool
    correlated_handles: int
    browser_dispatches: int


class JrtcEventDispatcher:
    """Bridge async broker handling to thread-sensitive Django mutations."""

    def __init__(
        self,
        *,
        receipts: DjangoEventReceiptStore | None = None,
        reconciler: DjangoJanusEventReconciler | None = None,
        emitter: SocketIoJanusEventEmitter | None = None,
    ) -> None:
        self.receipts = receipts or DjangoEventReceiptStore()
        self.reconciler = reconciler or DjangoJanusEventReconciler()
        self.emitter = emitter or SocketIoJanusEventEmitter()

    async def dispatch(self, event: JanusBrokerEvent) -> DispatchOutcome:
        """Commit domain/outbox work, deliver pending targets, then allow ACK."""

        result = await sync_to_async(
            self.receipts.process_once,
            thread_sensitive=True,
        )(
            event,
            lambda: self.reconciler.reconcile(event),
            self._enqueue_browser_dispatches,
        )
        pending = await sync_to_async(
            self._pending_browser_dispatches,
            thread_sensitive=True,
        )(event.event_id)
        delivered = 0
        for outbox_id, dispatch in pending:
            try:
                await self.emitter.emit_many((dispatch,))
            except BaseException as exc:
                await sync_to_async(
                    self._mark_browser_dispatch_failed,
                    thread_sensitive=True,
                )(outbox_id, exc)
                raise
            await sync_to_async(
                self._mark_browser_dispatch_delivered,
                thread_sensitive=True,
            )(outbox_id)
            delivered += 1

        dispatches = result.value or ()
        return DispatchOutcome(
            duplicate=result.duplicate,
            correlated_handles=0 if result.duplicate else len(dispatches),
            browser_dispatches=delivered,
        )

    @staticmethod
    def _enqueue_browser_dispatches(
        receipt: JrtcEventReceipt,
        dispatches: tuple[Any, ...],
    ) -> None:
        """Persist authorized Socket.IO targets in the receipt transaction."""

        rows = [
            JrtcBrowserEventOutbox(
                receipt=receipt,
                dispatch_index=index,
                socket_id=str(socket_id),
                payload=dispatch.payload,
            )
            for index, dispatch in enumerate(dispatches)
            for socket_id in dispatch.socket_ids
            if socket_id
        ]
        if rows:
            JrtcBrowserEventOutbox.objects.bulk_create(rows)

    @staticmethod
    def _pending_browser_dispatches(
        event_id: Any,
    ) -> tuple[tuple[Any, Any], ...]:
        """Load only undelivered targets for a new event or redelivery."""

        from apps.meetings.jrtc.events.handlers import SocketDispatch

        rows = JrtcBrowserEventOutbox.objects.filter(
            receipt__event_id=event_id,
            status=JrtcBrowserOutboxStatus.PENDING,
        ).order_by("created_at", "dispatch_index")
        return tuple(
            (
                row.pk,
                SocketDispatch(
                    payload=dict(row.payload),
                    socket_ids=(row.socket_id,),
                    session_room="",
                ),
            )
            for row in rows
        )

    @staticmethod
    def _mark_browser_dispatch_delivered(outbox_id: Any) -> None:
        observed_at = timezone.now()
        JrtcBrowserEventOutbox.objects.filter(
            pk=outbox_id,
            status=JrtcBrowserOutboxStatus.PENDING,
        ).update(
            status=JrtcBrowserOutboxStatus.DELIVERED,
            delivery_attempts=F("delivery_attempts") + 1,
            delivered_at=observed_at,
            last_error="",
            updated_at=observed_at,
        )

    @staticmethod
    def _mark_browser_dispatch_failed(outbox_id: Any, exc: BaseException) -> None:
        JrtcBrowserEventOutbox.objects.filter(
            pk=outbox_id,
            status=JrtcBrowserOutboxStatus.PENDING,
        ).update(
            delivery_attempts=F("delivery_attempts") + 1,
            last_error=type(exc).__name__,
            updated_at=timezone.now(),
        )


__all__ = ["DispatchOutcome", "JrtcEventDispatcher"]
