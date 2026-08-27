"""Logical event dispatch with durable receipt-backed side effects."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from asgiref.sync import sync_to_async
from django.db.models import F, Q
from django.utils import timezone

from apps.meetings.jrtc.errors import JrtcBrowserDispatchFailure
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


@dataclass(frozen=True, slots=True)
class OutboxRetryOutcome:
    """Low-cardinality result from one independent browser-outbox sweep."""

    attempted: int
    delivered: int
    discarded: int
    failed: int


@dataclass(frozen=True, slots=True)
class _ClaimedBrowserDispatch:
    """One database-leased Socket.IO target ready for an emit attempt."""

    outbox_id: Any
    claimed_at: Any
    dispatch: Any


class JrtcEventDispatcher:
    """Bridge async broker handling to thread-sensitive Django mutations."""

    def __init__(
        self,
        *,
        receipts: DjangoEventReceiptStore | None = None,
        reconciler: DjangoJanusEventReconciler | None = None,
        emitter: SocketIoJanusEventEmitter | None = None,
        outbox_lease_timeout: float = 30.0,
    ) -> None:
        if outbox_lease_timeout <= 0:
            raise ValueError("outbox lease timeout must be positive")
        self.receipts = receipts or DjangoEventReceiptStore()
        self.reconciler = reconciler or DjangoJanusEventReconciler()
        self.emitter = emitter or SocketIoJanusEventEmitter()
        self.outbox_lease_timeout = float(outbox_lease_timeout)

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
        outbox = await self._deliver_browser_outbox(
            event_id=event.event_id,
            limit=None,
            retry_delay=0.0,
            lease_timeout=self.outbox_lease_timeout,
            raise_on_failure=True,
        )

        dispatches = result.value or ()
        return DispatchOutcome(
            duplicate=result.duplicate,
            correlated_handles=0 if result.duplicate else len(dispatches),
            browser_dispatches=outbox.delivered,
        )

    async def retry_pending_browser_dispatches(
        self,
        *,
        limit: int,
        retry_delay: float,
        lease_timeout: float,
    ) -> OutboxRetryOutcome:
        """Retry durable targets without requiring broker redelivery.

        A conditional status/update timestamp pair acts as a cross-process
        lease. A crashed relay's ``delivering`` rows become claimable again
        after ``lease_timeout``. Browser payload ``event_id`` de-duplicates
        the rare emit-after-lease race at the application boundary.
        """

        if limit < 1:
            raise ValueError("outbox retry limit must be positive")
        if retry_delay <= 0 or lease_timeout <= 0:
            raise ValueError("outbox retry timing must be positive")
        return await self._deliver_browser_outbox(
            event_id=None,
            limit=limit,
            retry_delay=retry_delay,
            lease_timeout=lease_timeout,
            raise_on_failure=False,
        )

    async def _deliver_browser_outbox(
        self,
        *,
        event_id: Any | None,
        limit: int | None,
        retry_delay: float,
        lease_timeout: float,
        raise_on_failure: bool,
    ) -> OutboxRetryOutcome:
        """Claim, re-authorize, and deliver a bounded set of outbox rows."""

        delivered = discarded = failed = 0
        claims = await sync_to_async(
            self._claim_browser_dispatches,
            thread_sensitive=True,
        )(
            event_id=event_id,
            limit=limit,
            retry_delay=retry_delay,
            lease_timeout=lease_timeout,
        )
        for claim in claims:
            authorized = await sync_to_async(
                self._browser_dispatch_is_authorized,
                thread_sensitive=True,
            )(claim)
            if not authorized:
                await sync_to_async(
                    self._mark_browser_dispatch_discarded,
                    thread_sensitive=True,
                )(claim)
                discarded += 1
                continue
            try:
                await self.emitter.emit_many((claim.dispatch,))
            except asyncio.CancelledError as exc:
                await sync_to_async(
                    self._mark_browser_dispatch_failed,
                    thread_sensitive=True,
                )(claim, exc)
                failed += 1
                raise
            except Exception as exc:
                await sync_to_async(
                    self._mark_browser_dispatch_failed,
                    thread_sensitive=True,
                )(claim, exc)
                failed += 1
                if raise_on_failure:
                    raise JrtcBrowserDispatchFailure(
                        "Durable JRTC Socket.IO forwarding failed."
                    ) from exc
                continue
            changed = await sync_to_async(
                self._mark_browser_dispatch_delivered,
                thread_sensitive=True,
            )(claim)
            if changed:
                delivered += 1

        return OutboxRetryOutcome(
            attempted=len(claims),
            delivered=delivered,
            discarded=discarded,
            failed=failed,
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
    def _claim_browser_dispatches(
        *,
        event_id: Any | None,
        limit: int | None,
        retry_delay: float,
        lease_timeout: float,
    ) -> tuple[_ClaimedBrowserDispatch, ...]:
        """Conditionally lease pending or abandoned targets across processes."""

        from apps.meetings.jrtc.events.handlers import SocketDispatch

        observed_at = timezone.now()
        pending_filter = Q(status=JrtcBrowserOutboxStatus.PENDING)
        if retry_delay > 0:
            pending_filter &= Q(
                updated_at__lte=observed_at - timedelta(seconds=retry_delay)
            )
        eligible = pending_filter | Q(
            status=JrtcBrowserOutboxStatus.DELIVERING,
            updated_at__lte=observed_at - timedelta(seconds=lease_timeout),
        )
        candidates = JrtcBrowserEventOutbox.objects.filter(eligible)
        if event_id is not None:
            candidates = candidates.filter(receipt__event_id=event_id)
        candidate_ids = candidates.order_by(
            "updated_at",
            "created_at",
            "dispatch_index",
        ).values_list("pk", flat=True)
        if limit is not None:
            candidate_ids = candidate_ids[:limit]

        claimed: list[_ClaimedBrowserDispatch] = []
        for outbox_id in tuple(candidate_ids):
            changed = (
                JrtcBrowserEventOutbox.objects.filter(pk=outbox_id)
                .filter(eligible)
                .update(
                    status=JrtcBrowserOutboxStatus.DELIVERING,
                    updated_at=observed_at,
                )
            )
            if not changed:
                continue
            row = JrtcBrowserEventOutbox.objects.get(pk=outbox_id)
            claimed.append(
                _ClaimedBrowserDispatch(
                    outbox_id=row.pk,
                    claimed_at=observed_at,
                    dispatch=SocketDispatch(
                        payload=dict(row.payload),
                        socket_ids=(row.socket_id,),
                        session_room="",
                    ),
                )
            )
        return tuple(claimed)

    @staticmethod
    def _browser_dispatch_is_authorized(claim: _ClaimedBrowserDispatch) -> bool:
        """Recheck target ownership immediately before delayed forwarding."""

        from apps.meetings.models import (
            ParticipantConnection,
            RealtimeConnectionStatus,
        )

        payload = claim.dispatch.payload
        connection_id = payload.get("connection_id")
        session_id = payload.get("session_id")
        socket_ids = claim.dispatch.socket_ids
        if not connection_id or not session_id or len(socket_ids) != 1:
            return False
        return ParticipantConnection.objects.filter(
            pk=connection_id,
            session_id=session_id,
            socket_id=socket_ids[0],
            status__in=(
                RealtimeConnectionStatus.CONNECTED,
                RealtimeConnectionStatus.SUBSCRIBED,
                RealtimeConnectionStatus.ACTIVE,
            ),
        ).exists()

    @staticmethod
    def _mark_browser_dispatch_delivered(
        claim: _ClaimedBrowserDispatch,
    ) -> bool:
        observed_at = timezone.now()
        changed = JrtcBrowserEventOutbox.objects.filter(
            pk=claim.outbox_id,
            status=JrtcBrowserOutboxStatus.DELIVERING,
            updated_at=claim.claimed_at,
        ).update(
            status=JrtcBrowserOutboxStatus.DELIVERED,
            delivery_attempts=F("delivery_attempts") + 1,
            delivered_at=observed_at,
            last_error="",
            updated_at=observed_at,
        )
        return bool(changed)

    @staticmethod
    def _mark_browser_dispatch_failed(
        claim: _ClaimedBrowserDispatch,
        exc: BaseException,
    ) -> None:
        JrtcBrowserEventOutbox.objects.filter(
            pk=claim.outbox_id,
            status=JrtcBrowserOutboxStatus.DELIVERING,
            updated_at=claim.claimed_at,
        ).update(
            status=JrtcBrowserOutboxStatus.PENDING,
            delivery_attempts=F("delivery_attempts") + 1,
            last_error=type(exc).__name__,
            updated_at=timezone.now(),
        )

    @staticmethod
    def _mark_browser_dispatch_discarded(
        claim: _ClaimedBrowserDispatch,
    ) -> None:
        JrtcBrowserEventOutbox.objects.filter(
            pk=claim.outbox_id,
            status=JrtcBrowserOutboxStatus.DELIVERING,
            updated_at=claim.claimed_at,
        ).update(
            status=JrtcBrowserOutboxStatus.DISCARDED,
            delivery_attempts=F("delivery_attempts") + 1,
            last_error="target_not_authorized",
            updated_at=timezone.now(),
        )


__all__ = [
    "DispatchOutcome",
    "JrtcEventDispatcher",
    "OutboxRetryOutcome",
]
