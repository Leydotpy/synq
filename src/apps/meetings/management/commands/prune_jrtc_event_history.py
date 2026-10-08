"""Prune terminal receipts only after their last activity and completed delivery."""
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from apps.meetings.models import (
    JrtcBrowserEventOutbox, JrtcBrowserOutboxStatus,
    JrtcEventReceipt, JrtcEventReceiptStatus,
)


class Command(BaseCommand):
    help = "Prune old terminal JRTC receipts, preserving pending browser deliveries."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=settings.JRTC_EVENT_RECEIPT_RETENTION_DAYS)
        parser.add_argument("--batch-size", type=int, default=500)

    def handle(self, *args, **options):
        if options["days"] < 1 or not 1 <= options["batch_size"] <= 10000:
            raise CommandError("days must be positive and batch-size must be between 1 and 10000")
        cutoff = timezone.now() - timedelta(days=options["days"])
        unfinished = JrtcBrowserEventOutbox.objects.filter(receipt_id=OuterRef("pk")).exclude(
            status__in=[JrtcBrowserOutboxStatus.DELIVERED, JrtcBrowserOutboxStatus.DISCARDED])
        eligible = JrtcEventReceipt.objects.filter(
            status=JrtcEventReceiptStatus.PROCESSED, updated_at__lt=cutoff,
        ).alias(unfinished=Exists(unfinished)).filter(unfinished=False)
        count = 0
        while True:
            with transaction.atomic():
                ids = list(eligible.select_for_update().order_by("pk").values_list("pk", flat=True)[:options["batch_size"]])
                if not ids:
                    break
                # The receipt lock serializes pruning with receipt retry/finalization.
                eligible.filter(pk__in=ids).delete()
                count += len(ids)
        self.stdout.write(f"Pruned {count} JRTC receipts.")
