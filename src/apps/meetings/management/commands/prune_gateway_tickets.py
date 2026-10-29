"""Remove expired receipts in bounded batches; absent receipts always deny."""

from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from apps.meetings.models import GatewayTicket


class Command(BaseCommand):
    help = (
        "Delete up to 1000 gateway ticket receipts expired for at least five seconds."
    )

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=1000)

    def handle(self, *args, **options):
        limit = options["limit"]
        if not 1 <= limit <= 1000:
            raise CommandError("limit must be between 1 and 1000")
        ids = list(
            GatewayTicket.objects.filter(
                expires_at__lt=timezone.now() - timedelta(seconds=5),
            )
            .order_by("expires_at")
            .values_list("pk", flat=True)[:limit]
        )
        count, _ = GatewayTicket.objects.filter(pk__in=ids).delete()
        self.stdout.write(f"Removed {count} expired receipts.")
