from uuid import UUID

from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand, CommandError

from apps.meetings.exceptions import MeetingDomainError
from apps.meetings.services.gateway_cutover import drain, set_cohort


class Command(BaseCommand):
    help = "Select or pause a room for future gateway sessions, or end an exact session for failback."

    def add_arguments(self, parser):
        parser.add_argument("action", choices=("select", "pause", "drain"))
        parser.add_argument("room", type=UUID)
        parser.add_argument("--session", type=UUID)

    def handle(self, *, action, room, session, **options):
        try:
            if action == "drain":
                if session is None:
                    raise CommandError(
                        "Specify the gateway session UUID with --session."
                    )
                drain(room, session)
                return (
                    "Meeting ended. Wait for command outcomes and clean owner release."
                )
            if session is not None:
                raise CommandError("--session applies only to drain.")
            set_cohort(room, selected=action == "select")
            return "Future session selection updated."
        except (MeetingDomainError, ObjectDoesNotExist) as exc:
            raise CommandError(str(exc)) from exc
