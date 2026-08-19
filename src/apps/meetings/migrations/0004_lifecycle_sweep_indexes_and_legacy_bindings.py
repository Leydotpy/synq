from django.db import migrations, models


PROVIDER = "law_firm_workspace"


def backfill_legacy_external_bindings(apps, schema_editor):
    """Bind service rooms created before ExternalMeetingBinding existed."""

    del schema_editor
    MeetingRoom = apps.get_model("meetings", "MeetingRoom")
    ExternalMeetingBinding = apps.get_model("meetings", "ExternalMeetingBinding")

    legacy_by_external_id = {}
    for room in MeetingRoom.objects.all().iterator():
        metadata = room.metadata
        if not isinstance(metadata, dict) or metadata.get("source") != PROVIDER:
            continue
        external_id = str(metadata.get("external_id") or "").strip()
        if not external_id:
            continue
        existing_room_id = legacy_by_external_id.get(external_id)
        if existing_room_id is not None and existing_room_id != room.pk:
            raise RuntimeError(
                f"Multiple legacy meeting rooms use external_id '{external_id}'.",
            )
        legacy_by_external_id[external_id] = room.pk

    for external_id, room_id in legacy_by_external_id.items():
        room = MeetingRoom.objects.get(pk=room_id)
        existing = ExternalMeetingBinding.objects.filter(
            provider=PROVIDER,
            external_id=external_id,
        ).first()
        if existing is not None:
            if (
                existing.room_id != room.pk
                or existing.service_owner_profile_id != room.created_by_profile_id
            ):
                raise RuntimeError(
                    f"External meeting binding conflict for '{external_id}'.",
                )
            continue
        room_binding = ExternalMeetingBinding.objects.filter(
            provider=PROVIDER,
            room_id=room.pk,
        ).first()
        if room_binding is not None:
            raise RuntimeError(
                f"Legacy room '{room.pk}' is already bound to another external identity.",
            )
        ExternalMeetingBinding.objects.create(
            provider=PROVIDER,
            external_id=external_id,
            room_id=room.pk,
            service_owner_profile_id=room.created_by_profile_id,
        )


class Migration(migrations.Migration):
    dependencies = [
        ("meetings", "0003_meetinginvitation"),
    ]

    operations = [
        migrations.RunPython(backfill_legacy_external_bindings, migrations.RunPython.noop),
        migrations.AddIndex(
            model_name="meetingroom",
            index=models.Index(
                fields=["scheduled_end_at"],
                name="meet_room_sched_end_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="meetingsession",
            index=models.Index(
                fields=["lifecycle_state", "updated_at"],
                name="meet_sess_state_updated_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="meetingsession",
            index=models.Index(
                fields=["lifecycle_state", "cleanup_completed_at"],
                name="meet_sess_state_cleanup_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="meetingjoinrequest",
            index=models.Index(
                fields=["status", "created_at"],
                name="meet_join_status_created_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="participantconnection",
            index=models.Index(
                fields=["status", "last_heartbeat_at"],
                name="meet_conn_status_heartbeat_idx",
            ),
        ),
    ]
