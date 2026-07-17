from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("meetings", "0005_native_janus_videoroom_plugin"),
    ]

    operations = [
        migrations.AddField(
            model_name="meetingsession",
            name="cleanup_request_id",
            field=models.UUIDField(blank=True, editable=False, null=True),
        ),
        migrations.AddField(
            model_name="meetingsession",
            name="cleanup_requested_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddIndex(
            model_name="meetingsession",
            index=models.Index(
                fields=[
                    "lifecycle_state",
                    "cleanup_completed_at",
                    "cleanup_requested_at",
                ],
                name="meet_sess_cleanup_lease_idx",
            ),
        ),
    ]
