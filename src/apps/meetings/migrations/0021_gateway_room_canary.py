from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("meetings", "0020_gateway_route_recovery")]

    operations = [
        migrations.AddField(
            model_name="meetingroom",
            name="gateway_canary",
            field=models.BooleanField(default=None, editable=False, null=True),
        ),
    ]
