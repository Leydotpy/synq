from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("meetings", "0019_gateway_state_projection")]
    operations = [
        migrations.AddField(
            model_name="gatewaycommandroute",
            name="janus_pool",
            field=models.CharField(
                blank=True, default="", editable=False, max_length=64
            ),
        ),
        migrations.AddField(
            model_name="gatewaycommandroute",
            name="janus_session_id",
            field=models.PositiveBigIntegerField(editable=False, null=True),
        ),
    ]
