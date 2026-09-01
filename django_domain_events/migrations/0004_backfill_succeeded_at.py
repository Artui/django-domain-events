from django.db import migrations, models


def backfill(apps, schema_editor):
    """Give every already-succeeded delivery the success time it has on record.

    ``succeeded_at`` arrived empty, so on the upgrade every receiver read as
    having never succeeded - which is the one question the quiet-receiver query
    exists to answer, wrong on day one for anyone with history.

    ``completed_at`` is the right source and only for rows still SUCCEEDED: on
    those it is the moment the delivery was acknowledged. A row that succeeded
    and was later replayed had that column cleared by the replay, so no evidence
    survives and it correctly stays null.
    """
    DeliveryRecord = apps.get_model("django_domain_events", "DeliveryRecord")
    DeliveryRecord.objects.using(schema_editor.connection.alias).filter(
        status="succeeded", succeeded_at__isnull=True, completed_at__isnull=False
    ).update(succeeded_at=models.F("completed_at"))


class Migration(migrations.Migration):
    dependencies = [("django_domain_events", "0003_deliveryrecord_succeeded_at")]

    operations = [
        # Irreversible only in the sense that there is nothing to undo: 0003's
        # reverse drops the column outright.
        migrations.RunPython(backfill, migrations.RunPython.noop),
    ]
