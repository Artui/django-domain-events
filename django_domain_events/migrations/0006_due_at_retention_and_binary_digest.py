import django.db.models.deletion
from django.db import migrations, models

import django_domain_events.models.target_digest_field

APP = "django_domain_events"

BATCH = 2000
"""Rows converted per round trip, so a large table is never read into memory."""


def _copy(apps, schema_editor, source, dest, convert):
    """Fill ``dest`` from ``source`` on every delivery row, converted in Python.

    In Python rather than SQL because no expression spells hex-to-bytes on
    every backend: Postgres has ``decode(..., 'hex')``, SQLite has ``unhex()``
    only from 3.41, and MySQL has ``UNHEX()``. A branch per vendor would also
    leave all but one untested by the SQLite suite that gates coverage. The
    parameters are bound by the driver, which is what makes the same bytes
    land as a ``bytea``, a ``BLOB`` or a ``varbinary`` without this file
    knowing which.

    Paged by primary key rather than by offset, so each page is an index range
    and a row is neither skipped nor read twice
    (test_the_conversion_reaches_every_row_across_batches).
    """
    model = apps.get_model(APP, "DeliveryRecord")
    connection = schema_editor.connection
    rows = model.objects.using(connection.alias).order_by("pk")
    table = schema_editor.quote_name(model._meta.db_table)
    column = schema_editor.quote_name(dest)
    pk = schema_editor.quote_name(model._meta.pk.column)
    update = f"UPDATE {table} SET {column} = %s WHERE {pk} = %s"
    last = 0
    while page := list(rows.filter(pk__gt=last).values_list("pk", source)[:BATCH]):
        with connection.cursor() as cursor:
            cursor.executemany(update, [(convert(value), pk) for pk, value in page])
        last = page[-1][0]


def hex_to_raw(apps, schema_editor):
    _copy(apps, schema_editor, "target_digest", "target_digest_raw", bytes.fromhex)


def raw_to_hex(apps, schema_editor):
    _copy(apps, schema_editor, "target_digest_raw", "target_digest", lambda v: bytes(v).hex())


class Migration(migrations.Migration):
    dependencies = [
        ("django_domain_events", "0005_deliveryrecord_target"),
    ]

    # Every schema change of the release, in one migration, in an order chosen
    # for the size of the delivery table.
    #
    # 1. The redundant indexes go first, so the rewrite below does not
    #    maintain two indexes it is about to drop. ``event`` loses the index the
    #    unique constraint already leads with; ``receiver_key`` loses its
    #    single-column index and, on Postgres, the ``varchar_pattern_ops`` copy
    #    that came with it - its equality lookups move to ``dde_last_success``,
    #    built at the end.
    #
    # 2. The digest becomes 32 raw bytes. Not an AlterField: Django would cast
    #    the 64 hex characters to the 64 bytes that spell them on Postgres, and
    #    on SQLite copy the text into the rebuilt table unchanged - both a
    #    digest no new row would ever match. Instead a new column is filled
    #    from the old one and takes its place. The old constraint is dropped
    #    before the fill, so that in reverse it is re-added only after the hex
    #    has been written back; on Postgres and SQLite the migration is one
    #    transaction, so no other connection sees the table without it. This
    #    rewrites every delivery row.
    #
    # 3. The new columns and indexes. Each column is nullable or has a default
    #    that means "as before", so none needs a backfill.
    #
    # Plain index builds, not AddIndexConcurrently: that is Postgres-only, and
    # the package supports SQLite. On a large delivery table each build holds
    # a lock that blocks writes for its duration.
    operations = [
        migrations.AlterField(
            model_name="deliveryrecord",
            name="event",
            field=models.ForeignKey(
                db_index=False,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="deliveries",
                to="django_domain_events.eventrecord",
            ),
        ),
        migrations.AlterField(
            model_name="deliveryrecord",
            name="receiver_key",
            field=models.CharField(max_length=255),
        ),
        migrations.RemoveConstraint(
            model_name="deliveryrecord",
            name="unique_delivery_per_event_receiver_and_target",
        ),
        # Nullable for the length of the swap, and only for the reverse: undoing
        # the RemoveField below re-adds the hex column to a populated table
        # before the hex has been written back, which a NOT NULL column without
        # a default refuses. Placed before the fill so that in reverse NOT NULL
        # returns only once every row has its value again.
        migrations.AlterField(
            model_name="deliveryrecord",
            name="target_digest",
            field=models.CharField(editable=False, max_length=64, null=True),
        ),
        migrations.AddField(
            model_name="deliveryrecord",
            name="target_digest_raw",
            field=models.BinaryField(null=True),
        ),
        migrations.RunPython(hex_to_raw, raw_to_hex),
        migrations.RemoveField(
            model_name="deliveryrecord",
            name="target_digest",
        ),
        migrations.RenameField(
            model_name="deliveryrecord",
            old_name="target_digest_raw",
            new_name="target_digest",
        ),
        migrations.AlterField(
            model_name="deliveryrecord",
            name="target_digest",
            field=django_domain_events.models.target_digest_field.TargetDigestField(),
        ),
        migrations.AddConstraint(
            model_name="deliveryrecord",
            constraint=models.UniqueConstraint(
                fields=("event", "receiver_key", "target_digest"),
                name="unique_delivery_per_event_receiver_and_target",
            ),
        ),
        migrations.AddField(
            model_name="deliveryrecord",
            name="due_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="eventrecord",
            name="delete_when",
            field=models.CharField(blank=True, default="", max_length=16),
        ),
        migrations.AddField(
            model_name="eventrecord",
            name="retention_seconds",
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddIndex(
            model_name="deliveryrecord",
            index=models.Index(
                condition=models.Q(("status", "dead")),
                fields=["receiver_key"],
                name="dde_dead_by_receiver",
            ),
        ),
        migrations.AddIndex(
            model_name="deliveryrecord",
            index=models.Index(fields=["receiver_key", "succeeded_at"], name="dde_last_success"),
        ),
        migrations.AddIndex(
            model_name="eventrecord",
            index=models.Index(
                condition=models.Q(
                    ("retention_seconds__isnull", False),
                    models.Q(("delete_when", ""), _negated=True),
                    _connector="OR",
                ),
                fields=["recorded_at"],
                name="dde_own_retention",
            ),
        ),
    ]
