import django.db.models.deletion
from django.db import migrations, models
from django.db.migrations.operations.base import Operation

import django_domain_events.models.target_digest_field

APP = "django_domain_events"

BATCH = 2000
"""Rows converted per round trip, so a large table is never read into memory."""


def _copy(apps, schema_editor, source, dest, convert):
    """Fill ``dest`` from ``source`` on every delivery row, converted in Python.

    The path for every backend but Postgres, which converts in place (see
    ConvertDigest). In Python rather than SQL because no expression spells
    hex-to-bytes on the rest: SQLite has ``unhex()`` only from 3.41, and MySQL
    spells it ``UNHEX()``. The parameters are bound by the driver, which is
    what makes the same bytes land as a ``BLOB`` or a ``varbinary`` without
    this file knowing which.

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


# The portable swap: a new column filled from the old one, which then takes its
# place. The old constraint is dropped before the fill so that in reverse it is
# re-added only after the hex has been written back, and the old column is made
# nullable before the fill for the same reason: undoing the RemoveField re-adds
# it to a populated table before the hex is back, which a NOT NULL column
# without a default refuses.
_SWAP = [
    migrations.RemoveConstraint(
        model_name="deliveryrecord",
        name="unique_delivery_per_event_receiver_and_target",
    ),
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
    migrations.RemoveField(model_name="deliveryrecord", name="target_digest"),
    migrations.RenameField(
        model_name="deliveryrecord", old_name="target_digest_raw", new_name="target_digest"
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
]

_POSTGRES = {
    "forwards": "ALTER TABLE {table} ALTER COLUMN {column} TYPE bytea USING decode({column}, 'hex')",
    "backwards": (
        "ALTER TABLE {table} ALTER COLUMN {column} TYPE varchar(64) USING encode({column}, 'hex')"
    ),
}


class ConvertDigest(Operation):
    """The digest column, from 64 hex characters to the 32 bytes they spell.

    Not an AlterField: Django would cast the hex text to the 64 bytes that
    spell it on Postgres, and on SQLite copy the text into the rebuilt table
    unchanged - both a digest no new row would ever match.

    On Postgres, one ``ALTER COLUMN ... TYPE ... USING`` in each direction.
    That rewrites the table once, compactly, and rebuilds the unique index
    from the converted values, where the portable swap leaves a dead version
    of every row and the dropped column's bytes in every live one until a
    ``VACUUM FULL``. Every other backend runs the swap.

    The state is the swap's, whichever ran, so both paths end at the same
    model: the Postgres path is measured against it by the round-trip and
    makemigrations tests on Postgres, and its SQL by
    test_postgres_converts_the_column_in_place_in_both_directions.
    """

    reversible = True

    def state_forwards(self, app_label, state):
        for operation in _SWAP:
            operation.state_forwards(app_label, state)

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if schema_editor.connection.vendor == "postgresql":
            schema_editor.execute(self._postgres("forwards", schema_editor, from_state))
            return
        for operation, before, after in self._steps(app_label, from_state):
            if not self._printing_only(operation, schema_editor):
                operation.database_forwards(app_label, schema_editor, before, after)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        # Django passes the state *after* this operation as from_state when
        # unapplying, so the swap's states are rebuilt from to_state, the one
        # before it, and walked in reverse.
        if schema_editor.connection.vendor == "postgresql":
            schema_editor.execute(self._postgres("backwards", schema_editor, to_state))
            return
        for operation, before, after in reversed(self._steps(app_label, to_state)):
            if not self._printing_only(operation, schema_editor):
                operation.database_backwards(app_label, schema_editor, after, before)

    @staticmethod
    def _printing_only(operation, schema_editor):
        """Whether ``operation`` is the copy and ``sqlmigrate`` is only printing.

        Django marks a top-level RunPython as not writable as SQL and skips
        it, but it takes this operation for a schema one and runs it, so the
        copy would run against the live table under a command that must touch
        nothing (test_sqlmigrate_prints_the_conversion_without_running_it).
        Without the first conjunct an ordinary ``migrate`` never runs the copy,
        and test_every_hex_digest_becomes_the_raw_bytes_it_spelled goes red; without
        the second the schema steps print nothing either, which the sqlmigrate
        test catches.
        """
        if schema_editor.collect_sql and not operation.reduces_to_sql:
            schema_editor.collected_sql.append("-- THIS OPERATION CANNOT BE WRITTEN AS SQL")
            return True
        return False

    def describe(self):
        return "Store the delivery target digest as raw bytes"

    @staticmethod
    def _postgres(direction, schema_editor, state):
        model = state.apps.get_model(APP, "DeliveryRecord")
        return _POSTGRES[direction].format(
            table=schema_editor.quote_name(model._meta.db_table),
            column=schema_editor.quote_name("target_digest"),
        )

    @staticmethod
    def _steps(app_label, state):
        steps = []
        for operation in _SWAP:
            after = state.clone()
            operation.state_forwards(app_label, after)
            steps.append((operation, state, after))
            state = after
        return steps


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
    # 2. The digest becomes 32 raw bytes (ConvertDigest). This rewrites every
    #    delivery row. On Postgres and SQLite the migration is one transaction,
    #    so no other connection sees the table mid-conversion.
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
        ConvertDigest(),
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
                condition=models.Q(("retention_seconds__isnull", False)),
                fields=["recorded_at"],
                name="dde_own_window",
            ),
        ),
        migrations.AddIndex(
            model_name="eventrecord",
            index=models.Index(
                condition=models.Q(("delete_when", ""), _negated=True),
                fields=["delete_when", "recorded_at"],
                name="dde_consumed_by_policy",
            ),
        ),
    ]
