from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

import pytest
from django.contrib.auth.models import User
from django.db import transaction

from django_domain_events.attributed import attributed, current_scope
from django_domain_events.fire import fire
from django_domain_events.models.event_record import EventRecord
from django_domain_events.propagate_scope import propagate_scope
from tests.testapp.events import OrderPlaced

pytestmark = pytest.mark.django_db(transaction=True)


def test_the_actor_lands_in_all_three_columns(order: OrderPlaced, record: list[str]) -> None:
    """One derivation at capture time. The key covers actors that are not users,
    the label survives the user being deleted, and the foreign key is the join
    people actually want while the user exists."""
    user = User.objects.create(username="ada", pk=42)
    with attributed(actor=user), transaction.atomic():
        fire(order)

    row = EventRecord.objects.get()
    assert row.actor_key == "auth.User:42"
    assert row.actor_label == "ada"
    assert row.actor_id == 42


def test_a_non_user_actor_needs_no_model(order: OrderPlaced, record: list[str]) -> None:
    """A relay, a cron, a peer service. The foreign key stays null and the key
    carries the identity, which is why both columns exist."""
    with attributed(actor_key="system:relay", actor_label="the relay"), transaction.atomic():
        fire(order)

    row = EventRecord.objects.get()
    assert (row.actor_key, row.actor_label, row.actor_id) == ("system:relay", "the relay", None)


def test_arbitrary_facts_ride_along(order: OrderPlaced, record: list[str]) -> None:
    with attributed(source="checkout", request_id="abc"), transaction.atomic():
        fire(order)
    assert EventRecord.objects.get().scope == {"source": "checkout", "request_id": "abc"}


def test_nested_blocks_layer_rather_than_replace(order: OrderPlaced, record: list[str]) -> None:
    """An inner block that only adds a source must not erase the actor the
    request established."""
    user = User.objects.create(username="ada")
    # Deliberately nested rather than combined: the point is that one block is
    # inside another, which a single with-statement would not express.
    with attributed(actor=user, source="web"):  # noqa: SIM117
        with attributed(source="importer"), transaction.atomic():
            fire(order)

    row = EventRecord.objects.get()
    assert row.actor_label == "ada"
    assert row.scope == {"source": "importer"}


def test_the_outermost_block_roots_a_correlation_chain(
    order: OrderPlaced, record: list[str]
) -> None:
    with attributed(source="web") as scope, transaction.atomic():
        fire(order)
        fire(order)

    ids = set(EventRecord.objects.values_list("correlation_id", flat=True))
    assert ids == {scope.correlation_id}
    assert isinstance(scope.correlation_id, UUID)


def test_an_explicit_correlation_id_is_kept(order: OrderPlaced, record: list[str]) -> None:
    """Continuing a chain that started somewhere else - another service, a
    replay - is the case an id you cannot supply would make impossible."""
    given = uuid4()
    with attributed(correlation_id=given), transaction.atomic():
        fire(order)
    assert EventRecord.objects.get().correlation_id == given


def test_the_scope_does_not_leak_past_the_block(order: OrderPlaced, record: list[str]) -> None:
    """Workers and threads are reused. A scope left behind bleeds one request's
    attribution into the next, which for an attribution feature is a correctness
    bug with a privacy flavour rather than untidiness."""
    with attributed(actor_key="system:first"):
        pass
    assert current_scope().actor_key == ""

    with transaction.atomic():
        fire(order)
    assert EventRecord.objects.get().actor_key == ""


def test_a_raise_still_resets_the_scope() -> None:
    with pytest.raises(RuntimeError), attributed(actor_key="system:boom"):
        raise RuntimeError("boom")
    assert current_scope().actor_key == ""


def test_a_spawned_thread_loses_the_scope_without_help() -> None:
    """The gotcha that actually bites: a thread starts with an empty context,
    not a copy. Nothing fails - events simply arrive with no actor - so this is
    pinned rather than assumed."""
    seen: list[str] = []

    with attributed(actor_key="system:parent"):
        thread = threading.Thread(target=lambda: seen.append(current_scope().actor_key))
        thread.start()
        thread.join()

    assert seen == [""]


def test_propagate_scope_carries_it_into_a_thread() -> None:
    seen: list[str] = []

    with attributed(actor_key="system:parent"), ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(propagate_scope(lambda: seen.append(current_scope().actor_key))).result()

    assert seen == ["system:parent"]


def test_an_actor_that_is_not_a_model_still_gets_an_identity(
    order: OrderPlaced, record: list[str]
) -> None:
    """Plenty of things that fire events are not models: a service object, a
    management command, a job runner. Falling back to str() is what keeps
    ``actor=`` usable for them rather than a users-only parameter."""

    class Robot:
        def __str__(self) -> str:
            return "importer-7"

    with attributed(actor=Robot()), transaction.atomic():
        fire(order)

    row = EventRecord.objects.get()
    assert (row.actor_key, row.actor_label, row.actor_id) == (
        "importer-7",
        "importer-7",
        None,
    )
