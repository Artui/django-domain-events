from __future__ import annotations

from uuid import uuid4

from django_domain_events.types.scope import MAX_ACTOR_LENGTH, Actor, Scope


def test_data_is_merged_not_replaced() -> None:
    """An inner block that adds a key must not erase the request's."""
    outer = Scope(data={"request_id": "abc"})
    merged = outer.merged(Scope(data={"source": "importer"}))
    assert merged.data == {"request_id": "abc", "source": "importer"}


def test_an_inner_key_wins_for_the_same_name() -> None:
    merged = Scope(data={"source": "web"}).merged(Scope(data={"source": "importer"}))
    assert merged.data == {"source": "importer"}


def test_the_actor_is_inherited_whole() -> None:
    actor = Actor(key="auth.User:1", label="ada", user_pk=1)
    merged = Scope(actor=actor).merged(Scope(data={"source": "importer"}))
    assert merged.actor == actor


def test_the_actor_is_replaced_whole() -> None:
    """Merging the parts independently lets an inner key sit beside a primary key."""
    merged = Scope(actor=Actor(key="auth.User:1", label="ada", user_pk=1)).merged(
        Scope(actor=Actor(key="system:relay", label="the relay"))
    )
    assert merged.actor == Actor(key="system:relay", label="the relay", user_pk=None)


def test_the_correlation_id_is_inherited() -> None:
    root = uuid4()
    assert Scope(correlation_id=root).merged(Scope()).correlation_id == root


def test_an_inner_correlation_id_wins() -> None:
    inner = uuid4()
    assert Scope(correlation_id=uuid4()).merged(Scope(correlation_id=inner)).correlation_id == inner


def test_actor_strings_are_truncated_at_capture() -> None:
    """Both columns are varchar(255)."""
    actor = Actor(key="k" * 400, label="l" * 400)
    assert len(actor.key) == MAX_ACTOR_LENGTH
    assert len(actor.label) == MAX_ACTOR_LENGTH


def test_an_empty_actor_is_falsy_so_it_does_not_overwrite() -> None:
    assert not Actor()
    assert Actor(key="x")
    assert Actor(user_pk=1)
