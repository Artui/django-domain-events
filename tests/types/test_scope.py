from __future__ import annotations

from uuid import uuid4

import pytest

from django_domain_events.models.event_record import EventRecord
from django_domain_events.types.scope import Actor, Scope

_KEY_LIMIT = EventRecord._meta.get_field("actor_key").max_length
_LABEL_LIMIT = EventRecord._meta.get_field("actor_label").max_length


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


def test_an_actor_key_over_its_column_is_refused_naming_the_field_and_the_limit() -> None:
    with pytest.raises(ValueError, match=f"actor_key of {_KEY_LIMIT + 1} characters") as caught:
        Actor(key="k" * (_KEY_LIMIT + 1))
    assert f"the limit is {_KEY_LIMIT}" in str(caught.value)


def test_two_keys_sharing_the_column_width_as_a_prefix_never_merge_into_one() -> None:
    """A clip made these one actor; each is refused instead."""
    prefix = "k" * _KEY_LIMIT
    for key in (prefix + "a", prefix + "b"):
        with pytest.raises(ValueError, match="actor_key"):
            Actor(key=key)


def test_an_actor_key_of_exactly_its_column_width_is_kept_whole() -> None:
    assert Actor(key="k" * _KEY_LIMIT).key == "k" * _KEY_LIMIT


def test_the_actor_label_is_display_text_and_is_clipped_to_its_column() -> None:
    assert Actor(label="l" * (_LABEL_LIMIT + 1)).label == "l" * _LABEL_LIMIT


def test_an_empty_actor_is_falsy_so_it_does_not_overwrite() -> None:
    assert not Actor()
    assert Actor(key="x")
    assert Actor(user_pk=1)
