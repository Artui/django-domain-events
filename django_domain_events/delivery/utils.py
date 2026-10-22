"""Helpers shared by the delivery modules that are not themselves exported."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping

from django_domain_events.declaration.registry import registry
from django_domain_events.settings import setting

OnDeferral = Callable[[str, float], None]
"""Told the lane and the requested delay, in seconds, of a deferral that did
not count and was recorded. The relay pauses that lane on hearing it."""


def claim_size(batch_size: int | None, *, name: str = "batch_size") -> int:
    """The rows one claim takes: ``batch_size`` if given, else ``BATCH_SIZE``.

    Shared by ``run_relay`` and ``deliver_pending``, which both take a size of
    their own for their claims alone - ``BATCH_SIZE`` also sizes prune batches
    and requeue chunks, and a mail relay wants a batch it can send inside one
    lease. A size below one is refused rather than run: it claims nothing on
    every pass, so a relay sized that way idles forever with work owed
    (``test_a_relay_refuses_a_batch_that_claims_nothing``). ``name`` is what
    the refusal calls the value, so a per-lane size names its lane.
    """
    if batch_size is None:
        return setting("BATCH_SIZE")
    if batch_size < 1:
        raise ValueError(f"{name} must be positive, got {batch_size}")
    return batch_size


def served_lanes(lane: str | Collection[str] | None) -> tuple[str, ...] | None:
    """The lanes a relay serves, validated and sorted, or None for every lane.

    Shared by ``run_relay`` and ``deliver_pending``. A ``str`` is one lane, and
    is checked for before anything treats ``lane`` as a collection: a ``str``
    is one, and iterating ``"mail"`` would serve the lanes ``m``, ``a``, ``i``
    and ``l`` (``test_a_string_is_one_lane_not_its_letters``). A collection is
    reduced to its distinct members, sorted, which is the order a rotation
    visits them in. Duplicates are dropped rather than refused, because they
    name the same work and a manifest assembled by concatenation should not
    fail over one.

    Refused, each by its own check and its own test in
    ``tests/delivery/test_run_relay_lanes.py``: an empty collection
    (``test_an_empty_set_of_lanes_is_refused``), which is neither every lane nor
    any lane, and would claim nothing and look healthy doing it; a member that
    is not a ``str`` (``test_a_lane_that_is_not_a_string_is_refused``); and a
    member no receiver is declared in, through ``registry.require_lane``
    (``test_an_undeclared_member_is_refused_by_name``).

    None is returned as None rather than resolved here, because every lane is
    re-read from the registry at each pass, as lane membership is at each
    claim.
    """
    if lane is None:
        return None
    members = [lane] if isinstance(lane, str) else list(lane)
    if not members:
        raise ValueError(
            "lane is an empty collection, which serves no lane: name at least one, "
            "or pass None for every lane."
        )
    for member in members:
        if not isinstance(member, str):
            raise ValueError(f"A lane is named by a str, got {member!r}.")
        registry.require_lane(member)
    return tuple(sorted(set(members)))


def lane_claim_sizes(
    lanes: tuple[str, ...] | None,
    *,
    batch_size: int | None,
    lane_batch_sizes: Mapping[str, int] | None,
) -> Callable[[str], int]:
    """How many rows a claim of each lane takes, resolved once at start.

    ``batch_size`` sizes every lane ``lane_batch_sizes`` does not list, and is
    ``BATCH_SIZE`` when None. Each override goes through ``claim_size``, so a
    size below one is refused as a ``batch_size`` is
    (``test_a_lane_size_below_one_is_refused``). An override naming a lane this
    relay does not serve is refused too
    (``test_a_size_for_a_lane_not_served_is_refused``): it is a manifest
    sizing work that nothing here will claim, which is a typo or a lane left
    behind by a move. Under None, ``lanes`` is every lane the registry
    declares at start.

    The size of a lane in a shared relay is what it costs the others, which
    is a property of the deployment's mix rather than of the lane, so it is
    set here and not declared on the receivers.
    """
    default = claim_size(batch_size)
    served = registry.lanes() if lanes is None else lanes
    overrides: dict[str, int] = {}
    for name, size in sorted((lane_batch_sizes or {}).items()):
        if name not in served:
            raise ValueError(
                f"lane_batch_sizes names lane {name!r}, which is not served here. "
                f"Served lanes: {', '.join(served)}."
            )
        overrides[name] = claim_size(size, name=f"lane_batch_sizes[{name!r}]")
    return lambda name: overrides.get(name, default)
