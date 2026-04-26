"""Bracket probability tests: sum-to-1, tail handling, floor before renorm."""
from __future__ import annotations

import math
from datetime import date

import pytest

from weather_edge.models import BracketSpec, PredictedDistribution
from weather_edge.postprocess.emos import compute_brackets

_FLOOR = 1e-4


def _dist(mu: float = 20.0, sigma: float = 2.0) -> PredictedDistribution:
    return PredictedDistribution(mu=mu, sigma=sigma, station="EGLL",
                                 valid_date=date(2026, 4, 27), lead_hours=24)


def _brackets_standard() -> list[BracketSpec]:
    return [
        BracketSpec(label="Below 15°C", low=None, high=15.0),
        BracketSpec(label="15°C to 18°C", low=15.0, high=18.0),
        BracketSpec(label="18°C to 21°C", low=18.0, high=21.0),
        BracketSpec(label="21°C to 24°C", low=21.0, high=24.0),
        BracketSpec(label="Above 24°C", low=24.0, high=None),
    ]


def test_probabilities_sum_to_one() -> None:
    dist = _dist()
    brackets = _brackets_standard()
    probs = compute_brackets(dist, brackets)
    total = sum(p.model_prob for p in probs)
    assert abs(total - 1.0) < 1e-9, f"Sum = {total}"


def test_tail_brackets_handle_infinity() -> None:
    dist = _dist(mu=20.0, sigma=2.0)
    brackets = _brackets_standard()
    probs = compute_brackets(dist, brackets)

    # Lowest bracket = P(Y < 15) — should be a small positive number for mu=20
    lowest = next(p for p in probs if p.label == "Below 15°C")
    assert lowest.model_prob > 0

    # Highest bracket = P(Y >= 24) — also small positive
    highest = next(p for p in probs if p.label == "Above 24°C")
    assert highest.model_prob > 0


def test_floor_applied_before_normalisation() -> None:
    """With mu far from all brackets, tiny probabilities are floored to 1e-4."""
    # mu = 50°C, sigma = 1°C → P(Y < 24) ≈ 0 before flooring
    dist = _dist(mu=50.0, sigma=1.0)
    brackets = _brackets_standard()
    probs = compute_brackets(dist, brackets)

    for p in probs:
        # After floor and renorm, none should be below floor/sum
        assert p.model_prob > 0
        # The floored bracket should be close to floor / (floor * n_brackets + rest)
        # At minimum it's floor / (sum after flooring) which > 0
        assert p.model_prob >= _FLOOR / (len(brackets) * 1.1)


def test_all_probs_positive() -> None:
    dist = _dist()
    brackets = _brackets_standard()
    probs = compute_brackets(dist, brackets)
    for p in probs:
        assert p.model_prob > 0, f"Zero probability for {p.label}"


def test_single_bracket_covers_everything() -> None:
    """One bracket spanning all of R should have probability 1.0."""
    dist = _dist()
    brackets = [BracketSpec(label="all", low=None, high=None)]
    probs = compute_brackets(dist, brackets)
    assert len(probs) == 1
    assert abs(probs[0].model_prob - 1.0) < 1e-9


def test_adjacent_brackets_partition_space() -> None:
    """Sum of non-tail brackets equals middle chunk probability."""
    dist = _dist(mu=20.0, sigma=2.0)
    # Two complementary brackets
    brackets = [
        BracketSpec(label="low", low=None, high=20.0),
        BracketSpec(label="high", low=20.0, high=None),
    ]
    probs = compute_brackets(dist, brackets)
    total = sum(p.model_prob for p in probs)
    assert abs(total - 1.0) < 1e-9
    # By symmetry, each should be ~0.5
    for p in probs:
        assert abs(p.model_prob - 0.5) < 0.01


def test_bracket_prob_method_direct() -> None:
    """PredictedDistribution.bracket_prob uses Gaussian CDF correctly."""
    from scipy.stats import norm
    dist = _dist(mu=20.0, sigma=2.0)

    expected = float(norm.cdf(22.0, 20.0, 2.0) - norm.cdf(18.0, 20.0, 2.0))
    actual = dist.bracket_prob(18.0, 22.0)
    assert abs(actual - expected) < 1e-12

    # Tail: P(Y < 15)
    assert abs(dist.bracket_prob(None, 15.0) - float(norm.cdf(15.0, 20.0, 2.0))) < 1e-12

    # Tail: P(Y >= 25)
    assert abs(dist.bracket_prob(25.0, None) - (1 - float(norm.cdf(25.0, 20.0, 2.0)))) < 1e-12
