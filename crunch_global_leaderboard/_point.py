import math
from datetime import date
from functools import cache
from typing import List

from crunch_global_leaderboard._constants import PointParameters
from crunch_global_leaderboard._event import Event


def compute_point_distribution(
    number_of_participants: int,
) -> List[float]:
    """
    Compute point distribution weights for participants.

    Args:
        num_participants: Number of active participants

    Returns:
        List of weights for each rank position
    """

    # Power law distribution: P(rank) = k * rank^(-α)
    # Where α > 1 produces a sharp spike near rank 1 with a long tail
    # Higher α = steeper decay (more concentrated at top ranks)
    # Common values: α ∈ [1.5, 2.5]

    # Power law exponent (α > 1 for sharp spike and long tail)
    # alpha = 0.9
    alpha = 1.0

    return [
        1 / (i**alpha)
        for i in range(1, number_of_participants + 1)
    ]


@cache
def _compute_point_distribution_normalized_cached(
    leaderboard_size: int,
):
    weights = compute_point_distribution(leaderboard_size)

    unnormalized_weights_sum = sum(weights)
    weights = [
        weight / unnormalized_weights_sum
        for weight in weights
    ]

    return weights


def compute_raw_points(
    event: Event,
):
    rank = event["rank"]
    if rank >= PointParameters.MAX_REWARD_RANK:
        event["raw_points"] = 0.0
        return

    weights = _compute_point_distribution_normalized_cached(event["leaderboard_size"])
    target_weight = event["target"]["weight"]

    weight = weights[int(rank) - 1]  # rank start at 1, array at 0
    prize_pool = event["competition"]["prize_pool_usd"]

    phase_multiplier = event["phase"]["per_crunch_weight"]

    base_points = prize_pool * weight * target_weight
    raw_points = base_points * phase_multiplier

    event["raw_points"] = raw_points


def compute_decayed_points(
    event: Event,
    today: date,
):
    days_since_event = (today - event["start"]).days

    decay_factor = math.exp(-days_since_event / PointParameters.DECAY_CONSTANT)

    event["days_since_event"] = days_since_event
    event["decayed_points"] = math.ceil(event["raw_points"] * decay_factor)
    event["decayed_count"] += 1
