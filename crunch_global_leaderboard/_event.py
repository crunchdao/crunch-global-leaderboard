from datetime import date, datetime, time
from typing import List, Optional, Tuple, TypedDict, cast

from crunch_global_leaderboard._model import Competition, Crunch, CrunchId, Leaderboard, LeaderboardDefinition, Phase, PhaseId, RoundId, Target, TargetId, User
from crunch_global_leaderboard._repository import Repository


class Event(TypedDict):
    user: User
    competition: Competition
    target: Target
    crunch: Crunch
    phase: Phase
    start: date
    leaderboard_size: int
    rank: float

    raw_points: float
    days_since_event: int
    decayed_points: int
    decayed_count: int


def _new_event(
    user: User,
    competition: Competition,
    target: Target,
    crunch: Crunch,
    phase: Phase,
    /,
    *,
    start: date,
    rank: float,
    leaderboard_size: int,
) -> Event:
    return {
        "user": user,
        "competition": competition,
        "target": target,
        "crunch": crunch,
        "phase": phase,
        "start": start,
        "rank": rank,
        "leaderboard_size": leaderboard_size,
        "raw_points": 0.0,
        "days_since_event": 0,
        "decayed_points": 0,
        "decayed_count": 0,
    }


def _determine_datacrunch_legacy_events(
    repository: Repository,
    competition: Competition,
    user: User,
):
    positions = repository.find_all_legacy_leaderboard_entries(user)

    target: Target = {
        "id": cast(TargetId, -1),
        "competition_id": competition["id"],
        "name": "global",
        "weight": 1.0,
        "virtual": False,
    }

    phase: Phase = {
        "id": cast(PhaseId, -1),
        "round_id": cast(RoundId, -1),
        "type": "OUT_OF_SAMPLE",
        "per_crunch_weight": 0.9 / 260,
    }

    events: List[Event] = []
    for position in positions:
        crunch_date = position["crunch_date"]

        crunch: Crunch = {
            "id": cast(CrunchId, -1),
            "phase_id": phase["id"],
            "number": position["crunch_number"],
            "end": datetime.combine(crunch_date, time.min),
        }

        events.append(_new_event(
            user,
            competition,
            target,
            crunch,
            phase,
            start=crunch_date,
            rank=position["rank"],
            leaderboard_size=position["crunch_size"],
        ))

    return events


def _determine_real_time_events(
    repository: Repository,
    competition: Competition,
    user: User,
):
    payouts = repository.find_all_paid_checkpoint_payouts(competition)

    target: Target = {
        "id": cast(TargetId, -1),
        "competition_id": competition["id"],
        "name": "global",
        "weight": 1.0,
        "virtual": False,
    }

    phase: Phase = {
        "id": cast(PhaseId, -1),
        "round_id": cast(RoundId, -1),
        "type": "OUT_OF_SAMPLE",
        "per_crunch_weight": 0.9 / 52,
    }

    events: List[Event] = []
    for payout in payouts:
        recipient = repository.find_payout_recipient(payout, user)
        if recipient is None:
            continue

        crunch: Crunch = {
            "id": cast(CrunchId, -1),
            "phase_id": phase["id"],
            "number": 1,
            "end": datetime.combine(payout["date"], time.min),
        }

        events.append(_new_event(
            user,
            competition,
            target,
            crunch,
            phase,
            start=payout["date"],
            rank=recipient["rank"],
            leaderboard_size=payout["size"],
        ))

    return events


def _get_position_from_team(
    repository: Repository,
    competition: Competition,
    user: User,
    leaderboard: Leaderboard,
) -> Optional[float]:
    team = repository.find_user_team(competition, user)
    if team is None:
        return None

    return repository.find_team_best_rank(leaderboard, team["id"])


def _get_position(
    repository: Repository,
    competition: Competition,
    crunch: Crunch,
    target: Target,
    default_leaderboard_definition: LeaderboardDefinition,
    user: User,
    *,
    fallback_to_team: bool,
) -> Tuple[Optional[float], int]:
    crunch_target = repository.find_crunch_target(crunch, target)

    leaderboard = repository.find_leaderboard(crunch_target, default_leaderboard_definition)
    if leaderboard is None:
        # TODO investigate why
        return None, 0

    position = repository.find_user_position(leaderboard, user)
    if position is None:
        rank: Optional[float] = None

        if fallback_to_team:
            rank = _get_position_from_team(
                repository,
                competition,
                user,
                leaderboard,
            )

        return rank, leaderboard["size"]

    rank = position["reward_rank"]

    team_id = position["team_id"]
    if team_id:
        rank = repository.find_team_best_rank(leaderboard, team_id)

    return rank, leaderboard["size"]


def _determine_offline_events(
    repository: Repository,
    competition: Competition,
    user: User,
):
    default_leaderboard_definition = repository.find_default_leaderboard_definition(competition)
    targets = repository.find_all_usable_targets(competition)
    rounds = repository.find_all_rounds(competition)

    events: List[Event] = []
    for round in rounds:
        phases = repository.find_all_phases(round)

        for phase in phases:
            is_out_of_sample = phase["type"] == "OUT_OF_SAMPLE"

            crunches = repository.find_all_crunches(phase)
            if is_out_of_sample:
                crunches = crunches[-1:]

            for crunch in crunches:
                for target in targets:
                    rank, leaderboard_size = _get_position(
                        repository,
                        competition,
                        crunch,
                        target,
                        default_leaderboard_definition,
                        user,
                        fallback_to_team=is_out_of_sample,
                    )

                    if rank is None:
                        continue

                    events.append(_new_event(
                        user,
                        competition,
                        target,
                        crunch,
                        phase,
                        start=crunch["end"].date(),
                        rank=rank,
                        leaderboard_size=leaderboard_size,
                    ))

    return events


def determine_events(
    repository: Repository,
    competition: Competition,
    user: User,
):
    if competition["name"] == "datacrunch-legacy":
        return _determine_datacrunch_legacy_events(
            repository,
            competition,
            user,
        )

    if competition["mode"] == "REAL_TIME":
        return _determine_real_time_events(
            repository,
            competition,
            user,
        )

    return _determine_offline_events(
        repository,
        competition,
        user,
    )
