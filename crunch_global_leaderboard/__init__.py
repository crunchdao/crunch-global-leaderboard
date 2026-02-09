import math
import os
from collections import defaultdict
from logging import Logger
from datetime import date, datetime, time
from threading import Lock
from typing import DefaultDict, Dict, List, Optional, TypedDict, cast

from slugify import slugify
from tqdm.auto import tqdm

from crunch_global_leaderboard._database import Database
from crunch_global_leaderboard._event import Event, determine_events
from crunch_global_leaderboard._model import Competition, CompetitionId, CompetitionName, GlobalInstitutionPositionBody, GlobalLeaderboardId, GlobalUserPositionBody, Institution, InstitutionId, InstitutionName, InstitutionParticipationBody, University, User, UserId
from crunch_global_leaderboard._point import compute_decayed_points, compute_raw_points
from crunch_global_leaderboard._repository import LoadEverythingRepository, Repository
from crunch_global_leaderboard._utility import group_by, rank_by_points
from crunch_global_leaderboard._web import get_site_descriptions

DATABASE_HOST = os.environ["DATABASE_HOST"]
DATABASE_USER = os.environ["DATABASE_USER"]
DATABASE_PASSWORD = os.environ["DATABASE_PASSWORD"]
ACCOUNT_SERVICE_DATABASE_NAME = os.environ.get("ACCOUNT_SERVICE_DATABASE_NAME", f"tournament_account_service")
COMPETITION_SERVICE_DATABASE_NAME = os.environ.get("COMPETITION_SERVICE_DATABASE_NAME", f"tournament_competition_service")


def _compute_events(
    *,
    users: List[User],
    competitions: List[Competition],
    repository: Repository,
):
    all_events_by_user_id: Dict[UserId, List[Event]] = {}

    for user in tqdm(users, unit="user"):
        all_events: List[Event] = []

        for competition in competitions:
            events = determine_events(
                repository=repository,
                competition=competition,
                user=user,
            )

            all_events.extend(events)

        for event in all_events:
            compute_raw_points(event)

        all_events_by_user_id[user["id"]] = all_events

    return (
        all_events_by_user_id,
    )


def _compute_participations(
    *,
    dates: List[date],
    users: List[User],
    repository: Repository,
):
    participation_count_per_date_per_user_id: Dict[UserId, Dict[date, int]] = {}

    for user in tqdm(users, unit="user"):
        per_date = {}

        participants = repository.find_all_participants(user)

        for today in dates:
            count = sum(
                1
                for participant in participants
                if participant["created_at"] is None or participant["created_at"].date() <= today
            )

            per_date[today] = count

        participation_count_per_date_per_user_id[user["id"]] = per_date

    return (
        participation_count_per_date_per_user_id,
    )


def _compute_institutions(
    *,
    all_events_by_user_id: Dict[UserId, List[Event]],
    repository: Repository,
    skip_university_description: bool = False,
    rephrase_university_descriptions: bool = False,
):
    users_by_institution_name: DefaultDict[InstitutionName, List[User]] = defaultdict(list)
    university_by_institution_name: Dict[InstitutionName, University] = {}

    institution_by_user_id: Dict[UserId, Institution] = {}
    university_description_by_institution_name: Dict[InstitutionName, Optional[str]] = {}

    created_institution_count = 0

    for user_id in tqdm(all_events_by_user_id.keys(), total=len(all_events_by_user_id)):
        user = repository.find_user_by_id(user_id)

        university_display_name = user["university"]
        if university_display_name is None or university_display_name == "Self Taught":
            continue

        university = repository.find_first_university_by_display_name(university_display_name)
        if university is None:
            continue  # skip bad/old mapping

        institution_name = cast(InstitutionName, f"university.{slugify(university_display_name)}")
        users_by_institution_name[institution_name].append(user)
        university_by_institution_name[institution_name] = university

    institution_by_name = {
        key: institution
        for key in users_by_institution_name.keys()
        if (institution := repository.find_institution_by_name(key)) is not None
    }

    university_description_by_institution_name = (
        {}
        if skip_university_description
        else get_site_descriptions(
            {
                key: university_by_institution_name[key]
                for key in users_by_institution_name.keys()
                if key not in institution_by_name
            },
            rephrase=rephrase_university_descriptions,
        )
    )

    now = datetime.now()

    for institution_name, users in tqdm(users_by_institution_name.items()):
        institution = institution_by_name.get(institution_name)

        if institution is None:
            university = university_by_institution_name[institution_name]

            institution = repository.create_institution({
                "name": institution_name,
                "display_name": university["name"],
                "country": university.get("country_alpha3") or "???",
                "total_points": 0,
                "member_count": len(users),
                "global_rank": None,
                "about": university_description_by_institution_name.get(institution_name),
                "website_url": university.get("url"),
                "twitter_url": None,
                "linked_in_url": None,
                "created_at": now,
                "updated_at": now,
            })

            created_institution_count += 1

        for user in users:
            if repository.exists_institution_member(institution, user):
                continue

            repository.create_institution_member({
                "institution_id": institution["id"],
                "user_id": user["id"],
                "rank": None,
                "created_at": now,
                "updated_at": now,
            })

        for user in users:
            institution_by_user_id[user["id"]] = institution

    return (
        institution_by_user_id,
        created_institution_count,
    )


def _rank_users(
    points_per_user_id: Dict[UserId, int],
):
    return rank_by_points([
        {
            "id": user_id,
            "points": points,
        }
        for user_id, points in points_per_user_id.items()
    ])


def _rank_institutions(
    user_positions_by_institution_id: Dict[InstitutionId | None, List[GlobalUserPositionBody]]
):
    return rank_by_points([
        {
            "id": institution_id,
            "points": sum(global_position["points"] for global_position in global_user_positions),
        }
        for institution_id, global_user_positions in user_positions_by_institution_id.items()
        if institution_id is not None
    ])


class ParticipationData(TypedDict):
    competition_id: CompetitionId
    total_points: float
    member_count: int
    best_user_id: Optional[UserId]
    best_user_leaderboard_rank: Optional[int]


def _apply_user_ties(
    user_positions: List[GlobalUserPositionBody],
):
    for user_position, previous_user_position in zip(user_positions[1:], user_positions[:1]):
        if user_position["points"] == previous_user_position["points"]:
            user_position["rank"] = previous_user_position["rank"]


def _apply_institution_member_ties(
    user_positions: List[GlobalUserPositionBody],
):
    for user_position, previous_user_position in zip(user_positions[1:], user_positions[:1]):
        if user_position["points"] == previous_user_position["points"]:
            user_position["institution_member_rank"] = previous_user_position["institution_member_rank"]


def _apply_institution_member_ranks_and_ties(
    user_positions_by_institution_id: Dict[Optional[InstitutionId], List[GlobalUserPositionBody]],
):
    user_positions = user_positions_by_institution_id.get(None) or []
    for user_position in user_positions:
        user_position["institution_member_rank"] = None

    for institution_id, user_positions in user_positions_by_institution_id.items():
        if institution_id is None:
            continue

        for index, user_position in enumerate(user_positions):
            rank = index + 1
            user_position["institution_member_rank"] = rank

        _apply_institution_member_ties(user_positions)


def _compute_user_postitions(
    *,
    dates: List[date],
    users: List[User],
    all_events_by_user_id: Dict[UserId, List[Event]],
    institution_by_user_id: Dict[UserId, Institution],
    participation_count_per_date_per_user_id: Dict[UserId, Dict[date, int]],
    repository: Repository,
):
    user_positions_per_date: Dict[date, List[GlobalUserPositionBody]] = {}

    for today in tqdm(dates, unit="date", miniters=1):
        now = datetime.combine(today, time.min)

        points_per_user_id: DefaultDict[UserId, int] = defaultdict(lambda: 0)
        participation_data_per_competition_name_per_institution_id: DefaultDict[InstitutionId, DefaultDict[CompetitionName, ParticipationData]] = defaultdict(
            lambda: defaultdict(
                lambda: {
                    "competition_id": cast(CompetitionId, -1),
                    "total_points": 0,
                    "member_count": 0,
                    "best_user_id": None,
                    "best_user_leaderboard_rank": None,
                }
            )
        )

        for user in users:
            user_id = user["id"]

            events = [
                event
                for event in all_events_by_user_id[user_id]
                if event["start"] <= today
            ]

            if not events:
                continue

            for event in events:
                compute_decayed_points(event, today)

                points_per_user_id[user_id] += event["decayed_points"]

                institution = institution_by_user_id.get(user_id)
                if institution:
                    competition = event["competition"]
                    participation_data = participation_data_per_competition_name_per_institution_id[institution["id"]][competition["name"]]
                    participation_data["competition_id"] = competition["id"]
                    participation_data["total_points"] += event["raw_points"]
                    participation_data["member_count"] += 1

                    rank = event["rank"]
                    if rank:
                        best_user_rank = participation_data["best_user_leaderboard_rank"]

                        if best_user_rank is None or best_user_rank > rank:
                            participation_data["best_user_id"] = user["id"]
                            participation_data["best_user_leaderboard_rank"] = math.floor(rank)  # TODO is that a good idea?

        best_rank_per_user_id = repository.get_best_rank_per_user_id_before(today)
        submission_count_per_user = repository.count_submissions_up_to(today)

        daily_user_positions: List[GlobalUserPositionBody] = []
        for entry in _rank_users(points_per_user_id).values():
            user_id, points, rank = entry["id"], entry["points"], entry["rank"]

            best_rank = min(best_rank_per_user_id.get(user_id, 99999999), rank)

            institution = institution_by_user_id.get(user_id)

            daily_user_positions.append({
                "leaderboard_id": cast(GlobalLeaderboardId, -1),
                "user_id": user_id,
                "institution_id": institution["id"] if institution else None,
                "rank": rank,
                "institution_member_rank": rank,
                "points": points,
                "best_rank": best_rank,
                "participation_count": participation_count_per_date_per_user_id[user_id][today],
                "submission_count": submission_count_per_user.get(user_id, 0),
            })

        _apply_user_ties(daily_user_positions)

        user_positions_by_institution_id = group_by(
            daily_user_positions,
            key=lambda user_position: user_position["institution_id"],
        )

        _apply_institution_member_ranks_and_ties(user_positions_by_institution_id)

        institution_count = len(user_positions_by_institution_id)
        if None in user_positions_by_institution_id:
            institution_count -= 1

        repository.delete_global_leaderboard_by_date(today)
        leaderboard = repository.create_global_leaderboard({
            "date": today,
            "user_count": len(points_per_user_id),
            "institution_count": institution_count,
            "published": False,
            "updated_at": now,
            "created_at": now,
        })

        for daily_user_position in daily_user_positions:
            daily_user_position["leaderboard_id"] = leaderboard["id"]

        repository.create_global_user_positions(daily_user_positions)
        user_positions_per_date[today] = daily_user_positions

        institution_ranks = _rank_institutions(user_positions_by_institution_id)

        institution_positions: List[GlobalInstitutionPositionBody] = []
        for institution_id, user_positions in user_positions_by_institution_id.items():
            if institution_id is None:
                continue

            position = institution_ranks[institution_id]
            total_points = position["points"]

            institution_positions.append({
                "leaderboard_id": leaderboard["id"],
                "institution_id": institution_id,
                "rank": position["rank"],
                "total_points": total_points,
                "user_count": len(user_positions),  # TODO this is false, it must be on members, not active members?
                "top_user_1_id": user_positions[0]["user_id"] if len(user_positions) >= 1 else None,
                "top_user_2_id": user_positions[1]["user_id"] if len(user_positions) >= 2 else None,
                "top_user_3_id": user_positions[2]["user_id"] if len(user_positions) >= 3 else None,
                "average_points_per_user": total_points // len(user_positions),
            })

        institution_participations: List[InstitutionParticipationBody] = []
        for institution_id, participation_data_per_competition_name in participation_data_per_competition_name_per_institution_id.items():
            for participation_data in participation_data_per_competition_name.values():
                institution_participations.append({
                    "leaderboard_id": leaderboard["id"],
                    "institution_id": institution_id,
                    "competition_id": participation_data["competition_id"],
                    "best_user_id": participation_data["best_user_id"],
                    "best_user_leaderboard_rank": participation_data["best_user_leaderboard_rank"],
                    "member_count": participation_data["member_count"],
                    "total_points": math.ceil(participation_data["total_points"]),
                    "created_at": today,
                })

        repository.create_global_institution_positions(institution_positions)
        repository.create_institution_participations(institution_participations)

    return (
        user_positions_per_date,
    )


_lock = Lock()


def compute(
    dates: List[date],
    logger: Logger,
):
    dates = sorted(dates)

    database = Database(
        host=DATABASE_HOST,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        account_service_name=ACCOUNT_SERVICE_DATABASE_NAME,
        competition_service_name=COMPETITION_SERVICE_DATABASE_NAME,
        enable_caching=False,
        commit_on_close=True,
    )

    with _lock, database:
        repository = LoadEverythingRepository(
            database=database,
        )

        users = repository.find_all_users()
        competitions = repository.find_all_competitions()

        (
            all_events_by_user_id,
        ) = _compute_events(
            users=users,
            competitions=competitions,
            repository=repository,
        )

        (
            participation_count_per_date_per_user_id,
        ) = _compute_participations(
            dates=dates,
            users=users,
            repository=repository,
        )

        (
            institution_by_user_id,
            _created_institution_count,
        ) = _compute_institutions(
            all_events_by_user_id=all_events_by_user_id,
            repository=repository,
            rephrase_university_descriptions=True,
        )

        logger.info(f"created {_created_institution_count} institutions")

        _compute_user_postitions(
            dates=dates,
            users=users,
            all_events_by_user_id=all_events_by_user_id,
            institution_by_user_id=institution_by_user_id,
            participation_count_per_date_per_user_id=participation_count_per_date_per_user_id,
            repository=repository,
        )
