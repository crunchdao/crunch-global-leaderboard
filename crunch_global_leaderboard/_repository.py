from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import date as Date
from textwrap import dedent
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Tuple, cast

from tqdm.auto import tqdm

from crunch_global_leaderboard._constants import PointParameters
from crunch_global_leaderboard._database import Database, to_column_names
from crunch_global_leaderboard._model import (
    Competition,
    CompetitionId,
    Crunch,
    CrunchId,
    CrunchTarget,
    CrunchTargetId,
    DailyUserSubmissionCount,
    GlobalInstitutionPositionBody,
    GlobalLeaderboard,
    GlobalLeaderboardBody,
    GlobalLeaderboardId,
    GlobalUserPositionBody,
    Institution,
    InstitutionBody,
    InstitutionId,
    InstitutionMember,
    InstitutionMemberBody,
    InstitutionName,
    InstitutionParticipationBody,
    Leaderboard,
    LeaderboardDefinition,
    LeaderboardDefinitionId,
    LeaderboardId,
    LegacyLeaderboardEntry,
    Participant,
    Payout,
    PayoutId,
    PayoutRecipient,
    Phase,
    PhaseId,
    Position,
    Round,
    RoundId,
    Target,
    TargetId,
    Team,
    TeamId,
    TeamMember,
    University,
    User,
    UserId
)
from crunch_global_leaderboard._utility import daily_date_range, group_by, to_dict


class Repository(ABC):

    @abstractmethod
    def find_first_university_by_display_name(self, display_name: str) -> Optional[University]:
        ...

    @abstractmethod
    def find_all_competitions(self) -> List[Competition]:
        ...

    @abstractmethod
    def find_all_users(self) -> List[User]:
        ...

    @abstractmethod
    def find_user_by_id(self, id: UserId) -> User:
        ...

    @abstractmethod
    def find_user_by_login(self, login: str) -> User:
        ...

    @abstractmethod
    def find_default_leaderboard_definition(self, competition: Competition) -> LeaderboardDefinition:
        ...

    @abstractmethod
    def find_all_usable_targets(self, competition: Competition) -> List[Target]:
        ...

    @abstractmethod
    def find_all_rounds(self, competition: Competition) -> List[Round]:
        ...

    @abstractmethod
    def find_all_phases(self, round: Round) -> List[Phase]:
        ...

    @abstractmethod
    def find_all_crunches(self, phase: Phase) -> List[Crunch]:
        ...

    @abstractmethod
    def find_crunch_target(self, crunch: Crunch, target: Target) -> CrunchTarget:
        ...

    @abstractmethod
    def find_leaderboard(self, crunch_target: CrunchTarget, leaderboard_definition: LeaderboardDefinition) -> Optional[Leaderboard]:
        ...

    @abstractmethod
    def find_user_position(self, leaderboard: Leaderboard, user: User) -> Optional[Position]:
        ...

    @abstractmethod
    def find_team_best_rank(self, leaderboard: Leaderboard, team_id: TeamId) -> Optional[float]:
        ...

    @abstractmethod
    def find_all_paid_checkpoint_payouts(self, competition: Competition) -> List[Payout]:
        ...

    @abstractmethod
    def find_payout_recipient(self, payout: Payout, user: User) -> Optional[PayoutRecipient]:
        ...

    @abstractmethod
    def find_all_legacy_leaderboard_entries(self, user: User) -> List[LegacyLeaderboardEntry]:
        ...

    @abstractmethod
    def find_all_participants(self, user: User) -> List[Participant]:
        ...

    @abstractmethod
    def find_institution_by_id(self, id: InstitutionId) -> Optional[Institution]:
        ...

    @abstractmethod
    def exists_institution_member(self, institution: Institution, user: User) -> bool:
        ...

    @abstractmethod
    def find_institution_by_name(self, name: InstitutionName) -> Optional[Institution]:
        ...

    @abstractmethod
    def find_all_teams(self, competition: Competition) -> List[Team]:
        ...

    @abstractmethod
    def find_all_team_members(self, team: Team) -> List[TeamMember]:
        ...

    @abstractmethod
    def find_user_team(self, competition: Competition, user: User) -> Optional[Team]:
        ...

    @abstractmethod
    def get_best_rank_per_user_id_before(self, date: Date) -> Dict[UserId, int]:
        ...

    @abstractmethod
    def count_submissions_up_to(self, date: Date) -> Dict[UserId, int]:
        ...

    @abstractmethod
    def create_institution(self, body: InstitutionBody) -> Institution:
        ...

    @abstractmethod
    def create_institution_member(self, body: InstitutionMemberBody) -> None:
        ...

    @abstractmethod
    def create_global_leaderboard(self, body: GlobalLeaderboardBody) -> GlobalLeaderboard:
        ...

    @abstractmethod
    def create_global_user_positions(self, bodies: List[GlobalUserPositionBody]) -> None:
        ...

    @abstractmethod
    def create_global_institution_positions(self, bodies: List[GlobalInstitutionPositionBody]) -> None:
        ...

    @abstractmethod
    def create_institution_participations(self, bodies: List[InstitutionParticipationBody]) -> None:
        ...

    @abstractmethod
    def delete_global_leaderboard_by_date(self, date: Date) -> int:
        ...


class LoadEverythingRepository(Repository):

    _university_by_display_name: Dict[str, University]
    _competitions: List[Competition]
    _users: List[User]
    _user_by_id: Dict[UserId, User]
    _user_by_login: Dict[str, User]
    _default_leaderboard_definition_by_competition_id: Dict[CompetitionId, LeaderboardDefinition]
    _usable_targets_by_competition_id: Dict[CompetitionId, List[Target]]
    _rounds_by_competition_id: Dict[CompetitionId, List[Round]]
    _phases_by_round_id: Dict[RoundId, List[Phase]]
    _crunches_by_phase_id: Dict[PhaseId, List[Crunch]]
    _crunch_target_by_crunch_id_and_target_id: Dict[Tuple[CrunchId, TargetId], CrunchTarget]
    _leaderboard_by_crunch_target_id_and_leaderboard_definition_id: Dict[Tuple[CrunchTargetId, LeaderboardDefinitionId], Leaderboard]
    _position_by_leaderboard_id_and_user_id: Dict[Tuple[LeaderboardId, UserId], Position]
    _best_team_rank_by_leaderboard_id_and_team_id: Dict[Tuple[LeaderboardId, TeamId], int]
    _payouts_by_competition_id: Dict[CompetitionId, List[Payout]]
    _payout_recipient_by_payout_id_and_user_id: Dict[Tuple[PayoutId, UserId], PayoutRecipient]
    _legacy_leaderboard_entries_by_user_id: Dict[UserId, List[LegacyLeaderboardEntry]]
    _participants_by_user_id: Dict[UserId, List[Participant]]
    _institution_by_id: Dict[InstitutionId, Institution]
    _institution_by_name: Dict[InstitutionName, Institution]
    _user_ids_by_institution_id: Dict[InstitutionId, Set[UserId]]
    _global_leaderboard_by_date: Dict[Date, GlobalLeaderboard]
    _teams_by_competition_id: Dict[CompetitionId, List[Team]]
    _team_members_by_team_id: Dict[TeamId, List[TeamMember]]
    _team_by_competition_id_and_user_id: Dict[Tuple[CompetitionId, UserId], Team]
    _submission_count_per_user_per_date: DefaultDict[Date, Dict[UserId, int]]

    def __init__(self, database: Database):
        self._database = database

        self.load()

    def load(
        self,
        *,
        only: Optional[List[str]] = None,
    ):

        def _load_universities():
            self._university_by_display_name = to_dict(
                self._database.account.query_many_objects(University),
                key=lambda row: row["name"],
                merge=lambda a, b: a,
            )

        def _load_competitions():
            self._competitions = self._database.competition.query_many_objects(
                Competition,
                where="`visibility` = 'PUBLIC' AND NOT `external`",
            )

            datacrunch_legacy = self._database.competition.query_first_object(
                Competition,
                where="`name` = 'datacrunch-legacy'",
            )

            if datacrunch_legacy:
                self._competitions.insert(0, datacrunch_legacy)

        def _load_users():
            self._users = self._database.competition.query_many_objects(
                User,
                where="""
                    `id` IN (SELECT DISTINCT `user_id` FROM `positions`)
                    OR `id` IN (SELECT DISTINCT `user_id` FROM `team_members`)
                    OR `id` IN (SELECT DISTINCT `user_id` FROM `payout_recipients`)
                """
            )

            self._user_by_id = to_dict(
                self._users,
                key=lambda row: row["id"],
            )

            self._user_by_login = to_dict(
                self._users,
                key=lambda row: row["login"],
            )

        def _load_leaderboard_definitions():
            self._default_leaderboard_definition_by_competition_id = to_dict(
                self._database.competition.query_many_objects(
                    LeaderboardDefinition,
                    where="`default`",
                ),
                key=lambda row: row["competition_id"],
            )

        def _load_targets():
            self._usable_targets_by_competition_id = group_by(
                self._database.competition.query_many_objects(Target),
                key=lambda row: row["competition_id"],
            )

            for targets in self._usable_targets_by_competition_id.values():
                virtual_targets = [
                    target
                    for target in targets
                    if target["virtual"]
                ]

                if len(virtual_targets):
                    targets.clear()
                    targets.extend(virtual_targets)

        def _load_rounds():
            self._rounds_by_competition_id = group_by(
                self._database.competition.query_many_objects(Round),
                key=lambda row: row["competition_id"],
            )

        def _load_phases():
            self._phases_by_round_id = group_by(
                self._database.competition.query_many_objects(Phase),
                key=lambda row: row["round_id"],
            )

        def _load_crunches():
            self._crunches_by_phase_id = group_by(
                self._database.competition.query_many_objects(Crunch),
                key=lambda row: row["phase_id"],
            )

        def _load_crunch_targets():
            self._crunch_target_by_crunch_id_and_target_id = to_dict(
                self._database.competition.query_many_objects(CrunchTarget),
                key=lambda row: (row["crunch_id"], row["target_id"]),
            )

        def _load_leaderboards():
            self._leaderboard_by_crunch_target_id_and_leaderboard_definition_id = to_dict(
                self._database.competition.query_many_objects(Leaderboard),
                key=lambda row: (row["crunch_target_id"], row["definition_id"]),
            )

        def _load_positions():
            rows = self._database.competition.query_many_objects(Position)

            def _best_rank(left: Position, right: Position):
                left_rank = left["rank"]
                right_rank = right["rank"]

                if left_rank < right_rank:  # smallest is better
                    return left
                else:
                    return right

            self._position_by_leaderboard_id_and_user_id = to_dict(
                rows,
                key=lambda row: (row["leaderboard_id"], row["user_id"]),
                merge=_best_rank,
            )

            self._best_team_rank_by_leaderboard_id_and_team_id = to_dict(
                (
                    row
                    for row in rows
                    if row["team_id"] is not None and row["reward_rank"] is not None
                ),
                key=lambda row: (row["leaderboard_id"], cast(TeamId, row["team_id"])),
                value=lambda row: int(cast(float, row["reward_rank"])),
                merge=min,
            )

        def _load_paid_checkpoint_payouts():
            self._payouts_by_competition_id = group_by(
                self._database.competition.query_many_objects(
                    Payout,
                    where="`type` = 'CHECKPOINT' AND `status` = 'PAID'",
                ),
                key=lambda row: row["competition_id"],
            )

            payout_recipient_columns = to_column_names(PayoutRecipient, table_name="payout_recipients")
            self._payout_recipient_by_payout_id_and_user_id = to_dict(
                self._database.competition.query_many(
                    f"SELECT {payout_recipient_columns} FROM `payout_recipients` LEFT JOIN `payouts` ON `payouts`.`id` = `payout_recipients`.`payout_id` WHERE `type` = 'CHECKPOINT' AND `status` = 'PAID' AND `rank` <= {PointParameters.MAX_REWARD_RANK}",
                    type=PayoutRecipient,
                ),
                key=lambda row: (row["payout_id"], row["user_id"]),
            )

        def _load_legacy_leaderboard_entries():
            self._legacy_leaderboard_entries_by_user_id = group_by(
                self._database.competition.query_many_objects(LegacyLeaderboardEntry),
                key=lambda row: row["user_id"],
            )

        def _load_participants():
            self._participants_by_user_id = group_by(
                self._database.competition.query_many_objects(Participant),
                key=lambda row: row["user_id"],
            )

        def _load_institutions():
            institutions = self._database.competition.query_many_objects(Institution)

            self._institution_by_name = to_dict(
                institutions,
                key=lambda row: row["name"],
            )

            self._institution_by_id = to_dict(
                institutions,
                key=lambda row: row["id"],
            )

            self._user_ids_by_institution_id = {
                institution_id: set(member["user_id"] for member in members)
                for institution_id, members in group_by(
                    self._database.competition.query_many_objects(InstitutionMember),
                    key=lambda row: row["institution_id"],
                ).items()
            }

        def _load_global_leaderboards():
            self._global_leaderboard_by_date = to_dict(
                self._database.competition.query_many_objects(
                    GlobalLeaderboard,
                    order_by="`date` ASC"
                ),
                key=lambda row: row["date"],
            )

        def _load_teams():
            teams = self._database.competition.query_many_objects(
                Team,
                where="NOT `deleted`",
            )

            team_members = self._database.competition.query_many_objects(
                TeamMember,
                where="`team_id` NOT IN (SELECT `id` FROM `teams` WHERE `deleted`)",  # TODO Prefer join?
            )

            team_by_id: Dict[TeamId, Team] = to_dict(
                teams,
                key=lambda row: row["id"],
            )

            self._teams_by_competition_id = group_by(
                teams,
                key=lambda row: row["competition_id"],
            )

            self._team_members_by_team_id = group_by(
                team_members,
                key=lambda row: row["team_id"],
            )

            self._team_by_competition_id_and_user_id = to_dict(
                team_members,
                key=lambda row: (
                    team_by_id[row["team_id"]]["competition_id"],
                    row["user_id"],
                ),
                value=lambda row: team_by_id[row["team_id"]],
            )

        def _load_submissions():
            self._submission_count_per_user_per_date = defaultdict(dict)

            last_date = Date.today()
            count_per_date_per_user_id: Dict[UserId, Dict[Date, int]] = defaultdict(lambda: defaultdict(lambda: 0))

            def do_load(statement: str):
                raw_data = self._database.competition.query_many(
                    dedent(statement),
                    type=DailyUserSubmissionCount,
                )

                for row in tqdm(raw_data):
                    user_id = row["user_id"]
                    today = row["date"]
                    count = row["count"]

                    count_per_date_per_user_id[user_id][today] += count

            do_load("""
                SELECT
                    CAST(`submissions`.`created_at` AS DATE) AS `date`,
                    `user_id`,
                    COUNT(*) AS `count`
                FROM
                    `submissions`
                LEFT JOIN `projects` ON `submissions`.`project_id` = `projects`.`id`
                GROUP BY
                    `user_id`,
                    `date`
                ORDER BY
                    `date`;
            """)

            do_load("""
                SELECT
                    CAST(`uploaded_at` AS DATE) AS `date`,
                    `user_id`,
                    COUNT(*) AS `count`
                FROM
                    `legacy_submissions`
                GROUP BY
                    `user_id`,
                    `date`
                ORDER BY
                    `date`;
            """)

            for user_id, count_per_date in count_per_date_per_user_id.items():
                first_date = min(count_per_date.keys())

                previous_value = 0
                for date in daily_date_range(first_date, last_date):
                    next_value = previous_value + count_per_date.get(date, 0)

                    self._submission_count_per_user_per_date[date][user_id] = next_value
                    previous_value = next_value

        methods: List[Callable[[], None]] = [
            _load_universities,
            _load_competitions,
            _load_users,
            _load_leaderboard_definitions,
            _load_targets,
            _load_rounds,
            _load_phases,
            _load_crunches,
            _load_crunch_targets,
            _load_leaderboards,
            _load_positions,
            _load_paid_checkpoint_payouts,
            _load_legacy_leaderboard_entries,
            _load_participants,
            _load_institutions,
            _load_global_leaderboards,
            _load_teams,
            _load_submissions,
        ]

        for method in tqdm(methods, unit="method", miniters=1):
            name = method.__name__[6:]

            if only is not None and name not in only:
                continue

            print(f"load: {name}")
            method()

        for key, value in vars(self).items():
            if key.startswith("_") and isinstance(value, (list, dict)):
                print(f"len: {key}={len(value)}")  # type: ignore

    def find_first_university_by_display_name(self, display_name: str) -> Optional[University]:
        return self._university_by_display_name.get(display_name)

    def find_all_competitions(self) -> List[Competition]:
        return self._competitions

    def find_all_users(self) -> List[User]:
        return self._users

    def find_user_by_id(self, id: UserId) -> User:
        return self._user_by_id[id]

    def find_user_by_login(self, login: str) -> User:
        return self._user_by_login[login]

    def find_default_leaderboard_definition(self, competition: Competition):
        return self._default_leaderboard_definition_by_competition_id[competition["id"]]

    def find_all_usable_targets(self, competition: Competition) -> List[Target]:
        return self._usable_targets_by_competition_id[competition["id"]]

    def find_all_rounds(self, competition: Competition) -> List[Round]:
        return self._rounds_by_competition_id[competition["id"]]

    def find_all_phases(self, round: Round) -> List[Phase]:
        return self._phases_by_round_id[round["id"]]

    def find_all_crunches(self, phase: Phase) -> List[Crunch]:
        return self._crunches_by_phase_id[phase["id"]]

    def find_crunch_target(self, crunch: Crunch, target: Target) -> CrunchTarget:
        return self._crunch_target_by_crunch_id_and_target_id[(crunch["id"], target["id"])]

    def find_leaderboard(self, crunch_target: CrunchTarget, leaderboard_definition: LeaderboardDefinition) -> Optional[Leaderboard]:
        return self._leaderboard_by_crunch_target_id_and_leaderboard_definition_id.get((crunch_target["id"], leaderboard_definition["id"]))

    def find_user_position(self, leaderboard: Leaderboard, user: User) -> Optional[Position]:
        return self._position_by_leaderboard_id_and_user_id.get((leaderboard["id"], user["id"]))

    def find_team_best_rank(self, leaderboard: Leaderboard, team_id: TeamId) -> Optional[float]:
        return self._best_team_rank_by_leaderboard_id_and_team_id.get((leaderboard["id"], team_id))

    def find_all_paid_checkpoint_payouts(self, competition: Competition) -> List[Payout]:
        return self._payouts_by_competition_id.get(competition["id"]) or []

    def find_payout_recipient(self, payout: Payout, user: User) -> Optional[PayoutRecipient]:
        return self._payout_recipient_by_payout_id_and_user_id.get((payout["id"], user["id"]))

    def find_all_legacy_leaderboard_entries(self, user: User) -> List[LegacyLeaderboardEntry]:
        return self._legacy_leaderboard_entries_by_user_id.get(user["id"]) or []

    def find_all_participants(self, user: User) -> List[Participant]:
        return self._participants_by_user_id.get(user["id"]) or []

    def find_institution_by_id(self, id: InstitutionId) -> Optional[Institution]:
        return self._institution_by_id.get(id)

    def exists_institution_member(self, institution: Institution, user: User) -> bool:
        user_ids = self._user_ids_by_institution_id.get(institution["id"])
        if user_ids is None:
            return False

        return user["id"] in user_ids

    def find_institution_by_name(self, name: InstitutionName) -> Optional[Institution]:
        return self._institution_by_name.get(name)

    def find_all_teams(self, competition: Competition) -> List[Team]:
        return self._teams_by_competition_id.get(competition["id"]) or []

    def find_all_team_members(self, team: Team) -> List[TeamMember]:
        return self._team_members_by_team_id.get(team["id"]) or []

    def find_user_team(self, competition: Competition, user: User) -> Optional[Team]:
        return self._team_by_competition_id_and_user_id.get((competition["id"], user["id"]))

    def get_best_rank_per_user_id_before(self, date: Date) -> Dict[UserId, int]:
        return {
            row["user_id"]: row["best_rank"]
            for row in self._database.competition.query_many(
                "SELECT `user_id`, `best_rank` FROM `global_user_positions` WHERE `leaderboard_id` = (SELECT `id` FROM `global_leaderboards` WHERE `date` < %s ORDER BY `date` DESC LIMIT 1)",
                params=(date,),
                type=Dict[str, Any],
            )
        }

    def count_submissions_up_to(self, date: Date) -> Dict[UserId, int]:
        return self._submission_count_per_user_per_date.get(date) or dict()

    def create_institution(self, body: InstitutionBody) -> Institution:
        id = self._database.competition.insert_object(
            "institutions",
            body,
        )

        institution: Institution = {
            "id": cast(InstitutionId, id),
            **body,
        }

        self._institution_by_name[institution["name"]] = institution
        self._institution_by_id[institution["id"]] = institution

        return institution

    def create_institution_member(self, body: InstitutionMemberBody) -> None:
        self._database.competition.insert_object(
            "institution_members",
            body,
        )

        institution_id = body["institution_id"]

        user_ids = self._user_ids_by_institution_id.get(institution_id)
        if user_ids is None:
            user_ids = self._user_ids_by_institution_id[institution_id] = set()

        user_ids.add(body["user_id"])

    def create_global_leaderboard(self, body: GlobalLeaderboardBody) -> GlobalLeaderboard:
        id = self._database.competition.insert_object(
            "global_leaderboards",
            body,
        )

        leaderboard: GlobalLeaderboard = {
            "id": cast(GlobalLeaderboardId, id),
            **body,
        }

        self._global_leaderboard_by_date[leaderboard["date"]] = leaderboard

        return leaderboard

    def create_global_user_positions(self, bodies: List[GlobalUserPositionBody]) -> None:
        self._database.competition.insert_many_object(
            "global_user_positions",
            bodies,
        )

    def create_global_institution_positions(self, bodies: List[GlobalInstitutionPositionBody]) -> None:
        self._database.competition.insert_many_object(
            "global_institution_positions",
            bodies,
        )

    def create_institution_participations(self, bodies: List[InstitutionParticipationBody]) -> None:
        self._database.competition.insert_many_object(
            "institution_participations",
            bodies,
        )

    def delete_global_leaderboard_by_date(self, date: Date) -> bool:
        global_leaderboard = self._global_leaderboard_by_date.get(date)
        if global_leaderboard is None:
            return False

        global_leaderboard_id = global_leaderboard["id"]

        self._database.competition.insert(
            "DELETE FROM `global_user_positions` WHERE `leaderboard_id` = %s",
            (global_leaderboard_id,),
        )

        self._database.competition.insert(
            "DELETE FROM `global_institution_positions` WHERE `leaderboard_id` = %s",
            (global_leaderboard_id,),
        )

        self._database.competition.insert(
            "DELETE FROM `institution_participations` WHERE `leaderboard_id` = %s",
            (global_leaderboard_id,),
        )

        self._database.competition.insert(
            "DELETE FROM `global_leaderboards` WHERE `id` = %s",
            (global_leaderboard_id,),
        )

        del self._global_leaderboard_by_date[date]

        return True
