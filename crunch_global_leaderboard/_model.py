from datetime import date as Date
from datetime import datetime as DateTime
from typing import Literal, NewType, Optional, TypedDict

from typing_extensions import ReadOnly, TypeAlias

UniversityId = NewType("UniversityId", int)
UserId = NewType("UserId", int)
CompetitionId = NewType("CompetitionId", int)
CompetitionName = NewType("CompetitionName", str)
RoundId = NewType("RoundId", int)
LeaderboardId = NewType("LeaderboardId", int)
LeaderboardDefinitionId = NewType("LeaderboardDefinitionId", int)
PayoutId = NewType("PayoutId", int)
PhaseId = NewType("PhaseId", int)
PayoutRecipientId = NewType("PayoutRecipientId", int)
CrunchTargetId = NewType("CrunchTargetId", int)
TargetId = NewType("TargetId", int)
CrunchId = NewType("CrunchId", int)
TeamId = NewType("TeamId", int)
TeamMemberId = NewType("TeamMemberId", int)
InstitutionId = NewType("InstitutionId", int)
InstitutionName = NewType("InstitutionName", str)
InstitutionMemberId = NewType("InstitutionMemberId", int)
GlobalLeaderboardId = NewType("GlobalLeaderboardId", int)


class University(TypedDict):
    id: ReadOnly[UniversityId]
    name: str
    url: Optional[str]
    country_alpha3: Optional[str]


class User(TypedDict):
    id: ReadOnly[UserId]
    login: str
    university: Optional[str]


CompetitionMode: TypeAlias = Literal["OFFLINE", "REAL_TIME"]


class Competition(TypedDict):
    id: ReadOnly[CompetitionId]
    name: CompetitionName
    mode: CompetitionMode
    start: DateTime
    prize_pool_usd: int


class LeaderboardDefinition(TypedDict):
    id: ReadOnly[LeaderboardDefinitionId]
    competition_id: CompetitionId


class Target(TypedDict):
    id: ReadOnly[TargetId]
    competition_id: CompetitionId
    name: str
    weight: float
    virtual: bool


class Round(TypedDict):
    id: ReadOnly[RoundId]
    competition_id: CompetitionId
    end: DateTime


PhaseType: TypeAlias = Literal["SUBMISSION", "OUT_OF_SAMPLE"]


class Phase(TypedDict):
    id: ReadOnly[PhaseId]
    round_id: RoundId
    type: PhaseType
    per_crunch_weight: float


class Crunch(TypedDict):
    id: ReadOnly[CrunchId]
    phase_id: PhaseId
    number: int
    end: DateTime


class CrunchTarget(TypedDict):
    id: ReadOnly[CrunchTargetId]
    target_id: TargetId
    crunch_id: CrunchId


class Leaderboard(TypedDict):
    id: ReadOnly[LeaderboardId]
    crunch_target_id: CrunchTargetId
    definition_id: LeaderboardDefinitionId
    size: int


class Position(TypedDict):
    leaderboard_id: LeaderboardId
    user_id: UserId
    team_id: Optional[TeamId]
    rank: int
    reward_rank: Optional[float]


class Payout(TypedDict):
    competition_id: CompetitionId
    id: ReadOnly[PayoutId]
    date: Date
    size: int


class PayoutRecipient(TypedDict):
    id: ReadOnly[PayoutRecipientId]
    payout_id: PayoutId
    user_id: UserId
    rank: int


class LegacyLeaderboardEntry(TypedDict):
    crunch_date: Date
    crunch_number: int
    crunch_size: int
    user_id: UserId
    rank: int


class Participant(TypedDict):
    user_id: UserId
    created_at: Optional[DateTime]


class InstitutionBody(TypedDict):
    name: InstitutionName
    display_name: str
    country: str
    total_points: int
    member_count: int
    global_rank: Optional[int]
    about: Optional[str]
    website_url: Optional[str]
    twitter_url: Optional[str]
    linked_in_url: Optional[str]
    created_at: DateTime
    updated_at: DateTime


class Institution(InstitutionBody):
    id: ReadOnly[InstitutionId]


class InstitutionMemberBody(TypedDict):
    institution_id: InstitutionId
    user_id: UserId
    rank: Optional[int]
    created_at: DateTime
    updated_at: DateTime


class InstitutionMember(InstitutionMemberBody):
    id: ReadOnly[InstitutionMemberId]


class GlobalUserPositionBody(TypedDict):
    leaderboard_id: GlobalLeaderboardId
    user_id: UserId
    institution_id: Optional[InstitutionId]
    rank: int
    institution_member_rank: Optional[int]
    points: int
    best_rank: int
    participation_count: int
    submission_count: int


class GlobalInstitutionPositionBody(TypedDict):
    leaderboard_id: GlobalLeaderboardId
    institution_id: InstitutionId
    rank: int
    total_points: int
    user_count: int
    top_user_1_id: Optional[UserId]
    top_user_2_id: Optional[UserId]
    top_user_3_id: Optional[UserId]
    average_points_per_user: float


class GlobalLeaderboardBody(TypedDict):
    date: Date
    user_count: int
    institution_count: int
    published: bool
    updated_at: DateTime
    created_at: DateTime


class GlobalLeaderboard(GlobalLeaderboardBody):
    id: ReadOnly[GlobalLeaderboardId]


class InstitutionParticipationBody(TypedDict):
    leaderboard_id: GlobalLeaderboardId
    institution_id: InstitutionId
    competition_id: CompetitionId
    best_user_id: Optional[UserId]
    best_user_leaderboard_rank: Optional[int]
    member_count: int
    total_points: int
    created_at: Date


class Team(TypedDict):
    id: TeamId
    competition_id: CompetitionId


class TeamMember(TypedDict):
    id: TeamMemberId
    team_id: TeamId
    user_id: UserId


class DailyUserSubmissionCount(TypedDict):
    date: Date
    user_id: UserId
    count: int
