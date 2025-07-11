import logging
from enum import StrEnum
from typing import Optional, Any, Dict, List, ClassVar
from pydantic import field_validator, Field

from pyutils.utils import epoch_now
from pydantic_exportables import (
    JSONExportable,
    CSVExportable,
    TXTExportable,
    Idx,
    BackendIndex,
    IndexSortOrder,
    DESCENDING,
    ASCENDING,
    TEXT,
)


from blitzmodels import (
    Account,
    Release,
    AccountInfo,
    Tank,
    EnumNation,
    EnumVehicleTypeInt,
    EnumVehicleTier,
    EnumVehicleTypeStr,
)
from blitzmodels.wotinspector.wi_apiv2 import Replay

logger = logging.getLogger(__name__)
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

MIN_INACTIVITY_DAYS: int = 90  # days
MAX_UPDATE_INTERVAL: int = 365 * 24 * 3600  # 1 year


class StatsTypes(StrEnum):
    tank_stats = "updated_tank_stats"
    player_achievements = "updated_player_achievements"
    account_info = "updated_account_info"


class BSAccount(Account):
    # fmt: off
    updated_tank_stats			: int | None = Field(default=None, alias="ut")
    updated_player_achievements	: int | None = Field(default=None, alias="up")
    updated_account_info		: int | None = Field(default=None, alias="ui")
    added						: int  = Field(default_factory=epoch_now, alias="a")
    inactive					: bool = Field(default=False, alias="i")
    disabled					: bool = Field(default=False, alias="d")
    # fmt: on

    _min_inactivity_secs: ClassVar[int] = MIN_INACTIVITY_DAYS * 24 * 3600
    _exclude_defaults = False

    class Config:
        populate_by_name = True

        validate_assignment = True

    @classmethod
    def backend_indexes(cls) -> List[List[tuple[str, IndexSortOrder]]]:
        """return backend search indexes"""
        indexes: List[List[BackendIndex]] = list()
        indexes.append(
            [
                # ("inactive", ASCENDING),
                ("region", ASCENDING),
                ("last_battle_time", DESCENDING),
            ]
        )
        indexes.append(
            [
                ("disabled", ASCENDING),
                ("region", ASCENDING),
                ("last_battle_time", DESCENDING),
            ]
        )
        indexes.append(
            [
                ("disabled", ASCENDING),
                ("region", ASCENDING),
                ("id", ASCENDING),
            ]
        )
        indexes.append([("nickname", TEXT)])
        # indexes.append([ 	('disabled', ASCENDING),
        # 					('inactive', ASCENDING),
        # 					('id', 		ASCENDING),
        # 					('updated_tank_stats', ASCENDING)
        # 				])
        return indexes

    @classmethod
    def inactivity_limit(cls) -> int:
        return cls._min_inactivity_secs

    @classmethod
    def set_inactivity_limit(cls, days: int) -> None:
        cls._min_inactivity_secs = days * 24 * 3600

    @classmethod
    def get_update_field(cls, stats_type: str | None) -> str | None:
        try:
            if stats_type is not None:
                return StatsTypes(stats_type).value
        except Exception:
            error(f"Unknown stats_type: {stats_type}")
        return None

    @field_validator("updated_tank_stats", "updated_player_achievements")
    def check_epoch_ge_zero(cls, v):
        if v is None:
            return None
        elif v >= 0:
            return v
        else:
            raise ValueError("time field must be >= 0")

    @field_validator("added")
    def set_current_time(cls, v):
        """
        Set 'added' field to current UTC time if the field has not been set
        """
        if v is None:
            return epoch_now()
        elif v >= 0:
            return v
        else:
            ValueError("time field must be >= 0")

    # @model_validator(mode="after")
    # def set_inactive(self) -> Self:
    #     """ "
    #     Set 'inactive' field based in players inactivity.
    #     If 'last_battle_time' == 0, set the player ACTIVE since to avoid
    #     inactivating players when creating player objects with default field values
    #     """
    #     if self.last_battle_time > 0:
    #         self._set_skip_validation(
    #             "inactive",
    #             epoch_now() - self.last_battle_time > self._min_inactivity_secs,
    #         )
    #     else:
    #         self._set_skip_validation("inactive", False)
    #     return self

    def stats_updated(self, stats: StatsTypes) -> None:
        assert type(stats) is StatsTypes, "'stats' need to be type(StatsTypes)"
        setattr(self, stats.value, epoch_now())

    def is_inactive(self, stats_type: StatsTypes | None = None) -> bool:
        """Check if account is active"""
        stats_updated: int | None
        if (
            stats_type is None
            or (stats_updated := getattr(self, stats_type.value)) is None
        ):
            stats_updated = epoch_now()
        return (
            stats_updated - self.last_battle_time
            > self._min_inactivity_secs * 24 * 3600
        )

    def update_needed(self, stats_type: StatsTypes) -> bool:
        """Check whether the account needs an update"""
        stats_updated: int | None
        if (stats_updated := getattr(self, stats_type.value)) is None:
            stats_updated = 0
        return (epoch_now() - stats_updated) > min(
            MAX_UPDATE_INTERVAL, (stats_updated - self.last_battle_time) / 3
        )

    def update_info(self, update: AccountInfo) -> bool:
        """Update BSAccount() from WGACcountInfo i.e. from WG API"""
        updated: bool = False
        try:
            updated = super().update_info(update)
            self.updated_account_info = epoch_now()
            return updated
        except Exception as err:
            error(f"{err}")
        return False

    @classmethod
    def transform_Account(cls, in_obj: Account) -> Optional["BSAccount"]:
        """Transform Account object to BSAccount"""
        try:
            return BSAccount(
                id=in_obj.id,
                region=in_obj.region,
                last_battle_time=in_obj.last_battle_time,
                created_at=in_obj.created_at,
                updated_at=in_obj.updated_at,
                nickname=in_obj.nickname,
                updated_account_info=epoch_now(),
            )
        except Exception as err:
            error(f"{err}")
        return None

    @classmethod
    def transform_WGAccountInfo(cls, in_obj: AccountInfo) -> Optional["BSAccount"]:
        """Transform AccountInfo object to BSAccount"""
        try:
            account: Account | None = Account.transform(in_obj)
            return BSAccount.transform(account)
        except Exception as err:
            error(f"{err}")
        return None


BSAccount.register_transformation(AccountInfo, BSAccount.transform_WGAccountInfo)
BSAccount.register_transformation(Account, BSAccount.transform_Account)


class BSBlitzRelease(Release):
    _max_epoch: ClassVar[int] = 2**63 - 1  # MAX_INT64 (signed)
    cut_off: int = Field(default=_max_epoch)

    _exclude_defaults = False

    class Config:
        validate_assignment = True
        populate_by_name = True

    # @field_validator("cut_off")
    # def check_cut_off_now(cls, v):
    #     if v is None:
    #         return cls._max_epoch
    #     elif isinstance(v, str):
    #         if v == "now":
    #             return epoch_now()
    #         else:
    #             return int(v)
    #     elif isinstance(v, int):
    #         if v == 0:
    #             return cls._max_epoch
    #         return v

    @field_validator("cut_off")
    def validate_cut_off(cls, v: int) -> int:
        if v > 0:
            return v
        elif v == 0:
            return cls._max_epoch
        raise ValueError("cut_off has to be >= 0")

    def cut_off_now(self) -> None:
        self.cut_off = epoch_now()

    def txt_row(self, format: str = "") -> str:
        extra: str = ""
        if format == "rich":
            extra = f"\t{self.cut_off}"
        return super().txt_row(format) + extra


class BSReplay(Replay):
    """
    Replay model for Blitz-Stats
    """

    @field_validator("last_accessed_time", "download_url", "details_url")
    @classmethod
    def _set_none(cls, value: Any) -> None:
        return None


def Replay2BSReplay(replay: Replay) -> BSReplay | None:
    """
    Transform Replay to BSReplay
    """
    return BSReplay.model_validate(replay)


BSReplay.register_transformation(Replay, Replay2BSReplay)

# class BSReplay(ReplaySummary):
#     id: str | None = Field(default=None, alias="_id")

#     _ViewUrlBase: str = "https://replays.wotinspector.com/en/view/"
#     _DLurlBase: str = "https://replays.wotinspector.com/en/download/"

#     class Config:
#         arbitrary_types_allowed = True
#
#         validate_assignment = True
#         populate_by_name = True

#     @property
#     def index(self) -> Idx:
#         """return backend index"""
#         if self.id is not None:
#             return self.id
#         raise ValueError("id is missing")

#     @property
#     def view_url(self) -> str:
#         if self.id is not None:
#             return f"{self._ViewUrlBase}{self.id}"
#         raise ValueError("field 'id' is missing")

#     @property
#     def dl_url(self) -> str:
#         if self.id is not None:
#             return f"{self._DLurlBase}{self.id}"
#         raise ValueError("field 'id' is missing")

#     @property
#     def indexes(self) -> Dict[str, Idx]:
#         """return backend indexes"""
#         return {"id": self.index}

#     @classmethod
#     def backend_indexes(cls) -> List[List[tuple[str, BackendIndexType]]]:
#         """return backend search indexes"""
#         indexes: List[List[tuple[str, BackendIndexType]]] = list()
#         indexes.append(
#             [
#                 ("protagonist", ASCENDING),
#                 ("room_type", ASCENDING),
#                 ("vehicle_tier", ASCENDING),
#                 ("battle_start_timestamp", DESCENDING),
#             ]
#         )
#         indexes.append(
#             [
#                 ("room_type", ASCENDING),
#                 ("vehicle_tier", ASCENDING),
#                 ("battle_start_timestamp", DESCENDING),
#             ]
#         )
#         return indexes

#     @classmethod
#     def transform_WoTBlitzReplayData(
#         cls, in_obj: ReplayData
#     ) -> Optional["Replay"]:
#         res: Replay | None = None
#         try:
#             if (res := Replay.parse_obj(in_obj.summary.dict())) is not None:
#                 res.id = in_obj.id
#                 return res
#             else:
#                 error(f"failed to transform object: {in_obj}")
#         except ValidationError as err:
#             error(f"failed to transform object: {in_obj}: {err}")
#         return res

#     @classmethod
#     def transform_WoTBlitzReplayJSON(
#         cls, in_obj: ReplayJSON
#     ) -> Optional["Replay"]:
#         if (replay_data := ReplayData.transform(in_obj)) is not None:
#             return cls.transform_WoTBlitzReplayData(replay_data)
#         return None


# Replay.register_transformation(
#     ReplayData, Replay.transform_WoTBlitzReplayData
# )
# Replay.register_transformation(
#     ReplayJSON, Replay.transform_WoTBlitzReplayJSON
# )


# fmt: off
class BSTank(JSONExportable, CSVExportable, TXTExportable):
    tank_id 	: int						= Field(default=..., alias='_id')
    name 		: str | None				= Field(default=None, alias='n')
    code        : str | None                = Field(default=None)
    nation		: EnumNation | None 		= Field(default=None, alias='c')
    type		: EnumVehicleTypeInt | None	= Field(default=None, alias='v')
    tier		: EnumVehicleTier | None 	= Field(default=None, alias='t')
    is_premium 	: bool 						= Field(default=False, alias='p')
    next_tanks	: List[int] | None			= Field(default=None, alias='s')
    # fmt: on
    _exclude_defaults = False

    class Config:
        
        validate_assignment = True
        populate_by_name = True
        # use_enum_values			= True
        extra = Extra.allow

    @property
    def index(self) -> Idx:
        """return backend index"""
        return self.tank_id

    @property
    def indexes(self) -> Dict[str, Idx]:
        """return backend indexes"""
        return {'tank_id': self.index}

    @classmethod
    def backend_indexes(cls) -> List[List[tuple[str, IndexSortOrder]]]:
        indexes: List[List[BackendIndex]] = list()
        indexes.append([('tier', ASCENDING),
                        ('type', ASCENDING)
                        ])
        indexes.append([('tier', ASCENDING),
                        ('nation', ASCENDING)
                        ])
        indexes.append([('name', TEXT), ('code', TEXT)])
        
        return indexes

    @field_validator('next_tanks')
    def next_tanks2list(cls, v):
        try:
            if v is not None:
                return [int(k) for k in v.keys()]
        except Exception as err:
            error(f"Error validating 'next_tanks': {err}")
        return None

    @field_validator('tier')
    def prevalidate_tier(cls, v: Any) -> Any:
        if isinstance(v, str):
            return EnumVehicleTier[v.upper()].value
        else:
            return v

    @field_validator('tier')
    def validate_tier(cls, v):
        if isinstance(v, int):
            return EnumVehicleTier(v)
        else:
            return v

    def __str__(self) -> str:
        return f'{self.name}'
    
    
    def txt_row(self, format: str = '') -> str:
        """export data as single row of text"""
        if format == 'rich':
            return f'({self.tank_id}) {self.name} tier {self.tier} {self.type} {self.nation}'
        else:
            return f'({self.tank_id}) {self.name}'
        
        

def Tank2BSTank(in_obj: Tank) -> Optional[BSTank]:
    """Transform Tank object to BSTank"""
    try:

        tank_type: EnumVehicleTypeInt | None = None
        if in_obj.type is not None:
            tank_type = in_obj.type.as_int
        return BSTank(tank_id=in_obj.tank_id,
                    name=in_obj.name,
                    tier=in_obj.tier,
                    type=tank_type,
                    is_premium=in_obj.is_premium,
                    nation=in_obj.nation,
                    code=in_obj.code,
                    )
    except Exception as err:
        error(f'{err}')
    return None




def BSTank2Tank(in_obj: "BSTank") -> Optional["Tank"]:
    """Transform BSTank object to Tank"""
    try:
        tank_type: EnumVehicleTypeStr | None = None
        if in_obj.type is not None:
            tank_type = in_obj.type.as_str

        return Tank(
            tank_id=in_obj.tank_id,
            name=in_obj.name,
            tier=in_obj.tier,
            type=tank_type,
            is_premium=in_obj.is_premium,
            nation=in_obj.nation,
            code=in_obj.code,
        )
    except Exception as err:
        error(f"{err}")
    return None


# register model transformations
BSTank.register_transformation(Tank, Tank2BSTank)
Tank.register_transformation(BSTank, BSTank2Tank)
