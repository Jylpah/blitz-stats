import logging
from enum import StrEnum
from time import time
import json
from typing import Optional, ClassVar, Any
from math import ceil
from bson.objectid import ObjectId
from pydantic import validator, root_validator, Field, HttpUrl, ValidationError, Extra

from pyutils.utils import epoch_now
from pyutils import (
    JSONExportable,
    CSVExportable,
    TXTExportable,
    TXTImportable,
    TypeExcludeDict,
    I,
    Idx,
    BackendIndexType,
    BackendIndex,
    DESCENDING,
    ASCENDING,
    TEXT,
)


from blitzutils import (
    Account,
    Release,
    AccountInfo,
    ReplaySummary,
    ReplayData,
    ReplayJSON,
    Tank,
    EnumNation,
    EnumVehicleTypeInt,
    EnumVehicleTier,
    EnumVehicleTypeStr,
)

logger = logging.getLogger()
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

    _min_inactivity_days: int = MIN_INACTIVITY_DAYS
    _exclude_defaults = False

    class Config:
        allow_population_by_field_name = True
        allow_mutation = True
        validate_assignment = True

    @classmethod
    def backend_indexes(cls) -> list[list[tuple[str, BackendIndexType]]]:
        """return backend search indexes"""
        indexes: list[list[tuple[str, BackendIndexType]]] = list()
        indexes.append(
            [
                ("disabled", ASCENDING),
                ("inactive", ASCENDING),
                ("region", ASCENDING),
                ("last_battle_time", DESCENDING),
                # ('id', 		ASCENDING),
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
        return cls._min_inactivity_days

    @classmethod
    def set_inactivity_limit(cls, days: int) -> None:
        cls._min_inactivity_days = days

    @classmethod
    def get_update_field(cls, stats_type: str | None) -> str | None:
        UPDATED: str = "updated_"
        try:
            if stats_type is not None:
                return StatsTypes(stats_type).value
        except Exception:
            error(f"Unknown stats_type: {stats_type}")
        return None

    @validator("updated_tank_stats", "updated_player_achievements")
    def check_epoch_ge_zero(cls, v):
        if v is None:
            return None
        elif v >= 0:
            return v
        else:
            raise ValueError("time field must be >= 0")

    @validator("added")
    def set_current_time(cls, v):
        if v is None:
            return epoch_now()
        elif v >= 0:
            return v
        else:
            ValueError("time field must be >= 0")

    @root_validator()
    def set_inactive(cls, values: dict[str, Any]) -> dict[str, Any]:
        lbt: int | None
        if (lbt := values.get("last_battle_time")) is not None:
            inactive: bool = epoch_now() - lbt > cls._min_inactivity_days * 24 * 3600
            values["inactive"] = inactive
        return values

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
            > self._min_inactivity_days * 24 * 3600
        )

    def update_needed(self, stats_type: StatsTypes) -> bool:
        """Check whether the account needs an update"""
        stats_updated: int | None
        if (stats_updated := getattr(self, stats_type.value)) is None:
            stats_updated = 0
        return (epoch_now() - stats_updated) > min(
            MAX_UPDATE_INTERVAL, (stats_updated - self.last_battle_time) / 3
        )

    def update(self, update: AccountInfo) -> bool:
        """Update BSAccount() from WGACcountInfo i.e. from WG API"""
        updated: bool = False
        try:
            updated = super().update(update)
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
    _max_epoch: int = 2**63 - 1  # MAX_INT64 (signed)
    cut_off: int = Field(default=_max_epoch)

    _exclude_defaults = False

    class Config:
        allow_mutation = True
        validate_assignment = True
        allow_population_by_field_name = True

    @validator("cut_off", pre=True)
    def check_cut_off_now(cls, v):
        if v is None:
            return cls._max_epoch
        elif isinstance(v, str) and v == "now":
            return epoch_now()
        else:
            return int(v)

    @validator("cut_off")
    def validate_cut_off(cls, v: int) -> int:
        # ROUND_TO : int = 10*60
        if v >= 0:
            # return ceil(v / ROUND_TO) * ROUND_TO
            return v
        raise ValueError("cut_off has to be >= 0")

    def cut_off_now(self) -> None:
        self.cut_off = epoch_now()

    def txt_row(self, format: str = "") -> str:
        extra: str = ""
        if format == "rich":
            extra = f"\t{self.cut_off}"
        return super().txt_row(format) + extra


class BSBlitzReplay(ReplaySummary):
    id: str | None = Field(default=None, alias="_id")

    _ViewUrlBase: str = "https://replays.wotinspector.com/en/view/"
    _DLurlBase: str = "https://replays.wotinspector.com/en/download/"

    class Config:
        arbitrary_types_allowed = True
        allow_mutation = True
        validate_assignment = True
        allow_population_by_field_name = True

    @property
    def index(self) -> Idx:
        """return backend index"""
        if self.id is not None:
            return self.id
        raise ValueError("id is missing")

    @property
    def view_url(self) -> str:
        if self.id is not None:
            return f"{self._ViewUrlBase}{self.id}"
        raise ValueError("field 'id' is missing")

    @property
    def dl_url(self) -> str:
        if self.id is not None:
            return f"{self._DLurlBase}{self.id}"
        raise ValueError("field 'id' is missing")

    @property
    def indexes(self) -> dict[str, Idx]:
        """return backend indexes"""
        return {"id": self.index}

    @classmethod
    def backend_indexes(cls) -> list[list[tuple[str, BackendIndexType]]]:
        """return backend search indexes"""
        indexes: list[list[tuple[str, BackendIndexType]]] = list()
        indexes.append(
            [
                ("protagonist", ASCENDING),
                ("room_type", ASCENDING),
                ("vehicle_tier", ASCENDING),
                ("battle_start_timestamp", DESCENDING),
            ]
        )
        indexes.append(
            [
                ("room_type", ASCENDING),
                ("vehicle_tier", ASCENDING),
                ("battle_start_timestamp", DESCENDING),
            ]
        )
        return indexes

    @classmethod
    def transform_WoTBlitzReplayData(
        cls, in_obj: ReplayData
    ) -> Optional["BSBlitzReplay"]:
        res: BSBlitzReplay | None = None
        try:
            if (res := BSBlitzReplay.parse_obj(in_obj.summary.dict())) is not None:
                res.id = in_obj.id
                return res
            else:
                error(f"failed to transform object: {in_obj}")
        except ValidationError as err:
            error(f"failed to transform object: {in_obj}: {err}")
        return res

    @classmethod
    def transform_WoTBlitzReplayJSON(
        cls, in_obj: ReplayJSON
    ) -> Optional["BSBlitzReplay"]:
        if (replay_data := ReplayData.transform(in_obj)) is not None:
            return cls.transform_WoTBlitzReplayData(replay_data)
        return None


BSBlitzReplay.register_transformation(
    ReplayData, BSBlitzReplay.transform_WoTBlitzReplayData
)
BSBlitzReplay.register_transformation(
    ReplayJSON, BSBlitzReplay.transform_WoTBlitzReplayJSON
)


# fmt: off
class BSTank(JSONExportable, CSVExportable, TXTExportable):
    tank_id 	: int						= Field(default=..., alias='_id')
    name 		: str | None				= Field(default=None, alias='n')
    nation		: EnumNation | None 		= Field(default=None, alias='c')
    type		: EnumVehicleTypeInt | None	= Field(default=None, alias='v')
    tier		: EnumVehicleTier | None 	= Field(default=None, alias='t')
    is_premium 	: bool 						= Field(default=False, alias='p')
    next_tanks	: list[int] | None			= Field(default=None, alias='s')
    # fmt: on
    _exclude_defaults = False

    class Config:
        allow_mutation = True
        validate_assignment = True
        allow_population_by_field_name = True
        # use_enum_values			= True
        extra = Extra.allow

    @property
    def index(self) -> Idx:
        """return backend index"""
        return self.tank_id

    @property
    def indexes(self) -> dict[str, Idx]:
        """return backend indexes"""
        return {'tank_id': self.index}

    @classmethod
    def backend_indexes(cls) -> list[list[tuple[str, BackendIndexType]]]:
        indexes: list[list[BackendIndex]] = list()
        indexes.append([('tier', ASCENDING),
                        ('type', ASCENDING)
                        ])
        indexes.append([('tier', ASCENDING),
                        ('nation', ASCENDING)
                        ])
        indexes.append([('name', TEXT)
                        ])
        return indexes

    @validator('next_tanks', pre=True)
    def next_tanks2list(cls, v):
        try:
            if v is not None:
                return [int(k) for k in v.keys()]
        except Exception as err:
            error(f"Error validating 'next_tanks': {err}")
        return None

    @validator('tier', pre=True)
    def prevalidate_tier(cls, v: Any) -> Any:
        if isinstance(v, str):
            return EnumVehicleTier[v.upper()].value
        else:
            return v

    @validator('tier')
    def validate_tier(cls, v):
        if isinstance(v, int):
            return EnumVehicleTier(v)
        else:
            return v

    def __str__(self) -> str:
        return f'{self.name}'

    @classmethod
    def transform_WGTank(cls, in_obj: 'Tank') -> Optional['BSTank']:
        """Transform Tank object to BSTank"""
        try:
            # debug(f'type={type(in_obj)}')
            # debug(f'in_obj={in_obj}')
            tank_type: EnumVehicleTypeInt | None = None
            if in_obj.type is not None:
                tank_type = in_obj.type.as_int
            return BSTank(tank_id=in_obj.tank_id,
                        name=in_obj.name,
                        tier=in_obj.tier,
                        type=tank_type,
                        is_premium=in_obj.is_premium,
                        nation=in_obj.nation,
                        )
        except Exception as err:
            error(f'{err}')
        return None

    def csv_headers(self) -> list[str]:
        """Provide CSV headers as list"""
        return list(BSTank.__fields__.keys())

    def txt_row(self, format: str = '') -> str:
        """export data as single row of text"""
        if format == 'rich':
            return f'({self.tank_id}) {self.name} tier {self.tier} {self.type} {self.nation}'
        else:
            return f'({self.tank_id}) {self.name}'


def WGTank2Tank(in_obj: "BSTank") -> Optional["Tank"]:
    """Transform BSTank object to Tank"""
    try:
        # debug(f'type={type(in_obj)}')
        # debug(f'in_obj={in_obj}')
        tank_type: EnumVehicleTypeStr | None = None
        if in_obj.type is not None:
            tank_type = in_obj.type.as_str
        # debug(f'trying to transform tank:{in_obj.dict()}')
        return Tank(
            tank_id=in_obj.tank_id,
            name=in_obj.name,
            tier=in_obj.tier,
            type=tank_type,
            is_premium=in_obj.is_premium,
            nation=in_obj.nation,
        )
    except Exception as err:
        error(f"{err}")
    return None


# register model transformations
BSTank.register_transformation(Tank, BSTank.transform_WGTank)
Tank.register_transformation(BSTank, WGTank2Tank)
