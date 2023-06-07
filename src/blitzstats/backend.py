import queue
import logging
from configparser import ConfigParser
from argparse import Namespace, ArgumentParser
from abc import ABC, abstractmethod
from os.path import isfile
from typing import Optional, Any, Sequence, AsyncGenerator, TypeVar, cast
from datetime import datetime
from enum import StrEnum, IntEnum
from asyncio import Queue, CancelledError
from pydantic import Field

from blitzutils.region import Region
from blitzutils.wg_api import WGTankStat, WGPlayerAchievementsMaxSeries, WGPlayerAchievementsMain, WoTBlitzTankString

from blitzutils.tank import WGTank, EnumVehicleTier, EnumNation, EnumVehicleTypeStr
from blitzutils.replay import WoTBlitzReplayJSON, WoTBlitzReplayData
from blitzutils.region import Region

from pyutils import EventCounter, IterableQueue, JSONExportable, QueueDone
from pyutils.utils import epoch_now, is_alphanum

from .models import BSAccount, BSBlitzRelease, BSBlitzReplay, StatsTypes, Tank


# Setup logging
logger = logging.getLogger()
error = logger.error
message = logger.warning
verbose = logger.info
debug = logger.debug

# Constants
MAX_UPDATE_INTERVAL: int = 6 * 30 * 24 * 60 * 60  # 4 months
INACTIVE_THRESHOLD: int = 2 * 30 * 24 * 60 * 60  # 2 months
WG_ACCOUNT_ID_MAX: int = int(31e8)
MAX_RETRIES: int = 3
MIN_UPDATE_INTERVAL: int = 3  # days
ACCOUNTS_Q_MAX: int = 5000
TANK_STATS_BATCH: int = 1000

A = TypeVar("A")

##############################################
#
## Utils
#
##############################################


def get_type(name: str) -> type[object] | None:
    if name is None:
        raise ValueError("No type defined")

    type_class: type[object]
    try:
        if is_alphanum(name):
            type_class = globals()[name]
        else:
            raise ValueError(f"model {name}() contains illegal characters")
        return type_class
    except Exception as err:
        error(f"Could not find class {name}(): {err}")
    return None


T = TypeVar("T", bound=object)


def get_sub_type(name: str, parent: type[T]) -> Optional[type[T]]:
    if (model := get_type(name)) is not None:
        if issubclass(model, parent):
            return model
    return None


##############################################
#
## OptAccountsInactive()
#
##############################################


class OptAccountsInactive(StrEnum):
    auto = "auto"
    no = "no"
    yes = "yes"
    both = "both"

    @classmethod
    def default(cls) -> "OptAccountsInactive":
        return cls.auto


class OptAccountsDistributed:
    def __init__(self, mod: int, div: int):
        assert type(mod) is int and mod >= 0, "Modulus has to be integer >= 0"
        assert type(div) is int and div > 0, "Divisor has to be positive integer"
        self.div: int = div
        self.mod: int = mod % div

    @classmethod
    def parse(cls, input: str) -> Optional["OptAccountsDistributed"]:
        try:
            if input is None:
                return None
            res: list[str] = input.split(":")
            if len(res) != 2:
                raise ValueError(f'Input ({input} does not match format "I:N")')
            mod: int = int(res[0])
            div: int = int(res[1])
            return OptAccountsDistributed(mod, div)
        except Exception as err:
            error(f"{err}")
        return None

    def match(self, value: int) -> bool:
        assert type(value) is int, "value has to be integere"
        return value % self.div == self.mod


class BSTableType(StrEnum):
    Accounts = "Accounts"
    Tankopedia = "Tankopedia"
    TankStrings = "TankStrings"
    Releases = "Releases"
    Replays = "Replays"
    TankStats = "TankStats"
    PlayerAchievements = "PlayerAchievements"
    ErrorLog = "ErrorLog"
    AccountLog = "AccountLog"


class ErrorLogType(IntEnum):
    OK = 0
    Info = 1
    Warning = 2
    Error = 3
    Critical = 4

    ValidationError = 10
    ValueError = 11
    NotFoundError = 12

    Duplicate = 20


# ----------------------------------------
# ErrorLog - entry for error log entries
# ----------------------------------------
# WORK IN PROGRESS ##############################
class ErrorLog(JSONExportable, ABC):
    """Class for error log entries.
    * Should it be abstract at all?
    * How to deal with different backends with different indexes?
    * Should 'doc_id' field use JSONExportable.indexes field?"""

    table: str = Field(alias="t")
    doc_id: Any | None = Field(default=None, alias="did")
    date: datetime = Field(default=datetime.utcnow(), alias="d")
    msg: str | None = Field(default=None, alias="e")
    type: ErrorLogType = Field(default=ErrorLogType.Error, alias="t")

    class Config:
        arbitrary_types_allowed = True
        allow_mutation = True
        validate_assignment = True
        allow_population_by_field_name = True
        # json_encoders = { ObjectId: str }


async def batch_gen(aget: AsyncGenerator[T, None], batch: int = 100) -> AsyncGenerator[list[T], None]:
    res: list[T] = list()
    async for item in aget:
        res.append(item)
        if len(res) >= batch:
            yield res
            res = list()


class Backend(ABC):
    """Abstract class for a backend (mongo, postgres, files)"""

    driver: str = "Backend"
    _cache_valid: int = MIN_UPDATE_INTERVAL
    _backends: dict[str, type["Backend"]] = dict()

    def __init__(
        self,
        config: ConfigParser | None = None,
        db_config: dict[str, Any] | None = None,
        database: str | None = None,
        table_config: dict[BSTableType, str] | None = None,
        model_config: dict[BSTableType, type[JSONExportable]] | None = None,
        **kwargs,
    ):
        """Init MongoDB backend from config file and CLI args
        CLI arguments overide settings in the config file"""

        self._database: str = "BlitzStats"
        self._db_config: dict[str, Any]
        self._T: dict[BSTableType, str] = dict()
        self._Tr: dict[str, BSTableType] = dict()
        self._M: dict[BSTableType, type[JSONExportable]] = dict()

        # default tables
        self.set_table(BSTableType.Accounts, "Accounts")
        self.set_table(BSTableType.Tankopedia, "Tankopedia")
        self.set_table(BSTableType.TankStrings, "TankStrings")
        self.set_table(BSTableType.Releases, "Releases")
        self.set_table(BSTableType.Replays, "Replays")
        self.set_table(BSTableType.AccountLog, "AccountLog")
        self.set_table(BSTableType.ErrorLog, "ErrorLog")
        self.set_table(BSTableType.TankStats, "TankStats")
        self.set_table(BSTableType.PlayerAchievements, "PlayerAchievements")

        # set default models
        self.set_model(BSTableType.Accounts, BSAccount)
        self.set_model(BSTableType.Tankopedia, Tank)
        self.set_model(BSTableType.TankStrings, WoTBlitzTankString)
        self.set_model(BSTableType.Releases, BSBlitzRelease)
        self.set_model(BSTableType.Replays, WoTBlitzReplayJSON)
        self.set_model(BSTableType.AccountLog, ErrorLog)
        self.set_model(BSTableType.ErrorLog, ErrorLog)
        self.set_model(BSTableType.TankStats, WGTankStat)
        self.set_model(BSTableType.PlayerAchievements, WGPlayerAchievementsMaxSeries)

        if config is not None and "BACKEND" in config.sections():
            configBackend = config["BACKEND"]
            self._cache_valid = configBackend.getint("cache_valid", MIN_UPDATE_INTERVAL)
            self.set_table(BSTableType.Accounts, configBackend.get("t_accounts"))
            self.set_table(BSTableType.Tankopedia, configBackend.get("t_tankopedia"))
            self.set_table(BSTableType.TankStrings, configBackend.get("t_tank_strings"))
            self.set_table(BSTableType.Releases, configBackend.get("t_releases"))
            self.set_table(BSTableType.Replays, configBackend.get("t_replays"))
            self.set_table(BSTableType.TankStats, configBackend.get("t_tank_stats"))
            self.set_table(BSTableType.PlayerAchievements, configBackend.get("t_player_achievements"))
            self.set_table(BSTableType.AccountLog, configBackend.get("t_account_log"))
            self.set_table(BSTableType.ErrorLog, configBackend.get("t_error_log"))

            self.set_model(BSTableType.Accounts, configBackend.get("m_accounts"))
            self.set_model(BSTableType.Tankopedia, configBackend.get("m_tankopedia"))
            self.set_model(BSTableType.TankStrings, configBackend.get("m_tank_strings"))
            self.set_model(BSTableType.Releases, configBackend.get("m_releases"))
            self.set_model(BSTableType.Replays, configBackend.get("m_replays"))
            self.set_model(BSTableType.TankStats, configBackend.get("m_tank_stats"))
            self.set_model(BSTableType.PlayerAchievements, configBackend.get("m_player_achievements"))
            self.set_model(BSTableType.AccountLog, configBackend.get("m_account_log"))
            self.set_model(BSTableType.ErrorLog, configBackend.get("m_error_log"))

    @abstractmethod
    def debug(self) -> None:
        """Print out debug info"""
        raise NotImplementedError

    def config_tables(self, table_config: dict[BSTableType, str] | None = None) -> None:
        try:
            if table_config is not None:
                for table_type, table in table_config.items():
                    self.set_table(table_type, table)
        except Exception as err:
            error(f"{err}")
        return None

    def config_models(self, model_config: dict[BSTableType, type[JSONExportable]] | None = None) -> None:
        try:
            if model_config is not None:
                for table_type, model in model_config.items():
                    self.set_model(table_type, model)
        except Exception as err:
            error(f"{err}")
        return None

    @classmethod
    def register(cls, driver: str, backend: type["Backend"]) -> bool:
        try:
            debug(f"Registering backend: {driver}")
            if driver not in cls._backends:
                cls._backends[driver] = backend
                return True
            else:
                error(f"Backend {driver} has already been registered")
        except Exception as err:
            error(f"Error registering backend {driver}: {err}")
        return False

    @classmethod
    def get_registered(cls) -> list[type["Backend"]]:
        return list(cls._backends.values())

    @classmethod
    def get(cls, backend: str) -> Optional[type["Backend"]]:
        try:
            return cls._backends[backend]
        except:
            return None

    @classmethod
    def create(
        cls, driver: str, config: ConfigParser | None = None, copy_from: Optional["Backend"] = None, **kwargs
    ) -> Optional["Backend"]:
        try:
            debug("starting")
            if copy_from is not None and copy_from.driver == driver:
                return copy_from.copy(**kwargs)
            elif driver in cls._backends:
                return cls._backends[driver](config=config, **kwargs)
            else:
                assert False, f"Backend not implemented: {driver}"
        except Exception as err:
            error(f"Error creating backend {driver}: {err}")
        return None

    @classmethod
    def create_import_backend(
        cls,
        driver: str,
        args: Namespace,
        import_type: BSTableType,
        copy_from: Optional["Backend"] = None,
        config_file: str | None = None,
    ) -> Optional["Backend"]:
        try:
            import_model: type[JSONExportable] | None

            config: ConfigParser | None = None
            if config_file is not None and isfile(config_file):
                debug(f"Reading import config from {config_file}")
                config = ConfigParser()
                config.read(config_file)

            kwargs: dict[str, Any] = Backend.read_args(args, driver, importdb=True)
            if (import_db := Backend.create(driver, config=config, copy_from=copy_from, **kwargs)) is None:
                raise ValueError(f"Could not init {driver} to import releases from")

            if args.import_table is not None:
                import_db.set_table(import_type, args.import_table)
            elif copy_from is not None:
                if copy_from == import_db and copy_from.get_table(import_type) == import_db.get_table(import_type):
                    raise ValueError("Cannot import from itself")

            if (import_model := get_sub_type(args.import_model, JSONExportable)) is None:
                assert False, "--import-model not found or is not a subclass of JSONExportable"
            import_db.set_model(import_type, import_model)

            return import_db
        except Exception as err:
            error(f"Error creating import backend {driver}: {err}")
        return None

    @classmethod
    def add_args_import(cls, parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
        debug("starting")
        parser.add_argument(
            "--import-config",
            metavar="CONFIG",
            type=str,
            default=None,
            help="Config file for backend to import from. \
								Default is to use existing backend",
        )
        return True

    @classmethod
    def read_args(cls, args: Namespace, driver: str, importdb: bool = False) -> dict[str, Any]:
        """Read Argparse args for creating a Backend()"""
        debug("starting")
        if driver in cls._backends:
            return cls._backends[driver].read_args(args, driver=driver, importdb=importdb)
        else:
            raise ValueError(f"Backend not implemented: {driver}")

    @classmethod
    def read_args_helper(
        cls, args: Namespace, params: Sequence[str | tuple[str, str]], importdb: bool = False
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = dict()
        arg_dict: dict[str, Any] = vars(args)
        prefix: str = ""
        if importdb:
            prefix = "import_"
        for param in params:
            t: str
            s: str
            try:
                if isinstance(param, tuple):
                    s = param[0]
                    t = param[1]
                elif isinstance(param, str):
                    s = param
                    t = param
                else:
                    raise TypeError(f"wrong param given: {type(param)}")
                s = prefix + s
                kwargs[t] = arg_dict[s]
            except KeyError as err:
                error(f"{err}")
        return kwargs

    @classmethod
    def list_available(cls) -> list[str]:
        return ["mongodb"]

    @property
    def cache_valid(self) -> int:
        return self._cache_valid

    def __eq__(self, __o: object) -> bool:
        """Default __eq__() function"""
        return (
            __o is not None and isinstance(__o, Backend) and type(__o) is type(self) and self.database == __o.database
        )

    @abstractmethod
    async def init(self, tables: list[str] = [tt.value for tt in BSTableType]) -> bool:  # type: ignore
        """Init backend and indexes"""
        raise NotImplementedError

    @abstractmethod
    def copy(self, **kwargs) -> Optional["Backend"]:
        """Create a copy of backend"""
        raise NotImplementedError

    @abstractmethod
    async def test(self) -> bool:
        """Test connection to backend"""
        raise NotImplementedError

    @property
    @abstractmethod
    def backend(self) -> str:
        raise NotImplementedError

    def table_uri(self, table_type: BSTableType, full: bool = False) -> str:
        """Return full table URI. Override in subclass if needed"""
        if full:
            return f"{self.backend}.{self.get_table(table_type)}"
        else:
            return f"{self.driver}://{self.database}.{self.get_table(table_type)}"

    @property
    def database(self) -> str:
        return self._database

    @property
    def db_config(self) -> dict[str, Any]:
        return self._db_config

    @property
    def config(self) -> dict[str, Any]:
        return {
            "driver": self.driver,
            "config": None,
            "db_config": self.db_config,
            "database": self.database,
            "table_config": self.table_config,
            "model_config": self.model_config,
        }

    @property
    def table_config(self) -> dict[BSTableType, str]:
        return self._T

    @property
    def model_config(self) -> dict[BSTableType, type[JSONExportable]]:
        return self._M

    def set_database(self, database: str | None) -> None:
        """Set database"""
        if database is None:
            pass
        else:
            assert is_alphanum(database), f"Illegal characters in the table name: {database}"
            self._database = database
        return None

    def get_table(self, table_type: BSTableType) -> str:
        """Get database table/collection"""
        return self._T[table_type]

    def set_table(self, table_type: BSTableType, name: str | None) -> None:
        """Set database table/collection"""
        if name is None:
            return None
        assert len(name) > 0, "table name cannot be zero-sized"
        assert is_alphanum(name), f"Illegal characters in the table name: {name}"
        self._T[table_type] = name
        self._Tr[name] = table_type

    def get_model(self, table: BSTableType | str) -> type[JSONExportable]:
        """Get collection model"""
        if isinstance(table, str):
            return self._M[self._Tr[table]]
        else:
            return self._M[table]

    def set_model(self, table: BSTableType | str, model: type[JSONExportable] | str | None) -> None:
        """Set collection model"""
        debug(f"table: {table}, model: {model}")
        table_type: BSTableType
        model_class: type[JSONExportable]
        if model is None:
            return None
        if isinstance(table, str):
            table_type = self._Tr[table]
        else:
            table_type = table
        if isinstance(model, str):
            if (model_type := get_sub_type(model, JSONExportable)) is None:
                assert False, f"Could not set model {model}() for {table_type.value}"
            else:
                model_class = model_type
        else:
            model_class = model
        self._M[table_type] = model_class

    @property
    def table_accounts(self) -> str:
        return self.get_table(BSTableType.Accounts)

    @property
    def table_tankopedia(self) -> str:
        return self.get_table(BSTableType.Tankopedia)

    @property
    def table_tank_strings(self) -> str:
        return self.get_table(BSTableType.TankStrings)

    @property
    def table_releases(self) -> str:
        return self.get_table(BSTableType.Releases)

    @property
    def table_replays(self) -> str:
        return self.get_table(BSTableType.Replays)

    @property
    def table_tank_stats(self) -> str:
        return self.get_table(BSTableType.TankStats)

    @property
    def table_player_achievements(self) -> str:
        return self.get_table(BSTableType.PlayerAchievements)

    @property
    def table_account_log(self) -> str:
        return self.get_table(BSTableType.AccountLog)

    @property
    def table_error_log(self) -> str:
        return self.get_table(BSTableType.ErrorLog)

    @property
    def model_accounts(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.Accounts)

    @property
    def model_tankopedia(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.Tankopedia)

    @property
    def model_tank_strings(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.TankStrings)

    @property
    def model_releases(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.Releases)

    @property
    def model_replays(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.Replays)

    @property
    def model_tank_stats(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.TankStats)

    @property
    def model_player_achievements(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.PlayerAchievements)

    @property
    def model_account_log(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.AccountLog)

    @property
    def model_error_log(self) -> type[JSONExportable]:
        return self.get_model(BSTableType.ErrorLog)

    # ----------------------------------------
    # Objects
    # ----------------------------------------

    @abstractmethod
    async def obj_export(
        self, table_type: BSTableType, pipeline: list[dict[str, Any]] = list(), sample: float = 0
    ) -> AsyncGenerator[Any, None]:
        """Export raw object from backend"""
        raise NotImplementedError
        yield Any

    @abstractmethod
    async def objs_export(
        self, table_type: BSTableType, pipeline: list[dict[str, Any]] = list(), sample: float = 0, batch: int = 0
    ) -> AsyncGenerator[list[Any], None]:
        """Export raw objects from backend"""
        raise NotImplementedError
        yield [Any]

    # ----------------------------------------
    # accounts
    # ----------------------------------------

    @abstractmethod
    async def account_insert(self, account: BSAccount, force: bool = False) -> bool:
        """Store account to the backend. Returns False
        if the account was not added"""
        raise NotImplementedError

    @abstractmethod
    async def account_get(self, account_id: int) -> BSAccount | None:
        """Get account from backend"""
        raise NotImplementedError

    @abstractmethod
    async def account_update(
        self, account: BSAccount, update: dict[str, Any] | None = None, fields: list[str] | None = None
    ) -> bool:
        """Update an account in the backend. Returns False
        if the account was not updated"""
        raise NotImplementedError

    @abstractmethod
    async def account_delete(self, account_id: int) -> bool:
        """Delete account from the backend. Returns False
        if the account was not found/deleted"""
        raise NotImplementedError

    @abstractmethod
    async def accounts_get(
        self,
        stats_type: StatsTypes | None = None,
        regions: set[Region] = Region.API_regions(),
        inactive: OptAccountsInactive = OptAccountsInactive.default(),
        disabled: bool | None = False,
        active_since: int = 0,
        inactive_since: int = 0,
        dist: OptAccountsDistributed | None = None,
        sample: float = 0,
        cache_valid: float = 0,
    ) -> AsyncGenerator[BSAccount, None]:
        """Get accounts from backend"""
        raise NotImplementedError
        yield BSAccount(id=-1)

    @abstractmethod
    async def accounts_count(
        self,
        stats_type: StatsTypes | None = None,
        regions: set[Region] = Region.API_regions(),
        inactive: OptAccountsInactive = OptAccountsInactive.default(),
        disabled: bool | None = False,
        active_since: int = 0,
        inactive_since: int = 0,
        dist: OptAccountsDistributed | None = None,
        sample: float = 0,
        cache_valid: float = 0,
    ) -> int:
        """Get number of accounts from backend"""
        raise NotImplementedError

    async def accounts_get_worker(self, accountQ: Queue[BSAccount], **getargs) -> EventCounter:
        debug("starting")
        stats: EventCounter = EventCounter("accounts")
        try:
            async for account in self.accounts_get(**getargs):
                await accountQ.put(account)
                stats.log("queued")
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    @abstractmethod
    async def accounts_insert(self, accounts: Sequence[BSAccount]) -> tuple[int, int]:
        """Store accounts to the backend. Returns number of accounts inserted and not inserted"""
        raise NotImplementedError

    async def accounts_insert_worker(self, accountQ: Queue[BSAccount], force: bool = False) -> EventCounter:
        """insert/replace accounts. force=None: insert, force=True/False: upsert=force"""
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("accounts insert")
        try:
            while True:
                account = await accountQ.get()
                try:
                    debug(f"Trying to insert account_id={account.id} into {self.backend}.{self.table_accounts}")
                    await self.account_insert(account, force=force)
                    if force:
                        stats.log("added/updated")
                    else:
                        stats.log("added")

                except Exception as err:
                    debug(f"Error: {err}")
                    stats.log("not added/updated")
                finally:
                    accountQ.task_done()
        except QueueDone:
            # IterableQueue() support
            pass
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    @abstractmethod
    async def accounts_export(self, sample: float = 0) -> AsyncGenerator[BSAccount, None]:
        """import accounts"""
        raise NotImplementedError
        yield BSAccount()

    @abstractmethod
    async def accounts_latest(self, regions: set[Region]) -> dict[Region, BSAccount]:
        """Return the latest accounts (=highest account_id) per region"""
        raise NotImplementedError

    # ----------------------------------------
    # Releases
    # ----------------------------------------

    @abstractmethod
    async def release_insert(self, release: BSBlitzRelease, force: bool = False) -> bool:
        """Insert new release to the backend"""
        raise NotImplementedError

    @abstractmethod
    async def release_get(self, release: str) -> BSBlitzRelease | None:
        raise NotImplementedError

    @abstractmethod
    async def release_update(
        self, release: BSBlitzRelease, update: dict[str, Any] | None = None, fields: list[str] | None = None
    ) -> bool:
        """Update an release in the backend. Returns False
        if the release was not updated"""
        raise NotImplementedError

    @abstractmethod
    async def release_delete(self, release: str) -> bool:
        """Delete a release from backend"""
        raise NotImplementedError

    @abstractmethod
    async def release_get_latest(self) -> BSBlitzRelease | None:
        """Get the latest release in the backend"""
        raise NotImplementedError

    @abstractmethod
    async def release_get_current(self) -> BSBlitzRelease | None:
        """Get the latest release in the backend"""
        raise NotImplementedError

    @abstractmethod
    async def release_get_next(self, release: BSBlitzRelease) -> BSBlitzRelease | None:
        """Get next release"""
        raise NotImplementedError

    @abstractmethod
    async def release_get_previous(self, release: BSBlitzRelease) -> BSBlitzRelease | None:
        """Get previous release"""
        raise NotImplementedError

    @abstractmethod
    async def releases_get(
        self, release_match: str | None = None, since: int = 0, first: BSBlitzRelease | None = None
    ) -> AsyncGenerator[BSBlitzRelease, None]:
        raise NotImplementedError
        yield BSBlitzRelease()

    @abstractmethod
    async def releases_export(self, sample: float = 0) -> AsyncGenerator[BSBlitzRelease, None]:
        """Export releases"""
        raise NotImplementedError
        yield BSBlitzRelease()

    async def releases_insert_worker(self, releaseQ: Queue[BSBlitzRelease], force: bool = False) -> EventCounter:
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("releases insert")
        try:
            while True:
                release = await releaseQ.get()
                try:
                    debug(f"Trying to insert release={release} into {self.backend}.{self.table_releases}")
                    if await self.release_insert(release, force=force):
                        stats.log("releases added")
                    else:
                        stats.log("releases not added")
                except Exception as err:
                    debug(f"Error: {err}")
                    stats.log("errors")
                finally:
                    releaseQ.task_done()
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    # ----------------------------------------
    # replays
    # ----------------------------------------

    @abstractmethod
    async def replay_insert(self, replay: JSONExportable) -> bool:
        # async def replay_insert(self, replay: WoTBlitzReplayJSON) -> bool:
        """Store replay into backend"""
        raise NotImplementedError

    @abstractmethod
    async def replay_get(self, replay_id: str) -> BSBlitzReplay | None:
        """Get a replay from backend based on replayID"""
        raise NotImplementedError

    @abstractmethod
    async def replay_delete(self, replay_id: str) -> bool:
        """Delete replay from backend based on replayID"""
        raise NotImplementedError

    # replay fields that can be searched: protagonist, battle_start_timestamp, account_id, vehicle_tier
    @abstractmethod
    async def replays_get(self, since: int = 0, **summary_fields) -> AsyncGenerator[BSBlitzReplay, None]:
        """Get replays from backed"""
        raise NotImplementedError
        yield BSBlitzReplay()

    @abstractmethod
    async def replays_count(self, since: int = 0, sample: float = 0, **summary_fields) -> int:
        """Count replays in backed"""
        raise NotImplementedError

    @abstractmethod
    async def replays_insert(self, replays: Sequence[JSONExportable]) -> tuple[int, int]:
        """Store replays to the backend. Returns number of replays inserted and not inserted"""
        raise NotImplementedError

    async def replays_insert_worker(self, replayQ: Queue[JSONExportable], force: bool = False) -> EventCounter:
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("replays insert")
        try:
            while True:
                replay = await replayQ.get()
                try:
                    debug(f"Insertting replay={replay.index} into {self.table_uri(BSTableType.Releases)}")
                    if await self.replay_insert(replay):
                        stats.log("added")
                    else:
                        stats.log("not added")
                except Exception as err:
                    debug(f"Error: {err}")
                    stats.log("errors")
                finally:
                    replayQ.task_done()
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    async def replays_export(self, sample: float = 0) -> AsyncGenerator[BSBlitzReplay, None]:
        """Export replays from Mongo DB"""
        raise NotImplementedError
        yield BSBlitzReplay()

    # ----------------------------------------
    # tank stats
    # ----------------------------------------

    @abstractmethod
    async def tank_stat_insert(self, tank_stat: WGTankStat, force: bool = False) -> bool:
        """Store tank stats to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stat_get(self, account_id: int, tank_id: int, last_battle_time: int) -> WGTankStat | None:
        """Store tank stats to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stat_update(
        self, tank_stat: WGTankStat, update: dict[str, Any] | None = None, fields: list[str] | None = None
    ) -> bool:
        """Update an tank stat in the backend. Returns False
        if the tank stat was not updated"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stat_delete(self, account_id: int, tank_id: int, last_battle_time: int) -> bool:
        """Delete a tank stat from the backend. Returns True if successful"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stats_insert(self, tank_stats: Sequence[WGTankStat], force: bool = False) -> tuple[int, int]:
        """Store tank stats to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stats_get(
        self,
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        accounts: Sequence[BSAccount] | None = None,
        tanks: Sequence[Tank] | None = None,
        missing: str | None = None,
        since: int = 0,
        sample: float = 0,
    ) -> AsyncGenerator[WGTankStat, None]:
        """Return tank stats from the backend"""
        raise NotImplementedError
        yield WGTankStat()

    @abstractmethod
    async def tank_stats_export_career(
        self,
        account: BSAccount,
        release: BSBlitzRelease,
    ) -> AsyncGenerator[list[WGTankStat], None]:
        """Return tank stats from the backend"""
        raise NotImplementedError
        yield list()

    @abstractmethod
    async def tank_stats_count(
        self,
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        accounts: Sequence[BSAccount] | None = None,
        tanks: Sequence[Tank] | None = None,
        since: int = 0,
        sample: float = 0,
    ) -> int:
        """Get number of tank-stats from backend"""
        raise NotImplementedError

    @abstractmethod
    async def tank_stat_export(self, sample: float = 0) -> AsyncGenerator[WGTankStat, None]:
        """Export tank stats from Mongo DB"""
        raise NotImplementedError
        yield WGTankStat()

    @abstractmethod
    async def tank_stats_export(self, sample: float = 0, batch: int = 0) -> AsyncGenerator[list[WGTankStat], None]:
        """Export tank stats from Mongo DB"""
        raise NotImplementedError
        yield [WGTankStat()]

    @abstractmethod
    async def tank_stats_duplicates(
        self, tank: Tank, release: BSBlitzRelease, regions: set[Region] = Region.API_regions(), sample: int = 0
    ) -> AsyncGenerator[WGTankStat, None]:
        """Find duplicate tank stats from the backend"""
        raise NotImplementedError
        yield WGTankStat()

    @abstractmethod
    async def tank_stats_unique(
        self,
        field: str,
        field_type: type[A],
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        account: BSAccount | None = None,
        tank: Tank | None = None,
    ) -> AsyncGenerator[A, None]:
        """Return unique values of field"""
        raise NotImplementedError
        yield

    @abstractmethod
    async def tank_stats_unique_count(
        self,
        field: str,
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        account: BSAccount | None = None,
        tank: Tank | None = None,
    ) -> int:
        """Return count of unique values of field. **args see tank_stats_unique()"""
        raise NotImplementedError

    async def tank_stats_get_worker(self, tank_statsQ: Queue[WGTankStat], **getargs) -> EventCounter:
        debug("starting")
        stats: EventCounter = EventCounter("tank stats")
        try:
            if type(tank_statsQ) is IterableQueue:
                debug("tank_stats_get_worker(): producer added")
                await tank_statsQ.add_producer()

            async for ts in self.tank_stats_get(**getargs):
                await tank_statsQ.put(ts)
                stats.log("queued")

            if type(tank_statsQ) is IterableQueue:
                await tank_statsQ.finish()
                debug("tank_stats_get_worker(): finished")

        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    async def tank_stats_insert_worker(
        self, tank_statsQ: Queue[list[WGTankStat]], force: bool = False
    ) -> EventCounter:
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("tank-stats insert")
        try:
            added: int
            not_added: int
            read: int
            while True:
                added = 0
                not_added = 0
                tank_stats = await tank_statsQ.get()
                read = len(tank_stats)
                stats.log("read", read)
                try:
                    debug(f"Trying to insert {read} tank stats into {self.backend}.{self.table_tank_stats}")
                    added, not_added = await self.tank_stats_insert(tank_stats, force=force)
                    stats.log("added", added)
                    stats.log("not added", not_added)
                except Exception as err:
                    debug(f"Error: {err}")
                    stats.log("errors", read)
                finally:
                    tank_statsQ.task_done()
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    # ----------------------------------------
    # player achievements
    # ----------------------------------------

    @abstractmethod
    async def player_achievement_insert(
        self, player_achievement: WGPlayerAchievementsMaxSeries, force: bool = False
    ) -> bool:
        """Store player achievements to the backend.
        force=True will overwrite existing item"""
        raise NotImplementedError

    @abstractmethod
    async def player_achievement_get(self, account: BSAccount, added: int) -> WGPlayerAchievementsMaxSeries | None:
        """Store player achievements to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def player_achievement_delete(self, account: BSAccount, added: int) -> bool:
        """Store player achievements to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def player_achievements_insert(
        self, player_achievements: Sequence[WGPlayerAchievementsMaxSeries]
    ) -> tuple[int, int]:
        """Store player achievements to the backend. Returns number of stats inserted and not inserted"""
        raise NotImplementedError

    @abstractmethod
    async def player_achievements_get(
        self,
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        accounts: Sequence[BSAccount] | None = None,
        since: int = 0,
        sample: float = 0,
    ) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
        """Return player achievements from the backend"""
        raise NotImplementedError
        yield WGPlayerAchievementsMaxSeries()

    @abstractmethod
    async def player_achievements_count(
        self,
        release: BSBlitzRelease | None = None,
        regions: set[Region] = Region.API_regions(),
        accounts: Sequence[BSAccount] | None = None,
        sample: float = 0,
    ) -> int:
        """Get number of player achievements from backend"""
        raise NotImplementedError

    @abstractmethod
    async def player_achievement_export(
        self,
        sample: float = 0,
    ) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
        """Export player achievements from Mongo DB"""
        raise NotImplementedError
        yield WGPlayerAchievementsMaxSeries()

    @abstractmethod
    async def player_achievements_export(
        self,
        sample: float = 0,
        batch: int = 0,
    ) -> AsyncGenerator[list[WGPlayerAchievementsMaxSeries], None]:
        """Export player achievements in a batch from Mongo DB"""
        raise NotImplementedError
        yield [WGPlayerAchievementsMaxSeries()]

    @abstractmethod
    async def player_achievements_duplicates(
        self, release: BSBlitzRelease, regions: set[Region] = Region.API_regions(), sample: int = 0
    ) -> AsyncGenerator[WGPlayerAchievementsMaxSeries, None]:
        """Find duplicate player achievements from the backend"""
        raise NotImplementedError
        yield WGPlayerAchievementsMaxSeries()

    async def player_achievements_insert_worker(
        self, player_achievementsQ: Queue[list[WGPlayerAchievementsMaxSeries]], force: bool = False
    ) -> EventCounter:
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("player-achievements insert")
        try:
            added: int
            not_added: int
            read: int
            while True:
                added = 0
                not_added = 0
                player_achievements = await player_achievementsQ.get()
                read = len(player_achievements)
                try:
                    if force:
                        debug(
                            f"Trying to insert {read} player achievements into {self.backend}.{self.table_player_achievements}"
                        )
                        for pa in player_achievements:
                            if await self.player_achievement_insert(pa, force=True):
                                stats.log("stats added/updated")
                            else:
                                stats.log("stats not updated")
                    else:
                        debug(
                            f"Trying to insert {read} player achievements into {self.backend}.{self.table_player_achievements}"
                        )
                        added, not_added = await self.player_achievements_insert(player_achievements)
                        stats.log("accounts added", added)
                    stats.log("accounts not added", not_added)
                except Exception as err:
                    error(f"Unknown error: {err}")
                    stats.log("errors", read)
                finally:
                    player_achievementsQ.task_done()
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    # ----------------------------------------
    # ErrorLog
    # ----------------------------------------

    @abstractmethod
    async def error_log(self, error: ErrorLog) -> bool:
        """Log an error into the backend's ErrorLog"""
        raise NotImplementedError

    @abstractmethod
    async def errors_get(
        self, table_type: BSTableType | None = None, doc_id: Any | None = None, after: datetime | None = None
    ) -> AsyncGenerator[ErrorLog, None]:
        """Return errors from backend ErrorLog"""
        raise NotImplementedError
        yield ErrorLog(table="foo", error="bar")

    @abstractmethod
    async def errors_clear(
        self, table_type: BSTableType, doc_id: Any | None = None, after: datetime | None = None
    ) -> int:
        """Clear errors from backend ErrorLog"""
        raise NotImplementedError

    # ----------------------------------------
    # Tankopedia
    # ----------------------------------------

    @abstractmethod
    async def tankopedia_insert(self, tank: Tank, force: bool = True) -> bool:
        """ "insert tank into Tankopedia"""
        raise NotImplementedError

    @abstractmethod
    async def tankopedia_get(self, tank_id: int) -> Tank | None:
        raise NotImplementedError

    @abstractmethod
    async def tankopedia_get_many(
        self,
        tanks: list[Tank] | None = None,
        tier: EnumVehicleTier | None = None,
        tank_type: EnumVehicleTypeStr | None = None,
        nation: EnumNation | None = None,
        is_premium: bool | None = None,
    ) -> AsyncGenerator[Tank, None]:
        raise NotImplementedError
        yield Tank()

    @abstractmethod
    async def tankopedia_count(
        self,
        tanks: list[Tank] | None = None,
        tier: EnumVehicleTier | None = None,
        tank_type: EnumVehicleTypeStr | None = None,
        nation: EnumNation | None = None,
        is_premium: bool | None = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    async def tankopedia_export(self, sample: float = 0) -> AsyncGenerator[Tank, None]:
        """Export tankopedia"""
        raise NotImplementedError
        yield Tank()

    @abstractmethod
    async def tankopedia_update(
        self, tank: Tank, update: dict[str, Any] | None = None, fields: list[str] | None = None
    ) -> bool:
        """Update a tank in the backend's tankopedia. Returns False
        if the tank was not updated"""
        raise NotImplementedError

    @abstractmethod
    async def tankopedia_delete(self, tank: Tank) -> bool:
        """Delete a tank from Tankopedia"""
        raise NotImplementedError

    async def tankopedia_insert_worker(self, tankQ: Queue[Tank], force: bool = False) -> EventCounter:
        debug(f"starting, force={force}")
        stats: EventCounter = EventCounter("tankopedia insert")
        try:
            while True:
                tank: Tank = await tankQ.get()
                try:
                    debug(
                        "Trying to " + "update"
                        if force
                        else "insert" + f' tank "{tank}" into {self.table_uri(BSTableType.Tankopedia)}'
                    )
                    if await self.tankopedia_insert(tank, force=force):
                        stats.log("tanks added")
                    else:
                        stats.log("tanks not added")
                except Exception as err:
                    debug(f"Error: {err}")
                    stats.log("errors")
                finally:
                    tankQ.task_done()
        except CancelledError as err:
            debug(f"Cancelled")
        except Exception as err:
            error(f"{err}")
        return stats

    async def tankopedia_get_worker(
        self,
        tankQ: Queue[Tank],
        tanks: list[Tank] | None = None,
        tier: EnumVehicleTier | None = None,
        tank_type: EnumVehicleTypeStr | None = None,
        nation: EnumNation | None = None,
        is_premium: bool | None = None,
    ) -> EventCounter:
        stats: EventCounter = EventCounter("get tankopedia")
        try:
            async for tank in self.tankopedia_get_many(
                tanks=tanks, tier=tier, tank_type=tank_type, nation=nation, is_premium=is_premium
            ):
                await tankQ.put(tank)
                stats.log("tanks")
        except Exception as err:
            error(f"{err}")
        return stats

    # ----------------------------------------
    # Tank Strings
    # ----------------------------------------

    @abstractmethod
    async def tank_string_insert(self, tank_str: WoTBlitzTankString, force: bool = False) -> bool:
        """ "insert a tank string"""
        raise NotImplementedError

    @abstractmethod
    async def tank_string_get(self, code: str) -> WoTBlitzTankString | None:
        raise NotImplementedError

    @abstractmethod
    async def tank_strings_get(self, search: str | None) -> AsyncGenerator[WoTBlitzTankString, None]:
        raise NotImplementedError
        yield WoTBlitzTankString()
