from argparse import ArgumentParser, Namespace, SUPPRESS
from configparser import ConfigParser
from typing import Optional, Iterable, Any, cast
import logging
from asyncio import run, create_task, gather, Queue, CancelledError, Task, Runner, \
					sleep, wait

from alive_progress import alive_bar		# type: ignore
#from yappi import profile 					# type: ignore

from backend import Backend, OptAccountsInactive, BSTableType, \
					ACCOUNTS_Q_MAX, MIN_UPDATE_INTERVAL, get_sub_type
from models import BSAccount, BSBlitzRelease, StatsTypes
from blitzutils.models import Tank, EnumNation, EnumVehicleTier, EnumVehicleTypeStr
from pyutils import export, CSVExportable, JSONExportable, TXTExportable, EventCounter

logger 	= logging.getLogger()
error 	= logger.error
message	= logger.warning
verbose	= logger.info
debug	= logger.debug

########################################################
# 
# add_args_ functions  
#
########################################################

def add_args(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')		
		tankopedia_parsers = parser.add_subparsers(dest='tankopedia_cmd', 	
												title='tankopedia commands',
												description='valid commands',
												metavar='update | edit | import | export')
		tankopedia_parsers.required = True
		
		update_parser = tankopedia_parsers.add_parser('update', help="tankopedia update help")
		if not add_args_update(update_parser, config=config):
			raise Exception("Failed to define argument parser for: tankopedia update")

		edit_parser = tankopedia_parsers.add_parser('edit', help="tankopedia edit help")
		if not add_args_edit(edit_parser, config=config):
			raise Exception("Failed to define argument parser for: tankopedia edit")
		
		import_parser = tankopedia_parsers.add_parser('import', help="tankopedia import help")
		if not add_args_import(import_parser, config=config):
			raise Exception("Failed to define argument parser for: tankopedia import")

		export_parser = tankopedia_parsers.add_parser('export', help="tankopedia export help")
		if not add_args_export(export_parser, config=config):
			raise Exception("Failed to define argument parser for: tankopedia export")
		debug('Finished')	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_update(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_edit(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')	
	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_import(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
	
		return True
	except Exception as err:
		error(f'{err}')
	return False


def add_args_export(parser: ArgumentParser, config: Optional[ConfigParser] = None) -> bool:
	try:
		debug('starting')
		EXPORT_FORMAT 	= 'json'
		EXPORT_FILE 	= 'tanks'
		EXPORT_SUPPORTED_FORMATS : list[str] = ['json']    # , 'csv'

		if config is not None and 'TANKOPEDIA' in config.sections():
			configTP 	= config['TANKOPEDIA']
			EXPORT_FORMAT	= configTP.get('export_format', EXPORT_FORMAT)
			EXPORT_FILE		= configTP.get('export_file', EXPORT_FILE )
	
		parser.add_argument('format', type=str, nargs='?', choices=EXPORT_SUPPORTED_FORMATS, 
								default=EXPORT_FORMAT, help='Export file format')
		parser.add_argument('filename', metavar='FILE', type=str, nargs='?', default=EXPORT_FILE, 
								help='File to export tank-stats to. Use \'-\' for STDIN')
		parser.add_argument('--tanks', type=int, default=None, nargs='*', metavar='TANK_ID [TANK_ID1 ...]',
								help="Export tank stats for the listed TANK_ID(s)")
		parser.add_argument('--tier', type=int, default=None, metavar='TIER', choices=range(1,11),
								help="Export tanks of TIER")
		parser.add_argument('--type', type=str, default=None, metavar='TYPE',
								choices=[ n.name for n in EnumVehicleTypeStr ],
								help="Export tanks of TYPE")
		parser.add_argument('--nation', type=str, default=None, metavar='NATION',
								choices=[ n.name for n in EnumNation ], 
								help="Export tanks of NATION")
		parser.add_argument('--is-premium', type=bool, default=None, metavar='PREMIUM',
								choices=[ True, False ], 
								help="Export premium/non-premium tanks")

		return True
	except Exception as err:
		error(f'{err}')
	return False


###########################################
# 
# cmd_ functions  
#
###########################################

async def cmd(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		# if args.tank_stats_cmd == 'update':
		# 	return await cmd_update(db, args)

		# elif args.tank_stats_cmd == 'edit':
		# 	return await cmd_edit(db, args)

		if args.tank_stats_cmd == 'export':
			return await cmd_export(db, args)

		# elif args.tank_stats_cmd == 'import':
		# 	return await cmd_import(db, args)
			
	except Exception as err:
		error(f'{err}')
	return False

########################################################
# 
# cmd_export()
#
########################################################


async def cmd_export(db: Backend, args : Namespace) -> bool:
	try:
		debug('starting')
		stats 		: EventCounter 				= EventCounter('tankopedia export')
		tankQ 			: Queue[Tank] 			= Queue(100)
		filename	: str						= args.filename
		export_stdout : bool 					= filename == '-'
		nation 		: EnumNation | None 		= None
		tier 		: EnumVehicleTier | None 	= None
		tank_type 	: EnumVehicleTypeStr | None = None
		is_premium 	: bool | None 				= None
		tanks 		: list[Tank] 				= list()
		
		if args.nation is not None:
			nation = EnumNation[args.nation]
		if args.tier is not None:
			tier = EnumVehicleTier(args.tier)
		if args.type is not None:
			tank_type = EnumVehicleTypeStr[args.type]
		if args.is_premium is not None:
			is_premium = args.is_premium
		if args.tanks is not None:
			for tank_id in args.tanks:
				tanks.append(Tank(tank_id=tank_id))
	
		export_worker 	: Task
	
		export_worker = create_task( export(Q=cast(Queue[CSVExportable] | Queue[TXTExportable] | Queue[JSONExportable], 
															tankQ), 
											format=args.format, filename=filename, 
											force=args.force, append=False))
		
		stats.merge_child(await db.tankopedia_get_worker(tankQ, tanks=tanks, tier=tier,
										tank_type=tank_type, nation=nation,
										is_premium=is_premium))
		await tankQ.join()
		await stats.gather_stats([export_worker])
		stats.print()

	except Exception as err:
		error(f'{err}')
	return False