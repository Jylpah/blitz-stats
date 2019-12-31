#!/usr/bin/env python3.8

## script to fetch Blitz data from BlizStars.com
##

import sys, argparse, json, os, inspect

VERBOSE = False
DEBUG   = False

def main(argv):

    global VERBOSE, DEBUG

    parser = argparse.ArgumentParser(description='Convert Blitz tanks JSON file to WG format')
    parser.add_argument('--in', dest="infile", help='JSON File to read')
    parser.add_argument('--out', dest="outfile",help='JSON File to write')
    parser.add_argument('--debug', '-d', action='store_true', default=False, help='Debug mode')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Verbose mode')
    parser.add_argument('--force', '-f', action='store_true', default=False, help='Force update, discard cached files')

    args = parser.parse_args()
    VERBOSE = args.verbose
    DEBUG = args.debug
    if DEBUG: VERBOSE = True

    tanks_in = loadJSONfile(args.infile)
    tanks_out = {}
    tanks_out['status'] = "ok"
    tanks_out["data"] = {}

    tank_type = [ 'lightTank', 'mediumTank', 'heavyTank', 'AT-SPG' ]

    try:
        n = 0
        for tank_id in tanks_in.keys():
            debug("tank_id: " + tank_id)
            tanks_out["data"][str(tank_id)] = {}
            tanks_out["data"][str(tank_id)]["tank_id"] = int(tank_id)
            tanks_out["data"][str(tank_id)]["tier"] = tanks_in[tank_id]["tier"]
            tanks_out["data"][str(tank_id)]["name"] = tanks_in[tank_id]["name"]
            tanks_out["data"][str(tank_id)]["is_premium"] = (tanks_in[tank_id]["premium"] == 1)
            tanks_out["data"][str(tank_id)]["type"] = tank_type[tanks_in[tank_id]["type"]]
            n += 1
        
        tanks_out['meta'] = {"count" : n }
    except Exception as err:
        error(err)
        sys.exit(1)
    
    if os.path.exists(args.outfile):
        error("File exists: " + args.outfile)
        sys.exit(2)

    with open(args.outfile,'w') as outfile:
            json.dump(tanks_out,outfile)

    return 0


## Utils 

def loadJSONfile(filename: str) -> dict:
    """load JSON from file"""

    try:
 #       debug(filename)
        with open(filename, 'r') as fp:
            json_input = json.load(fp)
            return json_input

    except json.JSONDecodeError as err:
        error(err.msg) 
    except FileNotFoundError:
        error('File not found: ' + filename)
    except IOError as err:
        error(str(err))
    
    return None

def verbose(msg = ""):
    """Print a message"""
    if VERBOSE:
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        print(caller + '(): ' + msg)
    return None


def debug(msg = ""):
    """print a conditional debug message"""
    if DEBUG: 
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        print('DEBUG: ' + caller + '(): ' + msg)
    return None


def error(msg = ""):
    """Print an error message"""
    curframe = inspect.currentframe()
    calframe = inspect.getouterframes(curframe, 2)
    caller = calframe[1][3]
    print('ERROR: ' + caller + '(): ' + msg)
    
    return None


## MAIN --------------------------------------------

if __name__ == "__main__":
   main(sys.argv[1:])