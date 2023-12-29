import argparse
import json
import os
import shutil
import sys
import logging

import git

import dbdefs
import parser_logger

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser()
    debug = parser.add_mutually_exclusive_group()
    debug.add_argument('--debug', help='set logging to debug', action='store_true')
    debug.add_argument('--cdebug', help='set logging to debug and show debug messages in console', action='store_true')
    parser.add_argument('-b', type=str, help='set build number', required=True)
    args = parser.parse_args()

    # logging
    log_level = logging.INFO
    if args.debug or args.cdebug:
        log_level = logging.DEBUG
    tempFile = open("app.log", "w").close
    logger = parser_logger.set_logger(__name__)
    parser_logger.overwrite_setLevel(log_level)
    if not args.cdebug:
        parser_logger.remove_debug_stream_handler()
    logger.info("started logging")
    logger.debug("debug logging enabled")

    # handling WoWDBDefs Repo
    if os.path.isdir('./WoWDBDefs'):
        wowdbdefs = git.Git('./WoWDBDefs')
        if args.gitpull:
            print('Pulling git repositories...')
            print('WoWDBDefs: ' + wowdbdefs.pull())
            if os.path.isdir('./.cache'):
                shutil.rmtree('./.cache')

    else:
        print('cloning WoWDBDefs...')
        wowdbdefs_repo = git.Repo.clone_from('https://github.com/wowdev/WoWDBDefs', 'WoWDBDefs')
        wowdbdefs = git.Git('./WoWDBDefs')
        print('imported WoWDBDefs. Rerun program')
        sys.exit(0)

    definitions = dbdefs.read_definitions_folder('./WoWDBDefs/definitions')
    definitions_build = dbdefs.get_definitions_by_build('./WoWDBDefs/definitions', '10.2.0.51239')

    # write definition to .json files
    with open("definitions.json", "w") as f:
        print(f'writing definitions to definitions.json')
        f.writelines(json.dumps(definitions, indent=2))
    with open("definitions_build.json", "w") as f:
        print(f'writing definitions to definitions_build.json')
        f.writelines(json.dumps(definitions_build, indent=2))
