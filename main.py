import argparse
import json
from urllib.request import urlopen
import os
import shutil
import sys
import logging

import git

import dbdefs
import parser_logger


def update_listfile():
    url = r'https://github.com/wowdev/wow-listfile/releases/latest/download/community-listfile-withcapitals.csv'
    file_path = r'./community-listfile.csv'
    try:
        # Open the URL
        with urlopen(url) as response, open(file_path, 'wb') as out_file:
            # Read the content in chunks and save it to the file
            chunk_size = 4096
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                out_file.write(chunk)

        print(f"File downloaded successfully and saved as {file_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser()
    debug = parser.add_mutually_exclusive_group()
    debug.add_argument('--debug', help='set logging to debug', action='store_true')
    debug.add_argument('--cdebug', help='set logging to debug and show debug messages in console', action='store_true')
    parser.add_argument('-b', type=str, help='set build number', required=True)
    parser.add_argument("--listfile", help="Update Listfile", action="store_true")
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

    if not os.path.isfile('./community-listfile.csv'):
        print('Downloading community-listfile')
        update_listfile()
    else:
        print('community-listfile found')
    if args.listfile:
        print('Updating community-listfile')
        print('Renaming community-listfile.csv -> community-listfile.csv.old')
        os.rename('./community-listfile.csv', './community-listfile.csv.old')
        update_listfile()
        pass

    definitions = dbdefs.read_definitions_folder('./WoWDBDefs/definitions')
    definitions_build = dbdefs.get_definitions_by_build('./WoWDBDefs/definitions', '10.2.0.51239')

    # write definition to .json files
    with open("definitions.json", "w") as f:
        print(f'writing definitions to definitions.json')
        f.writelines(json.dumps(definitions, indent=2))
    with open("definitions_build.json", "w") as f:
        print(f'writing definitions to definitions_build.json')
        f.writelines(json.dumps(definitions_build, indent=2))
