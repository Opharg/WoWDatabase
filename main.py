import argparse
import json
from urllib.request import urlopen
import os
import shutil
import subprocess
import sys
import logging
import time

import git

import dbdefs
from mysql_scripts import mysql_connection
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

    logger.info('Reformatting listfile...')
    with open(r'./community-listfile.csv') as listfile:
        lines = listfile.readlines()
    new_lines = ['ID,Filename,Filepath\n']
    for line in lines:
        line = line.strip()
        fdid = line.split(';')[0]
        filename = line.split('/')[-1].split('.')[0]
        filepath = line.split(';')[1]

        new_lines.append(f'{fdid},"{filename}","{filepath}"\n')

    with open(r'./community-listfile-reformatted.csv', 'w') as listfile:
        listfile.writelines(new_lines)


def send_to_console(string):
    console = os.environ['DB_CONSOLE']
    if os.path.isfile(console):
        with open(console, "w") as f:
            f.writelines(string)
        logger.info(f"Query added to console {console}")
    else:
        subprocess.run("clip", text=True, input=string)
        logger.warning(
            "Invalid or no console set, or does not exist. Added query to clip. Paste into your console of choice")


def end_program(exit_code=0):
    execution_duration = time.time() - start_time
    logger.info("Finished execution in %s seconds" % (execution_duration))
    if not exit_code == 0:
        sys.exit(exit_code)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # timing everything
    start_time = time.time()

    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', type=str, help='set version', required=True)
    parser.add_argument('-db', type=str, help='set version', required=True)
    parser.add_argument('--c', help="Adds SQL query to console or clip", action="store_true")

    exec_group = parser.add_mutually_exclusive_group()
    exec_group.add_argument("--noexec", help="only add sql to the console/clip, no db connection", action="store_true")
    exec_group.add_argument("--data", help="Data -> database", action="store_true")
    exec_group.add_argument("--cdata", help="Data SQL -> console", action="store_true")
    parser.add_argument("--nokeys", help="skip writing foreign keys to the database", action="store_true")

    debug_group = parser.add_mutually_exclusive_group()
    debug_group.add_argument('--debug', help='enable debug logging', action='store_true')
    debug_group.add_argument('--cdebug', help='enable debug logging & print to console', action='store_true')

    parser.add_argument("--vdefs", help="output definitions of this version to .json", action="store_true")
    parser.add_argument("--fulldefs", help="output all definitions to .json", action="store_true")
    parser.add_argument("--dbdefspull", help="Pull WoWDBDefs changes. Deletes cache", action="store_true")
    parser.add_argument("--listfile", help="Update Listfile", action="store_true")
    parser.add_argument("--clearcache", help="clear cache", action="store_true")
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
    logger.debug("debug logging enabled")

    # handle WoWDBDefs Repo
    if os.path.isdir('./WoWDBDefs'):
        wowdbdefs = git.Git('./WoWDBDefs')
        if args.dbdefspull:
            logger.info('Pulling git repositories...')
            logger.info('WoWDBDefs: ' + wowdbdefs.pull())
            if os.path.isdir('./.cache'):
                shutil.rmtree('./.cache')
    else:
        logger.info('cloning WoWDBDefs...')
        wowdbdefs_repo = git.Repo.clone_from('https://github.com/wowdev/WoWDBDefs', 'WoWDBDefs')
        wowdbdefs = git.Git('./WoWDBDefs')
        logger.warning('imported WoWDBDefs. Rerun program')
        sys.exit(0)


    # listfile
    if not os.path.isfile('./community-listfile.csv'):
        logger.info('Downloading community-listfile')
        update_listfile()
    else:
        logger.info('community-listfile found')
    if args.listfile:
        logger.info('Renaming community-listfile.csv -> community-listfile.csv.old')
        if os.path.isfile('./community-listfile.csv.old'):
            os.remove('./community-listfile.csv.old')
        os.rename('./community-listfile.csv', './community-listfile.csv.old')
        logger.info('Updating community-listfile')
        update_listfile()
        pass

    # clear cache
    if args.clearcache:
        if os.path.isdir('./.cache'):
            shutil.rmtree('./.cache')
            logger.info('Cache cleared')

    # actually do the stuff
    match args.db:
        case 'mysql':
            definitions_build = dbdefs.get_definitions_by_build('./WoWDBDefs/definitions', args.v)
            combined_query = mysql_connection.build_database(definitions_build, args)
        case _:
            logger.critical(f'{args.db} is not a supported database type')
            end_program(1)


    # send to console
    if args.c or args.cdata:
        send_to_console(combined_query)

    # write definitions to .json files
    if args.fulldefs:
        definitions = dbdefs.read_definitions_folder('./WoWDBDefs/definitions')
        with open("definitions.json", "w") as file:
            logger.info(f'writing definitions to definitions.json')
            file.writelines(json.dumps(definitions, indent=2))
    if args.vdefs:
        with open("definitions_build.json", "w") as file:
            logger.info(f'writing definitions to definitions_build.json')
            file.writelines(json.dumps(definitions_build, indent=2))

    end_program()
