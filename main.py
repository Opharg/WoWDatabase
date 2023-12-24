import argparse
import json
import os
import shutil
import sys

import git

import dbdefs

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--gitpull", help="Run program and pull changes from GitHub. Deletes cache", action="store_true")
    parser.add_argument("--overwritecache", help="overwrite cache", action="store_true")
    args = parser.parse_args()

    overwrite_cache = args.overwritecache

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
