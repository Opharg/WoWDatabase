# WoWDatabase
Python tooling to create databases from World of Warcraft's client database files. Currently only supporting MySQL.

## Disclaimer
Don't expect higher quality than my-first-python-project. It works and speed is constrained by the database anyway.

## Limitations
- The table `filedata` is hardcoded and will always be build from [wow-listfile](https://github.com/wowdev/wow-listfile).
- A few builds seem to have files that aren't in WoWDBDefs. The program will tell you which are the offending ones and exit. They can be manually deleted from the folder, but no guarantees (tested with `10.0.7.49343`)
- Foreign Key names are hashed due to length limitations and resulting naming conflicts. 

## Usage
### Generating databases and tables
Run `main.py` with the required `-v` and `-db` arguments and either set up the Environment Variables to connect to your database, 
or use `--c` to output to your defined database console file (or clipboard, if it's not set correctly).

Schemas are named by their version.

### Loading Data into tables
Drop your dbfilesclient .csv files into `./dbfilesclient/{VERSION}` and run `main.py` with one of the data arguments.
- extract with [wow.tools.local](https://github.com/Marlamin/wow.tools.local)
- `dbc` files can be converted to `.csv` with [DBC2CSV](https://github.com/Marlamin/DBC2CSV)
    - I've included a `user_dbc2csv.bat` to automate using the DBC2CSV and moving created files into the correct directory. Just Edit the User Variables in it to automate the conversion and moving files. 
  
MySQL: ensure that `local_infile = 1` is set on your console and your sql console if applicable.
### Environment variables
| Name         | Description                                              |
|--------------|----------------------------------------------------------|
| DB_HOST      | Hostname                                                 |
| DB_USER      | Username                                                 |
| DB_USER_PASS | Password                                                 |
| DB_CONSOLE   | Absolute path to a console file; overwrites file content |

### Arguments
#### required
| Argument     | Description                                                                |
|--------------|----------------------------------------------------------------------------|
| -h, --help   | Show this help message and exit.                                           |
| -v           | REQUIRED, WoW version e.g. `-v 10.2.5.52902`                               |
| -db          | REQUIRED, database type e.g. `-db mysql`                                   |
#### execute (numbered items are mutually exclusive)
|          |                                                                                                                                                       |
|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
|          | 0: no argument; Write tables and foreign keys to the database. Return table creation and foreign key queries for console output                                                                                       |
| --noexec | 1: Don't write to the database. Return table creation and foreign key queries for console output                                                      |
| --cdata  | 2: **requires tables to be already written to a database.** Don't write to the database. Return table creation, foreign key, and data loading queries |
| --data   | 3: Write tables and foreign keys, load data into database; Return full query for console output                                                       |
|          |                                                                                                                                                       |
| --nokeys | skip writing foreign keys to the database. Return full query for console output. (required if foreign keys are already written from a prior run)      |
#### debug (numbered items are mutually exclusive)
|            |                                                         |
|------------|---------------------------------------------------------|
| --debug    | 1: enable debug logging                                 |
| --cdebug   | 2: enable debug logging & write log to console          |
|            |                                                         |
| --vdefs    | output definitions dict of the current version as .json |
| --fulldefs | output full definitions dict as .json                   |
#### miscellaneous
|              |                    |
|--------------|--------------------|
| --dbdefspull | git-pull WoWDBDefs |
| --listfile   | update listfile    |
| --clearcache | delete cache       |
