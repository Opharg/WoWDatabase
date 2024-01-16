# WoWDatabase
Python tooling to create dbc databases from World of Warcraft. Currently only supporting MySQL.

## Disclaimer
Don't expect anything better than baby's-first-python-project, but it works, so there's that.

## Limitations
- The table `filedata` is hardcoded and will always be build from the latest [wow-listfile](https://github.com/wowdev/wow-listfile).

## Usage
### Generating tables
Run `main.py` with the required `-v` argument and either set up the Environment Variables to connect to your database, 
or use `--c` to output to your defined console file or clipboard for manual execution.
### Loading Data into tables
Drop your dbfilesclient .csv files into `./dbfilesclient/{VERSION}` and run `main.py` with one of the data arguments.
- extract with [wow.tools.local](https://github.com/Marlamin/wow.tools.local)
- `dbc` files can be converted to `.csv` with [DBC2CSV](https://github.com/Marlamin/DBC2CSV)
    - I've included a `user_dbc2csv.bat` to automate using the DBC2CSV and moving created files into the correct directory. Just Edit the User Variables in it to automate the conversion and moving files. 
  
MySQL: ensure that `local_infile = 1`
### Environment variables
| Name               | Description                                              |
|--------------------|----------------------------------------------------------|
| MYSQL_HOST         | Hostname                                                 |
| MYSQL_DB_USER      | Username                                                 |
| MYSQL_DB_USER_PASS | Password                                                 |
| MYSQL_CONSOLE      | Absolute path to a console file; overwrites file content |

### Arguments
| Argument     | Description                                                         |
|--------------|---------------------------------------------------------------------|
| -h, --help   | Show this help message and exit.                                    |
| -v V         | REQUIRED, WoW version e.g. `-v 10.2.5.52902`                        |
| --c          | write query to the sql console, or clip if no console set/found     |
|              |                                                                     |
| --noexec     | 1: don't write to the database, write table creation query          |
| --cdata      | 2: don't add data to tables, write table creation + load data query |
| --data       | 3: add data to tables, output  table creation + load data query     |
|              |                                                                     |
| --debug      | 1: enable debug logging                                             |
| --cdebug     | 2: enable debug logging & print to console                          |
|              |                                                                     |
| --pull       | gitpull WoWDBDefs                                                   |
| --listfile   | update listfile                                                     |
| --clearcache | delete cache                                                        |
| --fulldefs   | output full definitions to .json                                    |
| --vdefs      | output definitions of the current version to .json                  |