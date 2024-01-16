import hashlib
import os
from sys import stdout
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy import text

import mysql.connector
from mysql_scripts import mysql_procedures
import parser_logger

# setup logger
logger = parser_logger.set_logger('mysql_connection')


def create_db_connection(database=''):
    # Configuration for MySQL connection
    db_config = {
        "host": os.environ['MYSQL_HOST'],
        "user": os.environ['MYSQL_DB_USER'],
        "password": os.environ['MYSQL_DB_USER_PASS'],
        "allow_local_infile": True
    }
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(buffered=True)
        if database != '':
            cursor.execute(f"USE `{database}`")

        mysql_console = os.environ['MYSQL_CONSOLE']

        return connection, cursor, mysql_console

    except mysql.connector.Error as err:
        logger.critical(f"Error: {err}")
        raise


def create_db_engine(database=''):
    host = os.environ['MYSQL_HOST']
    user = os.environ['MYSQL_DB_USER']
    password = os.environ['MYSQL_DB_USER_PASS']

    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    return engine


def create_database(name):
    connection, cursor, mysql_console = create_db_connection()

    cursor.execute("SHOW DATABASES")
    if (f'{name}',) not in cursor:
        logger.info(f'Creating database: {name}')
        cursor.execute(f'CREATE DATABASE `{name}`')
        cursor.execute(f'USE `{name}`')

        procedures_sql = ''.join(mysql_procedures.procedures)
        cursor.execute(procedures_sql)
    else:
        logger.info(f'Database {name} already exists')

    cursor.reset()
    cursor.close()
    connection.close()
    return


def build_database(definitions_build, args):
    # using BIGINT for every int, because of data_type mismatches for foreign keys in DBDefs
    data_types = {
        'int8': 'BIGINT',  # TINYINT
        'int16': 'BIGINT',  # SMALLINT
        'int32': 'BIGINT',  # INTEGER
        'int64': 'BIGINT',  # BIGINT
        'float': 'FLOAT',
        'string': 'TEXT',
        'locstring': 'TEXT'
    }

    # generate sql
    tables_sql, column_count, table_count = generate_tables_sql(args.b, data_types, definitions_build)
    foreign_key_sql, indices_sql, foreign_key_count = generate_foreign_key_sql(args.b, definitions_build)

    combined_no_data_sql = tables_sql + indices_sql + foreign_key_sql
    combined_full_sql = '# You should never see this. combined_full_sql. Data inserts cannot be loaded, because it requires a database connection for parity checks'
    load_data_sql = '# Data inserts cannot be loaded, because it needs a database connection for parity checks'

    if not args.noexec:
        create_database(args.b)
        connection, cursor, mysql_console = create_db_connection(database=args.b)

        cursor.execute(combined_no_data_sql)

        cursor.close()
        connection.close()

        # waiting until the Server has written all tables to disk, because .execute() is faster
        # TODO: Figure out if changing these to lists makes waitout_write() irrelevant
        logger.info(f'Writing {table_count} Tables with {column_count} columns to Disk')
        waitout_write(args.b, table_count, 'PROC_GET_TABLE_COUNT_IN_SCHEMA', 'Tables')
        logger.info(f'Writing {foreign_key_count} foreign keys to disk')
        waitout_write(args.b, foreign_key_count, 'PROC_GET_FOREIGN_KEY_COUNT_IN_SCHEMA', 'Foreign Keys')

        # write data. Needs everything to be written to disk for parity checks, thus separate execute
        if args.data:
            load_data_sql, load_data_sql_list = generate_load_data_sql(args.b)
            combined_full_sql = tables_sql + indices_sql + foreign_key_sql + load_data_sql

            try:
                connection, cursor, mysql_console = create_db_connection(database=args.b)
                connection.autocommit = True
                cursor.execute('SET foreign_key_checks = 0')
                #cursor.execute(load_data_sql)
                #connection.commit()

                load_data_sql_list_length = len(load_data_sql_list)

                for idx, e in enumerate(load_data_sql_list):
                    out_string = f"{idx+1}/{load_data_sql_list_length} - Writing Data to tables"
                    stdout.write('\r' + out_string)
                    cursor.execute(e)
                print()

                cursor.execute('SET foreign_key_checks = 1')
            except Exception as e:
                logger.critical(f"Error: {e}")
                raise
            finally:
                connection.autocommit = False
                if cursor:
                    cursor.close()
                if connection:
                    connection.close()


        cursor.close()
        connection.close()

    return combined_full_sql, combined_no_data_sql, load_data_sql


def generate_tables_sql(build_id, data_types, definitions_build):
    # generates sql query for table creation
    logger.info(f'Creating tables for {build_id}')
    tables_sql = f""

    #  manually add FileData
    tables_sql += (f"CREATE TABLE IF NOT EXISTS `FileData` (\n"
                   f"\t`ID` BIGINT PRIMARY KEY,\n"
                   f"\t`Filename` TEXT,\n"
                   f"\t`Filepath` TEXT\n);\n")
    column_count = 0
    table_count = 1
    for table in definitions_build:
        table_count += 1
        table_name = table
        tables_sql += f'CREATE TABLE IF NOT EXISTS `{table_name}` (\n'

        for column in definitions_build[table]:

            ## name
            column_name = column['name']

            ## primary
            is_primary = f''
            if 'is_primary' in column:
                is_primary += ' PRIMARY KEY'

            ## data_type
            data_type = ''
            data_type += column['data_type']
            if 'data_size' in column:
                data_size = column['data_size'].replace('u', '')
                data_type += data_size
                data_type = data_types[data_type]
                # # disabled: data_type mismatch in .dbd files
                # if 'u' in column['data_size']:
                #     data_type += ' UNSIGNED'
            else:
                data_type = data_types[data_type]
            data_type = f' {data_type}'

            # comment
            comment = ''
            if 'comment' in column or column['verified'] is False:
                comment += " COMMENT '"
                if not column['verified']:
                    comment += 'Name unverified. '
                if 'comment' in column:
                    comment += column['comment'].replace("'", "''").replace("\\", "\\\\")
                if len(comment) > 1024:  # 1024 char limit for MySQL comments, UiWidgetStringSource::Value_lang is longer
                    comment = comment[:1002] + ' ... COMMENT TRUNCATED'
                comment += "'"

            # combine column creation line
            if 'array_size' in column:
                for idx in range(column['array_size']):
                    tables_sql += f'\t`{column_name}[{idx}]`{data_type}{is_primary}{comment},\n'
                    column_count += 1
            else:
                tables_sql += f'\t`{column_name}`{data_type}{is_primary}{comment},\n'
                column_count += 1

        tables_sql = tables_sql[:-2]
        tables_sql += "\n);\n"

    return tables_sql, column_count, table_count


def generate_foreign_key_sql(build_id, definitions_build):
    # generates sql query to add foreign keys
    keys_sql_set = set()
    foreign_key_count = 0
    foreign_key_sql = f""
    for table in definitions_build:

        table_name = table
        table_foreign_sql = f"ALTER TABLE `{table_name}`\n"
        for column in definitions_build[table]:
            column_name = column['name']

            # catch if there is a relation, but it's not in client data
            if 'is_relation' in column and 'foreign_table' not in column:
                logger.debug(f"{table_name}::{column_name} has no relation in client data")
                continue

            # get foreign key data, skip if there is none. Ignores 'is_relation' because WoWDBDefs is confusing
            if 'foreign_table' in column:
                foreign_table = column['foreign_table']
                foreign_column = column['foreign_column']
            else:
                continue

            # catch self-referential fk
            if table_name == foreign_table and column_name == foreign_column:
                continue

            if foreign_table in definitions_build or foreign_table == 'FileData':
                if 'array_size' in column:
                    for x in range(column['array_size']):
                        foreign_key_count += 1
                        # creating has due to 64 char limit and duplicate names with shortening
                        fk_hash = hashlib.md5(
                            f"{table_name}_{column_name}[{x}]_{foreign_table}_{foreign_column}".encode()).hexdigest()
                        table_foreign_sql += (f"\tADD CONSTRAINT fk_{fk_hash}\n"
                                              f"\tFOREIGN KEY (`{column_name}[{x}]`) REFERENCES `{foreign_table}`(`{foreign_column}`),\n")
                else:
                    foreign_key_count += 1
                    # creating has due to 64 char limit and duplicate names with shortening
                    fk_hash = hashlib.md5(
                        f"{table_name}_{column_name}_{foreign_table}_{foreign_column}".encode()).hexdigest()
                    table_foreign_sql += (f"\tADD CONSTRAINT fk_{fk_hash}\n"
                                          f"\tFOREIGN KEY (`{column_name}`) REFERENCES `{foreign_table}`(`{foreign_column}`),\n")
            else:
                logger.debug(f"{table_name}::{column_name} has no relation in client data for this build")
                break

            try:
                if foreign_table != 'FileData':  # comes from listfile, manually handled
                    for check_column in definitions_build[foreign_table]:
                        if check_column['name'] == foreign_column and 'is_primary' not in check_column:
                            keys_sql_set.add(
                                f"CALL PROC_SET_PRIMARY_ANDOR_INDEX('{foreign_table}', '{foreign_column}');\n")
            except KeyError as e:
                logger.debug(
                    f'{table_name}::{column_name} -> {foreign_table}::{foreign_column}, table does not exist in build {build_id}')

        if table_foreign_sql == f"ALTER TABLE `{table_name}`\n":
            continue

        table_foreign_sql = table_foreign_sql[:-2]
        table_foreign_sql += ";\n"
        foreign_key_sql += table_foreign_sql

    keys_sql_set = ''.join(keys_sql_set)

    return foreign_key_sql, keys_sql_set, foreign_key_count


def generate_load_data_sql(build_id):
    #import csv_loader
    #frames = csv_loader.load_dir(build_id)

    # connection used for parity checks between database and data files in folder
    connection, cursor, mysql_console = create_db_connection(database=build_id)

    tables_in_db = []
    cursor.execute("SHOW TABLES")
    for table in cursor:
        tables_in_db.append(table[0])
    dataframe_names = []
    #dataframe_names = [key for key, value in frames.items()]
    for file in os.listdir(f'./dbfilesclient/{build_id}'):
        if not file.endswith('.csv'):
            logger.warning(f'non .csv file found in ./dbfilesclient/{build_id}: {file}')
            continue
        dataframe_names.append(os.path.splitext(file)[0])

    # compare lists to find unique items
    db_set = set(tables_in_db)
    folder_set = set(dataframe_names)
    symmectic_diff_set = db_set ^ folder_set
    intersection_set = db_set & folder_set
    logger.debug(f'symmectic_diff_set: {symmectic_diff_set}')
    logger.debug(f'intersection_set: {intersection_set}')

    error_list = []
    for item in symmectic_diff_set:
        if item in tables_in_db:
            logger.debug(f"{item} in database has no corresponding data in dbfilesclient. This is usually normal")
        if item in folder_set:
            logger.critical(f'{item}.csv in data folder has no corresponding table in database')
            error_list.append(item)

    if len(error_list) > 0:
        raise Exception(
            f"At least one file in ./dbfilesclient/{build_id} has no corresponding table in the database. Check app.log")
    #engine = create_db_engine(build_id)
    # Disable foreign key checks
    #cursor.execute('SET foreign_key_checks = 0')
    #logger.info('Foreign key checks disabled')
    # with engine.connect() as connection:
    #    connection.execute(text('SET foreign_key_checks = 0'))

    #frames_count = len(frames)
    #insert_sql = ''
    #for idx, df in enumerate(frames):
#
    #    out_string = f"{idx+1}/{frames_count} - Loading: {df}"
    #    stdout.write('\r' + out_string)
    #    # frames[df].to_sql(con=engine, name=dataframe_names[idx], if_exists='append', index=False, method='multi')
#
    #    csv_path = os.getcwd() + f"\\tmp\\{df}.csv"
    #    csv_path = csv_path.replace('\\', '\\\\')
    #    frames[df].to_csv(csv_path, index=False, sep=',', quotechar='"', na_rep=r'')
    #    rq = """LOAD DATA LOCAL INFILE '{file_path}' REPLACE INTO TABLE `{db}`.`{db_table}`
    #            FIELDS TERMINATED BY ','
    #            OPTIONALLY ENCLOSED BY '"'
    #            ESCAPED BY ''
    #            LINES TERMINATED BY '\\r\\n'
    #            IGNORE 1 LINES
    #             ({col});
    #            """.format(db=build_id,
    #                       file_path=csv_path,
    #                       db_table=df,
    #                       col='`'+"`,`".join(frames[df].columns.tolist())+'`')
    #    insert_sql += rq
    #    cursor.execute(rq)
    #    connection.commit()
#
    #cursor.execute('SET foreign_key_checks = 1')
    #logger.info('Foreign key checks enabled')
    #print()


    load_data_sql = ''
    load_data_sql_list = []

    for element in intersection_set:
        element_path = os.getcwd() + f"\dbfilesclient\\{build_id}\{element}.csv"
        element_path_slash = element_path.replace('\\', '\\\\')

        with open(element_path, 'r', encoding='utf-8') as file:
            column_names_file = file.readline().strip()
            column_names_file = column_names_file.split(',')
            column_names_file = tuple(column_names_file)

        # check parity between data and database
        cursor.execute(f"SELECT * FROM `{element}` LIMIT 0")
        if not column_names_file == cursor.column_names:
            logger.critical(f'{element} seems to have different columns between the database and data')
            raise Exception(f"{element} seems to have different columns between the database and data")

        cursor.reset()

        # rebuild tuple with ``
        columns_str = '('
        for column in column_names_file:
            column = column.replace("'", "`")
            columns_str += f"`{column}`,"
        columns_str = columns_str[:-1]
        columns_str += ')'

        load_data_sql_list.append(f"""LOAD DATA LOCAL INFILE '{element_path_slash}'
        REPLACE INTO TABLE `{build_id}`.`{element}`
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY ''
        LINES TERMINATED BY '\\r\\n'
        IGNORE 1 LINES
        {columns_str};\n""")

    return load_data_sql, load_data_sql_list


def waitout_write(build_id, total_count, procedure, object):
    # wait until everything is written to disk
    result = 0
    connection, cursor, mysql_console = create_db_connection(database=build_id)

    while result != total_count:
        cursor.execute(f"CALL {procedure}('{build_id}', @object_count);")
        cursor.execute("SELECT @object_count;")
        result = cursor.fetchone()[0]
        connection.commit()

        out_string = f"{object} written: {result}/{total_count}"
        stdout.write('\r' + out_string)

        if result == total_count:
            break
        sleep(1)

    print('')
    cursor.close()
    connection.close()
