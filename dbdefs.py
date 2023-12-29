import os
import re
from cachier import cachier
import parser_logger

# parses a given .dbd from path and returns a dict with
#
#   columns{}:
#       "column name": string, for lookup by column name
#           data_type: string
#           foreign_table: string, optional
#           foreign_column: string, optional
#           verified: bool
#           comment: string, optional
#   builds[]
#       layout: string list, optional
#       build: string
#       columns[]: in build
#           name: string
#           data_size: string (8,16,32,64, prefix 'u'), optional
#           array_size: int, optional


# setup logger
logger = parser_logger.set_logger('mysql_connection')


def read_definitions_folder(definitions_folder_path) -> dict:
    counter = 0
    files_amount = len(next(os.walk(definitions_folder_path))[2])
    definitions = {}
    for file in os.listdir(definitions_folder_path):
        # print progress counter
        counter += 1
        # print(f"{counter}/{files_amount}\t\t\t{file}")
        definitions[str(file.split('.')[0])] = read_definition_file(f'{definitions_folder_path}/{file}')

    return definitions


def read_definition_file(definition_file_path) -> dict:
    definition_dict = {}  # return dict

    with open(definition_file_path, 'r') as definition_file:
        text = definition_file.read()
        text = text.split('\n\n')  # blocks are always separated by a double linebreak

    # regex compiles
    foreign_pattern = re.compile(r'<[A-Za-z_\d]+::[A-Za-z_\d]+>')
    data_size_pattern = re.compile(r'<u?[0-9]+>')
    array_size_pattern = re.compile(r'\[\d+]')
    annotations_pattern = re.compile(r'\$[A-Za-z,]+\$')
    comment_pattern = re.compile(r'//(.*)')

    for block in text:

        # columns[]
        if block.startswith('COLUMNS'):
            columns_list = []
            block = block.split('\n')
            block = block[1:]

            for column in block:  # every line represents a different column
                column_dict = {}

                column_edited = re.sub(foreign_pattern, '', column)
                column_edited = column_edited.split(' ')
                column_dict['name'] = column_edited[1].replace('?', '')
                column_dict['data_type'] = column_edited[0]

                foreign = re.search(foreign_pattern, column)
                if foreign:
                    foreign = foreign.group()
                    foreign = foreign.strip('<').strip('>').split('::')
                    column_dict['foreign_table'] = foreign[0]
                    column_dict['foreign_column'] = foreign[1]

                if '?' in column:
                    column_dict['verified'] = False
                else:
                    column_dict['verified'] = True

                comment = re.search(comment_pattern, column)
                if comment:
                    column_dict['comment'] = comment.group().replace('//', '').strip()

                columns_list.append(column_dict)

            # hacked together rewrite for easier lookup
            definition_dict['columns'] = {}
            for column in columns_list:
                definition_dict['columns'][column['name']] = column
                del definition_dict['columns'][column['name']]['name']

        # builds[]
        elif block.startswith('LAYOUT') or block.startswith('BUILD'):
            versions_dict = {}
            layouts_list = []
            builds_list = []
            columns_list = []
            block = block.split('\n')

            for line in block:
                if line.startswith('LAYOUT'):
                    layouts_list = line.replace(',', '').split(' ')[1:]
                elif line.startswith('BUILD'):  # TODO: handle build ranges e.g. '3.0.1.8303-3.0.1.8471'
                    builds_list += line.replace(',', '').split(' ')[1:]
                elif line.startswith('COMMENT'):
                    continue
                else:  # should be the columns

                    if line == '':
                        break

                    column = {}

                    column['name'] = re.sub(array_size_pattern, '', line.split('<')[0].split('$')[-1])

                    data_size = re.search(data_size_pattern, line)
                    array_size = re.search(array_size_pattern, line)
                    annotations = re.search(annotations_pattern, line)
                    comment = re.search(comment_pattern, line)

                    if data_size:
                        column['data_size'] = data_size.group().replace('<', '').replace('>', '')
                    if array_size:
                        column['array_size'] = int(array_size.group().replace('[', '').replace(']', ''))
                    if annotations:
                        if 'id' in annotations.group():
                            column['is_primary'] = True
                        if 'relation' in annotations.group():
                            column['is_relation'] = True
                        if 'noninline' in annotations.group():
                            column['is_noninline'] = True
                    if comment:
                        column['comment'] = comment.group().replace('//', '').strip()

                    columns_list.append(column)

            if len(layouts_list) > 0:
                versions_dict['layout'] = layouts_list
            versions_dict['build'] = builds_list
            versions_dict['columns'] = columns_list

            try:
                definition_dict['builds'].append(versions_dict)
            except:
                definition_dict['builds'] = []
                definition_dict['builds'].append(versions_dict)

    return definition_dict


@cachier(cache_dir='./.cache', separate_files=True)
def get_definitions_by_build(path, build):
    logger.info(f'getting definitions for build {build}')
    definitions = read_definitions_folder(path)
    definitions_with_build = {}

    # get list of applicable definitions
    for dbd in definitions:
        if 'builds' not in definitions[dbd]:  # some definitions are empty
            continue
        try:
            flag = False
            for block in definitions[dbd]['builds']:  # block/layout from .dbd

                # break when build has been found and definition added to definitions_with_build
                if flag:
                    flag = False
                    break

                for build_id in block['build']:
                    if build_id == build:  # select block that has a particular build

                        columns = []

                        for column in block['columns']:
                            column_dict = {}
                            if 'name' in column:
                                column_dict['name'] = column['name']
                            if 'data_size' in column:
                                column_dict['data_size'] = column['data_size']
                            if 'is_primary' in column:
                                column_dict['is_primary'] = column['is_primary']
                            if 'is_relation' in column:
                                column_dict['is_relation'] = column['is_relation']

                            if 'data_type' in definitions[dbd]['columns'][column['name']]:
                                column_dict['data_type'] = definitions[dbd]['columns'][column['name']]['data_type']
                            if 'foreign_table' in definitions[dbd]['columns'][column['name']]:
                                column_dict['foreign_table'] = definitions[dbd]['columns'][column['name']][
                                    'foreign_table']
                            if 'foreign_column' in definitions[dbd]['columns'][column['name']]:
                                column_dict['foreign_column'] = definitions[dbd]['columns'][column['name']][
                                    'foreign_column']
                            if 'verified' in definitions[dbd]['columns'][column['name']]:
                                column_dict['verified'] = definitions[dbd]['columns'][column['name']]['verified']
                            if 'comment' in definitions[dbd]['columns'][column['name']]:
                                column_dict['comment'] = definitions[dbd]['columns'][column['name']]['comment']

                            columns.append(column_dict)

                        definitions_with_build[dbd] = columns
                        flag = True
                        break


        except Exception as e:
            logger.critical(f'Exception "{e}" in {dbd}')

    return definitions_with_build
