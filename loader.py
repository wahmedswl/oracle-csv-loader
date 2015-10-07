# CONFIG
GLOBAL_CONFIG_FILE = 'loader.cfg'
GLOBAL_SEPARATORS = ["|", ",", ";", "\t"]
GLOBAL_RESERVE_WORDS = ["ACCESS", "ACCOUNT", "ACTIVATE", "ADD", "ADMIN", "ADVISE", "AFTER", "ALL", "ALL_ROWS",
                        "ALLOCATE", "ALTER", "ANALYZE", "AND", "ANY", "ARCHIVE", "ARCHIVELOG", "ARRAY", "AS", "ASC",
                        "AT", "AUDIT", "AUTHENTICATED", "AUTHORIZATION", "AUTOEXTEND", "AUTOMATIC", "BACKUP", "BECOME",
                        "BEFORE", "BEGIN", "BETWEEN", "BFILE", "BITMAP", "BLOB", "BLOCK", "BODY", "BY", "CACHE",
                        "CACHE_INSTANCES", "CANCEL", "CASCADE", "CAST", "CFILE", "CHAINED", "CHANGE", "CHAR", "CHAR_CS",
                        "CHARACTER", "CHECK", "CHECKPOINT", "CHOOSE", "CHUNK", "CLEAR", "CLOB", "CLONE", "CLOSE",
                        "CLOSE_CACHED_OPEN_CURSORS", "CLUSTER", "COALESCE", "COLUMN", "COLUMNS", "COMMENT", "COMMIT",
                        "COMMITTED", "COMPATIBILITY", "COMPILE", "COMPLETE", "COMPOSITE_LIMIT", "COMPRESS", "COMPUTE",
                        "CONNECT", "CONNECT_TIME", "CONSTRAINT", "CONSTRAINTS", "CONTENTS", "CONTINUE", "CONTROLFILE",
                        "CONVERT", "COST", "CPU_PER_CALL", "CPU_PER_SESSION", "CREATE", "CURRENT", "CURRENT_SCHEMA",
                        "CURREN_USER", "CURSOR", "CYCLE", ",DANGLING", "DATABASE", "DATAFILE", "DATAFILES", "DATAOBJNO",
                        "DATE", "DBA", "DBHIGH", "DBLOW", "DBMAC", "DEALLOCATE", "DEBUG", "DEC", "DECIMAL", "DECLARE",
                        "DEFAULT", "DEFERRABLE", "DEFERRED", "DEGREE", "DELETE", "DEREF", "DESC", "DIRECTORY",
                        "DISABLE", "DISCONNECT", "DISMOUNT", "DISTINCT", "DISTRIBUTED", "DML", "DOUBLE", "DROP", "DUMP",
                        "EACH", "ELSE", "ENABLE", "END", "ENFORCE", "ENTRY", "ESCAPE", "EXCEPT", "EXCEPTIONS",
                        "EXCHANGE", "EXCLUDING", "EXCLUSIVE", "EXECUTE", "EXISTS", "EXPIRE", "EXPLAIN", "EXTENT",
                        "EXTENTS", "EXTERNALLY", "FAILED_LOGIN_ATTEMPTS", "FALSE", "FAST", "FILE", "FIRST_ROWS",
                        "FLAGGER", "FLOAT", "FLOB", "FLUSH", "FOR", "FORCE", "FOREIGN", "FREELIST", "FREELISTS", "FROM",
                        "FULL", "FUNCTION", "GLOBAL", "GLOBALLY", "GLOBAL_NAME", "GRANT", "GROUP", "GROUPS", "HASH",
                        "HASHKEYS", "HAVING", "HEADER", "HEAP", "IDENTIFIED", "IDGENERATORS", "IDLE_TIME", "IF",
                        "IMMEDIATE", "IN", "INCLUDING", "INCREMENT", "INDEX", "INDEXED", "INDEXES", "INDICATOR",
                        "IND_PARTITION", "INITIAL", "INITIALLY", "INITRANS", "INSERT", "INSTANCE", "INSTANCES",
                        "INSTEAD", "INT", "INTEGER", "INTERMEDIATE", "INTERSECT", "INTO", "IS", "ISOLATION",
                        "ISOLATION_LEVEL", "KEEP", "KEY", "KILL", "LABEL", "LAYER", "LESS", "LEVEL", "LIBRARY", "LIKE",
                        "LIMIT", "LINK", "LIST", "LOB", "LOCAL", "LOCK", "LOCKED", "LOG", "LOGFILE", "LOGGING",
                        "LOGICAL_READS_PER_CALL", "LOGICAL_READS_PER_SESSION", "LONG", "MANAGE", "MASTER", "MAX",
                        "MAXARCHLOGS", "MAXDATAFILES", "MAXEXTENTS", "MAXINSTANCES", "MAXLOGFILES", "MAXLOGHISTORY",
                        "MAXLOGMEMBERS", "MAXSIZE", "MAXTRANS", "MAXVALUE", "MIN", "MEMBER", "MINIMUM", "MINEXTENTS",
                        "MINUS", "MINVALUE", "MLSLABEL", "MLS_LABEL_FORMAT", "MODE", "MODIFY", "MOUNT", "MOVE",
                        "MTS_DISPATCHERS", "MULTISET", "NATIONAL", "NCHAR", "NCHAR_CS", "NCLOB", "NEEDED", "NESTED",
                        "NETWORK", "NEW", "NEXT", "NOARCHIVELOG", "NOAUDIT", "NOCACHE", "NOCOMPRESS", "NOCYCLE",
                        "NOFORCE", "NOLOGGING", "NOMAXVALUE", "NOMINVALUE", "NONE", "NOORDER", "NOOVERRIDE",
                        "NOPARALLEL", "NOPARALLEL", "NOREVERSE", "NORMAL", "NOSORT", "NOT", "NOTHING", "NOWAIT", "NULL",
                        "NUMBER", "NUMERIC", "NVARCHAR2", "OBJECT", "OBJNO", "OBJNO_REUSE", "OF", "OFF", "OFFLINE",
                        "OID", "OIDINDEX", "OLD", "ON", "ONLINE", "ONLY", "OPCODE", "OPEN", "OPTIMAL", "OPTIMIZER_GOAL",
                        "OPTION", "OR", "ORDER", "ORGANIZATION", "OSLABEL", "OVERFLOW", "OWN", "PACKAGE", "PARALLEL",
                        "PARTITION", "PASSWORD", "PASSWORD_GRACE_TIME", "PASSWORD_LIFE_TIME", "PASSWORD_LOCK_TIME",
                        "PASSWORD_REUSE_MAX", "PASSWORD_REUSE_TIME", "PASSWORD_VERIFY_FUNCTION", "PCTFREE",
                        "PCTINCREASE", "PCTTHRESHOLD", "PCTUSED", "PCTVERSION", "PERCENT", "PERMANENT", "PLAN",
                        "PLSQL_DEBUG", "POST_TRANSACTION", "PRECISION", "PRESERVE", "PRIMARY", "PRIOR", "PRIVATE",
                        "PRIVATE_SGA", "PRIVILEGE", "PRIVILEGES", "PROCEDURE", "PROFILE", "PUBLIC", "PURGE", "QUEUE",
                        "QUOTA", "RANGE", "RAW", "RBA", "READ", "READUP", "REAL", "REBUILD", "RECOVER", "RECOVERABLE",
                        "RECOVERY", "REF", "REFERENCES", "REFERENCING", "REFRESH", "RENAME", "REPLACE", "RESET",
                        "RESETLOGS", "RESIZE", "RESOURCE", "RESTRICTED", "RETURN", "RETURNING", "REUSE", "REVERSE",
                        "REVOKE", "ROLE", "ROLES", "ROLLBACK", "ROW", "ROWID", "ROWNUM", "ROWS", "RULE", "SAMPLE",
                        "SAVEPOINT", "SB4", "SCAN_INSTANCES", "SCHEMA", "SCN", "SCOPE", "SD_ALL", "SD_INHIBIT",
                        "SD_SHOW", "SEGMENT", "SEG_BLOCK", "SEG_FILE", "SELECT", "SEQUENCE", "SERIALIZABLE", "SESSION",
                        "SESSION_CACHED_CURSORS", "SESSIONS_PER_USER", "SET", "SHARE", "SHARED", "SHARED_POOL",
                        "SHRINK", "SIZE", "SKIP", "SKIP_UNUSABLE_INDEXES", "SMALLINT", "SNAPSHOT", "SOME", "SORT",
                        "SPECIFICATION", "SPLIT", "SQL_TRACE", "STANDBY", "START", "STATEMENT_ID", "STATISTICS", "STOP",
                        "STORAGE", "STORE", "STRUCTURE", "SUCCESSFUL", "SWITCH", "SYS_OP_ENFORCE_NOT_NULL$",
                        "SYS_OP_NTCIMG$", "SYNONYM", "SYSDATE", "SYSDBA", "SYSOPER", "SYSTEM", "TABLE", "TABLES",
                        "TABLESPACE", "TABLESPACE_NO", "TABNO", "TEMPORARY", "THAN", "THE", "THEN", "THREAD",
                        "TIMESTAMP", "TIME", "TO", "TOPLEVEL", "TRACE", "TRACING", "TRANSACTION", "TRANSITIONAL",
                        "TRIGGER", "TRIGGERS", "TRUE", "TRUNCATE", "TX", "TYPE", "UB2", "UBA", "UID", "UNARCHIVED",
                        "UNDO", "UNION", "UNIQUE", "UNLIMITED", "UNLOCK", "UNRECOVERABLE", "UNTIL", "UNUSABLE",
                        "UNUSED", "UPDATABLE", "UPDATE", "USAGE", "USE", "USER", "USING", "VALIDATE", "VALIDATION",
                        "VALUE", "VALUES", "VARCHAR", "VARCHAR2", "VARYING", "VIEW", "WHEN", "WHENEVER", "WHERE",
                        "WITH", "WITHOUT", "WORK", "WRITE", "WRITEDOWN", "WRITEUP", "XID", "YEAR", "ZONE"]
GLOBAL_COMMON_ACRONYM = {"ACTIVE": "ACTV", "INGREDIENT": "INGDT", "DRUG": "DRG", "FORMAT": "FMT", "DATE": "DT",
                         "PRODUCT": "PROD", "MANAGEMENT": "MGMT", "PATIENT": "PAT", "SPECIFIC": "SPFC",
                         "INFORMATION": "INFO", "GROUP": "GRP", "PACKAGE": "PKG", "VERSION": "VER", "ATTRIBUTE": "ATTR",
                         "ADMINISTRATION": "ADM", "THERAPEUTIC": "THR", "CONCEPT": "CEP", "DENORMALIZE": "DENORM",
                         "COMPOSITION": "COMP", "COMPOSIT": "COMP"}
GLOBAL_LOGS = []
GLOBAL_TABLES = []

import ConfigParser
import os
import time
import sys
import re
from optparse import OptionParser
from collections import OrderedDict
from subprocess import call


# used to estimate max_length for varchar2 fields
def get_varchar2_size(char_number):
    return min(char_number, 4000)


# return True if my_date is a valid date, False otherwise
def is_valid_date(my_date):
    # creates the python style string for date formatting
    py_date_format = config.get('CSV', 'date_format').replace("DD", "%d").replace("MM", "%m").replace("YYYY", "%Y");
    try:
        valid_date = time.strptime(my_date, py_date_format)
        return True
    except ValueError:
        return False


# return True if my_date is a valid number, False otherwise
def is_valid_number(my_number):
    try:
        float(my_number.replace(config.get('CSV', 'decimal_separator'), '.'))
        return True
    except ValueError:
        return False


# returns the length of integer and decimal part of a number
def number_info(my_number):
    number_parts = my_number.split(config.get('CSV', 'decimal_separator'))
    integer_part = len(number_parts[0])
    if len(number_parts) > 1:
        decimal_part = len(number_parts[1])
    else:
        decimal_part = 0
    return (integer_part, decimal_part)


# creates config file config file for current csv (with informations about columns type)
def create_csv_config_file(csv_file_name, csv_config_file_name):
    global config

    # read csv file to get info about columns
    csv_file = open(csv_file_name, "r")
    csv_data = []

    # loads file header into csv_header
    header = csv_file.readline().strip()
    detect_separator(header)

    if detect_double_quotes(header):
        config.set('CSV', 'double_quotes', 'yes')
    else:
        config.set('CSV', 'double_quotes', 'no')

    if config.getboolean('CSV', 'first_line_is_header'):
        if config.getboolean('CSV', 'double_quotes'):
            csv_header = re.split(config.get('CSV', 'separator') + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", header)
        else:
            csv_header = header.split(config.get('CSV', 'separator'))
    else:
        if config.getboolean('CSV', 'double_quotes'):
            csv_header = re.split(config.get('CSV', 'separator') + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", header)
        else:
            csv_header = header.split(config.get('CSV', 'separator'))
        csv_file.seek(0)

    if csv_header[len(csv_header) - 1] == "":
        csv_header.remove("")

    if config.getboolean('CSV', 'double_quotes'):
        csv_header = list(map(lambda item: item[1:-1], csv_header))

    fix_header_columns(csv_header)

    fields_number = len(csv_header)
    if fields_number == 0:
        return

    csv_fields = []
    for x in range(0, fields_number):
        csv_fields.append([])

    # loops trough the first n lines of current csv to detect data types
    has_data_rows = False
    for line_number in range(0, config.getint('CONFIG', 'csv_lines_to_parse')):
        line = csv_file.readline()
        if not line:
            if not has_data_rows:
                line = config.get('CSV', 'separator') * fields_number
            else:
                break
        has_data_rows = True
        # this is where the magic happens (csv data types are detected)
        # on each iteration the array csv_fields[] is updated with the
        # informations on data types. if no special type (number, date, etc.) is detected
        # or if special types are mixed the resulting type will default to varchar2.
        # csv_fields[] array looks like this:
        # csv_fields[0] = DATE | NUMBER | VARCHAR2
        # if csv_fields[0] = DATE then csv_fields[1] = length(date_string)
        # if csv_fields[0] = NUMBER then csv_fields[1] = length(number_string), csv_fields[2] = length(integer part), csv_fields[3] = length(decimal part)
        # if csv_fields[0] = VARCHAR2 then csv_fields[1] = length(varchar2_string)
        if config.getboolean('CSV', 'double_quotes'):
            values = re.split(config.get('CSV', 'separator') + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", line.strip())
            values = list(map(lambda item: item[1:-1], values))
        else:
            values = line.strip().split(config.get('CSV', 'separator'))
        len_values = len(values)
        if len_values > 0 and values[len_values - 1] == '' and len_values < fields_number - 1:
            GLOBAL_LOGS.append("Row skipped in file: " + csv_file_name + ' having content: ' + values)
            continue
        for index in range(0, fields_number):
            # if field type is not defined yet (first iteration)
            field_length = len(values[index])
            if not csv_fields[index]:
                # date
                if is_valid_date(values[index]):
                    csv_fields[index] = ['DATE', field_length]
                # number
                elif is_valid_number(values[index]):
                    int_len, dec_len = number_info(values[index])
                    csv_fields[index] = ['NUMBER', field_length, int_len, dec_len]
                # general string
                elif field_length <= 4000:
                    csv_fields[index] = ['VARCHAR2', field_length]
                else:
                    csv_fields[index] = ['CLOB', field_length]

            # if field type is date (so far)
            elif csv_fields[index][0] == 'DATE':
                # date
                if is_valid_date(values[index]):
                    csv_fields[index] = ['DATE', field_length]
                # general string
                else:
                    field_length = max([csv_fields[index][1], field_length])
                    if field_length <= 4000:
                        csv_fields[index] = ['VARCHAR2', field_length]
                    else:
                        csv_fields[index] = ['CLOB', field_length]
            # if field type is number (so far)
            elif csv_fields[index][0] == 'NUMBER':
                # number
                if is_valid_number(values[index]):
                    int_len, dec_len = number_info(values[index])
                    field_length = max([csv_fields[index][1], field_length])
                    csv_fields[index] = ['NUMBER', field_length,
                                         max([csv_fields[index][2], int_len]), max([csv_fields[index][3], dec_len])]
                # general string
                else:
                    field_length = max([csv_fields[index][1], field_length])
                    if field_length <= 4000:
                        csv_fields[index] = ['VARCHAR2', field_length]
                    else:
                        csv_fields[index] = ['CLOB', field_length]

            # if field type is varchar2
            elif csv_fields[index][0] == 'VARCHAR2':
                field_length = max([csv_fields[index][1], field_length])
                if field_length <= 4000:
                    csv_fields[index] = ['VARCHAR2', field_length]
                else:
                    csv_fields[index] = ['CLOB', field_length]

    # write config file for current csv
    csv_config = ConfigParser.RawConfigParser(
        dict_type=OrderedDict)  # dict_type=OrderedDict used to keep fields ordered

    # the options in section CSV of global config are copied to the section CONF of the config file for current csv
    csv_config.add_section('CONF')
    for name, value in config.items('CSV'):
        csv_config.set('CONF', name, value.replace('\t', "\\t"))

    # write fields type info to the config file for current csv
    csv_config.add_section('FIELDS')
    if options.manual_config:
        csv_config.set('FIELDS', '# = =-------------------------------------------------------------------- ', '')
        csv_config.set('FIELDS', '# = This section contains fields name and data types that will be used to ', '')
        csv_config.set('FIELDS', '# = create the table on database and the sql loader control file.         ', '')
        csv_config.set('FIELDS', '# = field_name = data_type                                                ', '')
        csv_config.set('FIELDS', '# = You can change here the field name and data types. Allowed types are: ', '')
        csv_config.set('FIELDS', '# = DATE                                                                  ', '')
        csv_config.set('FIELDS', '# = NUMBER integer_part_length decimal_part_length                        ', '')
        csv_config.set('FIELDS', '# = VARCHAR2 max_length                                                   ', '')
        csv_config.set('FIELDS', '# = --------------------------------------------------------------------- ', '')

    for index in range(0, fields_number):
        if csv_fields[index][0] == 'DATE':
            value = 'DATE'
        elif csv_fields[index][0] == 'NUMBER':
            value = 'NUMBER' + ' ' + str(
                csv_fields[index][2] + int(config.get('CONFIG', 'add_integer_length'))) + ' ' + str(
                csv_fields[index][3] + int(config.get('CONFIG', 'add_decimal_length')))
        elif csv_fields[index][0] == 'VARCHAR2':
            value = 'VARCHAR2' + ' ' + str(
                get_varchar2_size(csv_fields[index][1] + int(config.get('CONFIG', 'add_varchar_length'))))
        elif csv_fields[index][0] == "CLOB":
            value = 'CLOB' + ' ' + str(csv_fields[index][1] + int(config.get('CONFIG', 'add_clob_length')))

        csv_config.set('FIELDS', csv_header[index].upper().replace(' ', '_'), value)

    csv_conf_file = open(csv_config_file_name, "wb")
    csv_config.write(csv_conf_file)
    csv_conf_file.close()


# creates the sqlldr control file
def create_ctl_file(fileName):
    ctl_string = ''
    if csv_config.getboolean('CONF', 'first_line_is_header'):
        ctl_string += 'OPTIONS (skip = 1)\n'
    ctl_string += 'LOAD DATA\n'
    # base_dir = os.path.dirname(os.path.abspath(__file__))
    ctl_string += "INFILE '%s'\n" % (fileName)
    ctl_string += 'BADFILE %s%s\n' % (os.path.splitext(os.path.basename(fileName))[0], '.bad')
    if csv_config.getboolean('CONF', 'append'):
        ctl_string += 'APPEND\n'
    ctl_string += 'INTO TABLE %s\n' % suggest_name(os.path.splitext(fileName)[0])
    if csv_config.get('CONF', 'separator') == "\\t":
        ctl_string += 'FIELDS TERMINATED BY "' + "\t" + '"\n'
    else:
        ctl_string += 'FIELDS TERMINATED BY "' + csv_config.get('CONF', 'separator') + '"\n'

    if csv_config.get('CONF', 'double_quotes') == 'yes':
        ctl_string += 'OPTIONALLY ENCLOSED BY \'"\'\n'

    ctl_string += 'TRAILING NULLCOLS\n'
    ctl_string += '(\n'

    fields = OrderedDict(csv_config.items('FIELDS'))
    index = 0
    # loop over fields
    for key in fields:
        values = fields[key].split(' ')
        if values[0] == 'DATE':
            field_str = key + ' ' + 'DATE "' + csv_config.get('CONF', 'date_format') + '"'
        elif values[0] == 'NUMBER':
            field_str = key
            if config.get('DATABASE', 'decimal_separator') <> csv_config.get('CONF', 'decimal_separator'):
                field_str += ' "REPLACE(:' + key + ", '" + csv_config.get('CONF',
                                                                          'decimal_separator') + "', '" + config.get(
                    'DATABASE', 'decimal_separator') + "'" + ')"'
        elif values[0] == 'VARCHAR2' or values[0] == 'CLOB':
            field_str = key + ' char(' + values[1] + ') NULLIF ' + key + '=BLANKS'

        if index <> len(fields) - 1:
            field_str += ','

        field_str += '\n'
        ctl_string += field_str
        index = index + 1

    ctl_string += ')\n'

    ctl_file = open(
        config.get('CONFIG', 'output_dir') + os.sep + os.path.splitext(os.path.basename(fileName))[
            0] + '.' + config.get('CONFIG', 'control_file_extension'),
        "w")
    ctl_file.write(ctl_string)
    ctl_file.close()


# creates a sql text file containing the "create table" statement
def create_sql_file(fileName):
    sql_string = ''
    table_name = suggest_name(os.path.splitext(fileName)[0])

    GLOBAL_LOGS.append("Table Name: " + table_name)
    GLOBAL_TABLES.append("SELECT * FROM " + config.get('DATABASE', 'user') + "." + table_name + "\n/")

    if config.get('DATABASE', 'schema_name') != "":
        table_name = config.get('DATABASE', 'schema_name') + "." + table_name

    if not csv_config.getboolean('CONF', 'append'):
        sql_string += 'DROP TABLE %s;\n\n' % table_name
    sql_string += 'CREATE TABLE %s\n(\n' % table_name
    fields = OrderedDict(csv_config.items('FIELDS'))

    index = 0
    # loop over fields
    for key in fields:
        values = fields[key].split(' ')
        if values[0] == 'DATE':
            field_str = key.upper() + ' ' + 'DATE'
        elif values[0] == 'NUMBER':
            field_str = key.upper() + ' ' + 'NUMBER(' + str(int(values[1]) + int(values[2])) + ',' + values[2] + ')'
        elif values[0] == 'VARCHAR2':
            field_str = key.upper() + ' ' + 'VARCHAR2(' + values[1] + ')'
        elif values[0] == 'CLOB':
            field_str = key.upper() + ' ' + 'CLOB'

        if index <> len(fields) - 1:
            field_str += ','

        field_str += '\n'
        sql_string += field_str
        index = index + 1

    sql_string += ');\n\nquit;\n'

    sql_file = open(
        config.get('CONFIG', 'output_dir') + os.sep + fileName + '.' + config.get('CONFIG', 'sql_file_extension'), "w")
    sql_file.write(sql_string)
    sql_file.close()


# creates a batch file for running sqlldr
def create_batch_file(fileName):
    # locates the sqlplus executable
    if config.has_option('CONFIG', 'path_to_sqlplus_executable'):
        sqlplus_executable = config.get('CONFIG', 'path_to_sqlplus_executable') + os.sep + config.get('CONFIG',
                                                                                                      'sqlplus_executable')
    else:
        sqlplus_executable = config.get('CONFIG', 'sqlplus_executable')

    batch_string = sqlplus_executable
    batch_string += ' %s/%s' % (config.get('DATABASE', 'user'), config.get('DATABASE', 'password'))
    batch_string += '@'
    if config.get('DATABASE', 'tns') != "":
        batch_string += '%s' % (config.get('DATABASE', 'tns'))
    else:
        batch_string += '%s:%s/%s' % (
            config.get('DATABASE', 'host'), config.get('DATABASE', 'port'), config.get('DATABASE', 'service_name'))
    batch_string += ' @%s.%s\n' % (fileName, config.get('CONFIG', 'sql_file_extension'))

    # locates the sqlldr executable
    if config.has_option('CONFIG', 'path_to_sqlldr_executable'):
        sqlldr_executable = config.get('CONFIG', 'path_to_sqlldr_executable') + os.sep + config.get('CONFIG',
                                                                                                    'sqlldr_executable')
    else:
        sqlldr_executable = config.get('CONFIG', 'sqlldr_executable')

    batch_string += sqlldr_executable
    batch_string += ' %s/%s' % (config.get('DATABASE', 'user'), config.get('DATABASE', 'password'))
    batch_string += '@'
    if config.get('DATABASE', 'tns') != "":
        batch_string += '%s' % (config.get('DATABASE', 'tns'))
    else:
        batch_string += '%s:%s/%s' % (
        config.get('DATABASE', 'host'), config.get('DATABASE', 'port'), config.get('DATABASE', 'service_name'))
    batch_string += ' control=%s.%s' % (fileName, config.get('CONFIG', 'control_file_extension'))
    batch_string += ' %s' % config.get('CONFIG', 'sqlldr_options')
    if config.has_option('CONFIG', 'run_after_loading'):
        batch_string += '\n%s' % config.get('CONFIG', 'run_after_loading')

    batch_file = open(
        config.get('CONFIG', 'output_dir') + os.sep + fileName + '.' + config.get('CONFIG', 'batch_file_extension'),
        "w")
    # batch_file = open(fileName + '.' + config.get('CONFIG', 'batch_file_extension'), "w")
    batch_file.write(batch_string)
    batch_file.close()


# creates the table and loads the csv data into database
def load_data(fileName):
    current_dir = os.getcwd()
    os.chdir(config.get('CONFIG', 'output_dir'))
    call([fileName + '.' + config.get('CONFIG', 'batch_file_extension'), ''])
    os.chdir(current_dir)


def process_file(fileName):
    global csv_config

    print ('Processing file: ' + fileName)
    GLOBAL_LOGS.append('Processing file: ' + fileName)

    # config file for current csv
    csv_config = ConfigParser.ConfigParser()

    # full path of the temporary and saved config file for current csv (if they exists)
    tmp_csv_config_file = config.get('CONFIG', 'output_dir') + os.sep + os.path.splitext(os.path.basename(fileName))[
        0] + '.' + config.get(
        'CONFIG', 'config_file_extension')
    saved_csv_config_file = config.get('CONFIG', 'saved_dir') + os.sep + os.path.splitext(os.path.basename(fileName))[
        0] + '.' + config.get(
        'CONFIG', 'config_file_extension')

    if options.manual_config:
        # creates the manual config file for current csv, if it doesn't exists
        create_csv_config_file(fileName, saved_csv_config_file)
        csv_config_file = saved_csv_config_file
        print ('  Manual csv configuration file created: ' + saved_csv_config_file + '.')
        print ('  You can edit the file to customize configuration')
        print ('  (field types and column names) and run this program')
        print ('  without -m option to load csv data with custom  configuration')

    else:
        # find the config file for current csv, or create it if it doesn't exists
        if (os.path.exists(saved_csv_config_file)):
            csv_config_file = saved_csv_config_file
            print ('  Manual csv configuration file loaded: ' + saved_csv_config_file)

        else:
            # creates the config file for current csv, if it doesn't exists
            create_csv_config_file(fileName, tmp_csv_config_file)
            csv_config_file = tmp_csv_config_file
            print ('  Automatic csv configuration file created: ' + tmp_csv_config_file)

    # reads the config file for current csv
    csv_config.read(csv_config_file)

    if not options.manual_config:
        # creates the sqlldr control file
        create_ctl_file(os.path.basename(fileName))
        print ('  Control file created')
        GLOBAL_LOGS.append('Control file created')

        # creates a sql text file containing the "create table" statement
        create_sql_file(os.path.splitext(os.path.basename(fileName))[0])
        print ('  Sql file created')
        GLOBAL_LOGS.append('Sql file created')

        # creates a batch file for running sqlldr
        create_batch_file(os.path.splitext(os.path.basename(fileName))[0])
        print ('  Batch file created')
        GLOBAL_LOGS.append('Batch file created')

        if not options.no_load:
            # creates the table and loads the csv data into database
            load_data(os.path.splitext(os.path.basename(fileName))[0])
            print ('  CSV Data loaded')
            GLOBAL_LOGS.append('CSV Data loaded')
        else:
            print ('  Run the batch file to load CSV data to database')
            GLOBAL_LOGS.append('Run the batch file to load CSV data to database')


# detect if double quoted csv
def detect_double_quotes(header):
    if header.startswith('"') and header.endswith('"'):
        return True
    return False


# detect separator
def detect_separator(header):
    separator = config.get('CSV', 'separator')
    separated_items = header.split(separator)
    if len(separated_items) < 2:
        for sep in GLOBAL_SEPARATORS:
            if sep != separator:
                separated_items = header.split(sep)
                if len(separated_items) > 1:
                    separator = sep
                    config.set('CSV', 'separator', separator)
                    break


# header column name fixer
def fix_header_columns(columns):
    length = len(columns)
    for i in range(length):
        columns[i] = suggest_name(columns[i])


# trim column name upto 30 char, max limit for Table and Column name
def suggest_name(name):
    name = name.replace('-', '_').replace(' ', '_')
    name = transform_to_db_name(name)
    length = len(name)
    if length <= 30:
        if name not in GLOBAL_RESERVE_WORDS:
            return name.upper()
        else:
            return reserve_word_fix(name, length)
    return short_by_length(name)


def transform_to_db_name(name):
    if len(name.split("_")) == 1:
        idx_from = -1
        idx_to = -1
        suggested_name = []
        length = len(name)
        for idx, val in enumerate(name):
            if not val.isalnum():
                continue
            if idx_from == -1:
                idx_from = idx
            is_last = (idx == length - 1)
            if idx > 0:
                is_lower = val.islower() or val.isdigit()
                if is_lower and not is_last and name[idx + 1].isupper():
                    idx_to = idx + 1
                elif val.isupper() and not is_last and name[idx + 1].islower():
                    idx_to = idx
                elif is_last:
                    idx_to = idx + 1
            if idx_from != -1 and idx_to != -1 and idx_to - idx_from > 0:
                suggested_name.append(name[idx_from:idx_to].upper())
                if idx_to == idx:
                    idx_from = idx
                else:
                    idx_from = -1
                idx_to = -1

        name = '_'.join(suggested_name)
    return name.upper()


def reserve_word_fix(name, length):
    while length > 26:
        name = short_by_acronym(name, length)

    return "TBL_" + name.upper()


def short_by_acronym(name, length):
    if name in GLOBAL_COMMON_ACRONYM:
        return GLOBAL_COMMON_ACRONYM[name]
    for key in GLOBAL_COMMON_ACRONYM:
        if name.index(key) > -1:
            return ''.replace(key, GLOBAL_COMMON_ACRONYM[key])
    return name[:length - 1]


def short_by_length(name):
    while len(name) > 30:
        max_len = 0
        max_val = ""
        max_idx = 0
        splitted_name = name.split("_")
        for idx, val in enumerate(splitted_name):
            if len(val) > max_len:
                max_len = len(val)
                max_val = val
                max_idx = idx
        splitted_name[max_idx] = short_by_acronym(max_val, max_len)
        name = '_'.join(splitted_name)
    return name


def main(argv):
    global args, config, options

    # parse command line arguments
    parser = OptionParser()
    parser.add_option("-f", "--file", type="string", dest="fileName", default=False,
                      help="loads only the given file, default is all files", metavar="FILE")
    parser.add_option("-m", "--manual-config", action="store_true", dest="manual_config", default=False,
                      help="creates a csv configuration file that can be manually edited before loading data")
    parser.add_option("-n", "--no-load", action="store_true", dest="no_load", default=False,
                      help="creates the batch file to load the data but doesn't load the data automatically")
    (options, args) = parser.parse_args()

    # reads the global configuration file
    config = ConfigParser.ConfigParser()
    config.read(GLOBAL_CONFIG_FILE)

    # baseDir = os.getcwd()
    # print (baseDir)

    dir_log_file = os.path.join(config.get('CONFIG', 'output_dir'), "log.txt")
    if os.path.exists(dir_log_file):
        os.remove(dir_log_file)

    if options.fileName:
        # processes only the given file
        process_file(options.fileName)
    else:
        # processes all files in csv_files_dir with extension included in csv_extentions parameters
        for csv_file_name in os.listdir(config.get('CONFIG', 'csv_files_dir')):
            if "-" in csv_file_name or " " in csv_file_name:
                replaced_name = csv_file_name.replace('-', '_').replace(' ', '_')
                os.rename(os.path.join(config.get('CONFIG', 'csv_files_dir'), csv_file_name),
                          os.path.join(config.get('CONFIG', 'csv_files_dir'), replaced_name))
                csv_file_name = replaced_name
            for extension in config.get('CONFIG', 'csv_extentions').split(' '):
                if csv_file_name[-len(extension):].upper() == extension.upper():
                    try:
                        process_file(config.get('CONFIG', 'csv_files_dir') + os.sep + csv_file_name)
                    except:
                        exception_info = "Exception Occurred >> " + csv_file_name + ": " + sys.exc_info().__str__()
                        GLOBAL_LOGS.append(exception_info)
                        print(exception_info)
                    # list down all tables created
                    GLOBAL_LOGS.append('\n')
                    with open(dir_log_file, 'a') as log_file:
                        log_file.write("\n".join(GLOBAL_LOGS))
                        del GLOBAL_LOGS[:]
        with open(dir_log_file, 'a') as log_file:
            log_file.write("\n--TABLES--\n")
            log_file.write("\n".join(GLOBAL_TABLES))
            del GLOBAL_TABLES[:]


# global variables
args = None
config = None
csv_config = None
options = None

if __name__ == "__main__":
    main(sys.argv)
