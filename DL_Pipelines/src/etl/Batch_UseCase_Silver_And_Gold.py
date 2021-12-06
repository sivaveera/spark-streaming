import argparse
from pytz import timezone
from datetime import datetime
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import requests
import traceback
import json
import sys
import os

# us_tz = timezone('America/Los_Angeles')
us_tz = timezone('UTC')
us_time = datetime.now(us_tz)
print(us_time)

def get_current_datetime():
    return us_time.strftime('%Y-%m-%d %H:%M:%S')

def get_or_create_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()

#Print formatted message to driver logs
def print_msg(msg, job_name=None):
    msg = "DL LOG | {0} | {1} | {2}".format(get_current_datetime(), job_name, msg)
    msg_no = len(msg)
    print(msg_no * '#')
    print(msg)
    print(msg_no * '#')


def get_current_user():
    return 'DL User'

#Get Parameters from DL Config file
def get_dl_configs(job_name):
    args_dict = {}
    args_dict_new = {}
    config_df = get_or_create_spark().sql(
        "SELECT * FROM config_delta.dl_jobs_config WHERE JOB_NAME='{}'".format(job_name))
    config_map = map(lambda row: row.asDict(), config_df.collect())
    for f in config_map:
        args_dict = f

    for key in args_dict.keys():
        args_dict_new[key.lower()] = args_dict[key]
    return args_dict_new

# Parse the job parameters
def parse_arguments():
    argparser = argparse.ArgumentParser(description="DL Stream Job")
    argparser.add_argument('--job_name', type=str, required=True)
    argparser.add_argument('--count_flag_for_debug', default=False, type=lambda x: (str(x).lower() == 'true'))
    args = argparser.parse_args()

    job_name = args.job_name
    args_dict = get_dl_configs(job_name)
    args_dict['count_flag_for_debug'] = args.count_flag_for_debug

    print_msg('Parameters: {0}'.format(args_dict), job_name)
    return args_dict


def log_audit_status(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count,
                     exception_ind,
                     exception_dtl, rec_create_timestamp, rec_create_user_name):
    exception_dtl = exception_dtl.replace("'", "")
    #
    insert_stmt = """  INSERT INTO delta_logs.audit_logs
    VALUES('{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}', '{}')
    """.format(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count,
               exception_ind,
               exception_dtl, rec_create_timestamp, rec_create_user_name)
    get_or_create_spark().sql(insert_stmt)

# Execute Sql Statements
def execute_usecase(args_dict: dict, sql_stmt_path):
    job_name = args_dict['job_name']
    input_sql_df = get_or_create_spark().sparkContext.textFile(sql_stmt_path)
    sql_df_without_comments = input_sql_df.filter(lambda line: not line.lstrip().startswith('--'))
    sql_str = " ".join((sql_df_without_comments).collect())
    print(sql_str)
    sql_str_list = sql_str.split(';')
    #[:-3] to exclude last 3 elements as the last str will always be empty
    # All sql gets executed except till 3rd last
    counter = 0
    for sql_str in sql_str_list[:-3]:
        counter = counter + 1
        print(f'counter :: {counter}')
        print(sql_str)
        sql_str_without_whitespace = sql_str.strip()
        if sql_str_without_whitespace.lower().startswith("select") and args_dict['count_flag_for_debug']:
            get_or_create_spark().sql(sql_str_without_whitespace).show(1)
        if "DL_JOB_NAME" in sql_str_without_whitespace:
            replaced_str = sql_str_without_whitespace.replace("DL_JOB_NAME", job_name)
            print("Changed SQL::")
            print(replaced_str)
            get_or_create_spark().sql(replaced_str)
        else:
            get_or_create_spark().sql(sql_str_without_whitespace)
    else:
        # The 2nd last sql statement will contains the final intermediate table count gets executed,
        # this will be updated as target rowcount in audit table
        # The 3rd last sql statement will contain the initial input row count
        #this will be updated as input row count in audit table
        target_row_count_query = sql_str_list[-2]
        print(F"Target rowcount query {target_row_count_query}")
        input_row_count_query = sql_str_list[-3]
        print(F"Input rowcount query {input_row_count_query}")
        target_count = get_or_create_spark().sql(target_row_count_query.strip()).first()['row_count']
        input_row_count = get_or_create_spark().sql(input_row_count_query.strip()).first()['row_count']
        print("input_row_count :: {0} and target_row_count :: {1}".format(input_row_count, target_count))
    return input_row_count,target_count

if __name__ == "__main__":
    print_msg('Job Started @ {0}'.format(get_current_datetime()))
    try:
        args_dict = parse_arguments()
        print_msg("Arguments Parsed", args_dict['job_name'])
        # Get the input statement or the Merge Statement
        input_sql_stmt_path = args_dict['input_sql_stmt']
        # Execute Input SQL Statement
        if input_sql_stmt_path:
            try:
                input_row_count,target_row_count = execute_usecase(args_dict, input_sql_stmt_path)
                print_msg("Input Sql Statement Executed", input_sql_stmt_path)

                log_audit_status(args_dict['job_name'], 'S3', args_dict['source_type'], args_dict['dataset_name'], args_dict['trgt_table_name'], input_row_count,
                         target_row_count, 0, "", get_current_datetime(), get_current_user())
                print_msg("Loaded audit details into audit table", args_dict['job_name'])
            except Exception as ex:
                log_audit_status(args_dict['job_name'], 'S3', args_dict['source_type'], args_dict['dataset_name'], args_dict['trgt_table_name'], 0, 0, 1,
                         str(ex),
                         get_current_datetime(),
                         get_current_user())
                print_msg("Loaded Exception into audit table", args_dict['job_name'])
                raise ex
    except Exception as ex:
        traceback.print_exception(*sys.exc_info())
        print_msg('Job Failed @ {0}'.format(get_current_datetime()))
        raise ex
    print_msg('Job Ended @ {0}'.format(get_current_datetime()))