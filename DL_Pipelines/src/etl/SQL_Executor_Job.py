import argparse
from pytz import timezone
from datetime import datetime
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import *
# from delta.tables import *
import traceback
import json
import sys
import os

# us_tz = timezone('America/Los_Angeles')
us_tz = timezone('UTC')
us_time = datetime.now(us_tz)
print(us_time)


# Initialize Spark Session
def get_or_create_spark() -> SparkSession:
    return SparkSession.builder.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0").config('spark.sql.session.timeZone',
                                                                                                       'UTC').getOrCreate()


# This function can be used get current timestamp
def get_current_datetime():
    return us_time.strftime('%Y-%m-%d %H:%M:%S')


def get_date_stamp():
    tz = timezone.timezone('America/Los_Angeles')
    current_day = datetime.now(tz)
    datestamp = str(current_day.strftime("%Y%m%d"))
    return datestamp


# This function can be used print the output with current timestamp and the format
def print_msg(msg, job_name=None):
    msg = "DL LOG | {0} | {1} | {2}".format(get_current_datetime(), job_name, msg)
    msg_no = len(msg)
    print(msg_no * '#')
    print(msg)
    print(msg_no * '#')


# This function has been used to parse the parameters
def parse_arguments(sysargv=None):
    argparser = argparse.ArgumentParser(description="Parameterized SQL Job")
    argparser.add_argument('--job_name', type=str)
    argparser.add_argument('--sql_query', type=str)
    argparser.add_argument('--output_location', type=str)
    argparser.add_argument('--trgt_table_name', type=str)
    argparser.add_argument('--write_mode', type=str)
    if sysargv:
        args = argparser.parse_args(sysargv[1:])
    else:
        args = argparser.parse_args()
    args_dict = vars(args)

    return args_dict


def execute_sql_job(spark: SparkSession, args_dict: dict):
    sql_query: str = args_dict['sql_query']
    try:
        result_df = spark.sql(sql_query)
        write_mode = args_dict['write_mode']
        if write_mode not in ('append', 'overwrite'):
            write_mode = 'append'

        if args_dict['output_location']:
            result_df.write.format("delta").mode(write_mode).save(args_dict['output_location'])
        
        if args_dict['trgt_table_name']:
            ddl_stmt = "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}' ".format(args_dict['trgt_table_name'], args_dict['output_location'])
            spark.sql(ddl_stmt)

    except Exception as ex:
        print_msg('Error executing SQL job', args_dict['job_name'])
        print_msg('SQL Query: {0}'.format(sql_query))
        raise ex


if __name__ == "__main__":
    print_msg('Job Started @ {0}'.format(get_current_datetime()))
    try:
        dev_envt = os.environ.get('enable_local_dev')
        if dev_envt == 'true':
            print_msg('Starting Job in Dev Mode')
            args_dict = parse_arguments(sys.argv)
        else:
            from delta.tables import *
            args_dict = parse_arguments(None)

        print_msg("Arguments Parsed: {0}".format(args_dict))
        print_msg("Calling Parameterized SQL Job", args_dict['job_name'])

        spark = get_or_create_spark()
        execute_sql_job(spark, args_dict)
    except Exception as ex:
        traceback.print_exception(*sys.exc_info())
        print_msg('Job Failed @ {0}'.format(get_current_datetime()))
        raise ex
    print_msg('Job Ended @ {0}'.format(get_current_datetime()))
