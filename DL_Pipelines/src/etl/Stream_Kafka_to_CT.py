import argparse
from pytz import timezone
from datetime import datetime
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.avro.functions import from_avro, to_avro
import requests
from requests.auth import HTTPBasicAuth
from confluent_kafka.schema_registry import SchemaRegistryClient
import traceback
import json
import sys
import boto3
import os

# us_tz = timezone('America/Los_Angeles')
us_tz = timezone('UTC')
us_time = datetime.now(us_tz)
print(us_time)


# Initialize Spark Session
def get_or_create_spark() -> SparkSession:
    # return SparkSession.builder.config("spark.jars.packages", "io.delta:delta-core_2.11:0.5.0").getOrCreate()
    return SparkSession.builder.getOrCreate()


# This function can be used get current timestamp
def get_current_datetime():
    return us_time.strftime('%Y-%m-%d %H:%M:%S')


# This function can be used print the output with current timestamp and the format
def print_msg(msg, job_name=None):
    msg = "DL LOG | {0} | {1} | {2}".format(get_current_datetime(), job_name, msg)
    msg_no = len(msg)
    print(msg_no * '#')
    print(msg)
    print(msg_no * '#')


# The function that can be used get current user
def get_current_user():
    return 'DL User'


# This function can be used to all the parameters that are required for current run
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


# This function has been used to fetch the keys and secrets from the param stores
def get_variables_from_param_store() -> dict:
    env = dbutils.secrets.get("ssm_credentials", "env")
    aws_key = dbutils.secrets.get("ssm_credentials", "ssm_key")
    aws_secret = dbutils.secrets.get("ssm_credentials", "ssm_secret")

    env = env.lower()
    session = boto3.Session(region_name="us-west-2")
    ssm = session.client('ssm', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

    params = [
        '/elvis/dl/schemaregistry/{0}/url'.format(env),
        '/elvis/dl/schemaregistry/{0}/key'.format(env),
        '/elvis/dl/schemaregistry/{0}/secret'.format(env),
        '/elvis/dl/kafkabroker/{0}/key'.format(env),
        '/elvis/dl/kafkabroker/{0}/secret'.format(env),
        '/elvis/dl/dbfsenv/{0}/domain'.format(env),
        '/elvis/dl/dbfsenv/{0}/token'.format(env)
    ]

    response = ssm.get_parameters(Names=params, WithDecryption=True)

    all_parameters = {}
    for resp in response['Parameters']:
        all_parameters[resp['Name'].split('/')[3] + '_' + resp['Name'].split('/')[-1]] = resp['Value']

    return all_parameters


# This function has been used to parse the parameters
def parse_arguments():
    argparser = argparse.ArgumentParser(description="DL Stream Job")
    argparser.add_argument('--job_name', type=str, required=True)
    args = argparser.parse_args()

    job_name = args.job_name
    args_dict = get_dl_configs(job_name)

    print_msg('Parameters: {0}'.format(args_dict), job_name)
    return args_dict


# This function can be used to get KafKa credentials
def get_kafka_secrets():
    key = dbutils.secrets.get("kafka_broker", "broker_key")
    secret = dbutils.secrets.get("kafka_broker", "broker_secret")

    # params = get_variables_from_param_store()
    # key = params['kafkabroker_key']
    # secret = params['kafkabroker_secret']

    return key, secret


# This function can be used to get AVRO schema to parse the data
def get_avro_schema(topic_name: str):
    key = dbutils.secrets.get("schema_registry", "schema_registry_key")
    secret = dbutils.secrets.get("schema_registry", "schema_registry_secret")
    schema_registry_url = dbutils.secrets.get("schema_registry", "schema_registry_url")

    # params = get_variables_from_param_store()
    # key = params['schemaregistry_key']
    # secret = params['schemaregistry_secret']
    # schema_registry_url = params['schemaregistry_url']

    schema_registry_conf = {
        'url': schema_registry_url,
        'basic.auth.user.info': '{}:{}'.format(key, secret)}

    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_rest_response_schema = schema_registry_client.get_latest_version(topic_name + "-value").schema
    confluent_value_schema = value_rest_response_schema.schema_str

    return confluent_value_schema


# This function can be used to log the executions metrics into the logging table
def log_streaming_query_status(result):
    insert_stmt = """  INSERT INTO jobs_logs.streaming_audits
  VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, '{}')
  """.format(result['id'], result['runId'], result['batchId'], result['name'], result['sources'][0]['description'],
             result['sink']['description'], result['numInputRows'], result['sink']['numOutputRows'],
             result['timestamp'])
    get_or_create_spark().sql(insert_stmt)


# This function can be used to log the executions metrics into the audit table
def log_audit_status(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count, exception_ind,
                     exception_dtl, rec_create_timestamp, rec_create_user_name):
    exception_dtl = exception_dtl.replace("'","")
    insert_stmt = """  INSERT INTO delta_logs.audit_logs
    VALUES('{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}', '{}')
    """.format(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count, exception_ind,
               exception_dtl, rec_create_timestamp, rec_create_user_name)
    get_or_create_spark().sql(insert_stmt)


def load_schema(path) -> StructType:
    """
    Generates schema from Apache Spark formatted JSON file
    """
    json_str = " ".join(SparkSession.builder.getOrCreate().sparkContext.textFile(path).collect())
    return StructType.fromJson(json.loads(json_str))


# This function has been used to create delta tables/views if not exists
def create_delta_table(args_dict: dict, output_schema):
    """
    If output does not exist then create a delta table there otherwise return
    """
    # Create Delta output folder if does not exists
    try:
        get_or_create_spark().read.format("delta").load(args_dict['output_location'])
    except Exception as ex:
        empty_df = get_or_create_spark().createDataFrame([], output_schema)
        empty_df.write.format("delta").partitionBy("ingest_date").save(args_dict['output_location'])

    # Create Delta Table if does not exists
    try:
        get_or_create_spark().sql("SELECT 1 FROM {0} WHERE 1 = 2".format(args_dict['trgt_table_name']))
    except:
        ddl_stmt = "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}' ".format(args_dict['trgt_table_name'],
                                                                                       args_dict['output_location'])
        try:
            get_or_create_spark().sql(ddl_stmt)
        except Exception as ex:
            print_msg('Warning: ' + str(ex), args_dict['job_name'])

    src_table_name = args_dict['trgt_table_name']
    trgt_name = src_table_name
    trgt_name = trgt_name.replace('_ct_', '_hist_')

    sql_stmt = """CREATE OR REPLACE VIEW  {0} AS 
      SELECT data.*,  
      CASE 
        WHEN headers.operation = 'REFRESH' THEN CAST(ingest_date AS TIMESTAMP)
        ELSE headers.timestamp 
      END AS rec_create_timestamp, 
      'DL User' AS rec_create_user_name, 
      CASE 
        WHEN headers.operation = 'REFRESH' THEN CAST(ingest_date AS TIMESTAMP)
        ELSE headers.timestamp 
      END AS rec_update_timestamp,
      'DL User' AS rec_update_user_name, 
      CASE 
        WHEN headers.operation = 'REFRESH' THEN CAST(REPLACE(ingest_date, '-', '') AS INT)
        ELSE CAST(REPLACE(LEFT(headers.timestamp, 10), '-', '') AS INT)
      END AS rec_create_date_key,
      CASE 
        WHEN headers.operation = 'REFRESH' THEN 120000
        ELSE CAST(REPLACE(SUBSTR(headers.timestamp,12, 5), ':', '') AS INT)
      END AS rec_create_time_key,
      headers.operation AS rec_crud_text,
      CASE WHEN headers.operation = 'DELETE' THEN 1 ELSE 0 END AS delete_ind,
      CAST(NULL AS INT) AS exception_ind,
      CAST(NULL AS INT) AS resolved_ind
      FROM {1}""".format(trgt_name, src_table_name)
    try:
        get_or_create_spark().sql(sql_stmt)
        print('View created ', trgt_name)
    except Exception as ex:
        print_msg('Warning: ' + str(ex), args_dict['job_name'])


# This function has been used to for PII data masking
def encrypt_data(df: DataFrame, args_dict: dict) -> DataFrame:
    # 1. Get data tag to read columns and schema required for performing masking.
    data_df = df.selectExpr("data.*")
    output_schema = data_df.schema.json()
    output_schema_struct = StructType.fromJson(json.loads(output_schema))

    # 2. Get columns to be masked from the config
    cols_to_mask = args_dict['columns_to_be_masked']
    cols_to_mask = cols_to_mask.split(',')
    cols_to_mask = [col.strip() for col in cols_to_mask]

    # 3. Create select query with sha2() function for columns to be masked
    select_expression = ""
    for column in data_df.columns:
        if column not in cols_to_mask:
            select_expression += "data." + column + "|"
        else:
            select_expression += "sha2(cast(data." + column + " as string),256) as " + column + "|"

    select_expression = select_expression.strip()
    if (select_expression[-1:]).endswith('|'):
        select_expression = select_expression[:len(select_expression) - 1]

    # 4. Get dataframe with headers, beforeData and flattened data columns
    select_expression = select_expression.split('|')
    select_expression.append('headers')
    select_expression.append('beforeData')
    select_expression.append('ingest_date')
    encrypted_df = df.selectExpr(select_expression)

    # 5. Drop headers and beforeData from the final dataframe
    formatted_cols = encrypted_df.columns
    formatted_cols.remove('headers')
    formatted_cols.remove('beforeData')
    formatted_cols.remove('ingest_date')

    # 6. Separate flattened data columns to encapsulate them in a single data tag
    data_col_tag = []
    for column in formatted_cols:
        data_col_tag.append(col(column))

    # 7. Form dataframe with encapsulated data, beforeData and headers tag
    encrypted_df = encrypted_df.select(from_json(to_json(struct(data_col_tag)), output_schema_struct).alias("data"),
                                       "beforeData", "headers", "ingest_date")

    return encrypted_df

# This function has been used to initiate read stream and the streaming source
def read_stream(args_dict: dict):
    # Get Kafka connection details
    key, secret = get_kafka_secrets()

    # Create Read Stream from Kafka Topic
    df = (
        get_or_create_spark()
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args_dict["kafka_server"])
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username='{0}' password='{1}';".format(
                        key, secret))
            .option("kafka.ssl.endpoint.identification.algorithm", "https")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("subscribe", args_dict["kafka_topic"])
            .option("startingOffsets", "earliest")
            .load()
            .withColumn('fixed_value', expr("substring(value, 6, length(value)-5)"))
            .selectExpr('fixed_value', 'to_date(CAST(timestamp AS STRING)) as ingest_date')
    )

    print_msg('Schema: {0}'.format(df.printSchema), args_dict['job_name'])
    return df


# This function has been called for every batch and a set of operations have been performed on data
def parse_avro_data(df, epoch_id):
    print_msg("Parsing Avro Data", args_dict_glb['job_name'])
    # Get the schemas from the schema registry
    confluent_value_schema = get_avro_schema(topic_name=args_dict_glb['kafka_topic'])

    # Set the option for what to do with corrupt data - either stop on the first failure it finds (FAILFAST)
    # or just set corrupt data to null (PERMISSIVE)
    source_type = args_dict_glb['kafka_topic']
    if source_type:
        source_type = 'KAFKA'
    else:
        source_type = 'FILE'

    row_count = 0
    try:
        # from_avro_options = {"mode": "FAILFAST"}
        from_avro_options = {"mode": "PERMISSIVE"}

        print_msg("Writing Data to {0}".format(args_dict_glb['output_location']), args_dict_glb['job_name'])

        # Parse the Avro data, break out the three columns and write the micro-batch
        parsed_df = df.select(from_avro('fixed_value', confluent_value_schema, from_avro_options).alias('parsed_value'),
                              'ingest_date')
        parsed_df = parsed_df.select("parsed_value.data", "parsed_value.beforeData", "parsed_value.headers", "ingest_date")

        # Perform masking if necessary
        if args_dict['columns_to_be_masked']:
            parsed_df = encrypt_data(parsed_df, args_dict)

        # Create Delta Table if does not exists
        output_schema = parsed_df.schema.json()
        output_schema = StructType.fromJson(json.loads(output_schema))

        try:
            create_delta_table(args_dict_glb, output_schema)
        except Exception as ex:
            print_msg('Warning: ' + str(ex), args_dict['job_name'])

        # Get the row count
        row_count = parsed_df.count()
        # Write the avro data to output location
        (
            parsed_df
                .write
                .format("delta")
                .mode("append")
                .partitionBy("ingest_date")
                .option("mergeSchema", "true")
                .save(args_dict_glb["output_location"])
        )
        log_audit_status(args_dict_glb['job_name'], source_type, args_dict_glb['kafka_topic'], 'S3',
                         args_dict_glb['output_location'], row_count, row_count, 0, "", get_current_datetime(), get_current_user())
    except Exception as ex:
        log_audit_status(args_dict_glb['job_name'], source_type, args_dict_glb['kafka_topic'], 'S3',
                         args_dict_glb['output_location'], row_count, 0, 1, str(ex), get_current_datetime(), get_current_user())
        raise ex
    
    # Create Delta Table if does not exists
    output_schema = parsed_df.schema.json()

    try:
        create_delta_table(args_dict_glb, output_schema)
    except Exception as ex:
        print_msg('Warning: ' + str(ex), args_dict['job_name'])


# This function has been used to initiate write stream
def write_stream(target_df, args_dict):
    global args_dict_glb
    args_dict_glb = args_dict

    # Create Write Stream and call foreachBatch
    print_msg("In Write Stream Method", args_dict['job_name'])
    processing_time_args = args_dict["trigger_processing_time"].lower()
    processing_time = None
    trigger_once = None
    if processing_time_args == 'once':
        trigger_once = True
    else:
        processing_time = processing_time_args

    query_stream = target_df \
        .writeStream \
        .option("checkpointLocation", args_dict["checkpoint_location"]) \
        .trigger(processingTime=processing_time, once=trigger_once) \
        .foreachBatch(parse_avro_data) \
        .start()
    query_stream.awaitTermination()


if __name__ == "__main__":
    print_msg('Job Started @ {0}'.format(get_current_datetime()))
    try:
        args_dict = parse_arguments()
        print_msg("Arguments Parsed", args_dict['job_name'])

        df = read_stream(args_dict)
        print_msg("Read Stream Enabled", args_dict['job_name'])

        print_msg("Calling Write Stream", args_dict['job_name'])
        write_stream(df, args_dict)
    except Exception as ex:
        traceback.print_exception(*sys.exc_info())
        print_msg('Job Failed @ {0}'.format(get_current_datetime()))
        raise ex
    print_msg('Job Ended @ {0}'.format(get_current_datetime()))
