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


# return SparkSession.builder.getOrCreate()


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


# This function can be used to log the executions metrics into the audit table
def log_audit_status(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count, exception_ind, exception_dtl,
					 rec_create_timestamp, rec_create_user_name):
	exception_dtl = exception_dtl.replace("'", "")
	insert_stmt = """  INSERT INTO delta_logs.audit_logs
    VALUES ('{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}', '{}', '{}')
    """.format(job_name, source_type, source_name, sink_type, sink_name, input_row_count, output_row_count, exception_ind, exception_dtl,
			   rec_create_timestamp, rec_create_user_name)
	get_or_create_spark().sql(insert_stmt)


def get_column_list(columns: str):
	try:
		columns = columns.split(',')
		columns = [key.strip() for key in columns]
		return columns
	except Exception as ex:
		print(str(ex))
		return None


# This function has been used for validate the data for duplicate checks and null checks
def log_validation_assertions(df: DataFrame, primary_keys: list, args_dict: dict) -> DataFrame:
	# Create output path and table name
	excp_output_location = args_dict['output_location']
	excp_output_location = excp_output_location.replace('/curr', '/excp')
	excp_tbl_name = args_dict['trgt_table_name']
	excp_tbl_name = excp_tbl_name.replace('_curr_', '_excp_')
	not_null_columns = args_dict['not_null_columns']
	print('Not Null Cols: ', not_null_columns)

	# Rule 1 - Check for NULLs for primary keys and Not-Null columns
	not_null_cols = None
	if not_null_columns:
		not_null_cols = get_column_list(not_null_columns)

	# Validation Rules for Null
	validation_rules_dict = {'NullCheckForPKs': primary_keys}
	if not_null_cols:
		validation_rules_dict['NullCheckForNotNullCols'] = not_null_cols

	# Derive a column [validation_flag] for Null check (PK and Not Null columns)
	derived_col_str = " CASE "
	for validation_rule in validation_rules_dict.keys():
		print(validation_rule)
		validation_cols = validation_rules_dict[validation_rule]
		derived_col_str += "\n WHEN "
		condition = ["coalesce(" + col + ", '') = '' OR " for col in validation_cols]
		condition = "".join(condition)
		condition = condition[:-4]
		condition += " THEN '" + validation_rule + "'"
		derived_col_str += condition

	derived_col_str += "\n END AS rule_name "

	null_validation_df = df.selectExpr('*', derived_col_str)

	# Set exception_ind flag for all non-valid rows.
	null_validation_df = null_validation_df.withColumn('exception_ind', when(col('rule_name').isNotNull(), 1))

	# # Drop column exception_ind to update correct values
	# null_validation_df = null_validation_df.drop('exception_ind')

	# Add hash for PK columns and all columns
	null_validation_df = null_validation_df.withColumn("rec_row_hash", sha2(concat_ws("||", *df.columns), 256))
	null_validation_df = null_validation_df.withColumn("rec_pk_hash", sha2(concat_ws("||", *primary_keys), 256))

	print_msg('--------------- null_validation_df schema: {}'.format(null_validation_df.printSchema()))

	# Rule 2 - Check for Duplicates
	# Check Duplicates - Start
	primary_keys.remove('rec_crud_text')
	# Filter all records for which PK columns are not Null i.e, validation_flag is 'NullCheckForNotNullCols' OR NULL
	# Commented below statement to allow NULL PK fields.

	excp_duplicate_validation_df = null_validation_df.drop('rule_name')
	# Check Duplicates - End
	print('PKs for Ranking: {0}'.format(primary_keys))

	# Add rank column based on PKs and ordering - extra columns [rec_row_hash, rec_pk_hash]
	ranked = excp_duplicate_validation_df.withColumn("_rank", row_number().over(
		Window.partitionBy(primary_keys).orderBy(desc("hdr_timestamp")).orderBy(desc("changeSequence"))))

	# ** Capture all duplicates **
	duplicate_rows_hash_df = ranked.filter('_rank > 1').select('rec_pk_hash').dropDuplicates().withColumn('exception_ind_dup', lit(1))

	# Drop column exception_ind to update correct values
	# ranked = ranked.drop('exception_ind')

	# Get all duplicate records by joining against rec_pk_hash
	duplicate_rows_df = ranked.join(duplicate_rows_hash_df, 'rec_pk_hash', 'inner')
	excp_duplicate_validation_df = duplicate_rows_df.drop('_rank')

	# Replace the value of exception_ind if exception_ind for duplicate df is 1.
	excp_duplicate_validation_df = excp_duplicate_validation_df.withColumn('exception_ind', when(col('exception_ind_dup') == 1, 1))
	excp_duplicate_validation_df = excp_duplicate_validation_df.drop('exception_ind_dup')

	excp_duplicate_validation_df = excp_duplicate_validation_df.selectExpr("'DuplicateCheck' as rule_name", '*')

	# Get all NON duplicate records by joining against rec_pk_hash to insert to Current table.
	non_duplicate_rows_df = ranked.filter('_rank = 1')
	non_duplicate_validation_df = non_duplicate_rows_df.join(duplicate_rows_hash_df, 'rec_pk_hash', 'left')
	non_duplicate_validation_df = non_duplicate_validation_df.fillna(0, subset=['exception_ind'])
	final_current_df = non_duplicate_validation_df.drop('_rank', 'hdr_timestamp', 'changeSequence')

	# Sequence columns to perform Union
	cols = excp_duplicate_validation_df.columns
	excp_null_df = null_validation_df.select(cols)

	# Merge exception data frames and write to S3
	# a. Merge exception dataframes
	final_filtered_df = excp_duplicate_validation_df.union(excp_null_df)
	final_filtered_df = final_filtered_df.drop('hdr_timestamp', 'changeSequence')
	# b. Write exception dataframe to S3
	print_msg('----------------------- final_filtered_df schema: {0}'.format(final_filtered_df.printSchema()))
	# Rearrange Columns
	cols = final_filtered_df.columns
	cols.remove('rec_row_hash')
	cols.remove('rec_pk_hash')
	cols.append('rec_row_hash')
	cols.append('rec_pk_hash')
	final_filtered_df = final_filtered_df.selectExpr(cols)
	final_filtered_df = final_filtered_df.filter('exception_ind = 1')
	final_filtered_df.repartition(1).write.partitionBy('rec_create_date_key').format('delta').mode('append').save(excp_output_location)

	# Create table
	ddl_stmt = "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}' ".format(excp_tbl_name, excp_output_location)
	try:
		get_or_create_spark().sql(ddl_stmt)
	except Exception as ex:
		print_msg('Warning: ' + str(ex), args_dict['job_name'])

	return final_current_df


# The function that can be used get current user
def get_current_user():
	return 'DL User'


# This function has been used to parse the parameters
def parse_arguments(job_name=None):
	if not job_name:
		argparser = argparse.ArgumentParser(description="DL Stream Job")
		argparser.add_argument('--job_name', type=str, required=True)
		args = argparser.parse_args()
		job_name = args.job_name

	args_dict = get_dl_configs(job_name)

	print_msg('Parameters: {0}'.format(args_dict), job_name)
	return args_dict


def load_schema(path) -> StructType:
	"""
	Generates schema from Apache Spark formatted JSON file
	"""
	json_str = " ".join(get_or_create_spark().sparkContext.textFile(path).collect())
	return StructType.fromJson(json.loads(json_str))


# This function has been used to create delta tables if not exists
def create_delta_table(args_dict: dict, output_schema):
	"""
	If output does not exist then create a delta table there otherwise return
	"""
	# Create Delta output folder if does not exists
	try:
		get_or_create_spark().read.format("delta").load(args_dict['output_location'])
		print_msg('Delta Table Exists: {0}'.format(args_dict['output_location']))
	except Exception as ex:
		print_msg('Delta Table Does NOT Exist: {0}'.format(args_dict['output_location']))
		empty_df = get_or_create_spark().createDataFrame([], output_schema)
		empty_df.write.format("delta").partitionBy("rec_create_date_key").save(args_dict['output_location'])

	# Create Delta Table if does not exists
	try:
		get_or_create_spark().sql("SELECT 1 FROM {0} WHERE 1 = 2".format(args_dict['trgt_table_name']))
	except:
		ddl_stmt = "CREATE TABLE IF NOT EXISTS {0} USING DELTA LOCATION '{1}' ".format(args_dict['trgt_table_name'], args_dict['output_location'])
		try:
			get_or_create_spark().sql(ddl_stmt)
		except Exception as ex:
			print_msg('Warning: ' + str(ex), args_dict['job_name'])


# This function can be used to read csv files as a source
def create_read_stream_file_csv(spark, args) -> DataFrame:
	"""
	Reads a directory of CSVs as a stream
	"""
	input_schema = load_schema(args['input_schema'])

	return spark.readStream \
		.schema(input_schema) \
		.csv(args['input_location'], header=False, schema=input_schema, timestampFormat=args['input_timestamp_format'])


# This function can be used to read json files as a source
def create_read_stream_file_json(spark, args) -> DataFrame:
	"""
	Reads a directory of JSON files as a stream
	"""
	return spark.readStream \
		.schema(load_schema(args['input_schema'])) \
		.json(args['input_location'], timestampFormat=args['input_timestamp_format'])


def create_read_stream_delta_json(spark, args) -> DataFrame:
	"""
	Reads from a Delta Lake source (that is a replication of Kafka with JSON values)
	"""
	json_schema = load_schema(args['input_schema'])

	return spark.readStream \
		.format("delta").load(args['input_location']) \
		.withColumn('parsed_value', from_json('value', json_schema)) \
		.select("parsed_value.*", "*") \
		.drop("key", "value", 'parsed_value')


# This function can be used to read delta tables as a source
def create_read_stream_delta_avro(spark, args) -> DataFrame:
	"""
	Reads from a Delta Lake source (that is a replication of Kafka with JSON values)
	"""
	return spark.readStream.format("delta").load(args['input_location'])


# This function has been used to initiate read stream
def create_read_stream(spark, args: dict) -> DataFrame:
	"""
	Abstraction over read stream
	"""
	source_df: DataFrame = None
	if str(args['source_type']).upper() == 'FILES_CSV':
		source_df = create_read_stream_file_csv(spark, args)
	if str(args['source_type']).upper() == 'FILES_JSON':
		source_df = create_read_stream_file_json(spark, args)
	if str(args['source_type']).upper() == 'DELTA_JSON':
		source_df = create_read_stream_delta_json(spark, args)
	if str(args['source_type']).upper() == 'DELTA_AVRO':
		source_df = create_read_stream_delta_avro(spark, args)
	else:
		raise Exception("Unknown source type: " + str(args['source_type']).upper())

	print_msg('Schema: {0}'.format(source_df.printSchema), args['job_name'])
	print_msg('Adding meta columns to Dataframe', args['job_name'])
	source_df = add_meta_columns_to_df(source_df, args)

	return source_df


# This function has been used to add additional columns to a dataset as per the requirement
def add_meta_columns_to_df(source_df: DataFrame, args: dict) -> DataFrame:
	data_df = source_df.select('data.*')

	data_df = (
		data_df
			.withColumn('rec_create_timestamp', lit(None).cast('timestamp'))
			.withColumn('rec_create_user_name', lit(None).cast('string'))
			.withColumn('rec_update_timestamp', lit(None).cast('timestamp'))
			.withColumn('rec_update_user_name', lit(None).cast('string'))
			.withColumn('rec_create_date_key', lit(None).cast('int'))
			.withColumn('rec_create_time_key', lit(None).cast('int'))
			.withColumn('rec_crud_text', lit(None).cast('string'))
			.withColumn('delete_ind', lit(None).cast('int'))
			.withColumn('exception_ind', lit(None).cast('int'))
			.withColumn('resolved_ind', lit(None).cast('int'))
	)
	data_schema = data_df.schema.json()
	headers_schema = source_df.select('headers.*').schema.json()
	data_output_schema_struct = StructType.fromJson(json.loads(data_schema))
	headers_output_schema_struct = StructType.fromJson(json.loads(headers_schema))

	source_df = (
		source_df.withColumn("delete_ind", when(col("headers.operation") == "DELETE", 1).otherwise(0))
			.withColumn("rec_create_date_key", date_format(current_date(), "yyyyMMdd").cast('int'))
			.withColumn("rec_create_time_key", date_format(current_timestamp(), "HHmmss").cast('int'))
			.withColumn("rec_create_timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast('timestamp'))
			.withColumn("rec_create_user_name", lit(get_current_user()).cast('string'))
			.withColumn("rec_update_timestamp", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").cast('timestamp'))
			.withColumn("rec_update_user_name",
						when((col('headers.operation') == "UPDATE"), lit(get_current_user())).otherwise(lit("")).cast('string'))
			.withColumn('exception_ind', lit(0).cast('int'))
			.withColumn('resolved_ind', lit(0).cast('int'))
			# For initial loads
			.withColumn("hdr_operation", when((col('headers.operation') == "REFRESH"), "REFRESH").otherwise(col('headers.operation')))
			.withColumn("hdr_timestamp",
						when((col('headers.operation') == "REFRESH"), lit("1970-01-01T00:00:00.000")).otherwise(col('headers.timestamp')).cast(
							'timestamp'))
			.withColumn("hdr_changeSequence",
						when((col('headers.operation') == "REFRESH"), date_format(lit("1970-01-01T00:00:00.000"), "yyyyMMddHHmmssSS")).otherwise(
							col('headers.changeSequence')).cast('string'))
			.select(
			# Headers tag
			from_json(
				to_json(struct(col('headers.streamPosition'), col('headers.transactionId'), col('headers.changeMask'),
							   col('headers.columnMask'),
							   col('headers.transactionEventCounter'), col('headers.transactionLastEvent'),
							   col("hdr_operation").alias('operation'), col("hdr_timestamp").alias('timestamp'),
							   col("hdr_changeSequence").alias('changeSequence'))
						),
				headers_output_schema_struct
			).alias("headers"),
			# Data tag
			from_json(
				to_json(struct("data.*", col("rec_create_timestamp"), col("rec_create_user_name"), col("rec_update_timestamp"),
							   col("rec_update_user_name"),
							   col("rec_create_date_key"), col("rec_create_time_key"),
							   col("hdr_operation").alias('rec_crud_text'), col("delete_ind"), col("exception_ind"), col("resolved_ind"))
						),
				data_output_schema_struct
			).alias("data")
		)
	)

	return source_df


# This function can be used to extract columns from a given schema
def generate_column_list(output_schema) -> []:
	"""
	Generates a list of columns in the table
	"""
	return SparkSession.builder.getOrCreate().createDataFrame([], output_schema).columns


# This function can used to generate a dictionary of columns from a given schema that can be used for merge
def generate_update_dictionary(output_schema, source_table="s") -> {}:
	"""
	Takes in source schema and generates update set for CDC
	"""
	columns = generate_column_list(output_schema)
	output_dictionary = {}

	for column in columns:
		output_dictionary[column] = source_table + "." + column

	return output_dictionary


def execute_stream_job(spark, args_dict):
	"""
	Starts the stream job
	"""
	streaming_query = None
	pks = args_dict['primary_keys']
	pks = pks.split(',')
	source_df = create_read_stream(spark, args_dict)
	data_df = source_df.selectExpr("data.*")
	data_df = data_df.withColumn("rec_row_hash", sha2(concat_ws("||", *data_df.columns), 256))
	data_df = data_df.withColumn("rec_pk_hash", sha2(concat_ws("||", *pks), 256))

	source_df = source_df.selectExpr("data.*", "headers.timestamp as hdr_timestamp", "headers.changeSequence")

	try:
		if args_dict['epoch_dates']:
			epoch_dates = args_dict['epoch_dates'].split(',')
			for epoch_date in epoch_dates:
				source_df = source_df.withColumn(epoch_date, (col(epoch_date) / 1000).cast('int').cast('timestamp'))
				data_df = data_df.withColumn(epoch_date, (col(epoch_date) / 1000).cast('int').cast('timestamp'))
	except Exception as ex:
		print_msg('Warning: ' + str(ex), args_dict['job_name'])

	data_output_schema = data_df.schema.json()
	data_output_schema_struct = StructType.fromJson(json.loads(data_output_schema))

	# Create Output location and Delta Table
	create_delta_table(args_dict, data_output_schema_struct)

	processing_time_args = args_dict["trigger_processing_time"].lower()
	processing_time = None
	trigger_once = None
	if processing_time_args == 'once':
		trigger_once = True
	else:
		processing_time = processing_time_args

	load_type = args_dict['load_type']
	if load_type.upper() == "CDC":
		# execute CDC
		try:
			output_delta_table = DeltaTable.forPath(spark, args_dict['output_location'])
		except Exception as ex:
			create_delta_table(args_dict, data_output_schema_struct)
			output_delta_table = DeltaTable.forPath(spark, args_dict['output_location'])

		update_dictionary = generate_update_dictionary(data_output_schema_struct)
		streaming_query = source_df.writeStream \
			.option("checkpointLocation", args_dict["checkpoint_location"]) \
			.trigger(processingTime=processing_time, once=trigger_once) \
			.format("delta") \
			.foreachBatch(
			lambda cdc_df, epoch_id:
			execute_cdc_batch(cdc_df, output_delta_table, args_dict['primary_keys'], update_dictionary, args_dict['translation_type'])) \
			.outputMode("update") \
			.start()
	elif load_type.upper() == "APPEND":
		# execute append
		streaming_query = data_df.writeStream \
			.option('checkpointLocation', args_dict['checkpoint_location']) \
			.option("mergeSchema", "true") \
			.trigger(processingTime=processing_time, once=trigger_once) \
			.format("delta") \
			.outputMode("append") \
			.start(args_dict['output_location'])

	if streaming_query is not None:
		if int(args_dict['timeout']) > 0:
			streaming_query.awaitTermination(args_dict['timeout'])
		else:
			streaming_query.awaitTermination()
	else:
		raise Exception(load_type.upper() + " is not implemented")


def transform_csv_to_cdc(csv_df) -> DataFrame:
	"""
	Transforms CSV to match CDC framework
	"""
	return csv_df.withColumnRenamed('timestamp', 'databricks_cdc_timestamp_raw') \
		.withColumn('databricks_cdc_deleted', col('flag').isin('D')).persist()


# This function has been used to merge condition on the basis for composite key for merge operation
def get_primary_key_condition_for_merge(primary_keys):
	primary_keys = primary_keys.split(',')
	primary_keys = [key.strip() for key in primary_keys]
	pk_condition = ""
	for key in primary_keys:
		pk_condition = pk_condition + " AND S.{0} = T.{0}".format(key, key)

	pk_condition = pk_condition[5:]
	return pk_condition


# This function has been called for every batch and a set of operations have been performed on data
def execute_cdc_batch(cdc_df, output_delta_table, primary_key, update_dictionary, translation_type):
	"""
	Executes CDC - done on each microbatch of a streaming query
	"""
	row_count = 0
	input_row_count = 0
	source_type = args_dict['kafka_topic']
	if source_type:
		source_type = 'KAFKA'
	else:
		source_type = 'DELTA TABLE'

	try:
		# Step 1. Persist the Dataframe for optimized transformations
		cdc_df = cdc_df.persist()

		input_row_count = cdc_df.count()
		# Step 2. Flatten the dataframe after aggregating it based on timestamp
		if translation_type.upper() != 'ATTUNITY':
			cdc_df = transform_csv_to_cdc(cdc_df)

		print_msg('Primary Key: {0}'.format(primary_key))
		print_msg('CDC Schema: {0}'.format(cdc_df.printSchema))

		primary_key_for_validation = primary_key.split(",")

		# Perform validation
		primary_key_for_validation.append('rec_crud_text')
		changes_df = log_validation_assertions(cdc_df, primary_key_for_validation, args_dict)


		# Convert time to UTC format  --Removed conversion on Aug 5 2021 bring as in source
		#for i in changes_df.dtypes:
		#	if i[1] == "timestamp" and i[0].lower() not in ['rec_create_timestamp', 'rec_update_timestamp']:
		#		column_to_utc = i[0]
		#		print_msg("Column changed to UTC: {0}".format(column_to_utc))
		#		changes_df = changes_df.withColumn(column_to_utc, to_utc_timestamp(column_to_utc, "PST"))

		print_msg('DF Schema before Merge: {0}'.format(changes_df.printSchema))
		# Get primary key or composite keys for merge operation
		merge_condition = get_primary_key_condition_for_merge(args_dict['primary_keys'])

		# Get the row count
		row_count = changes_df.count()

		# spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

		update_dictionary_for_batch = update_dictionary.copy()
		update_dictionary_new_record = update_dictionary_for_batch.copy()

		del update_dictionary_for_batch['rec_create_timestamp']
		del update_dictionary_for_batch['rec_create_date_key']
		del update_dictionary_for_batch['rec_create_time_key']
		del update_dictionary_for_batch['rec_create_user_name']

		print('update_dictionary_for_batch: {}'.format(update_dictionary_for_batch))
		print('update_dictionary_new_record: {}'.format(update_dictionary_new_record))

		output_delta_table.alias('t') \
			.merge(changes_df.alias('s'), merge_condition) \
			.whenMatchedUpdate(set=update_dictionary_for_batch) \
			.whenNotMatchedInsert(values=update_dictionary_new_record) \
			.execute()

		cdc_df.unpersist()

		log_audit_status(args_dict['job_name'], source_type, args_dict['input_location'], 'S3',
						 args_dict['output_location'], input_row_count, row_count, 0, "", get_current_datetime(), get_current_user())
	except Exception as ex:
		log_audit_status(args_dict['job_name'], source_type, args_dict['input_location'], 'S3',
						 args_dict['output_location'], input_row_count, 0, 1, str(ex), get_current_datetime(), get_current_user())
		raise ex


if __name__ == "__main__":
	print_msg('Job Started @ {0}'.format(get_current_datetime()))
	try:
		envt_var = os.environ.get('enable_local_dev')
		if envt_var == 'true':
			job_name = os.environ.get('job_name')
			print_msg('Starting Job in Dev Mode: {0}'.format(job_name))
			args_dict = parse_arguments(job_name=job_name)
		else:
			from delta.tables import *

			args_dict = parse_arguments(None)

		print_msg("Arguments Parsed", args_dict['job_name'])
		print_msg("Calling Execute Stream Job", args_dict['job_name'])

		spark = get_or_create_spark()
		execute_stream_job(spark, args_dict)
	except Exception as ex:
		traceback.print_exception(*sys.exc_info())
		print_msg('Job Failed @ {0}'.format(get_current_datetime()))
		raise ex
	print_msg('Job Ended @ {0}'.format(get_current_datetime()))
