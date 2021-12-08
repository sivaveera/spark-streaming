import argparse
from pytz import timezone
from datetime import datetime
from datetime import date, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import *
from delta.tables import *
import traceback
import json
import sys
import requests
import boto3

us_tz = timezone('UTC')
us_time = datetime.now(us_tz)


# This function can be used get current timestamp
def get_current_datetime():
	return us_time.strftime('%Y-%m-%d %H:%M:%S')


# Initialize Spark Session
def get_or_create_spark() -> SparkSession:
	return SparkSession.builder.config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0").config('spark.sql.session.timeZone',
																									   'UTC').getOrCreate()


# This function can be used print the output with current timestamp and the format
def print_msg(msg, job_name=None):
	msg = "DL LOG | {0} | {1} | {2}".format(get_current_datetime(), job_name, msg)
	msg_no = len(msg)
	print(msg_no * '#')
	print(msg)
	print(msg_no * '#')


# This function has been used to parse the parameters
def parse_arguments():
	argparser = argparse.ArgumentParser(description="DL Stream Job entry point")
	argparser.add_argument('--s_area', type=str, required=True)
	args = argparser.parse_args()
	return args.s_area


def __log_job_run_status(run_type: str, job_name: str, job_id: int, job_type: str, job_method: str, log_timestamp: str, response: str):
	# response = response.replace("'", "").replace('"', '')
	insert_stmt = """  INSERT INTO delta_logs.dl_job_run_logs VALUES ('{}', '{}', {}, '{}', '{}', '{}', '{}')
    """.format(run_type, job_name, job_id, job_type, job_method, log_timestamp, response)
	get_or_create_spark().sql(insert_stmt)


def __get_job_run_dtls(run_id: int) -> str:
	BASE_URL_JOBS_RUNS_GET = 'https://{0}/api/2.0/jobs/runs/get?run_id={1}'.format(DOMAIN, run_id)
	status_resp = requests.get(BASE_URL_JOBS_RUNS_GET, headers={'Authorization': 'Bearer %s' % TOKEN})
	return status_resp.text


def __is_job_running(job_id: int) -> bool:
	json_payload = {"job_id": job_id, "active_only": "true"}
	status_resp = requests.get(BASE_URL_JOBS_RUNS_LIST, headers={'Authorization': 'Bearer %s' % TOKEN}, json=json_payload)
	status_resp_dict = status_resp.json()
	# check if the jobs is already running
	try:
		status_resp_dict['runs']
		print('Job is already Running.')
		return True
	except:
		# Trigger the jobs
		return False


def __get_job_id(jobs: list, job_name) -> int:
	for job in jobs:
		if job['settings']['name'] == job_name:
			return job['job_id']
	return None


if __name__ == "__main__":
	print_msg('Job Started @ {0}'.format(get_current_datetime()))

	try:
		sub_area = parse_arguments()
		sub_area = sub_area.lower()
		print_msg('Subject Area: %s' % sub_area)

		# DOMAIN = dbutils.secrets.get("dbfs_env", "domain")
		# TOKEN = dbutils.secrets.get("dbfs_env", "token")
		DOMAIN = "test.cloud.databricks.com"
		TOKEN = "dadddddpif50986"

		BASE_URL_JOBS_RUN_NOW = 'https://%s/api/2.0/jobs/run-now' % DOMAIN
		BASE_URL_JOBS_RUNS_LIST = 'https://%s/api/2.0/jobs/runs/list' % DOMAIN
		BASE_URL_JOBS_LIST = 'https://%s/api/2.0/jobs/list' % DOMAIN

		# List all the jobs from a Databricks cluster
		response = requests.get(BASE_URL_JOBS_LIST, headers={'Authorization': 'Bearer %s' % TOKEN})
		if response.status_code == 200:
			sj_jobs = response.json()
		else:
			print('Error %s' % response.text)
			raise Exception('Error getting jobs list.')

		sj1_jobs = [job for job in sj_jobs['jobs'] if 'SJ1-' in job['settings']['name'] and sub_area.lower() in job['settings']['name'].lower()]
		sj2_jobs = [job for job in sj_jobs['jobs'] if 'SJ2-' in job['settings']['name'] and sub_area.lower() in job['settings']['name'].lower()]

		sj1_running_jobs_dict = {}
		sj2_running_jobs_dict = {}
		sj1_running_jobs = []
		sj2_running_jobs = []
		rerun_jobs_dict = {}
		NO_OF_RETRIES = 1

		# Call SJ1 Jobs - For all jobs run, capture job name in a list `running_jobs`
		for job1 in sj1_jobs:
			job_name = job1['settings']['name']
			job_id = job1['job_id']

			if not __is_job_running(job_id):
				json_payload = {"job_id": job_id}
				run_now_resp = requests.post(BASE_URL_JOBS_RUN_NOW, headers={'Authorization': 'Bearer %s' % TOKEN}, json=json_payload)
				if run_now_resp.status_code == 200:
					resp_dict = run_now_resp.json()
					print('Called SJ1 Job %s' % job_name)
					sj1_running_jobs.append(job_name)
					sj1_running_jobs_dict[job_name] = resp_dict['run_id']
					__log_job_run_status('MASTER_JOB', job_name, job_id, 'SJ1', 'START', get_current_datetime(), run_now_resp.text)

		# Call SJ2 Jobs
		while sj1_running_jobs:
			for job2 in sj2_jobs:
				sj2_job_name = job2['settings']['name']
				sj1_job_name = sj2_job_name.replace('SJ2-', 'SJ1-')
				sj1_job_id = __get_job_id(sj1_jobs, sj1_job_name)
				sj2_job_id = job2['job_id']

				if sj1_job_name in sj1_running_jobs and not (__is_job_running(sj1_job_id) or __is_job_running(sj2_job_id)):
					print('Finished Running: %s' % sj1_job_name)

					resp_text = __get_job_run_dtls(sj1_running_jobs_dict[sj1_job_name])
					if 'TIMEDOUT' in resp_text and (sj1_job_name in rerun_jobs_dict.keys() and rerun_jobs_dict[sj1_job_name] <= NO_OF_RETRIES):
						__log_job_run_status('MASTER_JOB', sj1_job_name, sj1_job_id, 'SJ1', 'TIMEDOUT', get_current_datetime(), resp_text)
						rerun_jobs_dict[sj1_job_name] = rerun_jobs_dict[sj1_job_name] + 1
						break
					else:
						__log_job_run_status('MASTER_JOB', sj1_job_name, sj1_job_id, 'SJ1', 'STOP', get_current_datetime(), resp_text)
						sj1_running_jobs.remove(sj1_job_name)

					json_payload = {"job_id": sj2_job_id}
					run_now_resp = requests.post(BASE_URL_JOBS_RUN_NOW, headers={'Authorization': 'Bearer %s' % TOKEN}, json=json_payload)
					if run_now_resp.status_code == 200:
						resp_dict = run_now_resp.json()
						print('Called SJ2 Job %s' % sj2_job_name)
						sj2_running_jobs.append(sj2_job_name)
						sj2_running_jobs_dict[sj2_job_name] = resp_dict['run_id']
						__log_job_run_status('MASTER_JOB', sj2_job_name, sj2_job_id, 'SJ2', 'START', get_current_datetime(), run_now_resp.text)

		# Check for SJ2 Completion and Log Job Status
		while sj2_running_jobs:
			for sj2_job_name in sj2_running_jobs:
				sj2_job_id = __get_job_id(sj2_jobs, sj2_job_name)
				if not __is_job_running(sj2_job_id):
					print('Finished Running: %s' % sj2_job_name)
					# sj2_running_jobs.remove(sj2_job_name)
					resp_text = __get_job_run_dtls(sj2_running_jobs_dict[sj2_job_name])
					if 'TIMEDOUT' in resp_text and (sj2_job_name in rerun_jobs_dict.keys() and rerun_jobs_dict[sj2_job_name] <= NO_OF_RETRIES):
						__log_job_run_status('MASTER_JOB', sj2_job_name, sj2_job_id, 'SJ2', 'TIMEDOUT', get_current_datetime(), resp_text)
						rerun_jobs_dict[sj2_job_name] = rerun_jobs_dict[sj2_job_name] + 1
						break
					else:
						__log_job_run_status('MASTER_JOB', sj2_job_name, sj2_job_id, 'SJ2', 'STOP', get_current_datetime(), resp_text)
						sj2_running_jobs.remove(sj2_job_name)

	except Exception as ex:
		traceback.print_exc()
		print_msg('Master Job Failed @ {0}'.format(get_current_datetime()))

	print_msg('Job Ended @ {0}'.format(get_current_datetime()))
