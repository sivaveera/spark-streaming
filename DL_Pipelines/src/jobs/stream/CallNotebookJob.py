import argparse
import json
import requests
import os
import time

env = os.environ['ENVT']

# Tokens to access the databricks environments
# DEV
DOMAIN_DEV = 'test-dev.cloud.databricks.com'
# Stage
DOMAIN_STAGE = 'test-staging.cloud.databriget.com'
# Prod
DOMAIN_PROD = 'test-prod.cloud.databricks.com'

if env == 'dev':
	DOMAIN = DOMAIN_DEV
	TOKEN = os.environ['DB_TOKEN_DEV']
elif env == 'stage':
	DOMAIN = DOMAIN_STAGE
	TOKEN = os.environ['DB_TOKEN_STAGE']
elif env == 'prod':
	DOMAIN = DOMAIN_PROD
	TOKEN = os.environ['DB_TOKEN_PROD']

# Url that can be used to create a job on clusters
BASE_URL = 'https://%s/api/2.0/jobs/run-now' % DOMAIN
BASE_GET_RUNS_URL = 'https://%s/api/2.0/jobs/runs/get' % DOMAIN

json_req = '{"job_id": "INPUT_JOB_ID",  "notebook_params": {"domain_name": "INPUT_DOMAIN_NAME", "job_name": "INPUT_JOB_NAME"  } } '


# This function has been used to parse the parameters
# >> python CallNotebookJob.py --domain_name test --job_name "SJ1-test skeordrd"
def parse_arguments():
	argparser = argparse.ArgumentParser(description="Invoke Notebook Scheduler Job")
	argparser.add_argument('--job_id', type=str, help='Enter RunSubmit Job ID')
	argparser.add_argument('--domain_name', type=str, help='Enter Domain Name', default='')
	argparser.add_argument('--job_name', type=str, help='Enter Job Name', default='')
	args = argparser.parse_args()
	return args


# This function has been used make an api call to create jobs on clusters
def call_spark_job(body):
	_response = requests.post(
		BASE_URL,
		headers={'Authorization': 'Bearer %s' % TOKEN},
		json=body
	)
	return _response


def get_jobs_status(run_id: int):
	get_url = BASE_GET_RUNS_URL + "?run_id={0}".format(run_id)
	_response = requests.get(
		get_url,
		headers={'Authorization': 'Bearer %s' % TOKEN}
	)
	return _response


def __is_job_running(run_id: int) -> bool:
	status_resp = get_jobs_status(run_id)
	status_resp_dict = status_resp.json()
	# check if the jobs is already running
	try:
		print(status_resp_dict)
		if status_resp_dict['state']['life_cycle_state'] == 'RUNNING':
			return True
		else:
			return False
	except:
		return False


# Entry point of the deployment script
if __name__ == '__main__':
	os.chdir('../../../')
	curr_dir = os.getcwd()
	print(curr_dir)
	args = parse_arguments()
	print(args.domain_name)
	print(args.job_name)
	if not (args.domain_name or args.job_name):
		raise SystemExit('Enter either Domain Name or Job Name. Exiting application...')

	json_req = json_req.replace('INPUT_JOB_ID', args.job_id).replace('INPUT_DOMAIN_NAME', args.domain_name).replace('INPUT_JOB_NAME', args.job_name)
	json_body = json.loads(json_req)
	print('Input JSON: ', json_body)

	# Call RunSubmit Job with parameters
	response = call_spark_job(json_body)

	if response.status_code == 200:
		_resp_dict = response.json()
		_run_id = _resp_dict['run_id']

		# Check status of job continuously until it finishes running
		while __is_job_running(_run_id):
			print('Job is still running...')
			# Wait for 5 seconds.
			time.sleep(5)
		print('Job has finished running.')
	else:
		print("Error calling job: ", response.text)
