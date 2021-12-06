import argparse
import requests
import json
import os

DOMAIN = "skechers-prod.cloud.databricks.com"
TOKEN = os.environ['DB_TOKEN_DEV']

BASE_URL_JOBS_LIST = 'https://%s/api/2.0/jobs/list' % DOMAIN
BASE_URL_JOBS_UPDATE = 'https://%s/api/2.0/jobs/update' % DOMAIN

new_settings = """
{
  "job_id": REP_JOB_ID,
  "new_settings": {
  	"timeout_seconds": 7200
    }
}
"""


# This function has been used to parse the parameters
def parse_arguments():
	argparser = argparse.ArgumentParser(description="DL Stream Job entry point")
	argparser.add_argument('--s_area', type=str, required=True)
	args = argparser.parse_args()
	return args.s_area


# List all the jobs from a Databricks cluster
headers = {'Authorization': 'Bearer %s' % TOKEN}
response = requests.get(BASE_URL_JOBS_LIST, headers=headers)
if response.status_code == 200:
	sj_jobs = response.json()
	sj_jobs = sj_jobs['jobs']
	print(sj_jobs)
else:
	print('Error %s' % response.text)
	raise Exception('Error getting jobs list.')

for job in sj_jobs:
	job_name = job['settings']['name']

	if job_name.startswith('SJ') and ('GARPAC' in job_name or 'COMPASS' in job_name):
		job_id = job['job_id']
		new_settings_payload = new_settings.replace('REP_JOB_ID', str(job_id))

		headers = {'Authorization': 'Bearer %s' % TOKEN}
		payload = json.loads(new_settings_payload)
		print('******Request******', job_name, payload)
		response = requests.post(BASE_URL_JOBS_UPDATE, headers=headers, json=payload)
		if response.status_code == 200:
			print('Settings Updated for Job: %s' % job['settings']['name'])
		else:
			print('Update Failed for {0} :: {1}'.format(job['settings']['name'], response.text), payload)
