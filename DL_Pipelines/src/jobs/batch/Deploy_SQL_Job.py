import argparse
import json
import requests
import os
from pathlib import Path

env = 'dev'
# env = 'stage'
# env = 'prod'

# Tokens to access the databricks environments
# DEV
DOMAIN_DEV = 'skechers-dev.cloud.databricks.com'
TOKEN_DEV = os.environ['DB_TOKEN_DEV']

# Stage
DOMAIN_STAGE = 'skechers-staging.cloud.databriget_column_listcks.com'
# TOKEN_STAGE = os.environ['DB_TOKEN_STAGE']

# Prod
DOMAIN_PROD = 'skechers-prod.cloud.databricks.com'
# TOKEN_PROD = os.environ['DB_TOKEN_PROD']

if env == 'dev':
    DOMAIN = DOMAIN_DEV
    TOKEN = TOKEN_DEV
elif env == 'stage':
    DOMAIN = DOMAIN_STAGE
    TOKEN = TOKEN_STAGE
elif env == 'prod':
    DOMAIN = DOMAIN_PROD
    TOKEN = TOKEN_PROD

# Url that can be used to create a job on clusters
BASE_URL = 'https://%s/api/2.0/jobs/create' % DOMAIN


# This function has been used to parse the parameters
def parse_arguments():
    argparser = argparse.ArgumentParser(description="Invoke Batch Job")
    argparser.add_argument('--json_path', type=str)
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


# Entry point of the deployment script
if __name__ == '__main__':
    os.chdir('../../../')
    curr_dir = os.getcwd()
    print(curr_dir)

    file_path = '/job_input_jsons/batch/dev/BJ-SQL PARAM Deployment.json'

    json_path = curr_dir + file_path
    print("Input JSON: ", json_path)

    with open(json_path) as f:
        json_body = json.load(f)

    print(json_body)
    response = call_spark_job(json_body)
    print(response)
    if response.status_code == 200:
        print(response.json())
    else:
        print("Error calling job: ", response.text)
