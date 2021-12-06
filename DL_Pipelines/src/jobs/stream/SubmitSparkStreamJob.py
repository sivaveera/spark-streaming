import argparse
import json
import requests
import os
from pathlib import Path

DOMAIN = 'skechers-dev.cloud.databricks.com'
TOKEN = os.environ['DB_TOKEN']
BASE_URL = 'https://%s/api/2.0/jobs/runs/submit' % DOMAIN


def parse_arguments():
    argparser = argparse.ArgumentParser(description="Invoke Batch Job")
    argparser.add_argument('--json_path', type=str)
    args = argparser.parse_args()
    return args


def call_spark_job(body):
    _response = requests.post(
        BASE_URL,
        headers={'Authorization': 'Bearer %s' % TOKEN},
        json=body
    )
    return _response


if __name__ == '__main__':
    # curr_dir = os.path.abspath(os.path.dirname(__file__))
    os.chdir('../../../')
    curr_dir = os.getcwd()
    print(curr_dir)

    file_path = '/job_input_jsons/streams/1. kafka_to_delta/SJ1-ECOM order_items.json'
    json_path = curr_dir + file_path
    print("Input JSON: ", json_path)

    with open(json_path) as f:
        json_body = json.load(f)

    response = call_spark_job(json_body)
    print(response)
    if response.status_code == 200:
        print(response.json())
    else:
        print("Error calling job: ", response.json())

