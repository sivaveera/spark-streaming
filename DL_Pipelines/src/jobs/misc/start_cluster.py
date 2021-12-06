import os
import requests

DOMAIN = 'skechers-dev.cloud.databricks.com'
TOKEN = os.environ['DB_TOKEN']
BASE_URL = 'https://%s/api/2.0/clusters/start' % DOMAIN
CLUSTER_STATUS_URL = 'https://%s/api/2.0/clusters/get?cluster_id=' % DOMAIN

json_dict = {"cluster_id": "0428-134127-sea13"}


def call_spark_api_get(url):
    _response = requests.get(url, headers={'Authorization': 'Bearer %s' % TOKEN})
    return _response


def call_spark_api_post(body):
    _response = requests.post(
        BASE_URL,
        headers={'Authorization': 'Bearer %s' % TOKEN},
        json=body
    )
    return _response


if __name__ == '__main__':
    # To start the cluster
    print("JSON Body: ", json_dict)
    response = call_spark_api_post(json_dict)
    if response.status_code == 200:
        print(response.json())
    else:
        print("Error Calling API: ", response.json())

    # To check the status of the cluster
    response = call_spark_api_get(CLUSTER_STATUS_URL + json_dict["cluster_id"])
    if response.status_code == 200:
        response = response.json()
        if response and response["state"] == "RUNNING":
            print("Cluster is running.")
    else:
        print("Error Calling API: ", response.json())