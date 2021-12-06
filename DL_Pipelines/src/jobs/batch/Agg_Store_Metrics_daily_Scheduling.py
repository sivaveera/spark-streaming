import os
from time import sleep
import requests
import boto3

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

def get_run_id(job_list, job_name):
    for i in job_list['jobs']:
        job_name_from_list = i['settings']['name']
        if job_name_from_list.lower() in job_name:
            print(i['job_id'], i['settings']['name'])
            job_id = i['job_id']
            run_job_json = {"job_id": job_id}
            run_now_response = requests.post(BASE_URL_RUN_NOW, headers={'Authorization': 'Bearer %s' % TOKEN},
                                             json=run_job_json).json()
            run_id = run_now_response['run_id']
    return run_id

if __name__ == "__main__":
    #Set the environment

    #URLs that will be used for Jobs API

    #Prod Token
    DOMAIN = dbutils.secrets.get("dbfs_env", "domain")
    TOKEN = dbutils.secrets.get("dbfs_env", "token")

    # params = get_variables_from_param_store()
    # DOMAIN = params['dbfsenv_domain']
    # TOKEN = params['dbfsenv_token']

    BASE_URL_JOBS_LIST = 'https://%s/api/2.0/jobs/list' % DOMAIN
    BASE_URL_RUN_NOW = 'https://%s/api/2.0/jobs/run-now' % DOMAIN
    BASE_URL_RUNS_GET = 'https://%s/api/2.0/jobs/runs/get' % DOMAIN

    #Dependency job for agg store metric daily calculation
    dependency_job_names = ['sj3-slv dtl_retail_sales', 'sj3-slv agg_store_visits_daily']

    #Get the job list in the environment
    response_job_list = requests.get(BASE_URL_JOBS_LIST, headers={'Authorization': 'Bearer %s' % TOKEN})
    job_list = response_job_list.json()
    depenedency_run_id_list = []
    for dependency_job_name in dependency_job_names:
        depenedency_run_id_list.append(get_run_id(job_list, dependency_job_name))
    print(depenedency_run_id_list)

    job_life_cycle_state = ['terminated', 'internal_error']
    # run_status_map={}
    # dependency_running_flag =True
    while True:
        run_status_dtl_retail_sales = requests.get(F"{BASE_URL_RUNS_GET}?run_id={depenedency_run_id_list[0]}",
                                                   headers={'Authorization': 'Bearer %s' % TOKEN}).json()['state']['life_cycle_state']
        print(run_status_dtl_retail_sales)
        run_status_agg_store_visits_daily = requests.get(F"{BASE_URL_RUNS_GET}?run_id={depenedency_run_id_list[1]}",
                                                   headers={'Authorization': 'Bearer %s' % TOKEN}).json()['state']['life_cycle_state']
        print(run_status_dtl_retail_sales)


        if run_status_dtl_retail_sales.lower() in  job_life_cycle_state and run_status_agg_store_visits_daily.lower() in job_life_cycle_state:
            break
        sleep(120)

    #If both the silver usecase dependencies are successful then run the gold
    result_state_dtl_retail_sales = requests.get(F"{BASE_URL_RUNS_GET}?run_id={depenedency_run_id_list[0]}",
                 headers={'Authorization': 'Bearer %s' % TOKEN}).json()['state']['result_state']
    print(result_state_dtl_retail_sales)

    result_state_agg_store_visits_daily = requests.get(F"{BASE_URL_RUNS_GET}?run_id={depenedency_run_id_list[1]}",
                 headers={'Authorization': 'Bearer %s' % TOKEN}).json()['state']['result_state']
    print(result_state_dtl_retail_sales)

    if result_state_dtl_retail_sales == 'SUCCESS' and result_state_agg_store_visits_daily == 'SUCCESS':
        get_run_id(job_list, 'sj4-gold agg_store_metrics_daily')