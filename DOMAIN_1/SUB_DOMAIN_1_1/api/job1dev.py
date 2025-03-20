"""
 * Copyright (c) 2025 - Liverpool. All rights reserved
 *
 * Grupo de Asesores Profesionales en Servicios de Integracion {GAPSI} - CDMX - 2025
 *
"""

import datetime
import json
import requests
import os
import time
import subprocess
from airflow import models
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

"""
 * @author: Jorge Puc Carrillo
 * @updated:
 * @description: hana dag template
 * @since-version: 1.0
"""

#datos Generales
job_name = "1devdev"
pipeline_id = 1
pipeline_name = 'job1dev'
flex_template = 'api_flex_template'
stage = 'dev'
dataflow_bucket_name = 'crp-dev-data-platform-bkt01'
dataflow_project = 'crp-dev-data-platform-bkt01'
target_project = 'crp-dev-data-platform'
target_table = 'orders'
target_dataset = 'demo_dwh'
datetime_start = ''
service_account = 'id-crp-dev-data-platform-dataf@crp-dev-data-platform.iam.gserviceaccount.com'
subnetwork = 'projects/corp-dev-net-shared/regions/us-east4/subnetworks/corp-dev-nae4-data-platform-workers-sub-1'
region = 'us-east4'

#Datos Específicos
api_method = '<API_METHOD>'
api_host = 'qat-api.liverpool.com.mx'
api_headers = '[{"name":"apikey", "type":"secret", "value":"orders_apikey"}]'
api_port = ''
api_pathparams = ''
api_queryparams = '[{"name":"start_update_date", "type":"dynamic", "value":""}, {"name":"start_update_date", "type":"dynamic", "value":""}, {"name":"size", "type":"static", "value":"200"}]'
api_protocol = 'https'

default_dag_args = {
    "start_date": datetime.datetime(2025, 2, 10, 12, 59),
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=1),
}

def get_token(**kwargs):
    command = ['gcloud', 'auth', 'print-access-token']
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        token = result.stdout.strip()
        print("Output:", token)
        kwargs['ti'].xcom_push(key='token', value=token)
    except subprocess.CalledProcessError as e:
        print("An error occurred:")
        print(e.stderr)
        raise AirflowFailException

def trigger_job(**kwargs):
    token = kwargs['ti'].xcom_pull(task_ids='get_token', key='token')
    url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/flexTemplates:launch'
    headers = {
        # 'Authorization': f'Bearer {os.environ.get("TOKEN")}',
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        "launch_parameter": {
            "jobName": f'{job_name}',
            "containerSpecGcsPath": f'gs://{dataflow_bucket_name}/templates/{flex_template}',
            "environment": {
                "additionalExperiments": [],
                "additionalUserLabels": {},
                "subnetwork": f'https://www.googleapis.com/compute/v1/{subnetwork}'
            },
            "parameters": {
                "dataflow_bucket_name": f'{dataflow_bucket_name}',
                "dataflow_project": f'{dataflow_project}',
                'target_project': f'{target_project}',
                'target_dataset': f'{target_dataset}',
                'target_table': f'{target_table}',
                "machine_type": 'n1-standard-1',
                'pipeline_id': f'{pipeline_id}',
                'pipeline_name': f'{pipeline_name}',
                "service_account_email": f'{service_account}',
                'stage': f'{stage}',
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}',
                'api_method': f'{api_method}',
                'api_host': f'{api_host}',
                'api_headers': f'{api_headers}',
                'api_port': f'{api_port}',
                'api_pathparams': f'{api_pathparams}',
                'api_queryparams': f'{api_queryparams}',
                'api_protocol': f'{api_protocol}'
            }
        }
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        json_response = response.json()
        print(json_response)
        response.raise_for_status()
        kwargs['ti'].xcom_push(key='job_id', value=json_response['job']['id'])
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise AirflowFailException
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        raise AirflowFailException
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
        raise AirflowFailException
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
        raise AirflowFailException
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise AirflowFailException

def check_status(**kwargs):
    job_id = kwargs['ti'].xcom_pull(task_ids='execute_dataflow_job', key='job_id')
    token = kwargs['ti'].xcom_pull(task_ids='get_token', key='token')
    status = None
    if job_id is None:
        print('job_id is missing')
        raise AirflowFailException
    job_url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/jobs/{job_id}'
    headers = {
        # 'Authorization': f'Bearer {os.environ.get("TOKEN")}',
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    while status is None:
        time.sleep(60)
        response = requests.get(job_url, headers=headers).json()
        if response['currentState'] == 'JOB_STATE_FAILED':
            status = 'error'
        elif response['currentState'] == 'JOB_STATE_DONE':
            status = 'ok'
        print(f"Current status: {response['currentState']}")
    change_status_url = f'https://dataplatform-dev-back.liverpool.com.mx/api/pipelines/status/'
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        "pipeline_id": f'{pipeline_id}',
        "status": f'{status}'
    }
    response = requests.put(change_status_url, headers=headers, data=json.dumps(data)).json()
    print(response)

with models.DAG(
    f"{pipeline_name}",
    schedule_interval=None,   # @Once
    description='Pipeline para leer tablas de Hana y cargar en bigquery',
    default_args=default_dag_args,
) as dag:
    get_token = PythonOperator(
        task_id="get_token",
        python_callable=get_token,
        provide_context=True,
    )
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
        provide_context=True,
    )
    status = PythonOperator(
        task_id="check_status",
        python_callable=check_status,
        provide_context=True,
    )

get_token >> create_request >> status
