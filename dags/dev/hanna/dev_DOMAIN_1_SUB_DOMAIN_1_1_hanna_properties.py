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
from airflow import models
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator

"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: hana dag template
 * @since-version: 1.0
"""

job_name = "devhanna"
pipeline_id = 65
pipeline_name = 'dev_DOMAIN_1_SUB_DOMAIN_1_1_hanna_properties'    
flex_template = 'hana_flex_template.json'        
stage = 'dev'
secret_name_origin =  'hanasecret'
query = 'Select ID, KEY, VALUE, PROPERTY_TYPE, CREATED, UPDATED, BRAND, SITE from "DWH_AUTOMATION".properties'     
dataflow_bucket_name = 'demo_dwh_bkt'
dataflow_project = 'labuniformes'    
target_project = 'labuniformes'  
target_dataset =  'demo dwh' 
target_table = 'properties'  
datetime_start = '2025-02-18 12:30' 
service_account = '457816054800-compute@developer.gserviceaccount.com' 
subnetwork = 'projects/labuniformes/regions/us-east4/subnetworks/default' 
region = 'us-east4' 


default_dag_args = {
    "start_date": datetime.datetime(2025, 2, 10, 12, 59),
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=5),
}


def trigger_job(**kwargs):
    url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/flexTemplates:launch'
    headers = {
        'Authorization': f'Bearer {os.environ.get("TOKEN")}',
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
                'dest_table_name': f'{target_project}.{target_dataset}.{target_table}',
                "machine_type": 'n1-standard-1',
                'pipeline_id': f'{pipeline_id}',
                'pipeline_name': f'{pipeline_name}',
                'query': f'{query}',
                'secret_name_origin': f'{secret_name_origin}',
                "service_account_email": f'{service_account}',
                'stage': f'{stage}',
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}'
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
    status = None

    if job_id is None:
        print('job_id is missing')
        raise AirflowFailException

    job_url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/jobs/{job_id}'
    headers = {
        'Authorization': f'Bearer {os.environ.get("TOKEN")}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    while status is None:
        time.sleep(30)
        response = requests.get(job_url, headers=headers).json()

        if response['currentState'] == 'JOB_STATE_FAILED':
            status = 'error'

        elif response['currentState'] == 'JOB_STATE_DONE':
            status = 'ok'

        print(f"Current status: {response['currentState']}")


    change_status_url = f'http://localhost:8090/api/pipelines/status/{pipeline_id}'
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
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
        provide_context = True,
    )

    status = PythonOperator(
        task_id="check_status",
        python_callable=check_status,
        provide_context=True,
    )

create_request >> status
