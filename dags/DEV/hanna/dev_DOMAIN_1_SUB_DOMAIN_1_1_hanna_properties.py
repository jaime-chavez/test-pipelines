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
from airflow.operators.python_operator import PythonOperator


"""
 * @author: Jaime Arturo Chávez
 * @updated: 
 * @description: hana dag template
 * @since-version: 1.0
"""

pipeline_id = 5
pipeline_name = 'dev_DOMAIN_1_SUB_DOMAIN_1_1_hanna_properties'    
flex_template = 'hana_flex_template.json'        
stage = 'dev'
secret_name_origin =  'hanasecret'
query = 'Select VALUE, PROPERTY_TYPE, CREATED, UPDATED, BRAND, SITE from "DWH_AUTOMATION".properties'     
dataflow_bucket_name = 'gs://demo_dwh_bkt'
dataflow_project = 'labuniformes'    
target_project = 'labuniformes'  
target_dataset =  'demo dwh' 
target_table = 'demo_test_csv'  
datetime_start = '2025-02-18 12:30' 
service_account = '457816054800-compute@developer.gserviceaccount.com' 
subnetwork = 'projects/labuniformes/regions/us-east4/subnetworks/default' 
region = 'us-east4' 


default_dag_args = {
    "start_date": datetime.datetime(2025, 2, 10, 12, 59),
    # 'retries': 1,
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
            "jobName": "testhana",
            "containerSpecGcsPath": f'gs://{dataflow_bucket_name}/templates/{flex_template}',
            "parameters": {
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "dataflow_bucket_name": dataflow_bucket_name,
                "service_account_email": service_account,
                "machine_type": 'n1-standard-1',
                "dataflow_project": dataflow_project,
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}',
                'pipeline_name': f'{pipeline_name}',
                'dest_table_name': f'{target_project}.{target_dataset}.{target_table}',
                'secret_name_origin': f'{secret_name_origin}',
                'query': f'{query}',
                'pipeline_id': pipeline_id
            },
            "environment": {
                "subnetwork": f'https://www.googleapis.com/compute/v1/{subnetwork}',
                "additionalExperiments": [],
                "additionalUserLabels": {}
            }
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data)).json()
    print(response)
    kwargs['ti'].xcom_push(key='job_id', value=response['job']['id'])


def check_status(**kwargs):
    job_id = kwargs['ti'].xcom_pull(task_ids='execute_dataflow_job', key='job_id')
    status = None
    mensaje = ''

    url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/jobs/{job_id}'
    headers = {
        'Authorization': f'Bearer {os.environ.get("TOKEN")}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    while status != 'ok' and status != 'error':
        time.sleep(30)
        response = requests.get(url, headers=headers).json()

        if response['currentState'] == 'JOB_STATE_FAILED':
            status = 'error'
            mensaje = 'error description'

        elif response['currentState'] == 'JOB_STATE_DONE':
            status = 'ok'

        print(f"Current status: {response['currentState']}")


    # change_status_url = f'http://localhost:8090/api/pipelines/status/{pipeline_id}'
    # headers = {
    #     'Accept': 'application/json',
    #     'Content-Type': 'application/json'
    # }
    # data = {
    #     "status": f'{status}',
    #     "mensaje": f'{mensaje}'
    # }
    # response = requests.post(url, headers=headers, data=json.dumps(data)).json()
    # print(response)


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

