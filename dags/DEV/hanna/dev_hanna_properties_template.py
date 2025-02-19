import datetime
import json
import requests
import os
from airflow import models
from airflow.operators.python_operator import PythonOperator


# commons
flex_template = '<FLEX_TEMPLATE>'               # 'csv_flex_template.json' (properties)
service_account = '"457816054800-compute@developer.gserviceaccount.com"'           # '457816054800-compute@developer.gserviceaccount.com' (properties)
pipeline_name = '"null"'               # 'dwh_demo_dev_hana'
product_request_id = '"6"'
pipeline_id = '"null"'
dataflow_bucket_name = '<DATAFLOW_BUCKET_NAME>' # 'demo_dwh_bkt'  (properties)
domain = '"null"'
subdomain = '"null"'
stage = '"null">'
dataflow_project = '"labuniformes"'         # 'labuniformes'    Nombre del proyecto de la plataforma (properties)
target_project = '"project_dev"'             # 'labuniformes'    Nombre del proyecto de BQ destino
target_dataset =  '"dataset_dev"'            # 'demo_dwh'
target_table = '"table_dev"'                 # 'demo_test_csv'
datetime_start = '"2025-02-19T13:25:00.776356"'             # '2025-02-10 12:30'
datetime_end = '"null"'                 # '2025-02-10 12:30' default None
subnetwork = '"projects/labuniformes/regions/us-east4/subnetworks/default"'                     # 'projects/labuniformes/regions/us-east4/subnetworks/default' (properties)
private_ip = 'False'
machine_type = 'n1-standard-1'
region = '"us-east4"'                             # 'us-east4' (properties)

# especificos
secret_name_origin =  '<SECRET_NAME_ORIGIN>'    # 'hanasecret'
query = '"null"'                               # default None
schema_name = '<SCHEMA_NAME>'                   # 'dwh'


default_dag_args = {
    "start_date": datetime.datetime(2025, 2, 10, 12, 59),
}


def trigger_job(ti):
    url = f'https://dataflow.googleapis.com/v1b3/projects/{dataflow_project}/locations/{region}/flexTemplates:launch'
    headers = {
        'Authorization': f'Bearer {os.environ.get("TOKEN")}',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
        "launch_parameter": {
            "jobName": "testcsv1",
            "containerSpecGcsPath": f'gs://{dataflow_bucket_name}/templates/{flex_template}',
            "parameters": {
                "staging_location": f'gs://{dataflow_bucket_name}/staging/{stage}',
                "dataflow_bucket_name": dataflow_bucket_name,
                "service_account_email": service_account,
                "machine_type": machine_type,
                "dataflow_project": dataflow_project,
                "schema_name": schema_name,
                "temp_folder": f'gs://{dataflow_bucket_name}/temp/{stage}',
                'pipeline_name': pipeline_name,
                'dest_table_name': f'{target_project}.{target_dataset}.{target_table},
                "secret_name_origin": f'{secret_name_origin}',
                "query": f'{query}'
            },
            "environment": {
                "subnetwork": f'https://www.googleapis.com/compute/v1/{subnetwork}',
                "additionalExperiments": [],
                "additionalUserLabels": {}
            }
        }
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    print(response.json())


with models.DAG(
    f"{pipeline_name}",
    schedule_interval=None,   # @Once
    description='Pipeline para leer tablas de Hana y cargar en bigquery',
    default_args=default_dag_args,
) as dag:
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
    )



