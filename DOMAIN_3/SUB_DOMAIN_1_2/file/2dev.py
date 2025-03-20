import datetime
import json
import requests
import os
from airflow import models
from airflow.operators.python_operator import PythonOperator


# commons
flex_template = 'csv_flex_template'               # 'csv_flex_template.json' (properties)
service_account = 'id-crp-dev-data-platform-dataf@crp-dev-data-platform.iam.gserviceaccount.com'           # '457816054800-compute@developer.gserviceaccount.com' (properties)
pipeline_name = '2dev'               # 'dwh_demo_dev_csv'
product_request_id = 3
pipeline_id = 2
dataflow_bucket = 'crp-dev-data-platform-bkt01'           # 'gs://demo_dwh_bkt'  (properties)
domain = 'DOMAIN_3'
subdomain = 'SUB_DOMAIN_1_2'
stage = '<STAGE>>'
dataflow_project = 'crp-dev-data-platform-bkt01'         # 'labuniformes'    Nombre del proyecto de la plataforma (properties)
target_project = 'crp-dev-data-platform'             # 'labuniformes'    Nombre del proyecto de BQ destino
target_dataset =  'demo_dwh'            # 'demo_dwh'
target_table = 'oferta'                 # 'demo_test_csv'
datetime_start = ''             # '2025-02-10 12:30'
datetime_end = ''                 # '2025-02-10 12:30' default None
subnetwork = 'projects/corp-dev-net-shared/regions/us-east4/subnetworks/corp-dev-nae4-data-platform-workers-sub-1'                     # 'projects/labuniformes/regions/us-east4/subnetworks/default' (properties)
private_ip = 'False'
machine_type = 'n1-standard-1'
region = 'us-east4'                             # 'us-east4' (properties)

# especificos
origin_bucket =  'crp-dev-data-platform-bkt01'              # 'gs://demo_dwh_bkt'
prefix = 'null'                             # default ''
filename = 'null'                         # 'demo_dwh'
sufix = 'null'                               # default ''
separator = 'null'                       # '_'
ext = 'null'                                   # 'csv'
datetime_format = 'null'           # '"%Y-%m-%d-%H-%M'
delimitator = 'null'                   # ','
date_field_name = '<DATE_FIELD_NAME>'
allow_multi_date = TRUE         # False
allow_accum_date = TRUE         # False
allow_multi_file = TRUE         # False


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
            "containerSpecGcsPath": f'{dataflow_bucket}/templates/{flex_template}',
            "parameters": {
                "staging_location": f'{dataflow_bucket}/staging/{stage}',
                "service_account_email": service_account,
                "machine_type": machine_type,
                "schema_file_path": f'{dataflow_bucket}/schemas/{stage}/{pipeline_name}.json',
                "temp_folder": f'{dataflow_bucket}/temp/{stage}',
                'pipeline_name': pipeline_name,
                'dest_table_name': f'{target_project}.{target_dataset}.{target_table}',
                "origin_file_path": f'{origin_bucket}/source_csv',
                'prefix': prefix,
                'filename': filename,
                'sufix': sufix,
                'separator': separator,
                'ext': ext,
                'datetime_format': datetime_format,
                'delimitator': delimitator
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
    description='Pipeline para leer de csv y cargar en bigquery',
    default_args=default_dag_args,
) as dag:
    create_request = PythonOperator(
        task_id="execute_dataflow_job",
        python_callable=trigger_job,
    )



