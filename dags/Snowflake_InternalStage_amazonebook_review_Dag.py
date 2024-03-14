 
import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from utility.database_helper import database_helper,snowflake_helper
from utility.utility import get_lastextract_snowflake, get_currentdate_extract_mysql, download_data_local, load_data_to_snowsql_stage, load_stage_to_snowsql_table, drop_stage_snowswl_table, update_snowsql_config
import json

main_data = {
        'database_name' : 'amazonebooks',
        'schema' : 'PUBLIC',
        'stage' : 'amazonebook_reviews_stage',
        'table_name' : 'amazonebook_reviews',
        'output_dir_path' : '/home/de/Outputs/Snowflake/',
        'warehouse' : 'COMPUTE_WH',
        'load_type' : 'Incremental'
       }

def preparedata(ti):

    # Get Last extract Date from Snowflake Config Schema
    data = {
        "database_name":main_data["database_name"],
        "table_name":main_data["table_name"],
        "outdir_path":main_data["output_dir_path"]
        }
    snow_df = get_lastextract_snowflake(data)

    # Get Current Extract Dates (It couls be moer than 1 Date) get it from on-prem Database
    current_extract_date_objs = get_currentdate_extract_mysql(snow_df,data)


    data["current_extract_date_objs"]=current_extract_date_objs



    data_location = download_data_local(data)

    ti.xcom_push(key=main_data["table_name"],value=json.dumps(data_location))

def load_data_to_stage(ti):
    data_location = ti.xcom_pull(key=main_data["table_name"], task_ids='Prepare_Data')
    data_location_dict = json.loads(data_location)

    data = {
    "database_name": main_data["database_name"],
    "warehouse": main_data["warehouse"],
    "schema": main_data["schema"],
    "table_name":main_data["table_name"],
    "data_location_dict":data_location_dict
        }

    load_data_to_snowsql_stage(data)

def load_stage_to_table():
    data = {
    "database_name" : main_data["database_name"],
    "table_name" : main_data["table_name"],
    "warehouse": main_data["warehouse"],
    "schema" : main_data["schema"],
    "load_type": main_data["load_type"]
        }
    load_stage_to_snowsql_table(data)

def update_config():
    data = {
    "database_name" : main_data["database_name"],
    "table_name" : main_data["table_name"],
    "warehouse": main_data["warehouse"],
    "schema" : main_data["schema"],
    "load_type": main_data["load_type"]
        }
    update_snowsql_config(data)
    drop_stage_table()


def drop_stage_table():
    data = {
    "database_name" : main_data["database_name"],
    "table_name" : main_data["table_name"],
    "warehouse": main_data["warehouse"],
    "schema" : main_data["schema"],
    "load_type": main_data["load_type"],
    "stage": main_data["stage"]
        }
    drop_stage_snowswl_table(data)


with DAG(
    dag_id="Snowflake_InternalStage_amazonebook_review_Dag",
    start_date=datetime.datetime(2024,1,1),
    schedule=None
    ) as dag:

    Prepare_data_task = PythonOperator(
            task_id='Prepare_Data',
            python_callable=preparedata
        )

    Load_data_to_stage_task = PythonOperator(
            task_id='Load_Data_to_Stage',
            python_callable=load_data_to_stage
        )
    Load_stage_to_table_task = PythonOperator(
            task_id='Load_Stage_to_Table',
            python_callable=load_stage_to_table
        )
    Update_configs_task = PythonOperator(
            task_id='Update_Configs',
            python_callable=update_config
        )


    Start = EmptyOperator(task_id="Start")
    End   = EmptyOperator(task_id="End")
    Start >> Prepare_data_task >> Load_data_to_stage_task >> Load_stage_to_table_task >> Update_configs_task >> End
