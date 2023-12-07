from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import os

# Step 2: Initiating the default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
    'postgres_conn_id': 'postgres_default',
    'schema': 'public',
}

# Step 3: Creating DAG Object
dag = DAG(dag_id='DAG-1',
          default_args=default_args,
          schedule_interval='@daily',
        
          catchup=False
          )

# Step 4: Creating tasks
start = DummyOperator(task_id='start', dag=dag)

# ETL Process
def extract_data_from_data_folder(**kwargs):
    folder_path = '/opt/airflow/data'  # Change 'airflow' to 'dags'
    
    # Add a step to create the directory if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    data_frames = []

    file_list = os.listdir(folder_path)

    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)

        if file_name.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_name.endswith('.json'):
            df = pd.read_json(file_path)
        elif file_name.endswith('.avro'):
            import fastavro
            with open(file_path, 'rb') as avro_file:
                avro_reader = fastavro.reader(avro_file)
                df = pd.DataFrame.from_records(avro_reader)
        elif file_name.endswith('.parquet'):
            df = pd.read_parquet(file_path)
        elif file_name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_path)
        else:
            # Handle unsupported file format or skip the file
            continue

        data_frames.append(df)

    if not data_frames:
        # Add handling if no matching data files are found in the directory
        raise ValueError("No matching data files found in the directory.")

    combined_data = pd.concat(data_frames, ignore_index=True)
    return combined_data

def transform_data_from_data_folder(**kwargs):
    ti = kwargs['ti']
    extracted_data = ti.xcom_pull(task_ids='extract_data_task')
    
    # Ensure that extracted_data is not empty before proceeding
    if extracted_data.empty:
        raise ValueError("Extracted data is empty. Check the extraction task.")

    # Transformation logic
    transformed_data = extracted_data.copy()
    transformed_data['population_in_millions'] = transformed_data['population'] / 1000000

    return transformed_data

def create_postgresql_table(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data_task')

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_default",
        schema="public"
    )

    table_name = "your_target_table_name"  # Adjust the table name
    table_schema = """
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        population INT,
        population_in_millions FLOAT
    """
    
    postgres_hook.run(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema});")

def load_data_into_postgresql(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data_task')

    postgres_hook = PostgresHook(
        postgres_conn_id="postgres_default",
        schema="public"
    )

    table_name = "your_target_table_name"  # Adjust the table name
    transformed_data.to_sql(table_name, postgres_hook.get_sqlalchemy_engine(), if_exists='replace', index=False)

# Creating tasks
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data_from_data_folder,
    provide_context=True,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data_from_data_folder,
    provide_context=True,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_postgresql_table,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data_into_postgresql,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Setting up dependencies
start >> extract_data_task >> transform_data_task >> [create_table_task, load_data_task] >> end
