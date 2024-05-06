from airflow import DAG
from airflow.providers.cncf.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define DAG details
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start a day ago
}

with DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,  # Manual triggering for now
) as dag:

    # Sensor to wait for the input file
    wait_for_data = FileSensor(
        task_id='wait_for_data',
        filepath='/path/to/data.csv',  # Update with your file path
    )

    # Python function to perform data cleaning
    def clean_data(input_file, output_file):
        with open(input_file) as f_in, open(output_file, 'w') as f_out:
            # Simple cleaning example, remove header or empty lines
            data = [line.strip() for line in f_in.readlines() if line.strip()]
            f_out.writelines(data)

    # Task to clean the data
    clean_data_task = PythonOperator(
        task_id='clean_data',
        provide_context=True,  # Provide input file path
        python_callable=clean_data,
        op_args={'input_file': '/path/to/data.csv', 'output_file': '/path/to/cleaned_data.csv'},  # Update file paths
    )

    # Define task dependencies
    wait_for_data >> clean_data_task

# Set task dependencies explicitly (optional but good practice)
wait_for_data.set_downstream(clean_data_task)
