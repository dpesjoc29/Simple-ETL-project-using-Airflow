from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datetime import timedelta
import pandas as pd
from airflow.utils.dates import days_ago


default_args = {
    'owner':'Dipesh',
    'start_date':days_ago(7),
    'email': 'abc@gmail.com',
    'email_on_failure':True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay' : timedelta(minutes=1),
}


def extract_data_csv():
    input_file_csv = 'airflow/dags/finalassignment/vehicle-data.csv'
    output_file_csv = '/home/dpesjoc29/airflow/dags/finalassignment/staging/csv_data.csv'

    # Define column names based on the data structure
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Vehicle code']

    # Define the specific columns to extract
    selected_columns_csv = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']

    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_file_csv, names=column_names)

    # Extract specific columns
    extracted_data = df[selected_columns_csv]

    # Save the extracted data to a new CSV file
    extracted_data.to_csv(output_file_csv, index=False)


def extract_data_tsv():
    input_file_tsv = 'airflow/dags/finalassignment/tollplaza-data.tsv'
    output_file_tsv = '/home/dpesjoc29/airflow/dags/finalassignment/staging/tsv-data.csv'

    # Define column names based on the data structure
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles','Tollplaza id', 'Tollplaza code']
    selected_columns_tsv = ['Number of axles','Tollplaza id','Tollplaza code']
    
    df = pd.read_csv(input_file_tsv , sep='\t', names=column_names)
    extracted_data_tsv = df[selected_columns_tsv]

    extracted_data_tsv.to_csv(output_file_tsv, index = False)


def extract_data_fixed_width():
    input_file_text = 'airflow/dags/finalassignment/payment-data.txt'
    output_file_text ='/home/dpesjoc29/airflow/dags/finalassignment/staging/fixed_width_data.csv'

     # Define the column widths based on the data structure
    column_widths = [2, 3, 15, 6, 5, 12, 5]

     # Define the column names
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code']


    # Read fixed-width file and extract specific fields
    df = pd.read_fwf(input_file_text, widths=column_widths, names=column_names , header = None)
    
    # Extract specific columns
    selected_columns_txt = ['Type of Payment code', 'Vehicle Code']
    extracted_data_text = df[selected_columns_txt]
    
    # Save the extracted data to a new CSV file
    extracted_data_text.to_csv(output_file_text, index=False)


def transform_and_load_data():
    input_file = '/home/dpesjoc29/airflow/dags/finalassignment/staging/extracted_data.csv'  
    output_file = '/home/dpesjoc29/airflow/dags/finalassignment/staging/transformed_data.csv'  

    # Read CSV and transform data
    df = pd.read_csv(input_file)
    
    # Transform the 'vehicle_type' field to capital letters
    df['Vehicle type'] = df['Vehicle type'].str.upper()

    # Save the transformed data to a new CSV file in the staging directory
    df.to_csv(output_file, index=False)



with DAG(
        dag_id = 'ETL_toll_data',
        schedule_interval= '@once',   
        default_args= default_args,
        description= 'Apache Airflow Final Assignment'
    ) as dag:

    unzip_data  = BashOperator(
        task_id = 'unzip_data',
        bash_command = 'tar -xzvf /home/dpesjoc29/airflow/dags/finalassignment/tolldata.tgz -C /home/dpesjoc29/airflow/dags/finalassignment',
        
    )

    extract_data_from_csv = PythonOperator(
        task_id = 'extract_data_from_csv',
        python_callable= extract_data_csv,
        # dag = dag,
    )
    
    extract_data_from_tsv  = PythonOperator(
        task_id = 'extract_data_from_tsv',
        python_callable= extract_data_tsv,
        # dag = dag,
    )

    extract_data_from_fixed_width = PythonOperator(
        task_id ='extract_data_from_fixed_width',
        python_callable= extract_data_fixed_width,
        # dag= dag,
    )

    consolidate_data = BashOperator(
        task_id = 'consolidate_data',
        bash_command= "paste -d',' /home/dpesjoc29/airflow/dags/finalassignment/staging/csv_data.csv /home/dpesjoc29/airflow/dags/finalassignment/staging/tsv-data.csv /home/dpesjoc29/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/dpesjoc29/airflow/dags/finalassignment/staging/extracted_data.csv",
        # dag =dag,
    )

    transform_data = PythonOperator(
        task_id ='transform_data',
        python_callable= transform_and_load_data,
        # dag = dag,
    )


    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data 
    