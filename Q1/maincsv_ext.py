from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import io
from pymongo import MongoClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date
from datetime import datetime, timedelta

PROJECT_ID = "tawandang-tt"
BUCKET_NAME = f"{gcs_bucket}"
mongo_uri= f"{mongod_uri}"
ptn_yyyy=date.today().year
ptn_mm=date.today().month
ptn_dd= date.today().day
acc_raw = f"raw_data/account/ptn_yyyy={ptn_yyyy}/ptn_mm={ptn_mm}/ptn_dd={ptn_dd}/account_extract.csv" #updated path
txn_raw = f"raw_data/transaction/ptn_yyyy={ptn_yyyy}/ptn_mm={ptn_mm}/ptn_dd={ptn_dd}/transaction_extract.csv" #updated path
acc_pers = f"persist_data/account/ptn_yyyy={ptn_yyyy}/ptn_mm={ptn_mm}/ptn_dd={ptn_dd}/account.parquet" #updated path
txn_pers = f"persist_data/transaction/ptn_yyyy={ptn_yyyy}/ptn_mm={ptn_mm}/ptn_dd={ptn_dd}/transaction.parquet" #updated path
start_date = datetime.now() + timedelta(days=1)

default_args = {
    'owner': 'tana',
}

def save_dataframe_to_gcs(df, bucket_name,csv_file_name):
    

    try:
        # csv_file_name = f"ptn_yyyy={ptn_yyyy}/ptn_mm={year}/ptn_dd={year}/your_file.csv" #updated path
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(csv_file_name)

        with io.StringIO() as buffer:
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            blob.upload_from_string(buffer.read(), content_type='text/csv')

        print(f"DataFrame saved successfully to gs://{bucket_name}/{csv_file_name}")

    except Exception as e:
        print(f"Error saving DataFrame to GCS: {e}")
        raise

def load_csv_save_parquet( csv_file_path,bucket_name, parquet_file_path):
    
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(bucket_name)

        # Load CSV from GCS
        csv_blob = bucket.blob(csv_file_path)
        with io.StringIO(csv_blob.download_as_string().decode('utf-8')) as csv_file:
            df = pd.read_csv(csv_file)
            df.insert(0, 'data_date',  date.today())
            df['ptn_yyyy'] = ptn_yyyy
            df['ptn_mm'] = ptn_mm
            df['ptn_dd'] = ptn_dd

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Save Parquet to GCS
        parquet_blob = bucket.blob(parquet_file_path)
        with io.BytesIO() as buffer:
            pq.write_table(table, buffer)
            parquet_blob.upload_from_string(buffer.getvalue())

        print(f"Successfully saved Parquet file to: gs://{bucket_name}/{parquet_file_path}")
        return True

  
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

@task()
def extract_raw_transaction():
    client = MongoClient(mongo_uri)
    db = client['sample_analytics']
    collection = db['transactions']
    data = list(collection.find())
    client.close()

    df = pd.DataFrame(data)
    save_dataframe_to_gcs(df, BUCKET_NAME, txn_raw)
    print('done saving transaction')

@task()
def extract_raw_account():
    client = MongoClient(mongo_uri)
    db = client['sample_analytics']
    collection = db['accounts']
    data = list(collection.find())
    client.close()

    df = pd.DataFrame(data)
    save_dataframe_to_gcs(df, BUCKET_NAME, acc_raw)
    print('done saving account')

@task()
def transform_to_persist(csv_path,bucket_name,parquet_path):
    
    success = load_csv_save_parquet( csv_path,bucket_name, parquet_path)
    if not success:
        #Handle the failure appropriately in your application
        print("Parquet file saving failed.")
 

# @dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=["tawan"])
@dag(default_args=default_args, schedule_interval="0 21 * * *", start_date=start_date, tags=["tawan"])
def main_csvext():
    
    t1 = extract_raw_transaction()
    t2 = extract_raw_account()
    t3 = transform_to_persist(csv_path=txn_raw,bucket_name=BUCKET_NAME,parquet_path=txn_pers)
    t4 = transform_to_persist(csv_path=acc_raw,bucket_name=BUCKET_NAME,parquet_path=acc_pers)
    # t5 = merge()
    t5 = GCSToBigQueryOperator(
        task_id = 'txn_gcs_to_bigquery',
        bucket= BUCKET_NAME,
        source_objects=[txn_pers],
        source_format='PARQUET',
        destination_project_dataset_table='datalake.transaction_csv',
        write_disposition='WRITE_TRUNCATE'
    )
    t6 = GCSToBigQueryOperator(
        task_id = 'acc_gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[acc_pers],
        source_format='PARQUET',
        destination_project_dataset_table='datalake.account_csv',
        write_disposition='WRITE_TRUNCATE'
    )
    t1>>t3>>t5
    t2>>t4>>t6

main_csvext()




