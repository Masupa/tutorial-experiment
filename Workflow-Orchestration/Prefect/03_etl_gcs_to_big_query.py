# Import libraries
import pandas_gbq
import pandas as pd
from pathlib import Path

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.credentials import GcpCredentials

from datetime import timedelta


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def extract_from_gcs(gcs_file_path: str, file_save_path: str) -> None:
    """Extract data from GCS to local"""

    # GCS block
    gcs_block = GcsBucket.load("cs-dtc-practice-block")

    # Get data
    gcs_block.get_directory(
        from_path=gcs_file_path,
        local_path=file_save_path
    )

    return 

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def extract_from_local_machine(gcs_file_path: str, save_file_path: str) -> None:
    """Extract GCS data from local machine"""

    # TODO: Construct csv file path
    csv_file = "Workflow-Orchestration/Data/gcs_data/Data/CustomerDemographic.parquet"
    
    # Import CSV file from GCS
    df = pd.read_parquet(csv_file)

    return df


# Transform
@task(log_prints=True)
def change_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the dtypes of columns"""

    # Convert `DOB` dtypes to datetime
    print(f"{df['DOB'].dtype}")
    df['DOB'] = pd.to_datetime(df['DOB'])
    print(f"{df['DOB'].dtype}")

    return df


# Load to BigQuery Data Warehouse
@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=2))
def load_to_bq(df: pd.DataFrame) -> None:
    """Load Pandas DataFrame to BigQuery"""

    # GCS Credentials Block
    gcp_credentials_block = GcpCredentials.load("cs-dtc-practice-credentials")

    # Export DataFrame to BigQuery
    pandas_gbq.to_gbq(
        dataframe=df,
        project_id="root-setting-384013",
        destination_table="Customer_Dataset.customer_demographics",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        if_exists="append"
    )


@flow(name="Ingest data from GCS to BigQuery")
def etl(gcs_file_path: str, save_file_path: str) -> None:
    # Extract from GCS to local machine
    extract_from_gcs(gcs_file_path, save_file_path)

    # Extract from local machine
    df = extract_from_local_machine(gcs_file_path, save_file_path)

    # Transform
    df = change_dtypes(df)

    # Load to BigQuery Data Warehouse
    # load_to_bq(df) TODO: Fix to error returned here
    

if __name__ == "__main__":
    gcs_file_path = "Data/CustomerDemographic.parquet"

    # Local path to save data from GCS
    save_file_path = "Workflow-Orchestration/Data/gcs_data"

    etl(gcs_file_path, save_file_path)
