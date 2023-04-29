# Import libraries
import pandas as pd
from pathlib import Path

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket

from datetime import timedelta

# Extract
@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=30))
def extract():
    """load CSV file data"""

    # CSV file path
    file_path = "/Users/davidmasupa/development/tutorial-experiment/Workflow-Orchestration/Data/CustomerDemographic.csv"

    # Read CSV file into Pandas DataFrame
    df = pd.read_csv(file_path)

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

# Load
@task(log_prints=True, retries=3)
def load_to_gcs(df: pd.DataFrame) -> None:
    """Upload CSV file to Google Cloud Storage"""

    # GCS Bucket Block
    gcs_bucket = GcsBucket.load("cs-dtc-practice-block")

    # Upload DataFrame to GCS
    gcs_bucket.upload_from_dataframe(
        df=df,
        to_path=Path("Data/CustomerDemographic.csv"),
        serialization_format="parquet"
    )

    return


# ETL
@flow(name="Ingest CSV into GCS")
def etl():
    """Main ETL function"""
    # Extract
    df = extract()

    # Transform
    df = change_dtypes(df)

    # Upload to GCS
    load_to_gcs(df)


if __name__ == "__main__":
    etl()
