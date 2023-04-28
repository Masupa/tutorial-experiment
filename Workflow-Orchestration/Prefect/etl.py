# Import libraries
import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash

from datetime import timedelta

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract() -> pd.DataFrame:
    """Function extracts CSV file

    Returns:
    --------
    df : pd.DataFrame
        Pandas DataFrame with raw data
    """

    # file path "../Data/CustomerDemographic.csv"
    file_path = "/Users/davidmasupa/development/tutorial-experiment/workflow-orchestration/Data/CustomerDemographic.csv"

    # load CSV file
    df = pd.read_csv(file_path)

    return df

@task()
def change_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Functions ensure the columns data-types are
    correct

    Parameters:
    -----------
    df : pd.DataFrame
        Pandas DataFrame with raw data

    Returns:
    --------
    df : pd.DataFrame
        Pandas DataFrame with cleaned
    """

    # Convert `DOB` dtype to `datetime`
    print(df["DOB"].dtype)
    df["DOB"] = pd.to_datetime(df["DOB"])
    print(df["DOB"].dtype)

    return df

@task(log_prints=True, retries=3)
def export_to_csv(df: pd.DataFrame) -> None:
    """Function exports DataFrame to parquet 
    
    Parameters:
    -----------
    df : pd.DataFrame
        Cleaned pandas DataFrame

    Returns:
    --------
    None
    """

    file_name = "CustomerDemographicsCleaned"

    # file_path
    file_path = f"/Users/davidmasupa/development/tutorial-experiment/workflow-orchestration/Data/{file_name}.csv"

    df.to_csv(file_path, compression="gzip", index=False)


@flow(name="Clean flow")
def etl() -> None:
    """Main etl function"""
    # Extract data
    raw_data_df = extract()

    # Correct data-types
    df = change_dtypes(raw_data_df)

    # Export to `parquet`
    export_to_csv(df)


if __name__ == "__main__":
    etl()
