# Import libraries
import os
import pandas as pd
from pathlib import Path

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


if __name__ == "__main__":
    # Extract data
    raw_data_df = extract()

    # Correct data-types
    df = change_dtypes(raw_data_df)

    print(df.dtypes)
