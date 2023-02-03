from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}")

@task(retries=3)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path) 
    print(f"rows processed: {df['passenger_count'].count()}")
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df["passenger_count"].fillna(0, inplace=True)
    #print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="ny-taxi-zoom",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int , month: int, color: str ):
    """Main ETL flow to load data into Big Query"""
    #color = "yellow"
    #color = "green"
    #year = 2021
    #year = 2020
    #month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
    return len(df)


@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    rows = 0
    for month in months:
        rows += etl_gcs_to_bq(year, month, color)
    print(f"Total rows processed: {rows}")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Total rows processed: {rows}")