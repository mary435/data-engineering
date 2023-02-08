from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import wget

@task()
def write_local(dataset_url: str, dataset_file: str) -> Path:
    """Write file out locally return path"""

    wget.download(dataset_url, dataset_file)
    
    return dataset_file

@task()
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    
    gcs_block = GcsBucket.load("zoom-gcs")
    
    gcs_block.upload_from_path(
        from_path=path, 
        to_path=path)
    
    return

@flow()
def etl_web_to_gcs(year, month) -> None:
    """The main ETL function""" 
    
    dataset_file = f"data/fhv/fhv_tripdata_{year}-{month}.csv.gz"

    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month}.csv.gz"

    path = write_local(dataset_url, dataset_file)

    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[str] = ['2', '3'], year: int = 2019
):
    for month in months:
        etl_web_to_gcs(year, month)


if __name__ == "__main__":

    months = ['03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    year = 2019
    
    etl_parent_flow(months, year)



