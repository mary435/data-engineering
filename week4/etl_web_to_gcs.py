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
def etl_web_to_gcs(year, month, color) -> None:
    """The main ETL function""" 
    
    local_file = f"data/{color}/{color}_tripdata_{year}-{month}.csv.gz"

    dataset_file = f"{color}_tripdata_{year}-{month:02}"

    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    path = write_local(dataset_url, local_file)

    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [2, 3], years: list[int] = [2019, 2020], color: str = "green"
):
    for year in years:
        for month in months:
            etl_web_to_gcs(year, month, color)


if __name__ == "__main__":

    color = "green"
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    years = [2019, 2020]
    etl_parent_flow(months, years, color)



