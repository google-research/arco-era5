import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import os
import re

import typing as t

from arco_era5 import update_config_files, get_previous_month_dates, get_secret
from data_automate import check_data_availability, date_range, replace_non_alphanumeric_with_hyphen, subprocess_run, resize_zarr_target, parse_arguments, ingest_data_in_zarr_dataflow_job

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DIRECTORY = "/weather/config_files"
DIRECTORY = "/usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/raw"
FIELD_NAME = "date"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SDK_CONTAINER_IMAGE = os.environ.get("SDK_CONTAINER_IMAGE")
# MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
API_KEY_PATTERN = re.compile(r"^API_KEY_\d+$")
API_KEY_LIST = []

ZARR_FILES_LIST = [
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
    'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2'
]
BQ_TABLES_LIST = [
    "grid-intelligence-sandbox.dabhis_test.full_37-1h-0p25deg-chunk-1-v3",
    "grid-intelligence-sandbox.dabhis_test.model-level-moisture-v2",
    "grid-intelligence-sandbox.dabhis_test.model-level-wind-v2",
    "grid-intelligence-sandbox.dabhis_test.single-level-forecast-v2",
    "grid-intelligence-sandbox.dabhis_test.single-level-reanalysis-v2",
    "grid-intelligence-sandbox.dabhis_test.single-level-surface-v2"
]
REGION_LIST = [
    'us-east1',
    'us-west4',
    'us-central1',
    'us-east4',
    'us-west1',
    'us-east7',
]

dates_data = get_previous_month_dates()

# Functions
def raw_data_download_dataflow_job():
    """
    Launches a Dataflow job to process weather data.
    """
    current_day = datetime.date.today()
    job_name = f"raw-data-download-arco-era5-{current_day.month}-{current_day.year}"

    command = (
        f"python weather_dl/weather-dl /usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/raw/era5_ml_dve.cfg --runner "
        f"DataflowRunner --project {PROJECT} --region {REGION} --temp_location "
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--sdk_container_image {SDK_CONTAINER_IMAGE} --experiment use_runner_v2 "
        f"--use-local-code"
        # f"--manifest-location {MANIFEST_LOCATION} "
    )
    subprocess_run(command)


def data_splitting_dataflow_job(date: str):
    """
    Launches a Dataflow job to splitting soil & pcp weather data.
    """
    year = date[:4]
    month = year + date[5:7]
    DATASETS = ['soil', 'pcp']
    typeOfLevel = '{' + 'typeOfLevel' + '}'
    shortName = '{' + 'shortName' + '}'
    zero = '{' + '0' + '}'
    first = '{' + '1' + '}'
    commands = []
    for DATASET in DATASETS:
        command = (
            f'python weather_sp/weather-sp --input-pattern '
            f' "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_{DATASET}.grb2" '
            f'--output-template "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{first}/{zero}.grb2_{typeOfLevel}_{shortName}.grib" '
            f'--runner DataflowRunner --project {PROJECT} --region {REGION} '
            f'--temp_location gs://{BUCKET}/tmp --disk_size_gb 3600 '
            f'--job_name split-{DATASET}-data '
            f'--sdk_container_image "gcr.io/grid-intelligence-sandbox/miniconda3-beam:weather-tools-with-aria2" '
            f'--use-local-code '
        )
        commands.append(command)

    with ThreadPoolExecutor() as tp:
        for command in commands:
            tp.submit(subprocess_run, command)


def ingest_data_in_bigquery_dataflow_job(zarr_file: str, table_name: str, region: str,
                                         zarr_kwargs: str) -> None:
    """
    Ingests data from a Zarr file into BigQuery and runs a Dataflow job.

    Args:
        zarr_file (str): The Zarr file path.
        table_name (str): The name of the BigQuery table.
        zarr_kwargs (Any): Additional arguments for the Zarr ingestion.

    Returns:
        None
    """
    job_name = zarr_file.split('/')[-1]
    job_name = os.path.splitext(job_name)[0]
    job_name = (
        f"data-ingestion-into-bq-{replace_non_alphanumeric_with_hyphen(job_name)}")

    command = (
        f"python weather_mv/weather-mv bq --uris {zarr_file} --output_table {table_name} "
        f"--runner DataflowRunner --project {PROJECT} --region {region} "
        f"--temp_location gs://{BUCKET}/tmp --job_name {job_name} "
        f"--use-local-code --zarr --disk_size_gb 500 --machine_type n2-highmem-4 "
        f"--number_of_worker_harness_threads 1 --zarr_kwargs {zarr_kwargs} "
    )

    subprocess_run(command)


def process_zarr_and_table(z_file: str, table: str, region: str, start_date: str, end_date: str,
                           init_date: str):
    try:
        logger.info(f"Resizing zarr file: {z_file} started.")
        resize_zarr_target(z_file, end_date, init_date)
        logger.info(f"Resizing zarr file: {z_file} completed.")
        logger.info(f"data ingesting for {z_file} is started.")
        ingest_data_in_zarr_dataflow_job(z_file, region, start_date, end_date, init_date,
                                         PROJECT, BUCKET, SDK_CONTAINER_IMAGE)
        logger.info(f"data ingesting for {z_file} is completed.")
        start = f' "start_date": "{start_date}" '
        end = f'"end_date": "{end_date}" '
        zarr_kwargs = "'{" + f'{start},{end}' + "}'"
        logger.info(f"data ingesting into BQ table: {table} started.")
        ingest_data_in_bigquery_dataflow_job(z_file, table, region, zarr_kwargs)
        logger.info(f"data ingesting into BQ table: {table} completed.")
    except Exception as e:
        logger.error(
            f"An error occurred in process_zarr_and_table for {z_file}: {str(e)}")


def process(z_file: str, table: str, region: str, init_date: str):
    # Function to process a single pair of z_file and table
    if '/ar/' in z_file:
        process_zarr_and_table(z_file, table, region, dates_data["first_day_first_prev"],
                               dates_data["last_day_first_prev"], init_date)
    else:
        process_zarr_and_table(z_file, table, region, dates_data["first_day_third_prev"],
                               dates_data["last_day_third_prev"], init_date)


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments("Parse arguments.")

        logger.info("program is started.")
        co_date_range = date_range(
            dates_data["first_day_third_prev"], dates_data["last_day_third_prev"]
        )
        ar_date_range = date_range(
            dates_data["first_day_first_prev"], dates_data["last_day_first_prev"]
        )

        for env_var in os.environ:
            if API_KEY_PATTERN.match(env_var):
                api_key_value = os.environ.get(env_var)
                API_KEY_LIST.append(api_key_value)

        additional_content = ""
        for count, secret_key in enumerate(API_KEY_LIST):
            secret_key_value = get_secret(secret_key)
            additional_content += f'parameters.api{count}\n\
                api_url={secret_key_value["api_url"]}\napi_key={secret_key_value["api_key"]}\n\n'

        update_config_files(DIRECTORY, FIELD_NAME, additional_content)
        logger.info("Raw data downloading start.")
        raw_data_download_dataflow_job()
        logger.info("Raw data downloaded successfully.")

        logger.info("Raw data Splitting start.")
        data_splitting_dataflow_job(
            dates_data['first_day_first_prev'].strftime("%Y/%m"))
        logger.info("Raw data Splitting successfully.")

        data_is_missing = 1  # Initialize with a non-zero value
        while data_is_missing:
            data_is_missing = check_data_availability(co_date_range, ar_date_range)
            if data_is_missing:
                logger.warning("data is missing..")
                raw_data_download_dataflow_job()
        logger.info("Data availability check completed.")

        with ThreadPoolExecutor(max_workers=8) as tp:
            for z_file, table, region in zip(ZARR_FILES_LIST, BQ_TABLES_LIST, REGION_LIST):
                tp.submit(process, z_file, table, region, parsed_args.init_date)

        logger.info("All data ingested into BQ.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")