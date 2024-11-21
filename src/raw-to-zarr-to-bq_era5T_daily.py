# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os
import re

from concurrent.futures import ThreadPoolExecutor
from arco_era5 import (
    add_licenses_in_config_files,
    check_data_availability,
    data_splitting_dataflow_job,
    date_range,
    get_last_sixth_date,
    get_secret,
    ingest_data_in_zarr_dataflow_job,
    parse_arguments_raw_to_zarr_to_bq,
    raw_data_download_dataflow_job,
    replace_non_alphanumeric_with_hyphen,
    subprocess_run,
    update_date_in_config_file,
    update_zarr_metadata,
    )

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIRECTORY = "/arco-era5/raw"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
PYTHON_PATH = os.environ.get("PYTHON_PATH")
WEATHER_TOOLS_SDK_CONTAINER_IMAGE = os.environ.get("WEATHER_TOOLS_SDK_CONTAINER_IMAGE")
ARCO_ERA5_SDK_CONTAINER_IMAGE = os.environ.get("ARCO_ERA5_SDK_CONTAINER_IMAGE")
API_KEY_PATTERN = re.compile(r"^API_KEY_\d+$")
API_KEY_LIST = []

SPLITTING_DATASETS = ['soil', 'pcp']
ZARR_FILES_LIST = [
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
    'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2'
]
BQ_TABLES_LIST = json.loads(os.environ.get("BQ_TABLES_LIST"))
REGION_LIST = json.loads(os.environ.get("REGION_LIST"))

dates_data = get_last_sixth_date()


def ingest_data_in_bigquery_dataflow_job(zarr_file: str, table_name: str, region: str,
                                         zarr_kwargs: str) -> None:
    """Ingests data from a Zarr file into BigQuery and runs a Dataflow job.

    Args:
        zarr_file (str): The Zarr file path.
        table_name (str): The name of the BigQuery table.
        zarr_kwargs (Any): Additional arguments for the Zarr ingestion.

    Returns:
        None
    """
    if '/ar/' in zarr_file:
        job_name = zarr_file.split('/')[-1]
        job_name = os.path.splitext(job_name)[0]
        job_name = (
            f"data-ingestion-into-bq-{replace_non_alphanumeric_with_hyphen(job_name)}")

        command = (
            f"{PYTHON_PATH} /weather/weather_mv/weather-mv bq --uris {zarr_file} "
            f"--output_table {table_name} --runner DataflowRunner --project {PROJECT} "
            f"--region {region} --temp_location gs://{BUCKET}/tmp --job_name {job_name} "
            f"--use-local-code --zarr --disk_size_gb 500 --machine_type n2-highmem-4 "
            f"--number_of_worker_harness_threads 1 --zarr_kwargs {zarr_kwargs} "
        )

        subprocess_run(command)


def perform_data_operations(z_file: str, table: str, region: str, start_date: str,
                            end_date: str, init_date: str):
    # Function to process a single pair of z_file and table
    try:
        logger.info(f"Data ingesting for {z_file} is started.")
        ingest_data_in_zarr_dataflow_job(z_file, region, start_date, end_date, init_date,
                                         PROJECT, BUCKET,
                                         ARCO_ERA5_SDK_CONTAINER_IMAGE, PYTHON_PATH)
        logger.info(f"Data ingesting for {z_file} is completed.")
        logger.info(f"update metadata for zarr file: {z_file} started.")
        update_zarr_metadata(z_file, end_date)
        logger.info(f"update metadata for zarr file: {z_file} completed.")
        start = f' "start_date": "{start_date}" '
        end = f'"end_date": "{end_date}" '
        zarr_kwargs = "'{" + f'{start},{end}' + "}'"
        # TODO([#414](https://github.com/google/weather-tools/issues/414)): Faster ingestion into BQ by converting
        # the chunk into pd.Dataframe
        # logger.info(f"Data ingesting into BQ table: {table} started.")
        # ingest_data_in_bigquery_dataflow_job(z_file, table, region, zarr_kwargs)
        # logger.info(f"Data ingesting into BQ table: {table} completed.")
    except Exception as e:
        logger.error(
            f"An error occurred in process_zarr_and_table for {z_file}: {str(e)}")


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments_raw_to_zarr_to_bq("Parse arguments.")

        logger.info(f"Automatic update for ARCO-ERA5 started for {dates_data['last_sixth_date']}.")
        data_date_range = date_range(
            dates_data["last_sixth_date"], dates_data["last_sixth_date"]
        )

        for env_var in os.environ:
            if API_KEY_PATTERN.match(env_var):
                api_key_value = os.environ.get(env_var)
                API_KEY_LIST.append(api_key_value)

        licenses = ""
        for count, secret_key in enumerate(API_KEY_LIST):
            secret_key_value = get_secret(secret_key)
            licenses += f'parameters.api{count}\n\
                api_url={secret_key_value["api_url"]}\napi_key={secret_key_value["api_key"]}\n\n'

        logger.info("Config file updation started.")
        add_licenses_in_config_files(DIRECTORY, licenses)
        update_date_in_config_file(DIRECTORY, dates_data)
        logger.info("Config file updation completed.")

        logger.info("Raw data downloading started.")
        raw_data_download_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                       WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                       MANIFEST_LOCATION, DIRECTORY, 'ERA5T_DAILY')
        logger.info("Raw data downloaded successfully.")

        if dates_data.get('first_day_third_prev', None):
            logger.info("Raw data Splitting started.")
            data_splitting_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                        WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                        dates_data['first_day_third_prev'].strftime("%Y/%m"))
            logger.info("Raw data Splitting successfully.")

        logger.info("Data availability check started.")
        data_is_missing = True
        while data_is_missing:
            data_is_missing = check_data_availability(data_date_range, 'ERA5T_DAILY')
            if data_is_missing:
                logger.warning("Data is missing.")
                raw_data_download_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                               WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                               MANIFEST_LOCATION, DIRECTORY, 'ERA5T_DAILY')
                if dates_data.get('first_day_third_prev'):
                    data_splitting_dataflow_job(
                        PYTHON_PATH, PROJECT, REGION, BUCKET,
                        WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                        dates_data['first_day_third_prev'].strftime("%Y/%m"))
        logger.info("Data availability check completed successfully.")

        with ThreadPoolExecutor(max_workers=8) as tp:
            for z_file, table, region in zip(ZARR_FILES_LIST, BQ_TABLES_LIST,
                                             REGION_LIST):
                tp.submit(perform_data_operations, z_file, table, region,
                          dates_data["last_sixth_date"],
                          dates_data["last_sixth_date"], parsed_args.init_date)

        logger.info(
            f"Automatic update for ARCO-ERA5T completed for {dates_data['last_sixth_date']}.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
