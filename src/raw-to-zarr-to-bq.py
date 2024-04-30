# Copyright 2023 Google LLC
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
import datetime
import json
import logging
import os
import re

from concurrent.futures import ThreadPoolExecutor
from arco_era5 import (
    check_data_availability,
    date_range,
    ingest_data_in_bigquery_dataflow_job,
    ingest_data_in_zarr_dataflow_job,
    get_previous_month_dates,
    get_secret,
    parse_arguments_raw_to_zarr_to_bq,
    remove_licenses_from_directory,
    resize_zarr_target,
    subprocess_run,
    update_config_file,
    )

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIRECTORY = "/arco-era5/raw"
FIELD_NAME = "date"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SDK_CONTAINER_IMAGE = os.environ.get("SDK_CONTAINER_IMAGE") # Reference: gcr.io/gcp-public-data-era5/arco-era5:model
MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
PYTHON_PATH = os.environ.get("PYTHON_PATH")
AR_RAW_AVRO_FILE = os.environ.get("AR_RAW_AVRO_FILE")  # Reference: "gs://gcp-public-data-era5/ar-raw-avro"
ZARR_AVRO_CONVERSION_NETWORK = os.environ.get("ZARR_AVRO_CONVERSION_NETWORK", "")  # Reference: arco-era5
ZARR_AVRO_CONVERSION_SUBNET = os.environ.get("ZARR_AVRO_CONVERSION_SUBNET", "")  # Reference: regions/us-central1/subnetworks/arco-era5-subnet
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

dates_data = get_previous_month_dates()


def raw_data_download_dataflow_job():
    """Launches a Dataflow job to process weather data."""
    current_day = datetime.date.today()
    job_name = f"raw-data-download-arco-era5-{current_day.month}-{current_day.year}"

    command = (
        f"{PYTHON_PATH} /weather/weather_dl/weather-dl /arco-era5/raw/*.cfg "
        f"--runner DataflowRunner --project {PROJECT} --region {REGION} --temp_location "
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--experiment use_runner_v2 --use-local-code --manifest-location {MANIFEST_LOCATION} "
    )
    subprocess_run(command)


def data_splitting_dataflow_job(date: str):
    """Launches a Dataflow job to splitting soil & pcp weather data."""
    year = date[:4]
    month = year + date[5:7]
    typeOfLevel = '{' + 'typeOfLevel' + '}'
    shortName = '{' + 'shortName' + '}'
    zero = '{' + '0' + '}'
    first = '{' + '1' + '}'
    commands = []
    for DATASET in SPLITTING_DATASETS:
        command = (
            f'{PYTHON_PATH} /weather/weather_sp/weather-sp --input-pattern '
            f' "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_{DATASET}.grb2" '
            f'--output-template "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{first}/{zero}.grb2_{typeOfLevel}_{shortName}.grib" '
            f'--runner DataflowRunner --project {PROJECT} --region {REGION} '
            f'--temp_location gs://{BUCKET}/tmp --disk_size_gb 3600 '
            f'--job_name split-{DATASET}-data-{month} --use-local-code '
        )
        commands.append(command)

    with ThreadPoolExecutor(max_workers=4) as tp:
        for command in commands:
            tp.submit(subprocess_run, command)


def perform_data_operations(z_file: str, table: str, region: str, start_date: datetime.date,
                            end_date: datetime.date, init_date: str, data_process_month: str, data_process_year: str):
    # Function to process a single pair of z_file and table
    try:
        logger.info(f"Resizing zarr file: {z_file} started.")
        resize_zarr_target(z_file, end_date, init_date)
        logger.info(f"Resizing zarr file: {z_file} completed.")
        logger.info(f"Data ingesting for {z_file} is started.")
        ingest_data_in_zarr_dataflow_job(z_file, region, start_date, end_date, init_date,
                                         project=PROJECT, bucket=BUCKET, python_path=PYTHON_PATH,
                                         sdk_container_image=SDK_CONTAINER_IMAGE)
        logger.info(f"Data ingesting for {z_file} is completed.")
        
        ingest_data_in_bigquery_dataflow_job(z_file, data_process_month, data_process_year, AR_RAW_AVRO_FILE, table,
                                             project=PROJECT, bucket=BUCKET, python_path=PYTHON_PATH,
                                             sdk_container_image=SDK_CONTAINER_IMAGE,
                                             zarr_avro_conversion_network=ZARR_AVRO_CONVERSION_NETWORK,
                                             zarr_avro_conversion_subnet=ZARR_AVRO_CONVERSION_SUBNET)
            
    except Exception as e:
        logger.error(
            f"An error occurred in process_zarr_and_table for {z_file}: {str(e)}")


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments_raw_to_zarr_to_bq("Parse arguments.")

        logger.info(f"Automatic update for ARCO-ERA5 started for {dates_data['sl_month']}/{dates_data['sl_year']}.")
        data_date_range = date_range(
            dates_data["first_day_third_prev"], dates_data["last_day_third_prev"]
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
        logger.info("Config file updation started.")
        update_config_file(DIRECTORY, FIELD_NAME, additional_content)
        logger.info("Config file updation completed.")
        logger.info("Raw data downloading started.")
        raw_data_download_dataflow_job()
        logger.info("Raw data downloaded successfully.")

        logger.info("Raw data Splitting started.")
        data_splitting_dataflow_job(
            dates_data['first_day_third_prev'].strftime("%Y/%m"))
        logger.info("Raw data Splitting successfully.")

        logger.info("Data availability check started.")
        data_is_missing = True
        while data_is_missing:
            data_is_missing = check_data_availability(data_date_range)
            if data_is_missing:
                logger.warning("Data is missing.")
                raw_data_download_dataflow_job()
                data_splitting_dataflow_job(
                    dates_data['first_day_third_prev'].strftime("%Y/%m"))
        logger.info("Data availability check completed successfully.")

        remove_licenses_from_directory(DIRECTORY, len(API_KEY_LIST))
        logger.info("All licenses removed from the config file.")
        with ThreadPoolExecutor(max_workers=8) as tp:
            for z_file, table, region in zip(ZARR_FILES_LIST, BQ_TABLES_LIST,
                                             REGION_LIST):
                tp.submit(perform_data_operations, z_file, table, region,
                          dates_data["first_day_third_prev"],
                          dates_data["last_day_third_prev"], parsed_args.init_date,
                          dates_data["sl_month"], dates_data['sl_year'])

        logger.info(f"Automatic update for ARCO-ERA5 completed for {dates_data['sl_month']}/{dates_data['sl_year']}.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
