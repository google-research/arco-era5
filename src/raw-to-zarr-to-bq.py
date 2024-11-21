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
import json
import logging
import os
import re
import zarr

import xarray as xr

from concurrent.futures import ThreadPoolExecutor
from arco_era5 import (
    GCP_DIRECTORY,
    HOURS_PER_DAY,
    INIT_DATE,
    MODEL_LEVEL_WIND_VARIABLE,
    MODEL_LEVEL_MOISTURE_VARIABLE,
    MULTILEVEL_VARIABLES,
    SINGLE_LEVEL_FORECAST_VARIABLE,
    SINGLE_LEVEL_REANALYSIS_VARIABLE,
    SINGLE_LEVEL_SURFACE_VARIABLE,
    SINGLE_LEVEL_VARIABLES,
    add_licenses_in_config_files,
    check_data_availability,
    data_splitting_dataflow_job,
    date_range,
    generate_input_paths,
    generate_input_paths_of_ar_data,
    generate_offset,
    get_previous_month_dates,
    get_secret,
    ingest_data_in_zarr_dataflow_job,
    offset_along_time_axis,
    opener,
    parse_arguments_raw_to_zarr_to_bq,
    raw_data_download_dataflow_job,
    replace_non_alphanumeric_with_hyphen,
    subprocess_run,
    update_date_in_config_file,
    update_target_path_in_config_file,
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
WEATHER_TOOLS_SDK_CONTAINER_IMAGE  = os.environ.get("WEATHER_TOOLS_SDK_CONTAINER_IMAGE")
ARCO_ERA5_SDK_CONTAINER_IMAGE = os.environ.get("ARCO_ERA5_SDK_CONTAINER_IMAGE")
API_KEY_PATTERN = re.compile(r"^API_KEY_\d+$")
API_KEY_LIST = []

BQ_TABLES_LIST = json.loads(os.environ.get("BQ_TABLES_LIST"))
REGION_LIST = json.loads(os.environ.get("REGION_LIST"))
TEMP_TARGET_PATH = "gs://gcp-public-data-arco-era5/raw-era5"

zarr_to_netcdf_file_mapping = {
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3': MULTILEVEL_VARIABLES + SINGLE_LEVEL_VARIABLES,
    'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2' : MODEL_LEVEL_MOISTURE_VARIABLE,
    'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2' : MODEL_LEVEL_WIND_VARIABLE,
    'gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2': SINGLE_LEVEL_FORECAST_VARIABLE,
    'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2': SINGLE_LEVEL_REANALYSIS_VARIABLE,
    'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2': SINGLE_LEVEL_SURFACE_VARIABLE
}

dates_data = get_previous_month_dates()
data_date_range = date_range(
    dates_data["first_day"], dates_data["last_day"]
)
start_date = data_date_range[0].strftime("%Y/%m/%d")
end_date = data_date_range[-1].strftime("%Y/%m/%d")

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
                                         PROJECT, BUCKET, ARCO_ERA5_SDK_CONTAINER_IMAGE, PYTHON_PATH)
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


def open_dataset_from_url(url: str, engine: str) -> xr.Dataset:
    """
    Opens a dataset from a given URL using the specified engine(i.e. Netcdf4, grib).

    Args:
        url (str): The URL of the dataset to open.
        engine (str): The engine to use for reading the dataset.

    Returns:
        xr.Dataset: The opened xarray dataset.
    """
    with opener(url) as file:
        return xr.open_dataset(file, engine=engine)


def compare_datasets(url1: str, url2: str, engine: str) -> bool:
    """
    Compares two xarray datasets from given URLs to check if they are equal.

    Args:
        url1 (str): The URL of the first dataset.
        url2 (str): The URL of the second dataset.
        engine (str): The engine to use for reading the datasets.

    Returns:
        bool: True if the datasets are equal, False otherwise.
    """
    dataset1 = open_dataset_from_url(url1, engine)
    dataset2 = open_dataset_from_url(url2, engine)

    return dataset1.equals(dataset2)


def calculate_time_offset_for_ar_data(raw_file: str) -> slice:
    """
    Calculates the time offset for AR data.

    Args:
        raw_file (str): Path to the raw data file.

    Returns:
        slice: The time offset region as a slice object.
    """
    year, month, day = raw_file.split('/')[5:8]
    offset = offset_along_time_axis(INIT_DATE, year, month, day)
    return slice(offset, offset + HOURS_PER_DAY)


def synchronize_era5t_with_era5_data(z_file: str, new_raw_file: str, engine: str) -> None:
    """
    Updates ERA5T data using corresponding ERA5 data.

    Args:
        z_file (str): Path to the Zarr file containing ERA5T data.
        new_raw_file (str): Path to the raw ERA5 data file.
        engine (str): The engine used for reading raw  the datasets.

    Returns:
        None
    """
    zf = zarr.open(z_file)
    ds = open_dataset_from_url(new_raw_file, engine)
    logger.info(f"Data update process starts for {z_file}.")

    # Determine the appropriate time region
    if "model-level" in z_file or "single-level" in z_file:
        single_level = "single-level" in z_file
        region, _ = generate_offset(new_raw_file, single_level, INIT_DATE, HOURS_PER_DAY)
    else:
        region = calculate_time_offset_for_ar_data(new_raw_file)

    # Update variables
    for variable_name in ds.data_vars:
        zv = zf[variable_name]
        zv[region] = ds[variable_name].values

    logger.info(f"Data update process completed for {z_file}.")


def update_era5t_data(z_file: str) -> None:
    """Synchronizes ERA5T data with ERA5 data."""

    variables = zarr_to_netcdf_file_mapping[z_file]
    era5t_raw_files = []
    if "model-level" in z_file:
        era5t_raw_files = generate_input_paths(start_date, end_date, GCP_DIRECTORY, variables)
    elif "single-level" in z_file:
        era5t_raw_files = generate_input_paths(start_date, end_date, GCP_DIRECTORY, variables, True)
    elif "/ar/" in z_file:
        for date in data_date_range:
            era5t_raw_files.extend(generate_input_paths_of_ar_data(date, variables))
    
    era5_raw_files = [
        url.replace("gs://gcp-public-data-arco-era5/raw", TEMP_TARGET_PATH)
        for url in era5t_raw_files
    ]
    
    for old_file, new_file in zip(era5t_raw_files, era5_raw_files):
        logger.info(f"Comparing data between {old_file} and {new_file}.")
        engine = "netcdf4" if "/ar/" in z_file else "cfgrib"
        if not compare_datasets(old_file, new_file, engine):
            logger.info(f"Data mismatch detected for {new_file}. Updating data.")
            synchronize_era5t_with_era5_data(z_file, new_file, engine)
        logger.info(f"Data comparison completed for {old_file}.")


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments_raw_to_zarr_to_bq("Parse arguments.")

        logger.info(f"Automatic update for ARCO-ERA5 started for {dates_data['sl_month']}.")
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
        update_target_path_in_config_file(DIRECTORY, TEMP_TARGET_PATH)
        logger.info("Config file updation completed.")
        
        logger.info("Raw data downloading started.")
        raw_data_download_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                       WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                       MANIFEST_LOCATION, DIRECTORY)
        logger.info("Raw data downloaded successfully.")

        logger.info("Raw data Splitting started.")
        data_splitting_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                    WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                    dates_data['first_day'].strftime("%Y/%m"))
        logger.info("Raw data Splitting successfully.")

        logger.info("Data availability check started.")
        data_is_missing = True
        while data_is_missing:
            data_is_missing = check_data_availability(data_date_range)
            if data_is_missing:
                logger.warning("Data is missing.")
                raw_data_download_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                               WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                               MANIFEST_LOCATION, DIRECTORY)
                data_splitting_dataflow_job(PYTHON_PATH, PROJECT, REGION, BUCKET,
                                            WEATHER_TOOLS_SDK_CONTAINER_IMAGE,
                                            dates_data['first_day'].strftime("%Y/%m"))
        logger.info("Data availability check completed successfully.")

        # update raw ERA5T data with the ERA5. ## Pending
        with ThreadPoolExecutor(max_workers=8) as tp:
            for z_file in list(zarr_to_netcdf_file_mapping.keys()):
                tp.submit(update_era5t_data, z_file)

        logger.info(f"Automatic update for ARCO-ERA5 completed for {dates_data['sl_month']}.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
