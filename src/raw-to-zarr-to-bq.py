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
import logging
import os

import typing as t

from concurrent.futures import ThreadPoolExecutor
from arco_era5 import (
    check_data_availability,
    date_range,
    get_previous_month_dates,
    parse_arguments_raw_to_zarr_to_bq,
    remove_licenses_from_directory,
    run_sanity_job,
    update_config_file,
    ExecTypes,
    raw_data_download_dataflow_job, 
    data_splitting_dataflow_job,
    perform_data_operations,
    ARCO_ERA5_ZARR_FILES as ZARR_FILES
    )

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIRECTORY = "/arco-era5/raw"
FIELD_NAME = "date"
TEMP_PATH_FOR_RAW_DATA = os.environ.get("TEMP_PATH_FOR_RAW_DATA")
ROOT_PATH = os.environ.get("ROOT_PATH")
SKIP_DOWNLOAD = os.environ.get("SKIP_DOWNLOAD")


def get_zarr_files_to_process(mode: str) -> t.List[str]:
    """Get list of zarr files to process."""
    if mode == ExecTypes.ERA5T_DAILY.value:
        return [ZARR_FILES["ml_wind"], ZARR_FILES['ml_moisture'], ZARR_FILES['ar']]
    elif mode == ExecTypes.ERA5T_MONTHLY.value:
        return [ZARR_FILES["sl_surface"], ZARR_FILES['sl_forecast'], ZARR_FILES['sl_reanalysis']]
    else:
        return ZARR_FILES.values()


def retry_downloads(root_path: str, mode: str):
    """Start download and splitting jobs."""
    data_is_missing = False if SKIP_DOWNLOAD else True
    while data_is_missing:
        logger.info("Raw data downloading started.")
        raw_data_download_dataflow_job(mode, dates_data['first_day'])
        logger.info("Raw data downloaded successfully.")
        if mode != ExecTypes.ERA5T_DAILY.value:
            logger.info("Raw data Splitting started.")
            data_splitting_dataflow_job(
                dates_data['first_day'].strftime("%Y/%m"), root_path)
            logger.info("Raw data Splitting successfully.")
        logger.info("Data availability check started.")
        data_is_missing = check_data_availability(data_date_range, mode, root_path)
    logger.info("Data availability check completed successfully.")


def start_data_ingestion(zarr_files_to_process: t.List[str], first_day: datetime.date, last_day: datetime.date, init_date: str, mode: str):
    """Start data ingestion for zarr datasets."""
    first_day = first_day.strftime("%Y-%m-%d")
    last_day = last_day.strftime("%Y-%m-%d")
    with ThreadPoolExecutor(max_workers=8) as tp:
        for z_file in zarr_files_to_process:
            if parsed_args.mode == ExecTypes.ERA5.value:
                tp.submit(run_sanity_job, z_file, TEMP_PATH_FOR_RAW_DATA, parsed_args.init_date)
            else:
                tp.submit(perform_data_operations, z_file, first_day, last_day, init_date, mode)


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments_raw_to_zarr_to_bq("Parse arguments.")

        dates_data = get_previous_month_dates(parsed_args.mode)

        logger.info(f"Automatic update for ARCO-ERA5 started for {dates_data['sl_month']}.")
        data_date_range = date_range(
            dates_data["first_day"], dates_data["last_day"]
        )

        logger.info("Config file updation started.")
        temp_path = TEMP_PATH_FOR_RAW_DATA if parsed_args.mode == ExecTypes.ERA5.value else None
        update_config_file(DIRECTORY, FIELD_NAME, parsed_args.mode, temp_path)
        logger.info("Config file updation completed.")

        root_path = TEMP_PATH_FOR_RAW_DATA if parsed_args.mode == ExecTypes.ERA5.value else ROOT_PATH
        
        retry_downloads(root_path, parsed_args.mode)

        remove_licenses_from_directory(DIRECTORY)
        logger.info("All licenses removed from the config file.")
        zarr_files_to_process = get_zarr_files_to_process(parsed_args.mode)

        start_data_ingestion(
            zarr_files_to_process,
            dates_data["first_day"],
            dates_data['last_day'],
            parsed_args.init_date,
            parsed_args.mode
        )
        

        logger.info(f"Automatic update for ARCO-ERA5 completed for {dates_data['sl_month']}.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
