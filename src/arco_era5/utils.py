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
import argparse
import datetime
import logging
import re
import subprocess
import sys

import pandas as pd
import typing as t

from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

AR_FILES = ['era5_pl_hourly.cfg', 'era5_sl_hourly.cfg']
CO_MODEL_LEVEL_FILES = ['era5_ml_dve.cfg', 'era5_ml_o3q.cfg', 'era5_ml_qrqs.cfg', 'era5_ml_tw.cfg']
CO_SINGLE_LEVEL_FILES = ['era5_ml_lnsp.cfg', 'era5_ml_zs.cfg', 'era5_sfc_cape.cfg', 'era5_sfc_cisst.cfg',
                         'era5_sfc_pcp.cfg', 'era5_sfc_rad.cfg', 'era5_sfc_soil.cfg', 'era5_sfc_tcol.cfg',
                         'era5_sfc.cfg']
SPLITTING_DATASETS = ['soil', 'pcp']


def date_range(start_date: str, end_date: str, freq: str = "D") -> t.List[datetime.datetime]:
    """Generates a list of datetime objects within a given date range.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
        List[datetime.datetime]: A list of datetime objects.
    """
    return [
        ts.to_pydatetime()
        for ts in pd.date_range(start=start_date, end=end_date, freq=freq).to_list()
    ]


def replace_non_alphanumeric_with_hyphen(input_string: str) -> str:
    """
    Replace non-alphanumeric characters with hyphens in the input string.

    Args:
        input_string (str): The input string to process.

    Returns:
        str: The processed string with non-alphanumeric characters replaced by hyphens.
    """
    return re.sub(r'[^a-z0-9-]', '-', input_string)


def convert_to_date(date_str: str) -> datetime.date:
    """Converts a date string in the format 'YYYY-MM-DD' to a datetime object.

    Args:
        date_str (str): The date string to convert.

    Returns:
        datetime.datetime: A datetime object representing the input date.
    """
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()


def subprocess_run(command: str):
    """Runs a subprocess with the given command and prints the output.

    Args:
        command (str): The command to run.
    """

    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    with process.stdout:
        try:
            for line in iter(process.stdout.readline, b""):
                log_message = line.decode("utf-8").strip()
                if "Failed" in log_message or 'JOB_STATE_CANCELLING' in log_message:
                    sys.exit(f'Stopping subprocess as {log_message}')
                logger.info(log_message)
        except subprocess.CalledProcessError as e:
            logger.error(
                f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}'
            )


def parse_arguments_raw_to_zarr_to_bq(desc: str) -> t.Tuple[argparse.Namespace,
                                                            t.List[str]]:
    """Parse command-line arguments for the data processing pipeline.

    Args:
        desc (str): A description of the command-line interface.

    Returns:
        tuple: A tuple containing the parsed arguments as a namespace and
        a list of unknown arguments.

    Example:
        To parse command-line arguments, you can call this function like this:
        >>> parsed_args, unknown_args = parse_arguments_raw_to_zarr_to_bq(
            "Data Processing Pipeline")
    """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--init_date", type=str, default='1900-01-01',
                        help="Date to initialize the zarr store.")

    return parser.parse_known_args()


def raw_data_download_dataflow_job(python_path: str, project: str, region: str,
                                   bucket: str, sdk_container_image: str,
                                   manifest_location: str, directory: str,
                                   type: str = None) -> None:
    """
    Launches a Dataflow job to download weather data.
    
    Args:
        python_path (str): Path to the Python executable.
        project (str): Google Cloud project ID.
        region (str): Google Cloud region.
        bucket (str): Google Cloud Storage bucket for temporary data.
        sdk_container_image (str): SDK container image for Dataflow.
        manifest_location (str): Path to the manifest table.
        directory (str): Directory containing input files.
        job_type (Optional[str]): Type of job ('ERA5T_DAILY', 'ERA5T_MONTHLY', or None).

    Raises:
        subprocess.CalledProcessError: If the Dataflow job command fails.
    """

    AR_FILES = [ f'{directory}/{file}' for file in AR_FILES ]
    CO_MODEL_LEVEL_FILES = [ f'{directory}/{file}' for file in CO_MODEL_LEVEL_FILES ]
    CO_SINGLE_LEVEL_FILES = [ f'{directory}/{file}' for file in CO_SINGLE_LEVEL_FILES ]
    
    current_day = datetime.date.today()

    if type == 'ERA5T_DAILY':
        files = ' '.join(AR_FILES + CO_MODEL_LEVEL_FILES)
        job_name = f"raw-data-download-arco-era5T-Daily-{current_day}"
    elif type == 'ERA5T_MONTHLY':
        files = ' '.join(CO_SINGLE_LEVEL_FILES)
        job_name = f"raw-data-download-arco-era5T-Monthly-{current_day.month}-{current_day.year}"
    else:
        files = ' '.join(AR_FILES + CO_MODEL_LEVEL_FILES + CO_SINGLE_LEVEL_FILES)
        job_name = f"raw-data-download-arco-era5-{current_day.month}-{current_day.year}"
    command = (
        f"{python_path} /weather/weather_dl/weather-dl {files} "
        f"--runner DataflowRunner --project {project} --region {region} --temp_location "
        f'"gs://{bucket}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--sdk_container_image {sdk_container_image} --experiment use_runner_v2 "
        f"--manifest-location {manifest_location} "
    )
    subprocess_run(command)


def data_splitting_dataflow_job(python_path: str, project: str, region: str,
                                bucket: str, sdk_container_image: str, date: str) -> None:
    """
    Launches a Dataflow job to splitting soil & pcp weather data for a given date.
    
     Args:
        python_path (str): Path to the Python executable.
        project (str): Google Cloud project ID.
        region (str): Google Cloud region.
        bucket (str): Google Cloud Storage bucket for temporary data.
        sdk_container_image (str): SDK container image for Dataflow.
        date (str): The target date in 'YYYY-MM' format for the data splitting job.

    Raises:
        subprocess.CalledProcessError: If any of the Dataflow job commands fail.
    """
    year = date[:4]
    month = year + date[5:7]
    typeOfLevel = '{' + 'typeOfLevel' + '}'
    shortName = '{' + 'shortName' + '}'
    zero = '{' + '0' + '}'
    first = '{' + '1' + '}'
    commands = []
    for DATASET in SPLITTING_DATASETS:
        command = (
            f'{python_path} /weather/weather_sp/weather-sp --input-pattern '
            f' "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_{DATASET}.grb2" '
            f'--output-template "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{first}/{zero}.grb2_{typeOfLevel}_{shortName}.grib" '
            f'--runner DataflowRunner --project {project} --region {region} '
            f'--temp_location gs://{bucket}/tmp --disk_size_gb 3600 '
            f'--job_name split-{DATASET}-data-{month} '
            f'--sdk_container_image {sdk_container_image} '
        )
        commands.append(command)

    with ThreadPoolExecutor(max_workers=4) as tp:
        for command in commands:
            tp.submit(subprocess_run, command)
