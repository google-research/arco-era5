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
from itertools import product

from .source_data import (
    GCP_DIRECTORY,
    SINGLE_LEVEL_VARIABLES,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS
)

logger = logging.getLogger(__name__)

SINGLE_LEVEL_SUBDIR_TEMPLATE = (
    "ERA5GRIB/HRES/Month/{year}/{year}{month:02d}_hres_{chunk}.grb2"
)
MODELLEVEL_SUBDIR_TEMPLATE = (
    "ERA5GRIB/HRES/Daily/{year}/{year}{month:02d}{day:02d}_hres_{chunk}.grb2"
)

# File Templates
PRESSURELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/{pressure}.nc")
AR_SINGLELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/surface.nc")

# Data Chunks
MODEL_LEVEL_CHUNKS = ["dve", "tw", "o3q", "qrqs"]
SINGLE_LEVEL_CHUNKS = [
    "cape", "cisst", "sfc", "tcol", "soil_depthBelowLandLayer_istl1",
    "soil_depthBelowLandLayer_istl2", "soil_depthBelowLandLayer_istl3",
    "soil_depthBelowLandLayer_istl4", "soil_depthBelowLandLayer_stl1",
    "soil_depthBelowLandLayer_stl2", "soil_depthBelowLandLayer_stl3",
    "soil_depthBelowLandLayer_stl4", "soil_depthBelowLandLayer_swvl1",
    "soil_depthBelowLandLayer_swvl2", "soil_depthBelowLandLayer_swvl3",
    "soil_depthBelowLandLayer_swvl4", "soil_surface_tsn", "lnsp",
    "zs", "rad", "pcp_surface_cp", "pcp_surface_crr",
    "pcp_surface_csf", "pcp_surface_csfr", "pcp_surface_es",
    "pcp_surface_lsf", "pcp_surface_lsp", "pcp_surface_lspf",
    "pcp_surface_lsrr", "pcp_surface_lssfr", "pcp_surface_ptype",
    "pcp_surface_rsn", "pcp_surface_sd", "pcp_surface_sf",
    "pcp_surface_smlt", "pcp_surface_tp"]
PRESSURE_LEVEL = PRESSURE_LEVELS_GROUPS["full_37"]

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
                                   type: str = None):
    """Launches a Dataflow job to process weather data."""

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
                                bucket: str, sdk_container_image: str, date: str):
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


def generate_urls(data_date_range: t.List[datetime.datetime], start_date: str, end_date: str, type: str = None) -> t.List['str']:
    all_uri = []
    if type != 'ERA5T_MONTHLY':
        all_uri.extend(generate_input_paths(start_date, end_date, GCP_DIRECTORY, MODEL_LEVEL_CHUNKS))
        for date in data_date_range:
            for chunk in MULTILEVEL_VARIABLES + SINGLE_LEVEL_VARIABLES:
                if chunk in MULTILEVEL_VARIABLES:
                    for pressure in PRESSURE_LEVEL:
                        all_uri.append(
                            PRESSURELEVEL_DIR_TEMPLATE.format(year=date.year,
                                                            month=date.month,
                                                            day=date.day, chunk=chunk,
                                                            pressure=pressure))
                else:
                    if chunk == 'geopotential_at_surface':
                        chunk = 'geopotential'
                    all_uri.append(
                        AR_SINGLELEVEL_DIR_TEMPLATE.format(
                            year=date.year, month=date.month, day=date.day, chunk=chunk))

    if type != 'ERA5T_DAILY':   
        all_uri.extend(generate_input_paths(start_date, end_date, GCP_DIRECTORY, SINGLE_LEVEL_CHUNKS, True))
    
    return all_uri


def update_raw_data(data_date_range: t.List[datetime.datetime]):
    """
    Copies contents from src to dst in Google Cloud Storage, overriding existing files.

    Parameters:
    - src (str): Source directory URI (e.g., 'gs://bucket/source-dir/')
    - dst (str): Destination directory URI (e.g., 'gs://bucket/destination-dir/')
    """
    start_date = data_date_range[0].strftime("%Y/%m/%d")
    end_date = data_date_range[-1].strftime("%Y/%m/%d")

    all_uri = generate_urls(data_date_range, start_date, end_date, type)

    with ThreadPoolExecutor() as tp:
        for url in all_uri:
            src = url.replace("gcp-public-data-arco-era5/raw", "gcp-public-data-arco-era5/raw-era5")
            command = f'gsutil -m mv {src} {url}'
            tp.submit(subprocess_run, command)


def generate_input_paths(start: str, end: str, root_path: str, chunks: t.List[str],
                         is_single_level: bool = False) -> t.List[str]:
    """A method for generating the url using the combination of chunks and time range.

    Args:
        start (str): starting date for data files.
        end (str): last date for data files.
        root_path (str): raw file url prefix.
        chunks: (t.List(str)): List of chunks to process.
        is_single_level: (bool): Is the call for single level or model level

    Returns:
        t.List: List of file urls to process.
    """
    input_paths = []
    for time, chunk in product(date_range(start, end, freq="MS" if is_single_level else "D"), chunks):
        if is_single_level:
            url = f"{root_path}/{SINGLE_LEVEL_SUBDIR_TEMPLATE.format(year=time.year, month=time.month, day=time.day, chunk=chunk)}"
        else:
            url = f"{root_path}/{MODELLEVEL_SUBDIR_TEMPLATE.format(year=time.year, month=time.month, day=time.day, chunk=chunk)}"

        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            url = url.replace(chunk, chunk_)
            url = f"{url}_{level}_{var}.grib"
        input_paths.append(url)

    return input_paths