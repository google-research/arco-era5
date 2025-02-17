# Copyright 2025 Google LLC
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
import os

from concurrent.futures import ThreadPoolExecutor

from .utils import ExecTypes, subprocess_run

DIRECTORY = "/arco-era5/raw"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
PYTHON_PATH = os.environ.get("PYTHON_PATH")
WEATHER_TOOLS_SDK_CONTAINER_IMAGE  = os.environ.get("WEATHER_TOOLS_SDK_CONTAINER_IMAGE")

SPLITTING_DATASETS = ['soil', 'pcp']

def raw_data_download_dataflow_job(mode: str, first_day: datetime.date):
    """Launches a Dataflow job to process weather data."""
    job_name = f"raw-data-download-arco-era5-{first_day.strftime('%Y-%m')}"
    if mode == ExecTypes.ERA5T_DAILY.value:
        job_name = f"raw-data-download-arco-era5-{first_day.strftime('%Y-%m-%d')}"

    if mode == ExecTypes.ERA5.value:
        config_path = f"{DIRECTORY}/*/*.cfg"
    else:
        config_path = f"{DIRECTORY}/{mode}/*.cfg"

    command = (
        f"{PYTHON_PATH} /weather/weather_dl/weather-dl {config_path} "
        f"--runner DataflowRunner --project {PROJECT} --region {REGION} --temp_location "
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--sdk_container_image {WEATHER_TOOLS_SDK_CONTAINER_IMAGE} --experiment use_runner_v2 "
        f"--manifest-location {MANIFEST_LOCATION} "
    )
    subprocess_run(command)

def data_splitting_dataflow_job(date: str, root_path: str):
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
            f' "{root_path}/ERA5GRIB/HRES/Month/{year}/{month}_hres_{DATASET}.grb2" '
            f'--output-template "{root_path}/ERA5GRIB/HRES/Month/{first}/{zero}.grb2_{typeOfLevel}_{shortName}.grib" '
            f'--runner DataflowRunner --project {PROJECT} --region {REGION} '
            f'--temp_location gs://{BUCKET}/tmp --disk_size_gb 3600 '
            f'--job_name split-{DATASET}-data-{month} '
            f'--sdk_container_image {WEATHER_TOOLS_SDK_CONTAINER_IMAGE} '
        )
        commands.append(command)

    with ThreadPoolExecutor(max_workers=4) as tp:
        for command in commands:
            tp.submit(subprocess_run, command)
