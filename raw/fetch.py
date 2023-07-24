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
# ==============================================================================
import datetime
import json
import os
import re
import subprocess
from google.cloud import secretmanager
from typing import Tuple

DIRECTORY = "/weather/config_files"
FIELD_NAME = "date"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SDK_CONTAINER_IMAGE = os.environ.get("SDK_CONTAINER_IMAGE")
MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
API_KEY_PATTERN = re.compile(r'^API_KEY_\d+$')
API_KEY_LIST = []


def new_config_file(config_file: str, field_name: str, additional_content: str,
    co_file: bool, single_level_file: bool, first_day_first_prev: datetime.date,
    last_day_first_prev: datetime.date, first_day_third_prev: datetime.date,
    last_day_third_prev: datetime.date, sl_year: str, sl_month: str,
    sl_first_date: str, sl_last_date: str) -> None:
    """Modify the specified configuration file with new values.

    Parameters:
        config_file (str): The path to the configuration file to be modified.
        field_name (str): The name of the field to be updated with the new value.
        additional_content (str): The additional content to be added under the 
                                    '[selection]' section.
        co_file (bool): True if the configuration file is a 'hourly' file, False 
                        otherwise.
        single_level_file (bool): True if the configuration file is a 'sfc' file,
                                 False otherwise.
        first_day_first_prev (datetime.date): The first day of the first previous month.
        last_day_first_prev (datetime.date): The last day of the first previous month.
        first_day_third_prev (datetime.date): The first day of the third previous month.
        last_day_third_prev (datetime.date): The last day of the third previous month.
        sl_year (str): The year of the third previous month in 'YYYY' format.
        sl_month (str): The month of the third previous month in 'MM' format.
        sl_first_date (str): The first date of the third previous month in 'DD' format.
        sl_last_date (str): The last date of the third previous month in 'DD' format.
    """

    with open(config_file, "r") as file:
        lines = file.readlines()

    # Update the specified field with the new value
    updated_lines = []
    selection_line_found = False
    for line in lines:
        if not selection_line_found and line.strip() == "[selection]":
            updated_lines.append(f"{additional_content}\n")
            selection_line_found = True

        if single_level_file:
            if line.startswith("year"):
                line = f"year={sl_year}\n"
            if line.startswith("month"):
                line = f"month={sl_month}\n"
            if line.startswith("day"):
                line = f"day={sl_first_date}/to/{sl_last_date}\n"
        elif line.startswith(field_name):
            if co_file:
                line = (
                    f"{field_name}={first_day_first_prev}/to/{last_day_first_prev}\n"
                )
            else:
                line = (
                    f"{field_name}={first_day_third_prev}/to/{last_day_third_prev}\n"
                )
        updated_lines.append(line)

    with open(config_file, "w") as file:
        file.writelines(updated_lines)


def get_month_range(date: datetime.date) -> Tuple[datetime.date, datetime.date]:
    """Return the first and last date of the month from the input date.

    Parameters:
        date (datetime.date): The input date.

    Returns:
        tuple: A tuple containing the first and last date of the month as 
                datetime.date objects.
    """
    last_day = date.replace(day=1) - datetime.timedelta(days=1)
    first_day = last_day.replace(day=1)
    return first_day, last_day


def get_single_level_dates(first_day: datetime.date, 
                           last_day: datetime.date) -> Tuple[str, str, str, str]:
    """Return the year, month, first date, and last date of the input month.

    Parameters:
        first_day (datetime.date): The first day of the month.
        last_day (datetime.date): The last day of the month.

    Returns:
        tuple: A tuple containing the year, month, first date, and last date of the 
                month as strings.
    """
    year, month = str(first_day)[:4], str(first_day)[5:7]
    first_date, last_date = str(first_day)[8:], str(last_day)[8:]
    return (year, month, first_date, last_date)


def get_previous_month_dates() -> (Tuple[datetime.date, datetime.date, datetime.date,
                                         datetime.date, str, str, str, str]):
    """Return the first and third previous month's date from the current date.

    Returns:
        tuple: A tuple containing the first and third previous month's dates as 
            datetime.date objects, and the year, month, first date, and last date 
            of the third previous month as strings.
    """

    today = datetime.date.today()
    # Calculate the correct previous month considering months from 1 to 12
    prev_month = today.month + 10 if today.month < 3 else today.month - 2
    third_prev_month = today.replace(month=prev_month)
    first_prev_month = today.replace(month=today.month)
    first_day_third_prev, last_day_third_prev = get_month_range(third_prev_month)
    first_day_first_prev, last_day_first_prev = get_month_range(first_prev_month)

    sl_year, sl_month, sl_first_date, sl_last_date = get_single_level_dates(
        first_day_third_prev, last_day_third_prev
    )

    return (first_day_first_prev, last_day_first_prev, first_day_third_prev,
        last_day_third_prev, sl_year, sl_month, sl_first_date, sl_last_date)


def update_config_files(directory: str, field_name: str, 
                        additional_content: str) -> None:
    """Update the configuration files in the specified directory.

    Parameters:
        directory (str): The path to the directory containing the configuration files.
        field_name (str): The name of the field to be updated with the new value.
        additional_content (str): The additional content to be added under the 
                    '[selection]' section.
    """
    (first_day_first_prev, last_day_first_prev, first_day_third_prev,
    last_day_third_prev, sl_year, sl_month, sl_first_date,
    sl_last_date) = get_previous_month_dates()

    for filename in os.listdir(directory):
        single_level_file = False
        co_file = False
        if filename.endswith(".cfg"):
            if "sfc" in filename:
                single_level_file = True
            if "hourly" in filename:
                co_file = True
            config_file = os.path.join(directory, filename)
            new_config_file(config_file, field_name, additional_content,
                            co_file, single_level_file, first_day_first_prev,
                            last_day_first_prev, first_day_third_prev,
                            last_day_third_prev, sl_year, sl_month,
                            sl_first_date, sl_last_date)


def get_secret(api_key: str) -> dict:
    """Retrieve the secret value from the Google Cloud Secret Manager.

    Parameters:
        api_key (str): The name or identifier of the secret in the Google 
                        Cloud Secret Manager.

    Returns:
        dict: A dictionary containing the retrieved secret data.
    """
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": api_key})
    payload = response.payload.data.decode("UTF-8")
    secret_dict = json.loads(payload)
    return secret_dict


if __name__ == "__main__":

    for env_var in os.environ:
        if API_KEY_PATTERN.match(env_var):
            api_key_value = os.environ.get(env_var)
            API_KEY_LIST.append(api_key_value)
    
    additional_content = ""
    for count,key in enumerate(API_KEY_LIST):
        api_key_value = get_secret(key)
        additional_content += f'[parameters.api{count}]\n\
            api_url={api_key_value["api_url"]}\napi_key={api_key_value["api_key"]}\n\n'
    
    current_day = datetime.date.today()
    Job_name = f"wx-dl-arco-era5-{current_day.month}-{current_day.year}"

    command = f'python weather_dl/weather-dl /weather/config_files/*.cfg --runner \
    DataflowRunner --project {PROJECT} --region {REGION} --temp_location \
    "gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {Job_name} \
    --sdk_container_image {SDK_CONTAINER_IMAGE} \
    --manifest-location {MANIFEST_LOCATION} --experiment use_runner_v2'

    try:
        update_config_files(DIRECTORY, FIELD_NAME, additional_content)
        subprocess.run(command, shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}')
