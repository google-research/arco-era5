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
import configparser
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
        co_file (bool): True if the configuration file is a 'CO' type file, False 
                        otherwise.
        single_level_file (bool): True if the configuration file contains 'sfc' in 
                                filename, False otherwise.
        first_day_first_prev (datetime.date): The first day of the first previous month.
        last_day_first_prev (datetime.date): The last day of the first previous month.
        first_day_third_prev (datetime.date): The first day of the third previous month.
        last_day_third_prev (datetime.date): The last day of the third previous month.
        sl_year (str): The year of the third previous month in 'YYYY' format.
        sl_month (str): The month of the third previous month in 'MM' format.
        sl_first_date (str): The first date of the third previous month in 'DD' format.
        sl_last_date (str): The last date of the third previous month in 'DD' format.
    """

    config = configparser.ConfigParser()
    config.read(config_file)

    if single_level_file:
        config.set("selection", "year", sl_year)
        config.set("selection", "month", sl_month)
        config.set("selection", "day", f"{sl_first_date}/to/{sl_last_date}")
    else:
        if co_file:
            config.set("selection", field_name, 
                       f"{first_day_first_prev}/to/{last_day_first_prev}")
        else:
            config.set("selection", field_name, 
                       f"{first_day_third_prev}/to/{last_day_third_prev}")

    sections_list = additional_content.split("\n\n")
    for section in sections_list[:-1]:
        sections = section.split("\n")
        print("sections is here",sections)
        new_section_name= sections[0].strip()
        config.add_section(new_section_name)
        api_url_name, api_url_value = sections[1].split("=")
        config.set(new_section_name, api_url_name.strip(), api_url_value.strip())
        api_key_name, api_key_value = sections[2].split("=")
        config.set(new_section_name, api_key_name.strip(), api_key_value.strip())

    with open(config_file, "w") as file:
        config.write(file, space_around_delimiters=False)


def get_month_range(date: datetime.date) -> Tuple[datetime.date, datetime.date]:
    """Return the first and last date of the previous month based on the input date.

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


def get_previous_month_dates() -> dict:
    """Return a dictionary containing the first and third previous month's dates from 
    the current date.

    Returns:
        dict: A dictionary containing the following key-value pairs:
            - 'first_day_first_prev': The first day of the first previous month 
                                        (datetime.date).
            - 'last_day_first_prev': The last day of the first previous month 
                                        (datetime.date).
            - 'first_day_third_prev': The first day of the third previous month 
                                        (datetime.date).
            - 'last_day_third_prev': The last day of the third previous month 
                                        (datetime.date).
            - 'sl_year': The year of the third previous month in 'YYYY' format (str).
            - 'sl_month': The month of the third previous month in 'MM' format (str).
            - 'sl_first_date': The first date of the third previous month in 'DD' 
                                format (str).
            - 'sl_last_date': The last date of the third previous month in 'DD' 
                                format (str).
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

    return {
        'first_day_first_prev': first_day_first_prev,
        'last_day_first_prev': last_day_first_prev,
        'first_day_third_prev': first_day_third_prev,
        'last_day_third_prev': last_day_third_prev,
        'sl_year': sl_year,
        'sl_month': sl_month,
        'sl_first_date': sl_first_date,
        'sl_last_date': sl_last_date
    }


def update_config_files(directory: str, field_name: str, 
                        additional_content: str) -> None:
    """Update the configuration files in the specified directory.

    Parameters:
        directory (str): The path to the directory containing the configuration files.
        field_name (str): The name of the field to be updated with the new value.
        additional_content (str): The additional content to be added under the 
                    '[selection]' section.
    """
    dates_data = get_previous_month_dates()

    for filename in os.listdir(directory):
        single_level_file = False
        co_file = False
        if filename.endswith(".cfg"):
            if "sfc" in filename:
                single_level_file = True
            if "hourly" in filename:
                co_file = True
            config_file = os.path.join(directory, filename)
            # Pass the data as keyword arguments to the new_config_file function
            new_config_file(config_file, field_name, additional_content,
                            co_file=co_file, single_level_file=single_level_file,
                            **dates_data)


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
        additional_content += f'parameters.api{count}\n\
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
