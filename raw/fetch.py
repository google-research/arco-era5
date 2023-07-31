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
import logging
import os
import re
import subprocess
import signal
import typing as t

from google.cloud import secretmanager

DIRECTORY = "/weather/config_files"
FIELD_NAME = "date"
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SDK_CONTAINER_IMAGE = os.environ.get("SDK_CONTAINER_IMAGE")
MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION")
API_KEY_PATTERN = re.compile(r'^API_KEY_\d+$')
API_KEY_LIST = []

logger = logging.getLogger(__name__)


class ConfigArgs(t.TypedDict):
    """A class representing the configuration arguments for the new_config_file
    function.

    Attributes:
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
    """
    co_file: bool
    single_level_file: bool
    first_day_first_prev: datetime.date
    last_day_first_prev: datetime.date
    first_day_third_prev: datetime.date
    last_day_third_prev: datetime.date
    sl_year: str
    sl_month: str


class MonthDates(t.TypedDict):
    """A class representing the first and third previous month's dates.

    Attributes:
        first_day_first_prev (datetime.date): The first day of the first previous month.
        last_day_first_prev (datetime.date): The last day of the first previous month.
        first_day_third_prev (datetime.date): The first day of the third previous month.
        last_day_third_prev (datetime.date): The last day of the third previous month.
        sl_year (str): The year of the third previous month in 'YYYY' format.
        sl_month (str): The month of the third previous month in 'MM' format.
    """
    first_day_first_prev: datetime.date
    last_day_first_prev: datetime.date
    first_day_third_prev: datetime.date
    last_day_third_prev: datetime.date
    sl_year: str
    sl_month: str


def new_config_file(config_file: str, field_name: str, additional_content: str,
                    config_args: ConfigArgs) -> None:
    """Modify the specified configuration file with new values.

    Parameters:
        config_file (str): The path to the configuration file to be modified.
        field_name (str): The name of the field to be updated with the new value.
        additional_content (str): The additional content to be added under the
                                    '[selection]' section.
        config_args (ConfigArgs): A dictionary containing the configuration arguments
                                    as key-value pairs.
    """

    # Unpack the values from config_args dictionary
    co_file = config_args["co_file"]
    single_level_file = config_args["single_level_file"]
    first_day_first_prev = config_args["first_day_first_prev"]
    last_day_first_prev = config_args["last_day_first_prev"]
    first_day_third_prev = config_args["first_day_third_prev"]
    last_day_third_prev = config_args["last_day_third_prev"]
    sl_year = config_args["sl_year"]
    sl_month = config_args["sl_month"]

    config = configparser.ConfigParser()
    config.read(config_file)

    if single_level_file:
        config.set("selection", "year", sl_year)
        config.set("selection", "month", sl_month)
        config.set("selection", "day", "all")
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
        new_section_name = sections[0].strip()
        config.add_section(new_section_name)
        api_url_name, api_url_value = sections[1].split("=")
        config.set(new_section_name, api_url_name.strip(), api_url_value.strip())
        api_key_name, api_key_value = sections[2].split("=")
        config.set(new_section_name, api_key_name.strip(), api_key_value.strip())

    with open(config_file, "w") as file:
        config.write(file, space_around_delimiters=False)


def get_month_range(date: datetime.date) -> t.Tuple[datetime.date, datetime.date]:
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


def get_previous_month_dates() -> MonthDates:
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
    """

    today = datetime.date.today()
    # Calculate the correct previous month considering months from 1 to 12
    prev_month = today.month + 10 if today.month < 3 else today.month - 2
    third_prev_month = today.replace(month=prev_month)
    first_prev_month = today.replace(month=today.month)
    first_day_third_prev, last_day_third_prev = get_month_range(third_prev_month)
    first_day_first_prev, last_day_first_prev = get_month_range(first_prev_month)
    first_date_third_prev = first_day_third_prev
    sl_year, sl_month = str(first_date_third_prev)[:4], str(first_date_third_prev)[5:7]

    return {
        'first_day_first_prev': first_day_first_prev,
        'last_day_first_prev': last_day_first_prev,
        'first_day_third_prev': first_day_third_prev,
        'last_day_third_prev': last_day_third_prev,
        'sl_year': sl_year,
        'sl_month': sl_month,
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
    config_args = {
        "first_day_first_prev": dates_data['first_day_first_prev'],
        "last_day_first_prev": dates_data['last_day_first_prev'],
        "first_day_third_prev": dates_data['first_day_third_prev'],
        "last_day_third_prev": dates_data['last_day_third_prev'],
        "sl_year": dates_data['sl_year'],
        "sl_month": dates_data['sl_month'],
    }
    for filename in os.listdir(directory):
        config_args['single_level_file'] = config_args['co_file'] = False
        if filename.endswith(".cfg"):
            if "sfc" in filename:
                config_args["single_level_file"] = True
            if "hourly" in filename:
                config_args["co_file"] = True
            config_file = os.path.join(directory, filename)
            # Pass the data as keyword arguments to the new_config_file function
            new_config_file(config_file, field_name, additional_content,
                            config_args=config_args)


def get_secret(secret_key: str) -> dict:
    """Retrieve the secret value from the Google Cloud Secret Manager.

    Parameters:
        api_key (str): The name or identifier of the secret in the Google
                        Cloud Secret Manager.

    Returns:
        dict: A dictionary containing the retrieved secret data.
    """
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": secret_key})
    payload = response.payload.data.decode("UTF-8")
    secret_dict = json.loads(payload)
    return secret_dict


if __name__ == "__main__":
    for env_var in os.environ:
        if API_KEY_PATTERN.match(env_var):
            api_key_value = os.environ.get(env_var)
            API_KEY_LIST.append(api_key_value)

    additional_content = ""
    for count, secret_key in enumerate(API_KEY_LIST):
        secret_key_value = get_secret(secret_key)
        additional_content += f'parameters.api{count}\n\
            api_url={secret_key_value["api_url"]}\napi_key={secret_key_value["api_key"]}\n\n'

    current_day = datetime.date.today()
    job_name = f"wx-dl-arco-era5-{current_day.month}-{current_day.year}"
    # TODO(#373): Update the command once `--async` keyword added in weather-dl.
    command = (
        f'python weather_dl/weather-dl /weather/config_files/*.cfg --runner '
        f'DataflowRunner --project {PROJECT} --region {REGION} --temp_location '
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f'--sdk_container_image {SDK_CONTAINER_IMAGE} '
        f'--manifest-location {MANIFEST_LOCATION} --experiment use_runner_v2'
    )

    update_config_files(DIRECTORY, FIELD_NAME, additional_content)
    stop_on_log = 'JOB_STATE_RUNNING'
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    with process.stdout:
        try:
            for line in iter(process.stdout.readline, b''):
                log_message = line.decode("utf-8").strip()
                print(log_message)
                if stop_on_log in log_message:
                    print(f'Stopping subprocess as {stop_on_log}')
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        except subprocess.CalledProcessError as e:
            logger.error(
                f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}'
            )
