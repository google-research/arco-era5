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
import typing as t

from google.cloud import secretmanager


class ConfigArgs(t.TypedDict):
    """A class representing the configuration arguments for the new_config_file
    function.

    Attributes:
        year_wise_date (bool): True if the configuration file contains 'year',
                                'month' and 'day', False otherwise.
        first_day_third_prev (datetime.date): The first day of the third previous month.
        last_day_third_prev (datetime.date): The last day of the third previous month.
        sl_year (str): The year of the third previous month in 'YYYY' format.
        sl_month (str): The month of the third previous month in 'MM' format.
    """
    year_wise_date: bool
    first_day_third_prev: datetime.date
    last_day_third_prev: datetime.date
    sl_year: str
    sl_month: str


class MonthDates(t.TypedDict):
    """A class representing the first and third previous month's dates.

    Attributes:
        first_day_third_prev (datetime.date): The first day of the third previous month.
        last_day_third_prev (datetime.date): The last day of the third previous month.
        sl_year (str): The year of the third previous month in 'YYYY' format.
        sl_month (str): The month of the third previous month in 'MM' format.
    """
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
    year_wise_date = config_args["year_wise_date"]
    first_day_third_prev = config_args["first_day_third_prev"]
    last_day_third_prev = config_args["last_day_third_prev"]
    sl_year = config_args["sl_year"]
    sl_month = config_args["sl_month"]

    config = configparser.ConfigParser()
    config.read(config_file)

    if year_wise_date:
        config.set("selection", "year", sl_year)
        config.set("selection", "month", sl_month)
        config.set("selection", "day", "all")
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
            - 'first_day_third_prev': The first day of the third previous month
                                        (datetime.date).
            - 'last_day_third_prev': The last day of the third previous month
                                        (datetime.date).
            - 'sl_year': The year of the third previous month in 'YYYY' format (str).
            - 'sl_month': The month of the third previous month in 'MM' format (str).
    """

    today = datetime.date.today()
    # Calculate the correct previous third month considering months from 1 to 12
    third_prev_month = today - datetime.timedelta(days=2*366/12)
    first_day_third_prev, last_day_third_prev = get_month_range(third_prev_month)
    first_date_third_prev = first_day_third_prev
    sl_year, sl_month = str(first_date_third_prev)[:4], str(first_date_third_prev)[5:7]

    return {
        'first_day_third_prev': first_day_third_prev,
        'last_day_third_prev': last_day_third_prev,
        'sl_year': sl_year,
        'sl_month': sl_month,
    }


def update_config_file(directory: str, field_name: str,
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
        "first_day_third_prev": dates_data['first_day_third_prev'],
        "last_day_third_prev": dates_data['last_day_third_prev'],
        "sl_year": dates_data['sl_year'],
        "sl_month": dates_data['sl_month'],
    }
    for filename in os.listdir(directory):
        config_args["year_wise_date"] = False
        if filename.endswith(".cfg"):
            if "lnsp" in filename or "zs" in filename or "sfc" in filename:
                config_args["year_wise_date"] = True
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


def remove_license_from_config_file(config_file_path: str, num_licenses: int) -> None:
    """Remove licenses from a configuration file.

    Args:
        config_file_path (str): The path to the configuration file from
        which licenses will be removed.
        num_licenses (int): The number of licenses to remove from the file.

    """
    config = configparser.ConfigParser()
    config.read(config_file_path)
    for license_number in range(num_licenses):
        section_name = f'parameters.api{license_number}'
        config.remove_section(section_name)
    with open(config_file_path, "w") as file:
        config.write(file, space_around_delimiters=False)


def remove_licenses_from_directory(directory_path: str, num_licenses: int) -> None:
    """Remove licenses from all configuration files in a directory.

    Args:
        directory_path (str): The path to the directory containing configuration files.
        num_licenses (int): The number of licenses to remove from each
        configuration file.

    """
    for filename in os.listdir(directory_path):
        if filename.endswith(".cfg"):
            config_file_path = os.path.join(directory_path, filename)
            remove_license_from_config_file(config_file_path, num_licenses)
