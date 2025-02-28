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
import os
import re
import subprocess
import sys
import tempfile

import pandas as pd
import typing as t

from enum import Enum
from contextlib import contextmanager
from google.cloud import run_v2

logger = logging.getLogger(__name__)


class ExecTypes(Enum):

    ERA5 = "era5"
    ERA5T_DAILY = "daily"
    ERA5T_MONTHLY = "monthly"


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
    parser.add_argument("--mode", type=str, default="era5",
                        help="Mode to execute the flow. Supported values era5, daily, monthly")

    return parser.parse_known_args()


def copy(src: str, dst: str) -> None:
    """A method to copy remote file to local path.

    Args:
        src (str): The cloud storage path to the grib file.
        dst (str): A temp location to copy the file.
    """
    cmd = 'gsutil -m cp'
    try:
        subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True,
                       text=True, input="n/n")
        return
    except subprocess.CalledProcessError as e:
        msg = f"Failed to copy file {src!r} to {dst!r} Error {e}"
        logger.error(msg)


@contextmanager
def opener(fname: str) -> t.Any:
    """A method to copy remote file into temp.

    Args:
        url (str): The cloud storage path to the grib file.
    """
    _, suffix = os.path.splitext(fname)
    with tempfile.NamedTemporaryFile(suffix=suffix) as ntf:
        tmp_name = ntf.name
        logger.info(f"Copying '{fname}' to local file '{tmp_name}'")
        copy(fname, tmp_name)
        yield tmp_name


def remove_file(url: str):
    """Remove file from remote location."""
    cmd = 'gsutil rm -rf'
    try:
        subprocess.run(cmd.split() + [url], check=True, capture_output=True,
                       text=True, input="n/n")
        return
    except subprocess.CalledProcessError as e:
        msg = f"Failed to remove file {url!r} Error {e}"
        logger.error(msg)


def run_cloud_job(project: str, region: str, job: str, override_args: t.List[str]):
    client = run_v2.JobsClient()
    job_executor_path = client.job_path(
        project, region, job
    )
    override_spec = {
        "container_overrides": [{ "args": override_args }]
    }
    job_executor_request = run_v2.RunJobRequest(
        name=job_executor_path,
        overrides=override_spec,
    )
    try:
        response = client.run_job(request=job_executor_request)
        if response.running():
            print("job_executor triggered.")
    except Exception as e:
        print(f"Error starting ee_job_executor: {e}")
