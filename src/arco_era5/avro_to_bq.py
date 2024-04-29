# Copyright 2024 Google LLC
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
import numpy as np
import typing as t

from concurrent.futures import ThreadPoolExecutor
from gcsfs import GCSFileSystem
from google.cloud import bigquery
from math import ceil

logger = logging.getLogger(__name__)

def load_data(uris: np.ndarray, table: str, project: str):
    
    logger.info(f"Data loading of {uris[0]}:{uris[-1]} files are started.")

    client = bigquery.Client(project)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        use_avro_logical_types=True,
        autodetect=True
    )

    load_job = client.load_table_from_uri(
        uris.tolist(), table, job_config=job_config
    )

    load_job.result()
    logger.info(f"Data Loaded of {uris[0]}:{uris[-1]} files into the BQ.")

def load_files(files: t.List[str], total: int, table: str, project: str, skip: int = 10000):
    total_jobs = ceil(total / skip)
    logger.info(f"Performing {total_jobs} Load Jobs of BQ.")
    with ThreadPoolExecutor(max_workers=100) as executor:
        for file in np.array_split(files, total_jobs):
            executor.submit(load_data, file, table, project)

def int_to_zero_padded_string(number, num_digits):
    return f'{number:0{num_digits}d}'

def generate_file_map(month: str, base_path: str):

    month_year = datetime.datetime.strptime(month, "%m/%Y")
    year = month_year.year
    month = month_year.month
    equal = False
    fs = GCSFileSystem()
    try:
        files = fs.ls(f'{base_path}/{year}/{month:02}/', prefix='ar-00000')
    except Exception:
        files = fs.ls(f'{base_path}/{year}/{month:02}/', prefix='ar-000000')
        pass
    total_files = int(files[0].split(f"{year}/{month:02}/")[1].split('-of-')[1])

    name_splits = files[0].split("/")[-1].split("-")
    equal = len(name_splits[1]) == len(name_splits[-1])

    return total_files, equal

def avro_to_bq_func(input_path: str, month: str, table_name: str, project: str):

    fs = GCSFileSystem()

    total, equal = generate_file_map(month=month, base_path=input_path)

    if equal:
        digits = len(str(total))
        files = [ f'{input_path}/{month}/ar-{int_to_zero_padded_string(i, digits)}-of-{total}' for i in range(total) ]
    else:
        c_files = fs.ls(f"{input_path}/{month}/", prefix='ar-')
        files = [ f"gs://{file}" for file in c_files ]

    logger.info(f" total {total} AVRO files found for {month}.")

    load_files(files, total, table=table_name, project=project)
    logger.info(f"load jobs of BQ are completed.")