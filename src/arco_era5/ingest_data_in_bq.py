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
import logging

from .avro_to_bq import avro_to_bq_func
from .utils import subprocess_run

logger = logging.getLogger(__name__)

ZARR_TO_AVRO_FILE_PATH = '/arco-era5/src/arco_era5/zarr_to_avro.py'

def ingest_data_in_bigquery_dataflow_job(zarr_file: str, data_process_month: str,
                                         data_process_year: str, avro_file: str,
                                         table_name: str, project: str, bucket: str,
                                         python_path: str,
                                         sdk_container_image: str,
                                         zarr_avro_conversion_network: str,
                                         zarr_avro_conversion_subnet: str
                                         ) -> None:
    """Ingests data of zarr file to BigQuery through AVRO file conversion.

    Args:
        zarr_file (str): The path to the input zarr file.
        data_process_month (str): The month of the data to be processed (e.g., '01' for January).
        data_process_year (str): The year of the data to be processed (e.g., '2023').
        avro_file (str): The path to the output AVRO file.
        table_name (str): The name of the BigQuery table where the data will be ingested.

        project (str): The Google Cloud project ID in which this Dataflow job executed.
        bucket (str): The Google Cloud Storage bucket where the temparory data is located.
        python_path (str): The executable python path of the Docker container.
        sdk_container_image (str): The Docker container image URI for the Dataflow SDK
            harness used for Zarr to AVRO conversion.
        zarr_avro_conversion_network (str): The name of the network to be used by Dataflow for Zarr to AVRO conversion.
        zarr_avro_conversion_subnet (str): The name of the subnet to be used by Dataflow for Zarr to AVRO conversion.

    Returns:
        None
    """
    if '/ar/' in zarr_file:
        logger.info(f"Data conversion of {zarr_file} to AVRO file is : {avro_file} started.")

        month_year = f'{data_process_year}/{data_process_month}'

        command = (
            f"{python_path} {ZARR_TO_AVRO_FILE_PATH} -i {zarr_file} -m {month_year} -o {avro_file} "
            f"--temp_location gs://{bucket}/temp "
            f"--runner DataflowRunner --project {project} --region us-central1 "
            f"--sdk_container_image {sdk_container_image} "
            f"--experiments use_runner_v2 --disk_size_gb 300 --machine_type n1-highmem-4 "
        )
        if zarr_avro_conversion_network and zarr_avro_conversion_subnet:
            command = command + (
                f"--no_use_public_ips --network {zarr_avro_conversion_network} "
                f"--subnetwork {zarr_avro_conversion_subnet}"
            )
        subprocess_run(command)
        logger.info(f"Data conversion of {zarr_file} to AVRO file is : {avro_file} completed.")

        logger.info(f"Data ingesting of {avro_file} to BQ table: {table_name} started.")
        avro_to_bq_func(input_path=avro_file, month=month_year,
                        table_name=table_name, project=project)
        logger.info(f"Data ingesting of {avro_file} to BQ table: {table_name} completed.")

