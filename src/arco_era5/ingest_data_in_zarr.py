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
import logging
import os

from .resize_zarr import update_zarr_metadata
from .utils import ExecTypes, replace_non_alphanumeric_with_hyphen, run_cloud_job

logger = logging.getLogger(__name__)

PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
INGESTION_JOB_ID = os.environ.get("INGESTION_JOB_ID")
ROOT_PATH = os.environ.get("ROOT_PATH")
ARCO_ERA5_SDK_CONTAINER_IMAGE = os.environ.get("ARCO_ERA5_SDK_CONTAINER_IMAGE")

AR_FILE_PATH = '/arco-era5/src/update-ar-data.py'
CO_FILE_PATH = '/arco-era5/src/update-co-data.py'
CO_FILES_MAPPING = {
    'model-level-moisture': ['o3q', 'qrqs'],
    'model-level-wind': ['dve', 'tw'],
    'single-level-forecast': ['rad', 'pcp_surface_cp', 'pcp_surface_crr',
                              'pcp_surface_csf', 'pcp_surface_csfr', 'pcp_surface_es',
                              'pcp_surface_lsf', 'pcp_surface_lsp', 'pcp_surface_lspf',
                              'pcp_surface_lsrr', 'pcp_surface_lssfr',
                              'pcp_surface_ptype', 'pcp_surface_rsn', 'pcp_surface_sd',
                              'pcp_surface_sf', 'pcp_surface_smlt', 'pcp_surface_tp'],
    'single-level-reanalysis': ['cape', 'cisst', 'sfc', 'tcol',
                                'soil_depthBelowLandLayer_istl1',
                                'soil_depthBelowLandLayer_istl2',
                                'soil_depthBelowLandLayer_istl3',
                                'soil_depthBelowLandLayer_istl4',
                                'soil_depthBelowLandLayer_stl1',
                                'soil_depthBelowLandLayer_stl2',
                                'soil_depthBelowLandLayer_stl3',
                                'soil_depthBelowLandLayer_stl4',
                                'soil_depthBelowLandLayer_swvl1',
                                'soil_depthBelowLandLayer_swvl2',
                                'soil_depthBelowLandLayer_swvl3',
                                'soil_depthBelowLandLayer_swvl4',
                                'soil_surface_tsn'],
    'single-level-surface': ['lnsp', 'zs']
}

def generate_override_args(
        file_path: str,
        target_path: str,
        start_date: str,
        end_date: str,
        root_path: str,
        init_date: str,
        bucket: str,
        project: str,
        region: str,
        job_name: str
) -> list:
    args = [
        file_path,
        "--output_path", target_path,
        "-s", start_date,
        "-e", end_date,
        "--root_path", root_path,
        "--init_date", init_date,
        "--temp_location", f"gs://{bucket}/temp",
        "--runner", "DataflowRunner",
        "--project", project,
        "--region", region,
        "--experiments", "use_runner_v2",
        "--disk_size_gb", "250",
        "--setup_file", "/arco-era5/setup.py",
        "--job_name", job_name,
        "--number_of_worker_harness_threads", "1"
    ]
    return args

def ingest_data_in_zarr_dataflow_job(target_path: str, region: str, start_date: str,
                                     end_date: str, root_path: str, init_date: str, project: str,
                                     bucket: str, ingestion_job_id: str, mode: str) -> None:
    """Ingests data into a Zarr store and runs a Dataflow job.

    Args:
        target_path (str): The target Zarr store path.
        region (str): The region in which this job will run.
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.
        init_date (str): The initial date of the zarr store in the format of str.

    Returns:
        None
    """
    job_name = target_path.split('/')[-1]
    job_name = os.path.splitext(job_name)[0]
    process_date = start_date[:7]
    if mode == ExecTypes.ERA5T_DAILY.value:
        process_date = start_date
    job_name = (
        f"zarr-data-ingestion-{replace_non_alphanumeric_with_hyphen(job_name)}-{process_date}"
    )
    override_args = generate_override_args(CO_FILE_PATH, target_path, start_date, end_date, root_path, init_date, bucket, project, region, job_name)
    if '/ar/' in target_path:
        logger.info(f"Data ingestion for {target_path} of AR data.")
        override_args[0] = AR_FILE_PATH
        override_args.extend([
            "--pressure_levels_group", "full_37",
            "--machine_type", "n2-highmem-32"
        ])
    else:
        chunks = CO_FILES_MAPPING[target_path.split('/')[-1].split('.')[0]]
        time_per_day = 2 if 'single-level-forecast' in target_path else 24
        override_args.extend([
            "--time_per_day", str(time_per_day),
            "--machine_type", "n2-highmem-8",
            "--sdk_container_image", ARCO_ERA5_SDK_CONTAINER_IMAGE,
            "--c"
        ])
        override_args.extend(chunks)
        logger.info(f"Data ingestion for {target_path} of CO data.")
    
    run_cloud_job(project, region, ingestion_job_id, override_args)


def perform_data_operations(z_file: str, start_date: str,
                            end_date: str, init_date: str, mode: str):
    # Function to process a single pair of z_file and table
    try:
        logger.info(f"Data ingesting for {z_file} is started.")
        ingest_data_in_zarr_dataflow_job(z_file, REGION, start_date, end_date, ROOT_PATH, init_date,
                                         PROJECT, BUCKET, INGESTION_JOB_ID, mode)
        logger.info(f"Data ingesting for {z_file} is completed.")
        logger.info(f"update metadata for zarr file: {z_file} started.")

        update_zarr_metadata(z_file, end_date, mode)
        logger.info(f"update metadata for zarr file: {z_file} completed.")
    except Exception as e:
        logger.error(
            f"An error occurred in process_zarr for {z_file}: {str(e)}")
