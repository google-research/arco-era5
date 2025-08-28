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
# ==============================================================================
import datetime
import logging
import os
from typing import List, Tuple

from arco_era5 import get_month_range, run_cloud_job, update_zarr_metadata

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Env Variables
PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
INGESTION_JOB_ID = os.environ.get("INGESTION_JOB_ID")
ARCO_ERA5_WITH_MODEL_LEVEL_SDK_CONTAINER_IMAGE = os.environ.get("ARCO_ERA5_WITH_MODEL_LEVEL_SDK_CONTAINER_IMAGE")

MODEL_LEVEL_FILE_PATH = "/arco-era5/src/update-model-level-native-vertical-zarr-data.py"
MODEL_LEVEL_ZARR_PATH = "gs://gcp-public-data-arco-era5/ar/model-level-1h-0p25deg.zarr-v1"

def generate_args(
    file_path: str,
    target_path: str,
    start_date: str,
    end_date: str,
    bucket: str,
    project: str,
    region: str,
    job_name: str
) -> List[str]:
    """Generate the arguments for the Dataflow job."""
    return [
        file_path,
        "--output_path", target_path,
        "--start_date", start_date,
        "--end_date", end_date,
        "--temp_location", f"gs://{bucket}/temp",
        "--runner", "DataflowRunner",
        "--project", project,
        "--region", region,
        "--experiments", "use_runner_v2",
        "--save_main_session",
        "--worker_machine_type", "n2-standard-16",
        "--disk_size_gb", "1000",
        "--job_name", job_name,
        "--number_of_worker_harness_threads", "1",
        "--sdk_container_image", ARCO_ERA5_WITH_MODEL_LEVEL_SDK_CONTAINER_IMAGE,
        "--max_num_workers", "100"
    ]


def compute_previous_month_range() -> Tuple[str]:
    """Compute the first and last day of the previous third month from today."""
    today = datetime.date.today()
    third_prev_month = today - datetime.timedelta(days=2*366/12)
    first_day, last_day = get_month_range(third_prev_month)
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")


def create_job_name(zarr_path: str, start_date: str) -> str:
    """Create a unique job name based on the Zarr path and start date."""
    base_name = os.path.splitext(zarr_path.split("/")[-1])[0]
    return f"zarr-data-ingestion-{base_name}-{start_date[:7]}"


if __name__ == "__main__":
    try:
        first_day, last_day = compute_previous_month_range()
        job_name = create_job_name(MODEL_LEVEL_ZARR_PATH, first_day)

        override_args = generate_args(
            file_path=MODEL_LEVEL_FILE_PATH,
            target_path=MODEL_LEVEL_ZARR_PATH,
            start_date=first_day,
            end_date=last_day,
            bucket=BUCKET,
            project=PROJECT,
            region=REGION,
            job_name=job_name
        )
        
        logger.info(f"Data ingesting for {MODEL_LEVEL_FILE_PATH} is started.")
        run_cloud_job(PROJECT, REGION, INGESTION_JOB_ID, override_args)
        logger.info(f"Data ingesting for {MODEL_LEVEL_FILE_PATH} is completed.")
        
        logger.info(f"update metadata for zarr file: {MODEL_LEVEL_FILE_PATH} started.")
        update_zarr_metadata(MODEL_LEVEL_ZARR_PATH, last_day, 'era5')
        logger.info(f"update metadata for zarr file: {MODEL_LEVEL_FILE_PATH} completed.")

    except Exception as e:
        logger.error("Error running the job: %s", e, exc_info=True)
        raise
