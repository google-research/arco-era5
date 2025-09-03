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
from google.cloud import run_v2
import constants


def create_job(job_id: str, template: run_v2.ExecutionTemplate):
    # Create a client
    client = run_v2.JobsClient()

    # Initialize request argument(s)
    job = run_v2.Job()
    job.template = template

    request = run_v2.CreateJobRequest(
        parent=f"projects/{constants.PROJECT}/locations/{constants.REGION}",
        job=job,
        job_id=job_id,
    )

    # Make the request
    operation = client.create_job(request=request)

    print("Waiting for operation to complete...")

    response = operation.result()

    # Handle the response
    print(response)


def era5_job_creator(
    job_id: str,
    timeout_sec: int,
    container_args: list = None,
    contaner_env: list = None,
    container_memory_limit: str = None,
    max_retries: int = 0
):
    # Create an ExecutionTemplate
    template = run_v2.ExecutionTemplate()
    template.task_count = constants.TASK_COUNT

    template.template.timeout = {"seconds": timeout_sec}
    template.template.max_retries = max_retries

    # Set container details
    container = run_v2.Container()
    container.name = constants.CONTAINER_NAME
    container.image = constants.CLOUD_RUN_CONTAINER_IMAGE
    container.command = constants.CONTAINER_COMMAND
    container.args = container_args

    # Set environment variables (example)
    container.env = contaner_env

    # Set resource limits (example)
    container.resources.limits = {
        "cpu": constants.CONTAINER_CPU_LIMIT,
        "memory": container_memory_limit,
    }

    # Add the container to the template
    template.template.containers.append(container)

    create_job(job_id, template)


if __name__ == "__main__":
    era5_job_creator(
        job_id=constants.SANITY_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
    era5_job_creator(
        job_id=constants.INGESTION_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
    era5_job_creator(
        job_id=constants.DAILY_EXECUTOR_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_args=constants.DAILY_EXECUTOR_JOB_CONTAINER_ARGS,
        contaner_env=constants.JOB_CONTAINER_ENV_VARIABLES + constants.ERA5T_API_KEYS,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
    era5_job_creator(
        job_id=constants.MONTHLY_EXECUTOR_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_args=constants.MONTHLY_EXECUTOR_JOB_CONTAINER_ARGS,
        contaner_env=constants.JOB_CONTAINER_ENV_VARIABLES + constants.ERA5T_API_KEYS,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
    era5_job_creator(
        job_id=constants.EXECUTOR_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_args=constants.EXECUTOR_JOB_CONTAINER_ARGS,
        contaner_env=constants.JOB_CONTAINER_ENV_VARIABLES + constants.ERA5_API_KEYS,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
    era5_job_creator(
        job_id=constants.MODEL_LEVEL_ZARR_EXECUTOR_JOB_ID,
        timeout_sec=constants.TIMEOUT_SECONDS,
        container_args=constants.MODEL_LEVEL_ZARR_EXECUTOR_JOB_CONTAINER_ARGS,
        contaner_env=constants.JOB_CONTAINER_ENV_VARIABLES,
        container_memory_limit=constants.CONTAINER_MEMORY_LIMIT,
    )
