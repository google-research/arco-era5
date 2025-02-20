PROJECT = "<PROJECT_ID>"
REGION = "<REGION>"
BUCKET = "<BUCKET_NAME>"
API_KEY_1 = "projects/PROJECT/secrets/SECRET_NAME/versions/1"
API_KEY_2 = "projects/PROJECT/secrets/SECRET_NAME/versions/1"
API_KEY_3 = "projects/PROJECT/secrets/SECRET_NAME/versions/1"
API_KEY_4 = "projects/PROJECT/secrets/SECRET_NAME/versions/1"
WEATHER_TOOLS_SDK_CONTAINER_IMAGE = "WEATHER_TOOLS_SDK_CONTAINER_IMAGE"
ARCO_ERA5_SDK_CONTAINER_IMAGE = "ARCO_ERA5_SDK_CONTAINER_IMAGE"
MANIFEST_LOCATION = "fs://manifest?projectId=PROJECT_ID"
TEMP_PATH_FOR_RAW_DATA="<BUCKET_PATH>"
ROOT_PATH = "<BUCKET_PATH>"

TASK_COUNT = 1
MAX_RETRIES = 1

CONTAINER_NAME = "arco-era5-container"
CLOUD_RUN_CONTAINER_IMAGE = "CLOUD_RUN_CONTAINER_IMAGE"
CONTAINER_COMMAND = ["python"]
CONTAINER_CPU_LIMIT = "8000m"

INGESTION_JOB_ID = "arco-era5-zarr-ingestion"
SANITY_JOB_ID = "arco-era5-sanity"

EXECUTOR_JOB_ID = "arco-era5-executor"
DAILY_EXECUTOR_JOB_ID = "arco-era5t-daily-executor"
MONTHLY_EXECUTOR_JOB_ID = "arco-era5t-monthly-executor"

EXECUTOR_JOB_CONTAINER_ARGS = [
    "src/raw-to-zarr-to-bq.py"
]
DAILY_EXECUTOR_JOB_CONTAINER_ARGS = [
    "src/raw-to-zarr-to-bq.py", "--mode", "daily"
]
MONTHLY_EXECUTOR_JOB_CONTAINER_ARGS = [
    "src/raw-to-zarr-to-bq.py", "--mode", "monthly"
]

JOB_CONTAINER_ENV_VARIABLES = [
    {"name": "PROJECT", "value": PROJECT},
    {"name": "REGION", "value": REGION},
    {"name": "BUCKET", "value": BUCKET},
    {"name": "WEATHER_TOOLS_SDK_CONTAINER_IMAGE", "value": WEATHER_TOOLS_SDK_CONTAINER_IMAGE},
    {"name": "MANIFEST_LOCATION", "value": MANIFEST_LOCATION},
    {"name": "PYTHON_PATH", "value": "python"},
    {"name": "TEMP_PATH_FOR_RAW_DATA", "value": TEMP_PATH_FOR_RAW_DATA},
    {"name": "INGESTION_JOB_ID", "value": INGESTION_JOB_ID},
    {"name": "SANITY_JOB_ID", "value": SANITY_JOB_ID},
    {"name": "ROOT_PATH", "value": ROOT_PATH},
    {"name": "ARCO_ERA5_SDK_CONTAINER_IMAGE", "value": ARCO_ERA5_SDK_CONTAINER_IMAGE}
]

ERA5_API_KEYS = [
    {"name": "API_KEY_1", "value": API_KEY_3},
    {"name": "API_KEY_2", "value": API_KEY_4}
]

ERA5T_API_KEYS = [
    {"name": "API_KEY_1", "value": API_KEY_1},
    {"name": "API_KEY_2", "value": API_KEY_2}
]

CONTAINER_MEMORY_LIMIT = "34G"

# Timeout for a week
TIMEOUT_SECONDS = 604800

CONTAINER_MEMORY_LIMIT = "34G"
