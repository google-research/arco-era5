# Copyright 2022 Google LLC
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
from .constant import ARCO_ERA5_ZARR_FILES, variables_full_names, zarr_files
from .data_availability import check_data_availability
from .download import raw_data_download_dataflow_job, data_splitting_dataflow_job
from .ingest_data_in_zarr import perform_data_operations
from .pangeo import run, parse_args
from .resize_zarr import resize_zarr_target, update_zarr_metadata
from .source_data import (
    GCP_DIRECTORY,
    SINGLE_LEVEL_VARIABLES,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS,
    TIME_RESOLUTION_HOURS,
    HOURS_PER_DAY,
    get_var_attrs_dict,
    read_multilevel_vars,
    read_single_level_vars,
    daily_date_iterator,
    align_coordinates,
    parse_arguments,
    get_pressure_levels_arg,
    LoadTemporalDataForDateDoFn,
    _read_nc_dataset
    )
from .sanity import generate_raw_paths, OpenLocal, run_sanity_job, update_splittable_files, update_zarr
from .update_ar import UpdateSlice as ARUpdateSlice
from .update_co import GenerateOffset, UpdateSlice as COUpdateSlice, generate_input_paths
from .update_config_files import (
    get_secret,
    update_config_file,
    get_previous_month_dates,
    new_config_file,
    get_month_range,
    remove_licenses_from_directory
    )
from .update_model_level_native_vertical_zarr import LoadDataForDayDoFn, UpdateSlice as UpdateModelLevelNativeVerticalDataSlice, hourly_dates
from .utils import (
    date_range,
    replace_non_alphanumeric_with_hyphen,
    subprocess_run,
    convert_to_date,
    parse_arguments_raw_to_zarr_to_bq,
    ExecTypes
    )
