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
from .constant import variables_full_names, zarr_files
from .data_availability import (
    MODEL_LEVEL_MOISTURE_VARIABLE,
    MODEL_LEVEL_WIND_VARIABLE,
    SINGLE_LEVEL_FORECAST_VARIABLE,
    SINGLE_LEVEL_REANALYSIS_VARIABLE,
    SINGLE_LEVEL_SURFACE_VARIABLE,
    check_data_availability,
    generate_input_paths_of_ar_data,
    )
from .ingest_data_in_zarr import ingest_data_in_zarr_dataflow_job
from .pangeo import run, parse_args
from .resize_zarr import resize_zarr_target, update_zarr_metadata
from .source_data import (
    GCP_DIRECTORY,
    HOURS_PER_DAY,
    INIT_TIME,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS,
    SINGLE_LEVEL_VARIABLES,
    TIME_RESOLUTION_HOURS,
    _read_nc_dataset,
    align_coordinates,
    daily_date_iterator,
    get_pressure_levels_arg,
    get_var_attrs_dict,
    LoadTemporalDataForDateDoFn,
    offset_along_time_axis,
    parse_arguments,
    read_multilevel_vars,
    read_single_level_vars,
    )
from .update_ar import UpdateSlice as ARUpdateSlice
from .update_co import generate_input_paths, generate_offset, GenerateOffset, UpdateSlice as COUpdateSlice, opener
from .update_config_files import (    
    add_licenses_in_config_files,
    get_last_sixth_date,
    get_month_range,
    get_previous_month_dates,
    get_secret,
    new_config_file,
    update_date_in_config_file,
    update_target_path_in_config_file
    )
from .update_model_level_native_vertical_zarr import hourly_dates, LoadDataForDayDoFn, UpdateSlice as UpdateModelLevelNativeVerticalDataSlice
from .utils import (
    convert_to_date,
    data_splitting_dataflow_job,
    date_range,
    parse_arguments_raw_to_zarr_to_bq,
    raw_data_download_dataflow_job,
    replace_non_alphanumeric_with_hyphen,
    subprocess_run,
    )
