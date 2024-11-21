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
import datetime
import gcsfs
import logging

import typing as t

from .source_data import (
    GCP_DIRECTORY,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS,
    SINGLE_LEVEL_VARIABLES,
)
from .update_co import generate_input_paths

logger = logging.getLogger(__name__)

# File Templates
PRESSURELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/{pressure}.nc")
AR_SINGLELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/surface.nc")

# Data Chunks
MODEL_LEVEL_WIND_VARIABLE = ["dve", "tw"]
MODEL_LEVEL_MOISTURE_VARIABLE = ["o3q", "qrqs"]
SINGLE_LEVEL_SURFACE_VARIABLE = ["lnsp", "zs"]
SINGLE_LEVEL_REANALYSIS_VARIABLE = ["cape", "cisst", "soil_depthBelowLandLayer_istl1",
                                    "soil_depthBelowLandLayer_istl2",
                                    "soil_depthBelowLandLayer_istl3",
                                    "soil_depthBelowLandLayer_istl4",
                                    "soil_depthBelowLandLayer_stl1",
                                    "soil_depthBelowLandLayer_stl2",
                                    "soil_depthBelowLandLayer_stl3",
                                    "soil_depthBelowLandLayer_stl4",
                                    "soil_depthBelowLandLayer_swvl1",
                                    "soil_depthBelowLandLayer_swvl2",
                                    "soil_depthBelowLandLayer_swvl3",
                                    "soil_depthBelowLandLayer_swvl4",
                                    "soil_surface_tsn", "tcol", "sfc"]
SINGLE_LEVEL_FORECAST_VARIABLE = ["rad", "pcp_surface_cp", "pcp_surface_crr",
                                  "pcp_surface_csf", "pcp_surface_csfr",
                                  "pcp_surface_es", "pcp_surface_lsf",
                                  "pcp_surface_lsp", "pcp_surface_lspf",
                                  "pcp_surface_lsrr", "pcp_surface_lssfr",
                                  "pcp_surface_ptype", "pcp_surface_rsn",
                                  "pcp_surface_sd", "pcp_surface_sf",
                                  "pcp_surface_smlt", "pcp_surface_tp"]
PRESSURE_LEVEL = PRESSURE_LEVELS_GROUPS["full_37"]


def generate_input_paths_of_ar_data(date: datetime.datetime,
                                    variables: t.List[str]) -> t.List[str]:
    """
    Generate raw files path(.nc format) for the AR data based on the specified
    date and variables.

    Args:
        date (datetime): The date for which the AR raw file paths is required.
        variables (list[str]): An variables to fetch. Multi-level and
                               single-level variables are handled differently.

    Returns:
        List[str]: A list of URLs for the requested AR data.
    """
    all_urls = []
    for chunk in variables:
        if chunk in MULTILEVEL_VARIABLES:
            for pressure in PRESSURE_LEVEL:
                all_urls.append(
                    PRESSURELEVEL_DIR_TEMPLATE.format(
                        year=date.year,
                        month=date.month,
                        day=date.day,
                        chunk=chunk,
                        pressure=pressure))
        else:
            if chunk == 'geopotential_at_surface':
                chunk = 'geopotential'
            all_urls.append(AR_SINGLELEVEL_DIR_TEMPLATE.format(
                    year=date.year, month=date.month, day=date.day, chunk=chunk))
    return all_urls


def check_data_availability(data_date_range: t.List[datetime.datetime],
                            type: t.Optional[str] = None) -> bool:
    """
    Check the availability of data for a given date range and type.

    Args:
        data_date_range (List[datetime]): A list of dates defining the date range
            for which data availability should be checked.
        data_type (Optional[str]): The type of data to check availability for.
            Can be 'ERA5T_MONTHLY', 'ERA5T_DAILY', or None for a full check.
            Defaults to None.

    Returns:
        bool: True if any data is missing, False if all data is available.
    """

    fs = gcsfs.GCSFileSystem()
    start_date = data_date_range[0].strftime("%Y/%m/%d")
    end_date = data_date_range[-1].strftime("%Y/%m/%d")

    all_uri = []
    if type != 'ERA5T_MONTHLY':  # ERA5T_DAILY(model_level of CO & AR)
        all_uri.extend(
            generate_input_paths(
                start_date, end_date, GCP_DIRECTORY,
                MODEL_LEVEL_WIND_VARIABLE + MODEL_LEVEL_MOISTURE_VARIABLE))
        for date in data_date_range:
            all_uri.extend(
                generate_input_paths_of_ar_data(
                    date, MULTILEVEL_VARIABLES + SINGLE_LEVEL_VARIABLES))

    if type != 'ERA5T_DAILY':  # ERA5T_MONTHLY(single_level of CO)
        all_uri.extend(
            generate_input_paths(
                start_date, end_date, GCP_DIRECTORY,
                (SINGLE_LEVEL_SURFACE_VARIABLE + SINGLE_LEVEL_FORECAST_VARIABLE
                 + SINGLE_LEVEL_REANALYSIS_VARIABLE), True))

    data_is_missing = False
    for path in all_uri:
        if not fs.exists(path):
            data_is_missing = True
            logger.info(path)

    return True if data_is_missing else False
