import datetime
import gcsfs
import logging
import os

import typing as t

from .source_data import (
    SINGLE_LEVEL_VARIABLES,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS,
)

logger = logging.getLogger(__name__)

# File Templates
MODELLEVEL_DIR_VAR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/{year:04d}/{year:04d}{month:02d}{day:02d}_hres_{chunk}.grb2_{level}_{var}.grib")
MODELLEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/{year:04d}/{year:04d}{month:02d}{day:02d}_hres_{chunk}.grb2")
SINGLELEVEL_DIR_VAR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year:04d}/{year:04d}{month:02d}_hres_{chunk}.grb2_{level}_{var}.grib")
SINGLELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year:04d}/{year:04d}{month:02d}_hres_{chunk}.grb2")
PRESSURELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/{pressure}.nc")
AR_SINGLELEVEL_DIR_TEMPLATE = (
    "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/surface.nc")

# Data Chunks
MODEL_LEVEL_CHUNKS = ["dve", "tw", "o3q", "qrqs"]
SINGLE_LEVEL_CHUNKS = [
    "cape", "cisst", "sfc", "tcol", "soil_depthBelowLandLayer_istl1",
    "soil_depthBelowLandLayer_istl2", "soil_depthBelowLandLayer_istl3",
    "soil_depthBelowLandLayer_istl4", "soil_depthBelowLandLayer_stl1",
    "soil_depthBelowLandLayer_stl2", "soil_depthBelowLandLayer_stl3",
    "soil_depthBelowLandLayer_stl4", "soil_depthBelowLandLayer_swvl1",
    "soil_depthBelowLandLayer_swvl2", "soil_depthBelowLandLayer_swvl3",
    "soil_depthBelowLandLayer_swvl4", "soil_surface_tsn", "lnsp",
    "zs", "rad", "pcp_surface_cp", "pcp_surface_crr",
    "pcp_surface_csf", "pcp_surface_csfr", "pcp_surface_es",
    "pcp_surface_lsf", "pcp_surface_lsp", "pcp_surface_lspf",
    "pcp_surface_lsrr", "pcp_surface_lssfr", "pcp_surface_ptype",
    "pcp_surface_rsn", "pcp_surface_sd", "pcp_surface_sf",
    "pcp_surface_smlt", "pcp_surface_tp"]
PRESSURE_LEVEL = PRESSURE_LEVELS_GROUPS["full_37"]


def check_data_availability(data_date_range: t.List[datetime.datetime]) -> bool:
    """Checks the availability of data for a given date range.

    Args:
        data_date_range (List[datetime.datetime]): Date range for CO data.

    Returns:
        int: 1 if data is missing, 0 if data is available.
    """

    fs = gcsfs.GCSFileSystem(project=os.environ.get('PROJECT',
                                                    'ai-for-weather'))
    # update above project with ai-for-weather
    all_uri = []
    for date in data_date_range:
        for chunk in MODEL_LEVEL_CHUNKS:
            if "_" in chunk:
                chunk_, level, var = chunk.split("_")
                all_uri.append(
                    MODELLEVEL_DIR_VAR_TEMPLATE.format(year=date.year, month=date.month,
                                                       day=date.day, chunk=chunk_,
                                                       level=level, var=var))
                continue
            all_uri.append(
                MODELLEVEL_DIR_TEMPLATE.format(
                    year=date.year, month=date.month, day=date.day, chunk=chunk))
    single_date = data_date_range[0]
    for chunk in SINGLE_LEVEL_CHUNKS:
        if "_" in chunk:
            chunk_, level, var = chunk.split("_")
            all_uri.append(
                SINGLELEVEL_DIR_VAR_TEMPLATE.format(
                    year=single_date.year, month=single_date.month, chunk=chunk_,
                    level=level, var=var))
            continue
        all_uri.append(
            SINGLELEVEL_DIR_TEMPLATE.format(
                year=single_date.year, month=single_date.month, chunk=chunk))

    for date in data_date_range:
        for chunk in MULTILEVEL_VARIABLES + SINGLE_LEVEL_VARIABLES:
            if chunk in MULTILEVEL_VARIABLES:
                for pressure in PRESSURE_LEVEL:
                    all_uri.append(
                        PRESSURELEVEL_DIR_TEMPLATE.format(year=date.year,
                                                          month=date.month,
                                                          day=date.day, chunk=chunk,
                                                          pressure=pressure))
            else:
                if chunk == 'geopotential_at_surface':
                    chunk = 'geopotential'
                all_uri.append(
                    AR_SINGLELEVEL_DIR_TEMPLATE.format(
                        year=date.year, month=date.month, day=date.day, chunk=chunk))

    data_is_missing = False
    for path in all_uri:
        if not fs.exists(path):
            data_is_missing = True
            logger.info(path)

    return True if data_is_missing else False