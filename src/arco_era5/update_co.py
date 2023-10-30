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

import calendar
import datetime
import logging
import os
import subprocess
import tempfile
import zarr

import apache_beam as beam
import typing as t
import xarray as xr

from contextlib import contextmanager
from dataclasses import dataclass
from itertools import product

from .utils import date_range

logger = logging.getLogger(__name__)

SINGLE_LEVEL_SUBDIR_TEMPLATE = (
    "ERA5GRIB/HRES/Month/{year}/{year}{month:02d}_hres_{chunk}.grb2"
)

MODELLEVEL_SUBDIR_TEMPLATE = (
    "ERA5GRIB/HRES/Daily/{year}/{year}{month:02d}{day:02d}_hres_{chunk}.grb2"
)

VARIABLE_DICT: t.Dict[str, t.List[str]] = {
    'dve': ['d', 'vo'],  # model-level-wind
    'tw': ['t', 'w'],
    'o3q': ['q', 'o3', 'clwc', 'ciwc', 'cc'],  # model-level-moisture
    'qrqs': ['crwc', 'cswc'],
    'lnsp': ['lnsp'],  # single-level-surface
    'zs': ['z'],
    'cape': ['cape', 'p79.162', 'p80.162'],  # single-level-reanalysis
    'cisst': ['siconc', 'sst', 'skt'],
    'sfc': ['z', 'sp', 'tcwv', 'msl', 'tcc', 'u10', 'v10', 't2m',
            'd2m', 'lcc', 'mcc', 'hcc', 'u100', 'v100'],
    'tcol': ['tclw', 'tciw', 'tcw', 'tcwv', 'tcrw', 'tcsw'],
    'soil_depthBelowLandLayer_istl1': ['istl1'],
    'soil_depthBelowLandLayer_istl2': ['istl2'],
    'soil_depthBelowLandLayer_istl3': ['istl3'],
    'soil_depthBelowLandLayer_istl4': ['istl4'],
    'soil_depthBelowLandLayer_stl1': ['stl1'],
    'soil_depthBelowLandLayer_stl2': ['stl2'],
    'soil_depthBelowLandLayer_stl3': ['stl3'],
    'soil_depthBelowLandLayer_stl4': ['stl4'],
    'soil_depthBelowLandLayer_swvl1': ['swvl1'],
    'soil_depthBelowLandLayer_swvl2': ['swvl2'],
    'soil_depthBelowLandLayer_swvl3': ['swvl3'],
    'soil_depthBelowLandLayer_swvl4': ['swvl4'],
    'soil_surface_tsn': ['tsn'],
    'rad': ['ssrd', 'strd', 'str', 'ttr', 'gwd'],  # single-level-forecast
    'pcp_surface_cp': ['cp'],
    'pcp_surface_crr': ['crr'],
    'pcp_surface_csf': ['csf'],
    'pcp_surface_csfr': ['csfr'],
    'pcp_surface_es': ['es'],
    'pcp_surface_lsf': ['lsf'],
    'pcp_surface_lsp': ['lsp'],
    'pcp_surface_lspf': ['lspf'],
    'pcp_surface_lsrr': ['lsrr'],
    'pcp_surface_lssfr': ['lssfr'],
    'pcp_surface_ptype': ['ptype'],
    'pcp_surface_rsn': ['rsn'],
    'pcp_surface_sd': ['sd'],
    'pcp_surface_sf': ['sf'],
    'pcp_surface_smlt': ['smlt'],
    'pcp_surface_tp': ['tp']
}


def convert_to_date(date_str: str, format: str = '%Y-%m-%d') -> datetime.datetime:
    """A method to convert date string into datetime format.

    Args:
        date (str): string date in %Y-%m-%d format.

    Returns:
        datetime: datetime generated from string date.
    """
    return datetime.datetime.strptime(date_str, format)


def copy(src: str, dst: str) -> None:
    """A method for generating the offset along with time dimension.

    Args:
        src (str): The cloud storage path to the grib file.
        dst (str): A temp location to copy the file.
    """
    cmd = 'gcloud alpha storage cp'
    try:
        subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True,
                       text=True, input="n/n")
        return
    except subprocess.CalledProcessError as e:
        msg = f"Failed to copy file {src!r} to {dst!r} Error {e}"
        logger.error(msg)


@contextmanager
def opener(fname: str) -> t.Any:
    """A method to copy remote file into temp.

    Args:
        url (str): The cloud storage path to the grib file.
    """
    _, suffix = os.path.splitext(fname)
    with tempfile.NamedTemporaryFile(suffix=suffix) as ntf:
        tmp_name = ntf.name
        logger.info(f"Copying '{fname}' to local file '{tmp_name}'")
        copy(fname, tmp_name)
        yield tmp_name


def generate_input_paths(start: str, end: str, root_path: str, chunks: t.List[str],
                         is_single_level: bool = False) -> t.List[str]:
    """A method for generating the url using the combination of chunks and time range.

    Args:
        start (str): starting date for data files.
        end (str): last date for data files.
        root_path (str): raw file url prefix.
        chunks: (t.List(str)): List of chunks to process.
        is_single_level: (bool): Is the call for single level or model level

    Returns:
        t.List: List of file urls to process.
    """
    input_paths = []
    for time, chunk in product(date_range(start, end, freq="MS" if is_single_level else "D"), chunks):
        if is_single_level:
            url = f"{root_path}/{SINGLE_LEVEL_SUBDIR_TEMPLATE.format(year=time.year, month=time.month, day=time.day, chunk=chunk)}"
        else:
            url = f"{root_path}/{MODELLEVEL_SUBDIR_TEMPLATE.format(year=time.year, month=time.month, day=time.day, chunk=chunk)}"

        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            url = url.replace(chunk, chunk_)
            url = f"{url}_{level}_{var}.grib"
        input_paths.append(url)

    return input_paths


@dataclass
class GenerateOffset(beam.PTransform):
    """A Beam PTransform for generating the offset along with time dimension."""

    init_date: str = '1900-01-01'
    timestamps_per_file: int = 24
    is_single_level: bool = False

    def apply(self, url: str) -> t.Tuple[str, slice, t.List[str]]:
        """A method for generating the offset along with time dimension.

        Args:
            url (str): The cloud storage path to the grib file.

        Returns:
            t.Tuple: url with included variables and time offset.
        """
        file_name = url.rsplit('/', 1)[1].rsplit('.', 1)[0]
        int_date, chunk = file_name.split('_hres_')
        if "_" in chunk:
            chunk = chunk.replace(".grb2_", "_")
        if self.is_single_level:
            int_date += "01"
        start_date = convert_to_date(int_date, '%Y%m%d')
        days_diff = start_date - convert_to_date(self.init_date)
        start = days_diff.days * self.timestamps_per_file
        end = start + self.timestamps_per_file * (
            calendar.monthrange(start_date.year,
                                start_date.month)[1] if self.is_single_level else 1)
        return url, slice(start, end), VARIABLE_DICT[chunk]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self.apply)


@dataclass
class UpdateSlice(beam.PTransform):
    """A Beam PTransform to write zarr arrays from the raw grib files and time offset."""

    target: str

    def apply(self, url: str, region: slice, vars: t.List[str]) -> None:
        """A method to write zarr arrays from the raw grib files and time offset.

        Args:
            url (str): The cloud storage path to the grib file.
            region (slice): start and stop offset for time dimension.
            vars (t.List): List of variables to extract from the file.
        """
        zf = zarr.open(self.target)
        with opener(url) as file:
            logger.info(f"Opened {url}")
            ds = xr.open_dataset(file, engine='cfgrib')
            for vname in vars:
                logger.info(f"Started {vname} from {url}")
                zv = zf[vname]
                zv[region] = ds[vname].values
                logger.info(f"Done {vname} from {url}")
            logger.info(f"Finished for {url}")
            del zv
            del ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(self.apply)
