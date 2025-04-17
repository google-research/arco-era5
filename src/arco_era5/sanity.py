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
import logging
import os
import zarr

import apache_beam as beam
import numpy as np
import typing as t
import xarray as xr

from dataclasses import dataclass
from gcsfs import GCSFileSystem

from .data_availability import generate_input_paths_ar
from .download import SPLITTING_DATASETS
from .ingest_data_in_zarr import CO_FILES_MAPPING, replace_non_alphanumeric_with_hyphen
from .update_co import generate_offsets_from_url, generate_input_paths
from .source_data import HOURS_PER_DAY, offset_along_time_axis, GCP_DIRECTORY, PRESSURE_LEVELS_GROUPS
from .utils import copy, date_range, opener, remove_file, run_cloud_job

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PROJECT = os.environ.get("PROJECT")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SANITY_JOB_ID = os.environ.get("SANITY_JOB_ID")

SANITY_JOB_FILE = "/arco-era5/src/era5-sanity.py"

HARNESS_THREADS = {
    'model-level-moisture': 1,
    'model-level-wind': 4
}

fs = GCSFileSystem()


def generate_raw_paths(start_date: str, end_date: str, target_path: str, is_single_level: bool, is_analysis_ready: bool, root_path: str = GCP_DIRECTORY):
    """Generate raw input paths."""
    if is_analysis_ready:
        data_date_range = date_range(start_date, end_date)
        paths = generate_input_paths_ar(data_date_range, root_path)
    else:
        chunks = CO_FILES_MAPPING[target_path.split('/')[-1].split('.')[0]]
        paths = generate_input_paths(start_date, end_date, root_path, chunks, is_single_level)
    return paths

def parse_ar_url(url: str, init_date: str):
    """Parse raw file url for analysis ready data."""
    year, month, day, variable, file_name = url.rsplit("/", 5)[1:]
    time_offset_start = offset_along_time_axis(init_date, int(year), int(month), int(day))
    time_offset_end = time_offset_start + HOURS_PER_DAY
    if file_name == "surface.nc":
        return (slice(time_offset_start, time_offset_end),), variable
    else:
        level = int(file_name.split(".")[0])
        level_index = list(PRESSURE_LEVELS_GROUPS["full_37"]).index(level)
        return (slice(time_offset_start, time_offset_end), slice(level_index, level_index + 1)), variable

def add_sanity_files(path: str, data_changed: bool):
    dir_path, file_name = path.rsplit("/", 1)

    success_file = f"{dir_path}/{file_name.rsplit('.', 1)[0]}_ratified"
    fs = GCSFileSystem()
    fs.write_text(success_file, '')
    if data_changed:
        data_change_file = f"{dir_path}/{file_name.rsplit('.', 1)[0]}_data_changed"
        fs.write_text(data_change_file, '')

def replace_and_remove_file(path1: str, path2: str, data_changed: bool):
    """Replace root with latest era5 file and remove temp file"""
    logger.info(f"Replacing {path1} with {path2}.")
    copy(path2, path1)
    logger.info(f"Creating sentinel files.")
    add_sanity_files(path1, data_changed)
    logger.info(f"Removing temporary file {path2}.")
    remove_file(path2)

def combine_expver(ds: xr.Dataset):
    """Combine the dataset along expver dim."""
    if "expver" in ds.dims:
        ds = ds.sel(expver=1).combine_first(ds.sel(expver=5))
    return ds

def open_dataset(path: str):
    """Open xarray dataset."""
    ds = xr.open_dataset(path, engine="scipy" if ".nc" in path else "cfgrib").load()
    return ds

class OpenLocal(beam.DoFn):
    """class to open raw files and compare the data."""

    def process(self, paths: t.Tuple[str, str]):

        path1, path2 = paths

        temp_file_check = fs.exists(path2)

        if temp_file_check:
            with opener(path1) as file1:
                ds1 = combine_expver(open_dataset(file1))

            with opener(path2) as file2:
                ds2 = combine_expver(open_dataset(file2))

            check_condition = ds1.equals(ds2)
            if check_condition:
                beam.metrics.Metrics.counter('Success', 'Equal').inc()
                logger.info(f"For {path1} variables are equal.")
                replace_and_remove_file(path1, path2, False)
            else:
                beam.metrics.Metrics.counter('Success', 'Different').inc()
                logger.info(f"For {path1} variables are not equal.")
                yield path1, path2

@dataclass
class UpdateZarr(beam.DoFn):

    target_path: str
    init_date: str
    timestamps_per_file: int
    is_single_level: bool
    is_analysis_ready: bool

    def process(self, paths: t.Tuple[str, str]):
        path1, path2 = paths
        """Function to update zarr data if difference found."""
        zf = zarr.open(self.target_path)
        with opener(path2) as file:
            ds = open_dataset(file)
            variables = list(ds.data_vars)
            if self.is_analysis_ready:
                ds = combine_expver(ds)
                region, variable = parse_ar_url(path1, self.init_date)
                variables = [variable]
            else:
                start, end, _ = generate_offsets_from_url(path1, self.init_date, self.timestamps_per_file, self.is_single_level)
                region = (slice(start, end))
            var_iter = iter(ds.values())
            for vname in variables:
                zv = zf[vname]
                da = next(var_iter).values
                if len(region) == 2:
                    da = np.expand_dims(da, axis=1)
                zv[region] = da
        replace_and_remove_file(path1, path2, True)


def update_splittable_files(date: str, temp_path: str, target_path: str):
    """To replace and delete splittable files from temp path."""
    if "single-level-reanalysis" in target_path:
        dataset = SPLITTING_DATASETS[0]
    elif "single-level-forecast" in target_path:
        dataset = SPLITTING_DATASETS[1]
    else:
        return
    year = date[:4]
    month = year + date[5:7]
    root_file = f"{GCP_DIRECTORY}/ERA5GRIB/HRES/Month/{year}/{month}_hres_{dataset}.grb2"
    temp_file = f"{temp_path}/ERA5GRIB/HRES/Month/{year}/{month}_hres_{dataset}.grb2"
    copy(temp_file, root_file)
    remove_file(temp_file)


def generate_override_args(
        file_path: str,
        target_path: str,
        temp_path: str,
        init_date: str,
        bucket: str,
        project: str,
        region: str,
        job_name: str
) -> list:
    """Generate override args for cloud run job."""
    args = [
        file_path,
        "--target_path", target_path,
        "--temp_path", temp_path,
        "--init_date", init_date,
        "--temp_location", f"gs://{bucket}/temp",
        "--runner", "DataflowRunner",
        "--project", project,
        "--region", region,
        "--experiments", "use_runner_v2",
        "--machine_type", "n2-highmem-16",
        "--disk_size_gb", "250",
        "--setup_file", "/arco-era5/setup.py",
        "--job_name", job_name,
    ]
    return args

def run_sanity_job(target_path: str, temp_path: str, init_date: str):
    """Trigger job for era5 data sanity."""

    target_name = target_path.split('/')[-1].split('.')[0]
    job_name = f"arco-era5-3m-sanity-{replace_non_alphanumeric_with_hyphen(target_name)}"

    override_args = generate_override_args(
        SANITY_JOB_FILE, target_path, temp_path, init_date, BUCKET, PROJECT, REGION, job_name
    )

    if target_name in HARNESS_THREADS:
        override_args.extend(['--number_of_worker_harness_threads', str(HARNESS_THREADS[target_name])])

    if "single-level-forecast" in target_path:
        override_args.extend(['--timestamps_per_day', "2"])
    
    run_cloud_job(PROJECT, REGION, SANITY_JOB_ID, override_args)
