# Copyright 2024 Google LLC
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
# pylint: disable=line-too-long
"""
    Generate zarr store from init_date without data. Default init_date will be 1900-01-01.
    ```
    python src/model-level-native-vertical-zarr-initializer.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/model-level-1h-0p25deg.zarr-v1" \
      --start_date '1900-01-01' \
      --end_date '2024-03-31' \
      --init_date '1900-01-01' \
      --from_init_date \
    ```
"""
import argparse
import datetime
import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da

from arco_era5 import _read_nc_dataset, zarr_files, GCP_DIRECTORY

grib_variables = {
    'cc': 'fraction_of_cloud_cover',
    'ciwc': 'specific_cloud_ice_water_content',
    'clwc': 'specific_cloud_liquid_water_content',
    'crwc': 'specific_rain_water_content',
    'cswc': 'specific_snow_water_content',
    'd': 'divergence',
    'o3': 'ozone_mass_mixing_ratio',
    'q': 'specific_humidity',
    't': 'temperature',
    'vo': 'vorticity',
    'w': 'vertical_velocity'
    }
nc_variables = [ 'u_component_of_wind','v_component_of_wind', 'geopotential']
file_path_for_calculated_variable = ("date-variable-pressure_level/{time.year}/{time.month:02d}/"
                                     "{time.day:02d}/{variable}/1.nc")
dataset_paths = [ zarr_files['ml_moisture'], zarr_files['ml_wind'] ]

def parse_arguments(desc: str) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--output_path", type=str, required=True,
                        help="Path to the destination Zarr archive.")
    parser.add_argument('-s', "--start_date", default='2020-01-01',
                        help='Start date, iso format string.')
    parser.add_argument('-e', "--end_date", default='2020-01-02',
                        help='End date, iso format string.')
    parser.add_argument("--init_date", type=str, default='1900-01-01',
                        help="Date to initialize the zarr store.")
    parser.add_argument("--from_init_date", action='store_true', default=False,
                        help="To initialize the store from some previous date (--init_date). i.e. 1900-01-01")

    return parser.parse_known_args()


def get_var_attrs_dict() -> Dict[str, Dict]:
    """Get variable attributes dictionary."""

    # The variable attributes should be independent of the date chosen here
    # so we just choose any date.
    time = datetime.datetime(2021, 1, 1)
    var_attrs_dict = {}

    for dataset_path in dataset_paths:
        dataset = xr.open_zarr(dataset_path, chunks=None)
        for var in dataset.data_vars:
            var_attrs_dict[grib_variables[var]] = dataset[var].attrs

    for variable_name in nc_variables:
        path = file_path_for_calculated_variable.format(time = time, variable = variable_name)
        data_array = _read_nc_dataset(f"{GCP_DIRECTORY}/{path}")
        var_attrs_dict[variable_name] = data_array.attrs

    return var_attrs_dict

def make_template(start_date: str, end_date: str, zarr_chunks: Tuple[int, int, int, int]) -> xr.Dataset:
    """Create a template dataset for the Zarr store."""
    longitude = np.arange(0., 359.75 + 0.25, 0.25, dtype=np.float32)
    latitude = np.arange(90.0, -90.0 - 0.25, -0.25, dtype=np.float32)
    hybrid = np.arange(1, 137 + 1, 1.0, dtype=np.float32)
    time = pd.date_range( pd.Timestamp(start_date), pd.Timestamp(end_date),
                                   freq=pd.DateOffset(hours=1), inclusive="left").values
    
    coords = dict()
    coords["time"] = time
    coords['latitude'] = latitude
    coords['longitude'] = longitude
    coords['hybrid'] = hybrid

    data = da.full((time.size, hybrid.size, latitude.size, longitude.size), np.nan, dtype = np.float32, chunks=zarr_chunks)

    template_dataset = {}
    var_attrs_dict = get_var_attrs_dict()
    for variable_name in list(grib_variables.values()) + nc_variables:
        template_dataset[variable_name] = xr.Variable(
            dims=("time", "hybrid", "latitude", "longitude"),
            data=data,
            attrs=var_attrs_dict[variable_name]
            )
    
    template = xr.Dataset(template_dataset, coords=coords)
    return template

def main():
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments('')

    output_path=known_args.output_path
    start_date=known_args.start_date
    end_date=known_args.end_date
    init_date=known_args.init_date
    from_init_date=known_args.from_init_date

    zarr_chunks=(1, 18, 721, 1440)
    template = make_template(init_date if from_init_date else start_date, end_date, zarr_chunks)
    
    print("Template created successfully.")
    _ = template.to_zarr(output_path, compute=False, mode="w")
    print(f"{output_path} Zarr store created successfully.")

if __name__ == "__main__":
    main()
