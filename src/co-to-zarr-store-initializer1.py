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
# pylint: disable=line-too-long
"""
    Generate zarr store from init_date without data. Default init_date will be 1900-01-01.
    ```
    python src/co-to-zarr-store-initializer.py \
      --output_path="gs://gcp-public-data-arco-era5/regrid-co/model-level-1h-0p25deg-1959-2023.zarr-v1" \
      --start_date '1959-01-01' \
      --end_date '2021-12-31' \
      --init_date '1800-01-01' \
      --from_init_date \
    ```
"""
import argparse
import datetime
import logging

import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da

from arco_era5 import _read_nc_dataset

def parse_arguments(desc: str):
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

def get_var_attrs_dict():
    root_path = "/usr/local/google/home/dabhis/github_repo/sahil_personal_files/data" # remove for the GCP.

    # The variable attributes should be independent of the date chosen here
    # so we just choose any date.
    time = datetime.datetime(2021, 1, 1)
    file_path_for_calculated_variable = ("date-variable-pressure_level/{time.year}/{time.month:02d}/"
                                         "{time.day:02d}/{variable}/1.nc")
    var_attrs_dict = {}
    dataset_paths = ['gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr/', 
                     'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr/']
    for dataset_path in dataset_paths:
        dataset = xr.open_zarr(dataset_path, chunks=None)
        for var in dataset.data_vars:
            var_attrs_dict[grib_variables[var]] = dataset[var].attrs

    for variable_name in nc_variables:
        path = file_path_for_calculated_variable.format(time = time, variable = variable_name)
        data_array = _read_nc_dataset(f"{root_path}/{path}")
        var_attrs_dict[variable_name] = data_array.attrs

    return var_attrs_dict

def make_template(start_date: str, end_date: str, zarr_chunks: tuple):
    
    coords = dict()
    coords["time"] = pd.date_range( pd.Timestamp(start_date), pd.Timestamp(end_date),
                                   freq=pd.DateOffset(hours=1), inclusive="left").values
    
    longitude_value = np.arange(0., 359.75 + 0.25, 0.25) # add dtype here and remove next line & do this for the next 3 var.
    longitude = np.array(longitude_value, dtype=np.float32)

    latitude_value = np.arange(90.0, -90.0 - 0.25, -0.25)
    latitude = np.array(latitude_value, dtype=np.float32)

    hybrid_value = np.arange(1, 137 + 1, 1.0)
    hybrid = np.array(hybrid_value, dtype=np.float32)

    coords['latitude'] = latitude
    coords['longitude'] = longitude
    coords['hybrid'] = hybrid

    time_size = len(coords["time"])
    lat_size = len(coords['latitude'])
    lon_size = len(coords['longitude'])
    hybrid_size = len(coords['hybrid'])

    sample_data = da.full((time_size, hybrid_size, lat_size, lon_size), np.nan, dtype = np.float32, chunks=zarr_chunks)

    template_dataset = {}
    var_attrs_dict = get_var_attrs_dict()
    for variable_name in list(grib_variables.values()) + nc_variables:
        template_dataset[variable_name] = xr.Variable(
            dims=("time", "hybrid", "latitude", "longitude"),
            data=sample_data,
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
    

    print("template made successfully")
    _ = template.to_zarr(output_path, compute=False, mode="w")
    print(f"{output_path} zarr store created successfully.")

if __name__ == "__main__":
    main()
