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
"""
    append the data into the zarr store. Default init_date will be 1900-01-01.
    ```
    python src/update-co-to-zarr-data.py \
      --output_path="gs://gcp-public-data-arco-era5/regrided-co/model-level-1h-0p25deg-1959-2023.zarr-v1" \
      --start_date '1981-03-16' \
      --end_date '1981-03-17' \
      --init_date '1981-03-16'
    ```
"""

import apache_beam as beam
import datetime
import logging
import metview as mv
import numpy as np
import pandas as pd
import xarray as xr
import xarray_beam as xb
import zarr

from dataclasses import dataclass

logger = logging.getLogger(__name__)

TIME_RESOLUTION_HOURS = 1
HOURS_PER_DAY = 24

class LoadDataForDateDoFn(beam.DoFn):
    """A Beam DoFn for loading data for a specific date.

    Args:
        start_date (str): The start date in ISO format (YYYY-MM-DD).
    Methods:
        process(args): Load data for a specific date and yields it with an xarray_beam key.
    """
    def __init__(self, start_date):
        """Initialize the LoadDataForDateDoFn.
        Args:
            start_date (str): The start date in ISO format (YYYY-MM-DD).
        """
        self.start_date = start_date

    def attribute_fix(self, ds):
        """Needed to fix a low-level bug in ecCodes.
        
        Sometimes, shortNames get overloaded in ecCodes's table. 
        To eliminate ambiguity in their string matching, we
        force ecCodes to make use of the paramId, which is a
        consistent source-of-truth.
        """
        for var in ds:
            attrs = ds[var].attrs
            _ = attrs.pop('GRIB_cfName', None)
            _ = attrs.pop('GRIB_cfVarName', None)
            _ = attrs.pop('GRIB_shortName', None)
            ds[var].attrs.update(attrs)
        return ds

    def process_hourly_data(self, data_list : list):
        try:
            wind_fieldset, moisture_fieldset, surface_fieldset = data_list

            wind_gg_data = mv.read(data=wind_fieldset,grid='N320')
            surface_gg_data = mv.read(data=surface_fieldset,grid='N320')
            uv_wind_spectral = mv.uvwind(data=wind_fieldset,truncation=639)
            uv_wind_gg_data = mv.read(data=uv_wind_spectral,grid='N320')
            uv_wind_ll_data = mv.read(data=uv_wind_gg_data,grid=[0.25, 0.25])

            t_gg = wind_gg_data.select(shortName='t')
            q_gg = moisture_fieldset.select(shortName='q')
            lnsp_gg = surface_gg_data.select(shortName="lnsp")
            zs_gg = surface_gg_data.select(shortName="z")

            zm_gg = mv.mvl_geopotential_on_ml(t_gg, q_gg, lnsp_gg, zs_gg)
            del t_gg
            del q_gg
            del lnsp_gg
            del zs_gg

            gg_fieldset = mv.merge(wind_gg_data, zm_gg, moisture_fieldset)

            del wind_gg_data
            del zm_gg
            del moisture_fieldset

            ll_fieldset = mv.read(data=gg_fieldset, grid=[0.25, 0.25])
            ll_fieldset = mv.merge(ll_fieldset, uv_wind_ll_data)

            dataset = ll_fieldset.to_dataset()
            return dataset
        except BaseException as e:
            # Make sure we print the date as part of the error for easier debugging
            # if something goes wrong. Note "from e" will also raise the details of the
            # original exception.
            raise RuntimeError(f"Error while loading dataset") from e

    def process(self, args):
        """Load data for a day, with an xarray_beam key for it.
        Args:
            args (tuple): A tuple containing the year, month, and day.
        Yields:
            tuple: A tuple containing an xarray_beam key and the loaded dataset.
        """
        year, month, day, hour = args
        logger.info(f"args is this : {args}")
        current_timestamp=f"{year}-{month}-{day}T{hour:02d}"
        logger.info(f"started operation for the date of {current_timestamp}")
        
        ml_wind = xr.open_zarr('gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2/', chunks=None)
        ml_moisture = xr.open_zarr('gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2/', chunks=None)
        sl_surface = xr.open_zarr('gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2/', chunks=None)
        print("data accessed of v2 version of zarr file.")
        wind_slice = ml_wind.sel(time=current_timestamp).compute()
        moisture_slice = ml_moisture.sel(time=current_timestamp).compute()
        surface_slice = sl_surface.sel(time=current_timestamp).compute()
        
        wind_fieldset = mv.dataset_to_fieldset(self.attribute_fix(wind_slice).squeeze())
        moisture_fieldset = mv.dataset_to_fieldset(self.attribute_fix(moisture_slice).squeeze())
        surface_fieldset = mv.dataset_to_fieldset(self.attribute_fix(surface_slice).squeeze())

        dataset = self.process_hourly_data([wind_fieldset, moisture_fieldset, surface_fieldset])
        variables_full_names = {
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
            'w': 'vertical_velocity',
            'u': 'u_component_of_wind',
            'v': 'v_component_of_wind',
            'z': 'geopotential'
            }
        dataset = dataset.rename(variables_full_names)
        dataset = align_coordinates(dataset)
        offsets = {"time": offset_along_time_axis(self.start_date, year, month, day, hour)}
        key = xb.Key(offsets, vars=set(dataset.data_vars.keys()))
        logger.info("Finished loading data for %s-%s-%s-%s", year, month, day, hour)
        yield key, dataset
        dataset.close()

# remove reset_coords from this function if more coords are needed.
def align_coordinates(dataset: xr.Dataset) -> xr.Dataset:
    """Align coordinates of variables in the dataset before consolidation.

    Args:
        dataset (xr.Dataset): The dataset containing variables.

    Returns:
        xr.Dataset: The dataset with aligned coordinates.

    This function removes non-index coordinates and downcasts latitude and longitude
    coordinates to float32.
    """

    # It's possible to have coordinate metadata for coordinates which aren't
    # actually used as dimensions of any variables (called 'non-index'
    # coordinates), and some of the source NetCDF files use these, e.g. a scalar
    # 'height' coordinate (= 2.0) in the NetCDF files for the 2-meter temperature
    # variable tas. We remove these, for simplicity and since once the variables
    # are combined in a single zarr dataset it won't be clear what these
    # additional coordinates are referring to.
    dataset = dataset.reset_coords(drop=True)

    # Downcast lat and lon coordinates to float32. This is because there are
    # small rounding-error (~1e-14 relative error) discrepancies between the
    # float64 latitude coordinates across different source NetCDF files, and
    # xarray_beam complains about this. After downcasting to float32 the
    # differences go away, and the extra float64 precision isn't important to us.
    # (Ideally you'd be able to specify a tolerance for the comparison, but this
    # works for now.)
    dataset = dataset.assign_coords(
        latitude=dataset["latitude"].astype(np.float32),
        longitude=dataset["longitude"].astype(np.float32))

    return dataset

def offset_along_time_axis(start_date: str, year: int, month: int, day: int, hour: int) -> int:
    """Offset in indices along the time axis, relative to start of the dataset."""
    # Note the length of years can vary due to leap years, so the chunk lengths
    # will not always be the same, and we need to do a proper date calculation
    # not just multiply by 365*24.
    time_delta = pd.Timestamp(
        year=year, month=month, day=day, hour=hour) - pd.Timestamp(start_date)
    return int(time_delta.total_seconds() //60 //60)

@dataclass
class UpdateSlice(beam.PTransform):
    """A Beam PTransform to write zarr arrays from the xarray datasets and time offset."""

    target: str
    init_date: str

    def apply(self, key: xb.Key, ds: xr.Dataset) -> None:
        """A method to write zarr arrays from the xarray datasets and time offset.

        Args:
            key (xb.Key): offset dict for dimensions.
            ds (xr.Dataset): Merged dataset for a single day.
        """
        logger.info("inside the updateslice function of the AR data.")
        offset = key.offsets['time']
        date = (datetime.datetime.strptime(self.init_date, '%Y-%m-%d') + 
                datetime.timedelta(hours=offset))
        zf = zarr.open(self.target)
        region = slice(offset, offset + 1)
        start_date = date.strftime('%Y-%m-%dT%H')
        end_date = datetime.datetime.strptime(start_date, '%Y-%m-%dT%H') + datetime.timedelta(hours=1)
        end_date = end_date.strftime('%Y-%m-%dT%H')
        for vname in ds.data_vars:
            logger.info(f"Started {vname} for {start_date}")
            zv = zf[vname]
            ds[vname] = ds[vname].expand_dims(dim={'time': 1})
            zv[region] = ds[vname].values
            logger.info(f"Done {vname} for {start_date}")
        logger.info(f"data appended successfully for {start_date} to {end_date} timestamp")
        del zv
        del ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(self.apply)
    

def hourly_dates(start_date: str, end_date: str):
    """Iterate through all (year, month, day, hour) tuples between start_date and
    end_date (inclusive).

    Args:
        start_date (str): The start date in ISO format (YYYY-MM-DD).
        end_date (str): The end date in ISO format (YYYY-MM-DD).

    Yields:
        tuple: A tuple containing the year, month, day, and hour for each hour in the range.

    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='H', inclusive='left')
    date_tuples = [(date.year, date.month, date.day, date.hour) for date in date_range]
    return date_tuples