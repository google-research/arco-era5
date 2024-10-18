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
import logging
import zarr

import apache_beam as beam
import xarray as xr
import xarray_beam as xb

from dataclasses import dataclass

from .source_data import HOURS_PER_DAY

logger = logging.getLogger(__name__)


@dataclass
class UpdateSlice(beam.PTransform):
    """A Beam PTransform to write zarr arrays from the raw grib files and time offset."""

    target: str
    init_date: str

    def apply(self, key: xb.Key, ds: xr.Dataset) -> None:
        """A method to write zarr arrays from the raw grib files and time offset.

        Args:
            key (xb.Key): offset dict for dimensions.
            ds (xr.Dataset): Merged dataset for a single day.
        """
        offset = key.offsets['time']
        date = (datetime.datetime.strptime(self.init_date, '%Y-%m-%d') +
                datetime.timedelta(days=offset / HOURS_PER_DAY))
        zf = zarr.open(self.target)
        region = slice(offset, offset + HOURS_PER_DAY)
        for vname in ds.data_vars:
            logger.info(f"Started {vname} for {date.strftime('%Y-%m-%d')}")
            zv = zf[vname]
            zv[region] = ds[vname].values
            logger.info(f"Done {vname} for {date.strftime('%Y-%m-%d')}")
        del zv
        del ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.MapTuple(self.apply)
