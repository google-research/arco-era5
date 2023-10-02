import apache_beam as beam
import datetime
import logging
import xarray as xr
import zarr

from arco_era5 import HOURS_PER_DAY
from dataclasses import dataclass
import xarray_beam as xb

logger = logging.getLogger(__name__)

@dataclass
class UpdateSlice(beam.PTransform):

    target: str
    init_date: str

    def apply(self, key: xb.Key, ds: xr.Dataset):
        """Generate region slice and update zarr array directly"""
        offset = key.offsets['time']
        date = datetime.datetime.strptime(self.init_date, '%Y-%m-%d') + datetime.timedelta(days=offset / HOURS_PER_DAY)
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
