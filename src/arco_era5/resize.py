import zarr as zr
import apache_beam as beam
import xarray as xr
import numpy as np
from dataclasses import dataclass
import datetime
import logging
from pangeo_forge_recipes.types import Index
from pangeo_forge_recipes.writers import _store_data
from contextlib import contextmanager
import subprocess
import os
import tempfile

logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)

@dataclass
class ResizeZarrTarget(beam.PTransform):

    start: str
    end: str
    file_index: tuple[Index, str]

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(resize_zarr_target, start=self.start, end=self.end, file_index=self.file_index)

def convert_to_date(date_str: str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d')

def copy_to_local(src, dst):
    for cmd in ['gcloud alpha storage cp', 'gsutil cp']:
        try:
            subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True, text=True, input="n/n")
            logger.debug("_copy_btw_filesystems done")
            return
        except subprocess.CalledProcessError as e:
            logger.error(e)

    msg = f'Failed to copy file {src!r} to {dst!r}'
    err_msgs = ', '.join(map(lambda err: repr(err.stderr.decode('utf-8'))))
    logger.error(f'{msg} due to {err_msgs}.')

@contextmanager
def opener(fname: str):
    _, suffix = os.path.splitext(fname)
    ntf = tempfile.NamedTemporaryFile(suffix=suffix)
    tmp_name = ntf.name
    logger.info(f"Copying '{fname}' to local file '{tmp_name}'")
    copy_to_local(fname, tmp_name)
    yield tmp_name
    ntf.close()

def resize_zarr_target(target_store: zr.storage.FSStore, start: str, end: str, file_index: tuple[Index, str]):

    index, url = file_index

    zf = zr.open(target_store)
    ds = xr.open_zarr(target_store)

    with opener(url) as init_file:
        init_ds = xr.open_dataset(init_file, engine='cfgrib')
        for vname, da in init_ds.coords.items():
            if 'time' not in da.dims:
                _store_data(vname, da.variable, index, zf)

    day_diff = convert_to_date(end) - convert_to_date(start)

    ds_dims_map = ds.dims.mapping
    del ds_dims_map['time']

    total = (day_diff.days + 1) * 24

    for dim in ['time', 'valid_time']:
        arr = zf[dim]

        attrs = dict(arr.attrs)

        time_range = np.array(range(0, (day_diff.days + 1) * 24))

        shape_arr = [ zf[c].size for c in ds[dim].dims ]
        shape_arr[0] = total
        if dim == 'time':
            time_range = time_range.reshape(tuple(shape_arr))
        else:
            time_range = np.zeros(tuple(shape_arr))
        
        new = zf.array(dim, time_range,
            chunks = total if dim == 'time' else shape_arr,
            dtype = arr.dtype,
            compressor = arr.compressor,
            fill_value = arr.fill_value,
            order = arr.order,
            filters = arr.filters,
            overwrite = True,
        )
        attrs.update({ 'units': f"hours since {start}" })
        new.attrs.update(attrs)

    logger.info("Consolidated Time")
    for var in ds.data_vars:
        zf[var].resize(total, *ds_dims_map.values())
    logger.info("Resized Data Vars")

    zr.consolidate_metadata(zf.store)

    logger.info("Done")