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
import itertools
import json
import logging
import zarr

import numpy as np
import typing as t

from gcsfs import GCSFileSystem
from .utils import convert_to_date

logger = logging.getLogger(__name__)

TIME_DIMS = ["time", "valid_time"]


def consolidate_metadata(url: str, existing: str, total: str,
                         metadata_key: str = '.zmetadata',
                         overwrite_time_chunk: bool = True) -> None:
    """
    Consolidates metadata in a Zarr dataset by updating the 'total' value in the metadata
    and optionally preserving the 'time' chunk information.

    Args:
        url (str): The URL of the Zarr dataset.
        existing (str): The existing value of 'time' to be replaced.
        total (str): The new value of 'time' to be replaced with the existing one.
        metadata_key (str): The key for the metadata file (default is '.zmetadata').
        overwrite_time_chunk (bool): If True, the 'time' chunk information will be overwritten,
            otherwise, it will be preserved.

    Returns:
        None
    """
    metadata_path = f"{url}/{metadata_key}"
    fs = GCSFileSystem()
    with fs.open(metadata_path) as f:
        meta_str = f.read().decode()
        meta = json.loads(meta_str)
        existing_time_chunk = meta['metadata']['time/.zarray']['chunks']
        new_meta_str = meta_str.replace(str(existing), str(total))
        if not overwrite_time_chunk:
            new_meta = json.loads(new_meta_str)
            new_meta['metadata']['time/.zarray']['chunks'] = existing_time_chunk
            new_meta_str = json.dumps(new_meta)
        fs.write_text(metadata_path, new_meta_str)


def gather_coordinate_dimensions(group: zarr.Group) -> t.List[str]:
    """
    Gather and return a list of unique coordinate dimensions found in a Zarr-store.

    Args:
        group (zarr.Group): The Zarr store to inspect.

    Returns:
        List[str]: A list of unique coordinate dimensions found in the store.
    """
    return set(
        itertools.chain(*(group[var].attrs.get("_ARRAY_DIMENSIONS", []) for var in group)))


def resize_zarr_target(target_store: str, end_date: datetime, init_date: str,
                       interval: int = 24) -> None:
    """Resizes a Zarr target and consolidates metadata.

    Args:
        target_store (str): The Zarr target store path.
        end_date (str): The end date in the format 'YYYY-MM-DD'.
        init_date (str): The initial date of the zarr store in the format of str.
        interval (int, optional): The interval to use for resizing. Default is 24.

    Returns:
        None
    """
    zf = zarr.open(target_store)
    data_vars = list(set(zf.keys()) - gather_coordinate_dimensions(zf))

    day_diff = end_date - convert_to_date(init_date)
    interval = 2 if 'single-level-forecast' in target_store else 24
    total = (day_diff.days + 1) * interval
    zf_time = zf["time"]
    existing = zf_time.size

    if existing != total:
        if '/ar/' in target_store:
            zf_time.append(list(range(existing, total)))
        else:
            for dim in TIME_DIMS:
                arr = zf[dim]
                attrs = dict(arr.attrs)
                time_range = np.array(range(0, (day_diff.days + 1) * 24, 24//interval))
                shape_arr = [zf[c].size for c in zf[dim].attrs["_ARRAY_DIMENSIONS"]]
                if dim == 'valid_time' and interval == 2:
                    data = []
                    for time in time_range:
                        d = list(range(time, time + 19))
                        data.append(d)
                    shape_arr[0] = 1
                    time_range = data

                new = zf.array(
                    dim,
                    time_range,
                    chunks=shape_arr if dim == 'valid_time' and interval == 2 else total,
                    dtype=arr.dtype,
                    compressor=arr.compressor,
                    fill_value=arr.fill_value,
                    order=arr.order,
                    filters=arr.filters,
                    overwrite=True,
                )
                init_date_obj = init_date
                if interval == 2:
                    init_date_obj = datetime.datetime.strptime(init_date_obj, '%Y-%m-%d') + datetime.timedelta(hours=6)

                attrs.update({'units': f"hours since {init_date_obj}"})
                new.attrs.update(attrs)

        logger.info(f"Time Consolidated for {target_store}.")
        for vname in data_vars:
            var = zf[vname]
            if "time" in var.attrs["_ARRAY_DIMENSIONS"]:
                shape = list(var.shape)
                shape[0] = total
                zf[vname].resize(*shape)
        logger.info(f"All data vars of {target_store} are resized.")

        if '/ar/' in target_store:
            consolidate_metadata(target_store, existing, total, overwrite_time_chunk=False)
        else:
            consolidate_metadata(target_store, existing, total)
        logger.info(f"Consolidation of {target_store} is completed.")
    else:
        logger.info(f"Data is already resized for {target_store}.")


def update_zarr_metadata(url: str, time_end: datetime.date, metadata_key: str = '.zmetadata') -> None:
    try:
        attrs = {"valid_time_start": "1940-01-01",
                 "valid_time_end": str(time_end),
                 "last_updated": str(datetime.datetime.now())
                 }
        root_group = zarr.open(url)

        # update zarr_store/.zattrs file.
        for k,v in attrs.items():
            root_group.attrs[k] = v

        # update zarr_store/.zmetadata file.
        metadata_path = f"{url}/{metadata_key}"
        fs = GCSFileSystem()
        with fs.open(metadata_path) as f:
            meta_str = f.read().decode()
            meta = json.loads(meta_str)

            existing_attrs = meta['metadata'].get('.zattrs')
            existing_attrs.update(attrs)
            meta['metadata']['.zattrs'] = existing_attrs
            new_meta_str = json.dumps(meta)

            fs.write_text(metadata_path, new_meta_str)
            logging.info(f"Metadata successfully updated for {url}.")
    except Exception as e:
        logging.error(f"Failed to update metadata for {url}: {e}")