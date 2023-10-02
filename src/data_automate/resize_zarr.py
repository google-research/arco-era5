import datetime
import logging
import zarr

import numpy as np
import xarray as xr

from arco_era5 import convert_to_date

logger = logging.getLogger(__name__)


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
    ds = xr.open_zarr(target_store)
    zf = zarr.open(target_store)
    day_diff = end_date - convert_to_date(init_date)
    interval = 2 if 'single-level-forecast' in target_store else 24
    total = (day_diff.days + 1) * interval
    time = zf["time"]
    existing = time.size

    if existing != total:
        if '/ar/' in target_store:
            logger.info(f"Time resize for {target_store} of AR data.")
            time.resize(total)
            time[slice(existing, total)] = list(range(existing, total))
        else:
            logger.info(f"Time resize for {target_store} of CO data.")

            for dim in ["time", "valid_time"]:
                arr = zf[dim]
                attrs = dict(arr.attrs)
                time_range = np.array(range(0, (day_diff.days + 1) * 24,
                                            12 if interval == 2 else 1))
                shape_arr = [zf[c].size for c in ds[dim].dims]
                shape_arr[0] = total
                if dim == 'time':
                    time_range = time_range.reshape(tuple(shape_arr))
                else:
                    time_range = np.zeros(tuple(shape_arr))

                new = zf.array(dim, time_range,
                               chunks=total if dim == 'time' else shape_arr,
                               dtype=arr.dtype,
                               compressor=arr.compressor,
                               fill_value=arr.fill_value,
                               order=arr.order,
                               filters=arr.filters,
                               overwrite=True,)
                if 'single-level-forecast' in target_store:
                    init_date_obj = datetime.datetime.strptime(init_date, '%Y-%m-%d')
                    init_date_obj = init_date_obj + datetime.timedelta(hours=6)
                    init_date_obj = init_date_obj.strftime('%Y-%m-%dT%H:%M:%S.%f')
                    attrs.update({'units': f"hours since {init_date_obj}"})
                else:
                    attrs.update({'units': f"hours since {convert_to_date(init_date)}"})

                new.attrs.update(attrs)

        logger.info(f"Consolidated Time for {target_store}.")
        for vname, var in ds.data_vars.items():
            if "time" in var.dims:
                shape = [ds[i].size for i in var.dims]
                shape[0] = total
                zf[vname].resize(*shape)
        logger.info(f"Resized data vars of {target_store}.")
        zarr.consolidate_metadata(zf.store)
    else:
        logger.info(f"Data is already resized for {target_store}.")
