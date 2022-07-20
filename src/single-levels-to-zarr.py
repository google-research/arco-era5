# Copyright 2022 Google LLC
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
r"""Convert Surface level Era 5 Data to an unprocessed Zarr dataset.

Examples:
    Check if there's any missing data:
    ```
    python src/single-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr gs://$BUCKET/an-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --setup_file ./setup.py \
     --find-missing
    ```

    Perform the conversion for the reanalysis dataset...
    ```
    python src/single-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr gs://$BUCKET/an-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location "gs://$BUCKET/tmp/" \
     --setup_file ./setup.py \
     --disk_size_gb 50 \
     --machine_type n2-highmem-2 \
     --no_use_public_ips  \
     --network=$NETWORK \
     --subnetwork=regions/$REGION/subnetworks/$SUBNET \
     --job_name reanalysis-to-zarr
    ```

    Perform the conversion for the forecast dataset...
    ```
    python src/single-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr gs://$BUCKET/fc-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --chunks rad pcp_surface_cp pcp_surface_crr pcp_surface_csf pcp_surface_csfr pcp_surface_es pcp_surface_lsf \
        pcp_surface_lsp pcp_surface_lspf pcp_surface_lsrr pcp_surface_lssfr pcp_surface_ptype pcp_surface_rsn \
        pcp_surface_sd pcp_surface_sf pcp_surface_smlt pcp_surface_tp \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location "gs://$BUCKET/tmp/" \
     --setup_file ./setup.py \
     --disk_size_gb 50 \
     --machine_type n2-highmem-2 \
     --no_use_public_ips  \
     --network=$NETWORK \
     --subnetwork=regions/$REGION/subnetworks/$SUBNET \
     --job_name forecasts-to-zarr
    ```

    Perform the conversion for the surface dataset...
    ```
    python src/single-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/single-level-surface.zarr gs://$BUCKET/sfc-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --chunks lnsp zs \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location "gs://$BUCKET/tmp/" \
     --setup_file ./setup.py \
     --disk_size_gb 50 \
     --machine_type n2-highmem-2 \
     --no_use_public_ips  \
     --network=$NETWORK \
     --subnetwork=regions/$REGION/subnetworks/$SUBNET \
     --job_name surface-to-zarr
    ```
"""
import datetime
import logging

import pandas as pd

from arco_era5 import run, parse_args

if __name__ == "__main__":
    logging.getLogger('pangeo_forge_recipes').setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.INFO)

    def make_path(time: datetime.datetime, chunk: str) -> str:
        """Make path to Era5 data from timestamp and variable."""

        # Handle chunks that have been manually split into one-variable files.
        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return (
                f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/"
                f"{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
            )

        return (
            f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/"
            f"{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk}.grb2"
        )


    default_chunks = [
        'cape', 'cisst', 'sfc', 'tcol',
        # the 'soil' chunk split by variable
        'soil_depthBelowLandLayer_istl1',
        'soil_depthBelowLandLayer_istl2',
        'soil_depthBelowLandLayer_istl3',
        'soil_depthBelowLandLayer_istl4',
        'soil_depthBelowLandLayer_stl1',
        'soil_depthBelowLandLayer_stl2',
        'soil_depthBelowLandLayer_stl3',
        'soil_depthBelowLandLayer_stl4',
        'soil_depthBelowLandLayer_swvl1',
        'soil_depthBelowLandLayer_swvl2',
        'soil_depthBelowLandLayer_swvl3',
        'soil_depthBelowLandLayer_swvl4',
        'soil_surface_tsn',
    ]

    parsed_args, unknown_args = parse_args('Convert Era 5 Single Level data to Zarr', default_chunks)

    date_range = [
        ts.to_pydatetime()
        for ts in pd.date_range(start=parsed_args.start,
                                end=parsed_args.end,
                                freq="MS").to_list()
    ]

    run(make_path, date_range, parsed_args, unknown_args)
