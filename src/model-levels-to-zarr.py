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
r"""Convert Model level Era 5 Data to an unprocessed Zarr dataset.

Examples:
    Check if there's any missing data:
    ```
    python src/model-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/model-level-all.zarr gs://$BUCKET/ml-cache/ \
     --start 1979-01-01 \
     --end 2021-08-31 \
     --chunks o3q qrqs dve tw \
     --target-chunk '{"time": 1, "hybrid": 1}' \
     --setup_file ./setup.py \
     --find-missing
    ```

    Produce the output file locally:
    ```
    python src/model-levels-to-zarr.py ~/model-level-all.zarr ~/ml-cache/ \
    --start 1979-01-01 \
    --end 2021-08-31 \
    --chunks o3q qrqs dve tw \
    --target-chunk '{"time": 1, "hybrid": 1}' \
    --local-run \
    --setup_file ./setup.py
    ```

    Perform the conversion for the moisture dataset...
    ```
    python src/model-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr gs://$BUCKET/ml-moist-cache/ \
     --start 1979-01-01 \
     --end 2021-08-31 \
     --chunks o3q qrqs \
     --target-chunk '{"time": 1, "hybrid": 1}' \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --setup_file ./setup.py \
     --disk_size_gb 3600 \
     --machine_type m1-ultramem-40 \
     --no_use_public_ips  \
     --network=$NETWORK \
     --subnetwork=regions/$REGION/subnetworks/$SUBNET \
     --job_name model-level-moisture-to-zarr

    ```
    Perform the conversion for the wind dataset...
    ```
    python src/model-levels-to-zarr.py gs://gcp-public-data-arco-era5/co/model-level-wind.zarr gs://$BUCKET/ml-wind-cache/ \
     --start 1979-01-01 \
     --end 2021-08-31 \
     --chunks dve tw \
     --target-chunk '{"time": 1, "hybrid": 1}' \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --setup_file ./setup.py \
     --disk_size_gb 3600 \
     --machine_type m1-ultramem-40 \
     --no_use_public_ips  \
     --network=$NETWORK \
     --subnetwork=regions/$REGION/subnetworks/$SUBNET \
     --job_name model-level-wind-to-zarr
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
        """
        Create a path to Era5 data file from timestamp and variable chunk.

        Args:
            time (datetime.datetime): The timestamp for the data.
            chunk (str): The variable chunk specification.

        Returns:
            str: The path to the Era5 data file.

        This function constructs a path to an Era5 data file based on the timestamp and variable chunk.

        Example:
            >>> timestamp = datetime.datetime(2023, 9, 11)
            >>> variable_chunk = 'dve'
            >>> path = make_path(timestamp, variable_chunk)
            >>> print(path)
            gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/2023/20230911_hres_dve.grb2
        """

        # Handle chunks that have been manually split into one-variable files.
        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return (
                f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/"
                f"{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
            )

        return (
            f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/"
            f"{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk}.grb2"
        )

    default_chunks = ['dve', 'tw']

    parsed_args, unknown_args = parse_args('Convert Era 5 Model Level data to Zarr', default_chunks)

    date_range = [
        ts.to_pydatetime()
        for ts in pd.date_range(start=parsed_args.start,
                                end=parsed_args.end,
                                freq="D").to_list()
    ]

    run(make_path, date_range, parsed_args, unknown_args)
