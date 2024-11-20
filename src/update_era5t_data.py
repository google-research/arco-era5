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
import apache_beam as beam
import argparse
import logging

import typing as t

from arco_era5 import (
    daily_date_iterator
)

logging.getLogger().setLevel(logging.INFO)


def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:
    parser = argparse.ArgumentParser(description=desc)
    return parser.parse_known_args()


known_args, pipeline_args = parse_arguments("Update era5 data.")

ZARR_FILES_LIST = [
    'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
    'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2',
    'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2'
]

with beam.Pipeline(argv=pipeline_args) as p:
    path = (
        p
        | "CreateDayIterator" >> beam.Create(ZARR_FILES_LIST)
        | beam.Map(print)
    )
