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

import apache_beam as beam
import argparse
import logging
import typing as t

from arco_era5 import (
    INIT_TIME,
    hourly_dates,
    LoadDataForDayDoFn,
    UpdateModelLevelNativeVerticalDataSlice
)

logging.getLogger().setLevel(logging.INFO)


def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--output_path", type=str, required=True,
                        help="Path to the destination Zarr archive.")
    parser.add_argument('-s', "--start_date", required=True,
                        help='Start date, iso format string.')
    parser.add_argument('-e', "--end_date", required=True,
                        help='End date, iso format string.')
    parser.add_argument("--init_date", type=str, default=INIT_TIME,
                        help="Date to initialize the zarr store.")

    return parser.parse_known_args()


known_args, pipeline_args = parse_arguments('')
dates = hourly_dates(known_args.start_date, known_args.end_date)

with beam.Pipeline(argv=pipeline_args) as p:
    path = (
        p
        | "CreateDayIterator" >> beam.Create(dates)
        | "LoadDataForDay" >> beam.ParDo(LoadDataForDayDoFn(start_date=known_args.init_date))
        | "UpdateDataSlice" >> UpdateModelLevelNativeVerticalDataSlice(
            target=known_args.output_path, init_date=known_args.init_date)
    )
