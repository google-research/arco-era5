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
    daily_date_iterator,
    LoadTemporalDataForDateDoFn,
    GCP_DIRECTORY,
    ARUpdateSlice,
)

logging.getLogger().setLevel(logging.INFO)


def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--output_path", type=str, required=True,
                        help="Path to the destination Zarr archive.")
    parser.add_argument('--root_path', type=str, default=GCP_DIRECTORY,
                        help='Root path to raw files.')
    parser.add_argument('-s', "--start_date", required=True,
                        help='Start date, iso format string.')
    parser.add_argument('-e', "--end_date", required=True,
                        help='End date, iso format string.')
    parser.add_argument("--pressure_levels_group", type=str, default="weatherbench_13",
                        help="Group label for the set of pressure levels to use.")
    parser.add_argument("--init_date", type=str, default='1900-01-01',
                        help="Date to initialize the zarr store.")

    return parser.parse_known_args()


known_args, pipeline_args = parse_arguments("Update Data Slice")

with beam.Pipeline(argv=pipeline_args) as p:
    path = (
        p
        | "CreateDayIterator" >> beam.Create(daily_date_iterator(known_args.start_date, known_args.end_date))
        | "LoadDataForDay" >> beam.ParDo(LoadTemporalDataForDateDoFn(
            data_path=known_args.root_path, start_date=known_args.init_date,
            pressure_levels_group=known_args.pressure_levels_group))
        | "UpdateSlice" >> ARUpdateSlice(target=known_args.output_path, init_date=known_args.init_date)
    )
