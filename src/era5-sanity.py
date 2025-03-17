# Copyright 2025 Google LLC
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
import argparse

import apache_beam as beam
import typing as t

from arco_era5 import (
    ExecTypes,
    generate_raw_paths,
    get_previous_month_dates,
    OpenLocal,
    update_splittable_files,
    update_zarr,
    update_zarr_metadata
)

def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace,
                                                            t.List[str]]:
    """Parse command-line arguments for the data processing pipeline."""
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--temp_path", required=True, type=str,
                        help="Temporary path for era5 raw.")
    parser.add_argument("--target_path", required=True, type=str,
                        help="Path to zarr store.")
    parser.add_argument('-i', '--init_date', default='1900-01-01',
                        help='Start date, iso format string.')
    parser.add_argument('--timestamps_per_day', type=int, default=24,
                        help='Timestamps Per Day.')

    return parser.parse_known_args()


if __name__ == "__main__":

    parsed_args, pipeline_args = parse_arguments("Parse arguments.")

    is_single_level = "single-level" in parsed_args.target_path
    is_analysis_ready = "/ar/" in parsed_args.target_path

    dates = get_previous_month_dates("era5")

    original_paths = generate_raw_paths(
        dates['first_day'], dates['last_day'], parsed_args.target_path, is_single_level, is_analysis_ready
    )
    temp_paths = generate_raw_paths(
        dates['first_day'], dates['last_day'], parsed_args.target_path, is_single_level, is_analysis_ready, parsed_args.temp_path
    )

    input_paths = [(original_path, temp_path) for original_path, temp_path in zip(original_paths, temp_paths)]

    with beam.Pipeline(argv=pipeline_args) as p:
        _ = (
            p
            | "Create" >> beam.Create(input_paths)
            | "OpenLocal" >> beam.ParDo(OpenLocal(
                    target_path=parsed_args.target_path,
                    init_date=parsed_args.init_date,
                    timestamps_per_file=parsed_args.timestamps_per_day,
                    is_analysis_ready=is_analysis_ready,
                    is_single_level=is_single_level
                ))
            | "WriteToZarr" >> beam.MapTuple(update_zarr)
        )
    
    update_zarr_metadata(parsed_args.target_path, dates['last_day'], mode=ExecTypes.ERA5.value)

    update_splittable_files(dates['first_day'].strftime("%Y/%m"), parsed_args.temp_path)
