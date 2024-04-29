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
import json
import logging
import typing as t
import xarray as xr
import xarray_beam as xbeam

from arco_era5 import chunks_to_rows, replace_non_alphanumeric_with_hyphen

logger = logging.getLogger(__name__)

input_chunks = {"time": 1, "level": 1, "latitude": 200, "longitude": 400}
  
def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-i', "--uri", required=True, help='input zarr file.')
    parser.add_argument('-m', "--month", required=True, help='month of the data which needs to change, iso format string.')
    parser.add_argument('-o', "--output", required=True, help='Output path to store raw files.')

    return parser.parse_known_args()

def run_pipeline(ds: xr.Dataset, month: str, input_chunks: t.Dict, output: str, pipeline_args: t.List[str]):

    ds = ds.sel(time=month)

    avro_scheam = json.load(open("/arco-era5/src/arco_era5/era5-avro-schema.json"))

    with beam.Pipeline(argv=pipeline_args) as p:
        _ = (
            p
            | "DatasetToChunks" >> xbeam.DatasetToChunks(ds, input_chunks)
            | "ExtractRows" >> beam.FlatMapTuple(chunks_to_rows, input_chunks=input_chunks)
            | "WriteToAvro" >> beam.io.WriteToAvro(f"{output}/{month}/ar", schema=avro_scheam)
        )

if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
  
    known_args, pipeline_args = parse_arguments("Update Data Slice")

    ds, _ = xbeam.open_zarr(known_args.uri)
    month = known_args.month

    pipeline_args.extend(['--job_name', f"ar-avro-generation-{replace_non_alphanumeric_with_hyphen(month)}", '--save_main_session'])
    run_pipeline(ds, month, input_chunks, output=known_args.output, pipeline_args=pipeline_args)
