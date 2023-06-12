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
"""Common Pangeo-Forge Recipe definition for converting ERA5 datasets to Zarr."""
import argparse
import cfgrib
import contextlib
import datetime
import itertools
import json
import logging
import os
import tempfile
import typing as t
import shutil
import subprocess
from urllib import parse

import apache_beam as beam
import gcsfs
from apache_beam.io.filesystem import CompressionTypes, FileSystem, CompressedFile, DEFAULT_READ_BUFFER_SIZE
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget, StorageConfig

PROGRESS = itertools.cycle(''.join([c * 10 for c in '|/–-\\']))
logger = logging.getLogger(__name__)


def normalize_path(path: str) -> str:
    parsed_output = parse.urlparse(path)
    return f'{parsed_output.netloc}{parsed_output.path}'


def copy(src: str, dst: str) -> None:
    """Copy data via `gcloud alpha storage` or `gsutil`."""
    errors: t.List[subprocess.CalledProcessError] = []
    for cmd in ['gcloud alpha storage cp', 'gsutil cp']:
        try:
            subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True, text=True, input="n/n")
            return
        except subprocess.CalledProcessError as e:
            errors.append(e)

    msg = f'Failed to copy file {src!r} to {dst!r}'
    err_msgs = ', '.join(map(lambda err: repr(err.stderr.decode('utf-8')), errors))
    logger.error(f'{msg} due to {err_msgs}.')
    raise EnvironmentError(msg, errors)

@contextlib.contextmanager
def open_local(uri: str) -> t.Iterator[str]:
    """Copy a cloud object (e.g. a netcdf, grib, or tif file) from cloud storage, like GCS, to local file."""
    print("inside open_local")
    with tempfile.NamedTemporaryFile() as dest_file:
        # Transfer data with gsutil or gcloud alpha storage (when available)
        copy(uri, dest_file.name)

        # Check if data is compressed. Decompress the data using the same methods that beam's
        # FileSystems interface uses.
        compression_type = FileSystem._get_compression_type(uri, CompressionTypes.AUTO)
        if compression_type == CompressionTypes.UNCOMPRESSED:
            yield dest_file.name
            return

        dest_file.seek(0)
        with tempfile.NamedTemporaryFile() as dest_uncompressed:
            with CompressedFile(dest_file, compression_type=compression_type) as dcomp:
                shutil.copyfileobj(dcomp, dest_uncompressed, DEFAULT_READ_BUFFER_SIZE)
                yield dest_uncompressed.name


def run(make_path: t.Callable[..., str], date_range: t.List[datetime.datetime],
        parsed_args: argparse.Namespace, other_args: t.List[str]):
    """Perform the Zarr conversion pipeline with Pangeo Forge Recipes."""

    date_dim = ConcatDim("time", date_range)
    chunks_dim = MergeDim("chunk", parsed_args.chunks)
    pattern = FilePattern(make_path, date_dim, chunks_dim, file_type='grib')
    with open_local(list(pattern.items())[0][1]) as local_path:
        temp_data = cfgrib.open_datasets(local_path)
        target_chunk = {}
        #considering that all objects have same data.
        for var_name in list(temp_data[0].indexes):
            target_chunk[var_name] = 1

    # remove 'indexpath' experimental feature; it breaks the pipeline
    # see https://github.com/ecmwf/cfgrib#grib-index-file
    xarray_kwargs = {
        'engine': 'cfgrib',
        'backend_kwargs': {
            'indexpath': '',
            'read_keys': [
                'pv',
                'latitudeOfFirstGridPointInDegrees',
                'longitudeOfFirstGridPointInDegrees',
                'latitudeOfLastGridPointInDegrees',
                'longitudeOfLastGridPointInDegrees'
            ]
        },
        'cache': False,
        'chunks': {'time': 4},
    }

    fs = gcsfs.GCSFileSystem(project=os.environ.get('PROJECT', 'ai-for-weather'))

    if parsed_args.find_missing:
        print('Finding missing data...')
        data_is_missing = False

        for _, path in pattern.items():
            print(next(PROGRESS), end='\r')
            if not fs.exists(path):
                data_is_missing = True
                print(path)

        if data_is_missing:
            print('Found missing data.')
        else:
            print('No missing data found.')
        return

    output_path = normalize_path(parsed_args.output)
    temp_path = normalize_path(parsed_args.temp)

    storage_config = StorageConfig(
        target=FSSpecTarget(fs, output_path),
        metadata=MetadataTarget(fs, f'{temp_path}meta/')
    )

    recipe = XarrayZarrRecipe(pattern,
                              storage_config=storage_config,
                              target_chunks=target_chunk,
                              subset_inputs=parsed_args.subset_inputs,
                              copy_input_to_local_file=True,
                              cache_inputs=False,
                              lock_timeout=120,  # seconds until lock times out.
                              consolidate_zarr=True,
                              xarray_open_kwargs=xarray_kwargs)

    with beam.Pipeline(argv=other_args) as p:
        p | recipe.to_beam()


def parse_args(desc: str, default_chunks: t.List[str]) -> t.Tuple[argparse.Namespace, t.List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('output', type=str, help='Path to output Zarr in Cloud bucket.')
    parser.add_argument('temp', type=str, help='Path to cloud bucket for temporary data cache.')
    parser.add_argument('--find-missing', action='store_true', default=False,
                        help='Print all paths to missing input data.')
    parser.add_argument('-s', '--start', default='2020-01-01', help='Start date, iso format string.')
    parser.add_argument('-e', '--end', default='2020-02-01', help='End date, iso format string.')
    parser.add_argument('-c', '--chunks', metavar='chunks', nargs='+',
                        default=default_chunks,
                        help='Chunks of variables to merge together.')
    parser.add_argument('--subset-inputs', type=json.loads, default='{"time": 4}',
                        help='A JSON string; when reading a chunk from disk, divide them into smaller chunks across '
                             'each dimension. Think of this as the inverse of a chunk size (e.g. the total number of '
                             'sub chunks). Default: `{"time": 4}`')

    return parser.parse_known_args()
