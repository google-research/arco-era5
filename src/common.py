"""Convert Surface level Era 5 Data to an unprocessed Zarr dataset.

Example:
    ```
    python src/single-levels-to-zarr.py gs://anthromet-external-era5/single-level-reanalysis.zarr gs://$BUCKET/cache1/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location "gs://$BUCKET/tmp/" \
     --setup_file ./setup.py \
     --experiment=use_runner_v2 \
     --disk_size_gb 50 \
     --machine_type n2-highmem-2 \
     --sdk_container_image=gcr.io/ai-for-weather/ecmwf-beam-worker:latest \
     --job_name reanalysis-to-zarr
    ```
"""
import itertools
import argparse
import datetime
import logging
import os
import typing as t
from urllib import parse

import apache_beam as beam
import gcsfs
import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.storage import FSSpecTarget, MetadataTarget

PROGRESS = itertools.cycle(''.join([c * 10 for c in '|/–-\\']))


def normalize_path(path: str) -> str:
    parsed_output = parse.urlparse(path)
    return f'{parsed_output.netloc}{parsed_output.path}'


def run(make_path: t.Callable[..., str], parsed_args: argparse.Namespace, other_args: t.List[str]):
    """Perform the Zarr conversion pipeline with Pangeo Forge Recipes."""

    date_range = [
        ts.to_pydatetime()
        for ts in pd.date_range(start=parsed_args.start,
                                end=parsed_args.end,
                                freq="MS").to_list()
    ]
    date_dim = ConcatDim("time", date_range)
    chunks_dim = MergeDim("chunk", parsed_args.chunks)
    pattern = FilePattern(make_path, date_dim, chunks_dim)

    # remove 'indexpath' experimental feature; it breaks the pipeline
    # see https://github.com/ecmwf/cfgrib#grib-index-file
    xarray_kwargs = {
        'engine': 'cfgrib',
        'backend_kwargs': {
            'indexpath': '',
        },
        'cache': False,
        'chunks': {'time': 4}
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

    metadata_target = MetadataTarget(fs, f'{temp_path}meta/')
    target = FSSpecTarget(fs, output_path)

    recipe = XarrayZarrRecipe(pattern,
                              target=target,
                              target_chunks={'time': 1},
                              subset_inputs={'time': 4},
                              copy_input_to_local_file=True,
                              cache_inputs=False,
                              lock_timeout=120,  # seconds until lock times out.
                              metadata_cache=metadata_target,
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

    return parser.parse_known_args()



