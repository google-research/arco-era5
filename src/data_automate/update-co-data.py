import logging
import datetime
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
import argparse
import pandas as pd
from typing import List, Tuple
import apache_beam as beam
from arco_era5 import GenerateOffset, COUpdateSlice, GCP_DIRECTORY

logger = logging.getLogger()

model_level_default_chunks = ['dve', 'tw']
single_level_default_chunks = [
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

def parse_args(desc: str) -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('--output_path', type=str, required=True,
                        help='Path to output Zarr in Cloud bucket.')
    parser.add_argument('-s', '--start_date', required=True,
                        help='Start date, iso format string.')
    parser.add_argument('-e', '--end_date', required=True,
                        help='End date, iso format string.')
    parser.add_argument('-i', '--init_date', default='1900-01-01',
                        help='Start date, iso format string.')
    parser.add_argument('-c', '--chunks', metavar='chunks', nargs='+',
                        default=model_level_default_chunks, help='Chunks of variables to merge together.')
    parser.add_argument('--time_per_day', type=int, default=24,
                        help='Timestamps Per Day.')

    return parser.parse_known_args()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, unknown_args = parse_args('Convert Era 5 Model Level data to Zarr')

    is_single_level = "single" in known_args.output_path

    if is_single_level:
        if known_args.chunks == model_level_default_chunks:
            known_args.chunks = single_level_default_chunks

    date_range = [
        ts.to_pydatetime()
        for ts in pd.date_range(start=known_args.start_date,end=known_args.end_date, freq="MS" if is_single_level else "D").to_list()
    ]

    def model_level_make_path(time: datetime.datetime, chunk: str) -> str:
        """Make path to Era5 data from timestamp and variable."""
        HRES_PATH = f"{GCP_DIRECTORY}/ERA5GRIB/HRES/Daily"
        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return f"{HRES_PATH}/{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
        return f"{HRES_PATH}/{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk}.grb2"

    def single_level_make_path(time: datetime.datetime, chunk: str) -> str:
        """Make path to Era5 data from timestamp and variable."""
        HRES_PATH = f"{GCP_DIRECTORY}/ERA5GRIB/HRES/Month"
        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return f"{HRES_PATH}/{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
        return f"{HRES_PATH}/{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk}.grb2"

    date_dim = ConcatDim("time", date_range)
    chunks_dim = MergeDim("chunk", known_args.chunks)
    pattern = FilePattern(single_level_make_path if is_single_level else model_level_make_path, date_dim, chunks_dim, file_type='grib')
    files = [ p[1] for p in pattern.items() ]
    
    with beam.Pipeline(argv=unknown_args) as p:
        paths = (
            p
            | "Create" >> beam.Create(files)
            | "GenerateOffset" >> GenerateOffset(init_date=known_args.init_date, is_single_level=is_single_level, timestamps_per_file=known_args.time_per_day)
            | "Reshuffle" >> beam.Reshuffle()
            | "UpdateSlice" >> COUpdateSlice(target=known_args.output_path)
        )
