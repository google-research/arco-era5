import logging
import datetime
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
import argparse
import pandas as pd
from typing import List, Tuple
import apache_beam as beam
from arco_era5 import GenerateOffset, UpdateSlice

logger = logging.getLogger()


def parse_args(desc: str) -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('output', type=str, help='Path to output Zarr in Cloud bucket.')
    parser.add_argument('-s', '--start', required=True, help='Start date, iso format string.')
    parser.add_argument('-e', '--end', required=True, help='End date, iso format string.')
    parser.add_argument('-i', '--init-date', default='1900-01-01', help='Start date, iso format string.')
    parser.add_argument('-c', '--chunks', metavar='chunks', nargs='+', default=['dve', 'tw'], help='Chunks of variables to merge together.')

    return parser.parse_known_args()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    known_args, unknown_args = parse_args('Convert Era 5 Model Level data to Zarr')

    date_range = [
        ts.to_pydatetime()
        for ts in pd.date_range(start=known_args.start,end=known_args.end, freq="D").to_list()
    ]

    def make_path(time: datetime.datetime, chunk: str) -> str:
        """Make path to Era5 data from timestamp and variable."""
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

    date_dim = ConcatDim("time", date_range)
    chunks_dim = MergeDim("chunk", known_args.chunks)
    pattern = FilePattern(make_path, date_dim, chunks_dim, file_type='grib')
    files = [ p[1] for p in pattern.items() ]
    
    with beam.Pipeline(argv=unknown_args) as p:
        paths = (
            p
            | "Create" >> beam.Create(files)
            | "GenerateOffset" >> GenerateOffset(init_date=known_args.init_date)
            | "Reshuffle" >> beam.Reshuffle()
            | "UpdateSlice" >> UpdateSlice(target=known_args.output)
        )
