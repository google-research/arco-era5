import apache_beam as beam
import datetime
import argparse
import logging
import json
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim
from pangeo_forge_recipes.transforms import OpenWithXarray, OpenURLWithFSSpec, DetermineSchema, PrepareZarrTarget
from arco_era5 import ResizeZarrTarget

logging.getLogger().setLevel(logging.INFO)
logging.getLogger('pangeo_forge_recipes').setLevel(logging.DEBUG)

default_chunks = [
    'cape', 'cisst', 'sfc', 'tcol',
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

parser = argparse.ArgumentParser()

parser.add_argument('output', type=str, help='Path to output Zarr in Cloud bucket.')
parser.add_argument('-s', '--start', default='2021-01-01', help='Start date, iso format string.')
parser.add_argument('-e', '--end', default='2021-01-31', help='End date, iso format string.')
parser.add_argument('-c', '--chunks', metavar='chunks', nargs='+', default=default_chunks, help='Chunks of variables to merge together.')
parser.add_argument('-t', '--target-chunk', type=json.loads, default='{"time": 1}', help='A JSON string; Divide target chunks of variables at output level.')

known_args, pipeline_args = parser.parse_known_args()

date_range = [ datetime.datetime.strptime(known_args.end, '%Y-%m-%d') ]
concat_dim = ConcatDim("time", date_range)

merge_dim = MergeDim("chunk", known_args.chunks)

def make_full_path(chunk: str, time: datetime.datetime):
    if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return (
                f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/"
                f"{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
            )
    return (
        f"gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/"
        f"{time.year:04d}/{time.year:04d}{time.month:02d}_hres_{chunk}.grb2"
    )

pattern = FilePattern(make_full_path, merge_dim, concat_dim, file_type='grib')

target_root, store_name = known_args.output.rsplit('/', 1)
    
with beam.Pipeline(argv=pipeline_args) as p:
    paths = (
        p 
        | "Create" >> beam.Create(pattern.items())
        | "OpenURLWithFSSpec" >> OpenURLWithFSSpec()
        | "OpenDatasets" >> OpenWithXarray(file_type=pattern.file_type, load=True, copy_to_local=True)
        | "DetermineSchema" >> DetermineSchema(combine_dims=pattern.combine_dim_keys)
        | "PrepareZarrTarget" >> PrepareZarrTarget(known_args.output, target_chunks=known_args.target_chunk)
        | "ResizeZarrTarget" >> ResizeZarrTarget(start=known_args.start, end=known_args.end, file_index=list(pattern.items())[0])
    )
