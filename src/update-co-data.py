import logging
import argparse
from typing import List, Tuple
import apache_beam as beam
from arco_era5 import GenerateOffset, COUpdateSlice, GCP_DIRECTORY, daily_date_iterator, generate_input_paths

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
                        default=model_level_default_chunks,
                        help='Chunks of variables to merge together.')
    parser.add_argument('--time_per_day', type=int, default=24,
                        help='Timestamps Per Day.')

    return parser.parse_known_args()

if __name__ == '__main__':
    known_args, unknown_args = parse_args('Convert Era 5 Model Level data to Zarr')

    is_single_level = "single" in known_args.output_path

    if is_single_level:
        if known_args.chunks == model_level_default_chunks:
            known_args.chunks = single_level_default_chunks

    files = generate_input_paths(known_args.start_date, known_args.end_date, GCP_DIRECTORY, known_args.chunks, is_single_level=is_single_level)
    
    with beam.Pipeline(argv=unknown_args) as p:
        paths = (
            p
            | "Create" >> beam.Create(files)
            | "GenerateOffset" >> GenerateOffset(init_date=known_args.init_date, is_single_level=is_single_level, timestamps_per_file=known_args.time_per_day)
            | "Reshuffle" >> beam.Reshuffle()
            | "UpdateSlice" >> COUpdateSlice(target=known_args.output_path)
        )
