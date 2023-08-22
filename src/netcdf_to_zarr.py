# pylint: disable=line-too-long
r"""Create a single Zarr dataset from ERA5 NetCDF files.

While this pipeline is intended as a one off on the full data, it should be
possible to make some simple edits to the pipeline to append data for
subsequent years by:
* In a new location (otherwise xarray beam will delete everything!)
* Override the template to take into account any new dates.
* Change "daily_date_iterator" to only iterate over the new data to append (
  while keeping the original "start_date" so chunk indices are still computed
  correctly).
* Move all the generated files into the previous location, overwriting when
  necessary (e.g. template/static files).

Example usage:

  $ python src/netcdf_to_zarr.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/$USER-1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --runner DataflowRunner \
      --project $PROJECT \
      --region $REGION \
      --temp_location "gs://$BUCKET/tmp/" \
      --setup_file ./setup.py \
      --disk_size_gb 50 \
      --machine_type n2-highmem-2 \
      --no_use_public_ips  \
      --network=$NETWORK \
      --subnetwork=regions/$REGION/subnetworks/$SUBNET \
      --job_name $USER-ar-zarr-full
"""

# TODO(alvarosg): Make this pipeline resumable in case of error in the middle
# of execution.

__author__ = 'Matthew Willson, Alvaro Sanchez, Peter Battaglia, Stephan Hoyer, Stephan Rasp'

import dask
import fsspec
import logging

import apache_beam as beam
import numpy as np
import pandas as pd
import typing as t
import xarray as xa
import xarray_beam as xb

from apache_beam.options.pipeline_options import PipelineOptions

from arco_era5 import (
    GCP_DIRECTORY,
    STATIC_VARIABLES,
    SINGLE_LEVEL_VARIABLES,
    MULTILEVEL_VARIABLES,
    PRESSURE_LEVELS_GROUPS,
    TIME_RESOLUTION_HOURS,
    get_var_attrs_dict,
    read_multilevel_vars,
    read_single_level_vars,
    read_static_vars,
    daily_date_iterator,
    align_coordinates,
    parse_arguments
)

INPUT_PATH = GCP_DIRECTORY
_HOURS_PER_DAY = 24
# TODO(alvarosg): Add pressure level chunk size.


def _get_pressure_levels_arg(pressure_levels_group: str):
    return PRESSURE_LEVELS_GROUPS[pressure_levels_group]


def make_template(data_path: str, start_date: str, end_date: str, time_chunk_size: int,
                  pressure_levels_group: str) -> t.Tuple[xa.Dataset, t.Dict[str, int]]:
    """A lazy template with same dims/coords/vars as our expected results."""

    # Get the variable attributes.
    var_attrs_dict = get_var_attrs_dict(root_path=data_path)

    # Get some sample multi-level data to get coordinates, only for one var,
    # so it downloads quickly.
    logging.info("Downloading one variable of sample data for template.")
    first_year, first_month, first_day = next(iter(
        daily_date_iterator(start_date, end_date)))
    sample_multilevel_vars = align_coordinates(
        read_multilevel_vars(
            # Date is irrelevant.
            first_year,
            first_month,
            first_day,
            root_path=data_path,
            variables=MULTILEVEL_VARIABLES[:1],
            pressure_levels=_get_pressure_levels_arg(pressure_levels_group)))
    logging.info("Finished downloading.")

    lat_size = sample_multilevel_vars.sizes["latitude"]
    lon_size = sample_multilevel_vars.sizes["longitude"]
    level_size = sample_multilevel_vars.sizes["level"]
    assert level_size == len(
        _get_pressure_levels_arg(pressure_levels_group)
    ), "Mismatched level sizes"

    # Take the coordinates from the richer, multi-level dataset.
    coords = dict(sample_multilevel_vars.coords)
    coords["time"] = pd.date_range(
        pd.Timestamp(start_date),
        pd.Timestamp(end_date),
        freq=pd.DateOffset(hours=TIME_RESOLUTION_HOURS),
        ).values
    time_size = len(coords["time"])

    template_dataset = {}
    for name in STATIC_VARIABLES:
        template_dataset[name] = xa.Variable(
            dims=("latitude", "longitude"),
            data=dask.array.zeros(
                shape=(lat_size, lon_size),
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    for name in SINGLE_LEVEL_VARIABLES:
        template_dataset[name] = xa.Variable(
            dims=("time", "latitude", "longitude"),
            data=dask.array.zeros(
                shape=(time_size, lat_size, lon_size),
                chunks=-1,  # Will give chunk info directly to `ChunksToZarr``.
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    for name in MULTILEVEL_VARIABLES:
        template_dataset[name] = xa.Variable(
            dims=("time", "level", "latitude", "longitude"),
            data=dask.array.zeros(
                shape=(time_size, level_size, lat_size, lon_size),
                chunks=-1,  # Will give chunk info directly to `ChunksToZarr``.
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    chunk_sizes = {"time": time_chunk_size}
    return xa.Dataset(template_dataset, coords=coords), chunk_sizes


def load_temporal_data_for_date(args, data_path: str, start_date: str,
                                pressure_levels_group:
                                str) -> t.Tuple[xb.Key, xa.Dataset]:
    """Loads temporal data for a day, with an xarray_beam key for it.."""
    year, month, day = args
    logging.info("Loading NetCDF files for %d-%d-%d", year, month, day)

    try:
        single_level_vars = read_single_level_vars(
            year,
            month,
            day,
            variables=SINGLE_LEVEL_VARIABLES,
            root_path=data_path)
        multilevel_vars = read_multilevel_vars(
            year,
            month,
            day,
            variables=MULTILEVEL_VARIABLES,
            pressure_levels=_get_pressure_levels_arg(pressure_levels_group),
            root_path=data_path)
    except BaseException as e:
        # Make sure we print the date as part of the error for easier debugging
        # if something goes wrong. Note "from e" will also raise the details of the
        # original exception.
        raise Exception(f"Error loading {year}-{month}-{day}") from e

    # It is crucial to actually "load" as otherwise we get a pickle error.
    single_level_vars = single_level_vars.load()
    multilevel_vars = multilevel_vars.load()

    dataset = xa.merge([single_level_vars, multilevel_vars])
    dataset = align_coordinates(dataset)
    offsets = {"latitude": 0, "longitude": 0, "level": 0,
               "time": offset_along_time_axis(start_date, year, month, day)}
    key = xb.Key(offsets, vars=set(dataset.data_vars.keys()))
    logging.info("Finished loading NetCDF files for %s-%s-%s", year, month, day)
    return key, dataset


def offset_along_time_axis(start_date: str, year: int, month: int, day: int) -> int:
    """Offset in indices along the time axis, relative to start of the dataset."""
    # Note the length of years can vary due to leap years, so the chunk lengths
    # will not always be the same, and we need to do a proper date calculation
    # not just multiply by 365*24.
    time_delta = pd.Timestamp(
        year=year, month=month, day=day) - pd.Timestamp(start_date)
    return time_delta.days * _HOURS_PER_DAY // TIME_RESOLUTION_HOURS


def load_static_data(args, data_path: str) -> t.Tuple[xb.Key, xa.Dataset]:
    """Loads all static data, with an xarray_beam key for it.."""

    logging.info("Loading static data")

    dataset = read_static_vars(
        variables=STATIC_VARIABLES, root_path=data_path)

    logging.info("static data loaded.")
    # It is crucial to actually "load" as otherwise we get a pickle error.
    dataset = dataset.load()

    # Technically the static data has a time coordinate, but we don't need it.
    dataset = dataset.squeeze("time").drop("time")
    dataset = align_coordinates(dataset)

    offsets = {"latitude": 0, "longitude": 0}
    key = xb.Key(offsets, vars=set(dataset.data_vars.keys()))
    logging.info("Finished loading static data")
    return key, dataset


def define_pipeline(
    root: beam.Pipeline,
    input_path: str,
    output_path: str,
    time_chunk_size: int,
    start_date: str,
    end_date: str,
    pressure_levels_group: str
) -> t.Tuple[beam.Pipeline, beam.Pipeline]:
    """Defines a beam pipeline to convert the ERA5 NetCDF files to zarr."""

    template, chunk_sizes = make_template(
        input_path, start_date, end_date, time_chunk_size, pressure_levels_group)

    # We will create a single `chunks_to_zarr` object, but connect it at the end
    # of the two pipelines separately. This causes the full transformation to be
    # fused into a single worker operation, which writes chunks directly to final
    # location (So long as we provide a template and chose a chunk size that
    # avoids the need for `xb.ConsolidateChunks`).
    # This works much faster than:
    # return ((temporal_variables_chunks, static_variables_chunks)
    #         | beam.Flatten()
    #         | chunks_to_zarr)
    # which persists all data in an intermeddiate location.
    chunks_to_zarr = xb.ChunksToZarr(
        output_path,
        # `xb.make_template` should not be necessary as `template`` is already
        # a single chunk dask array of zeros, which is what `xb.make_template`
        # converts it to.
        template=xb.make_template(template),
        zarr_chunks=chunk_sizes)

    logging.info("Setting up temporal variables.")
    temporal_variables_chunks = (
            root
            | "DayIterator" >> beam.Create(daily_date_iterator(start_date, end_date))
            | "TemporalDataForDay"
            >> beam.Map(load_temporal_data_for_date, data_path=input_path,
                        start_date=start_date,
                        pressure_levels_group=pressure_levels_group)
            | xb.SplitChunks(chunk_sizes)
            # We can skip the consolidation if we are using a `time_chunk_size` that
            # evenly divides a day worth of data.
            # | xb.ConsolidateChunks(chunk_sizes)
            | "ChunksToZarrTemporal" >> chunks_to_zarr
    )

    logging.info("Setting up static variables.")
    static_variables_chunks = (
            root
            # This is a single element with no parameters.
            | "DummySingleElement" >> beam.Create(range(1))
            | "StaticVariableFromNetCDFFile" >> beam.Map(load_static_data,
                                                         data_path=input_path)
            | "ChunksToZarrStatic" >> chunks_to_zarr
    )
    logging.info("Finished defining pipeline.")
    return (temporal_variables_chunks, static_variables_chunks)


def main():
    logging.getLogger().setLevel(logging.INFO)
    fs = fsspec.filesystem('gcs')

    known_args, pipeline_args = parse_arguments(
        "Create a Zarr dataset from NetCDF files."
    )
    pipeline_args.append('--save_main_session')
    pipeline_args.append('True')

    if fs.exists(known_args.output_path):
        raise ValueError(f"{known_args.output_path} already exists")

    num_steps_per_day = _HOURS_PER_DAY // TIME_RESOLUTION_HOURS
    if num_steps_per_day % known_args.time_chunk_size != 0:
        raise ValueError(
            f"time_chunk_size {known_args.time_chunk_size} must evenly divide {num_steps_per_day}"
        )

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as root:
        define_pipeline(
            root,
            input_path=INPUT_PATH,
            output_path=known_args.output_path,
            start_date=known_args.start_date,
            end_date=known_args.end_date,
            time_chunk_size=known_args.time_chunk_size,
            pressure_levels_group=known_args.pressure_levels_group
        )


if __name__ == "__main__":
    main()
