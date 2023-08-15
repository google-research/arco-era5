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
      --output_path="gs://gcp-public-data-arco-era5/test/ar/1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --runner DataflowRunner \
      -- \
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

import logging
from typing import Iterable, Tuple, Dict

import fsspec
from absl import app
from absl import flags
import apache_beam as beam
import dask
import numpy as np
import pandas as pd
import xarray as xa
import xarray_beam as xb

from arco_era5 import source_data


INPUT_PATH = flags.DEFINE_string(
    "input_path", source_data.GCP_DIRECTORY,
    "Path under which netcdf files live.")

START_DATE = flags.DEFINE_string(
    "start_date", source_data.FIRST_DATE,
    "The first date of source data to use.")

END_DATE = flags.DEFINE_string(
    "end_date", source_data.LAST_DATE,
    "The last date (exclusive) of source data to use.")

# TODO(peterbattaglia): Consider making several canonical sets of static
# variables, and referencing that instead of the individual ones.
STATIC_VARIABLES = flags.DEFINE_spaceseplist(
    "static_variables", " ".join(source_data.STATIC_VARIABLES),
    "Space-separated list of static variables to use.")

# TODO(peterbattaglia): Consider making several canonical sets of single level
# variables, and referencing that instead of the individual ones.
SINGLE_LEVEL_VARIABLES = flags.DEFINE_spaceseplist(
    "single_level_variables", " ".join(source_data.SINGLE_LEVEL_VARIABLES),
    "Space-separated list of single level variables to use.")

# TODO(peterbattaglia): Consider making several canonical sets of multilevel
# variables, and referencing that instead of the individual ones.
MULTILEVEL_VARIABLES = flags.DEFINE_spaceseplist(
    "multilevel_variables", " ".join(source_data.MULTILEVEL_VARIABLES),
    "Space-separated list of multilevel variables to use.")

PRESSURE_LEVELS_GROUP = flags.DEFINE_enum(
    "pressure_levels_group", "weatherbench_13",
    source_data.PRESSURE_LEVELS_GROUPS.keys(),
    "Group label for set of pressure levels to use.")

# TODO(peterbattaglia): Add check for dir existence/permissions, which are
# currently generating opaque
OUTPUT_PATH = flags.DEFINE_string(
    "output_path", None, "Path to destination zarr archive.", required=True)

# TODO(alvarosg): Add pressure level chunk size.
TIME_CHUNK_SIZE = flags.DEFINE_integer(
    "time_chunk_size", None,
    "Number of 1-hourly timesteps to include in a single chunk. Must evenly "
    "divide 24.",
    required=True)
RUNNER = flags.DEFINE_string(
    "runner", None, "Apache Beam Runner.", required=True)

_HOURS_PER_DAY = 24


def _get_pressure_levels_arg():
    return source_data.PRESSURE_LEVELS_GROUPS[PRESSURE_LEVELS_GROUP.value]


def make_template(
        data_path: str, start_date: str, end_date: str, time_chunk_size: int,
) -> Tuple[xa.Dataset, Dict[str, int]]:
    """A lazy template with same dims/coords/vars as our expected results."""

    # Get the variable attributes.
    var_attrs_dict = source_data.get_var_attrs_dict(root_path=data_path)

    # Get some sample multi-level data to get coordinates, only for one var,
    # so it downloads quickly.
    logging.info("Downloading one variable of sample data for template.")
    first_year, first_month, first_day = next(iter(
        daily_date_iterator(start_date, end_date)))
    sample_multilevel_vars = align_coordinates(
        source_data.read_multilevel_vars(
            # Date is irrelevant.
            first_year,
            first_month,
            first_day,
            root_path=data_path,
            variables=MULTILEVEL_VARIABLES.value[:1],
            pressure_levels=_get_pressure_levels_arg()))
    logging.info("Finished downloading.")

    lat_size = sample_multilevel_vars.sizes["latitude"]
    lon_size = sample_multilevel_vars.sizes["longitude"]
    level_size = sample_multilevel_vars.sizes["level"]
    assert level_size == len(_get_pressure_levels_arg()), "Mismatched level sizes"

    # Take the coordinates from the richer, multi-level dataset.
    coords = dict(sample_multilevel_vars.coords)
    coords["time"] = pd.date_range(
        pd.Timestamp(start_date),
        pd.Timestamp(end_date),
        freq=pd.DateOffset(hours=source_data.TIME_RESOLUTION_HOURS),
        closed="left").values
    time_size = len(coords["time"])

    template_dataset = {}
    for name in STATIC_VARIABLES.value:
        template_dataset[name] = xa.Variable(
            dims=("latitude", "longitude"),
            data=dask.array.zeros(
                shape=(lat_size, lon_size),
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    for name in SINGLE_LEVEL_VARIABLES.value:
        template_dataset[name] = xa.Variable(
            dims=("time", "latitude", "longitude"),
            data=dask.array.zeros(
                shape=(time_size, lat_size, lon_size),
                chunks=-1,  # Will give chunk info directly to `ChunksToZarr``.
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    for name in MULTILEVEL_VARIABLES.value:
        template_dataset[name] = xa.Variable(
            dims=("time", "level", "latitude", "longitude"),
            data=dask.array.zeros(
                shape=(time_size, level_size, lat_size, lon_size),
                chunks=-1,  # Will give chunk info directly to `ChunksToZarr``.
                dtype=np.float32),
            attrs=var_attrs_dict[name])

    chunk_sizes = {"time": time_chunk_size}
    return xa.Dataset(template_dataset, coords=coords), chunk_sizes


def daily_date_iterator(start_date: str, end_date: str
                        ) -> Iterable[Tuple[int, int, int]]:
    """Iterates all (year, month, day) tuples between start_date and end_date."""
    first_day = pd.Timestamp(start_date)
    final_day = pd.Timestamp(end_date)  # non-inclusive
    time_delta = pd.Timedelta("1 day")
    current_day = first_day
    while current_day < final_day:
        yield current_day.year, current_day.month, current_day.day
        current_day += time_delta


def load_temporal_data_for_date(
        year: int, month: int, day: int, *, data_path: str, start_date: str,
) -> Tuple[xb.Key, xa.Dataset]:
    """Loads temporal data for a day, with an xarray_beam key for it.."""

    logging.info("Loading NetCDF files for %s-%s-%s", year, month, day)

    try:
        single_level_vars = source_data.read_single_level_vars(
            year,
            month,
            day,
            variables=SINGLE_LEVEL_VARIABLES.value,
            root_path=data_path)
        multilevel_vars = source_data.read_multilevel_vars(
            year,
            month,
            day,
            variables=MULTILEVEL_VARIABLES.value,
            pressure_levels=_get_pressure_levels_arg(),
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


def offset_along_time_axis(start_date: str, year: int, month: int, day: int
                           ) -> int:
    """Offset in indices along the time axis, relative to start of the dataset."""
    # Note the length of years can vary due to leap years, so the chunk lengths
    # will not always be the same, and we need to do a proper date calculation
    # not just multiply by 365*24.
    time_delta = pd.Timestamp(
        year=year, month=month, day=day) - pd.Timestamp(start_date)
    return time_delta.days * _HOURS_PER_DAY // source_data.TIME_RESOLUTION_HOURS


def load_static_data(*, data_path: str) -> Tuple[xb.Key, xa.Dataset]:
    """Loads all static data, with an xarray_beam key for it.."""

    logging.info("Loading static data")

    dataset = source_data.read_static_vars(
        variables=STATIC_VARIABLES.value, root_path=data_path)

    # It is crucial to actually "load" as otherwise we get a pickle error.
    dataset = dataset.load()

    # Technically the static data has a time coordinate, but we don't need it.
    dataset = dataset.squeeze("time").drop("time")
    dataset = align_coordinates(dataset)

    offsets = {"latitude": 0, "longitude": 0}
    key = xb.Key(offsets, vars=set(dataset.data_vars.keys()))
    logging.info("Finished loading static data")
    return key, dataset


def align_coordinates(dataset: xa.Dataset) -> xa.Dataset:
    """Align coordinates of per-variable datasets prior to consolidation."""

    # It's possible to have coordinate metadata for coordinates which aren't
    # actually used as dimensions of any variables (called 'non-index'
    # coordinates), and some of the source NetCDF files use these, e.g. a scalar
    # 'height' coordinate (= 2.0) in the NetCDF files for the 2-meter temperature
    # variable tas. We remove these, for simplicity and since once the variables
    # are combined in a single zarr dataset it won't be clear what these
    # additional coordinates are referring to.
    dataset = dataset.reset_coords(drop=True)

    # Downcast lat and lon coordinates to float32. This is because there are
    # small rounding-error (~1e-14 relative error) discrepancies between the
    # float64 latitude coordinates across different source NetCDF files, and
    # xarray_beam complains about this. After downcasting to float32 the
    # differences go away, and the extra float64 precision isn't important to us.
    # (Ideally you'd be able to specify a tolerance for the comparison, but this
    # works for now.)
    dataset = dataset.assign_coords(
        latitude=dataset["latitude"].astype(np.float32),
        longitude=dataset["longitude"].astype(np.float32))

    return dataset


def define_pipeline(
        root: beam.Pipeline,
        input_path: str,
        output_path: str,
        time_chunk_size: int,
        start_date: str,
        end_date: str,
) -> Tuple[beam.Pipeline, beam.Pipeline]:
    """Defines a beam pipeline to convert the ERA5 NetCDF files to zarr."""

    template, chunk_sizes = make_template(
        input_path, start_date, end_date, time_chunk_size)

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
            | "TemporalDataForDay" >> beam.Map(
        lambda args: load_temporal_data_for_date(  # pylint: disable=g-long-lambda
            *args, data_path=input_path, start_date=start_date))
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
            | "StaticVariableFromNetCDFFile" >> beam.Map(
        lambda unused_i: load_static_data(data_path=input_path))
            | "ChunksToZarrStatic" >> chunks_to_zarr
    )
    logging.info("Finished defining pipeline.")
    return (temporal_variables_chunks, static_variables_chunks)


def main(argv):
    fs = fsspec.filesystem('gcs')

    if fs.exists(OUTPUT_PATH.value):
        raise ValueError(f"{OUTPUT_PATH.value} already exists")

    # Forcing the model to avoid a slow `ConsolidateChunks` bottleneck.
    num_steps_per_day = _HOURS_PER_DAY // source_data.TIME_RESOLUTION_HOURS
    if num_steps_per_day % TIME_CHUNK_SIZE.value != 0:
        raise ValueError(f"time_chunk_size{TIME_CHUNK_SIZE.value} must "
                         f"evenly divide {num_steps_per_day}")

    with beam.Pipeline(runner=RUNNER.value, argv=argv) as root:
        define_pipeline(
            root,
            input_path=INPUT_PATH.value,
            output_path=OUTPUT_PATH.value,
            start_date=START_DATE.value,
            end_date=END_DATE.value,
            time_chunk_size=TIME_CHUNK_SIZE.value,
        )


if __name__ == "__main__":
    app.run(main)
