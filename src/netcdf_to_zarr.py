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

    Generate zarr store from start_date with data

    python src/netcdf_to_zarr.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/$USER-1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --start_date '1959-01-01' \
      --end_date '2021-12-31' \
      --runner DataflowRunner \
      --project $PROJECT \
      --region $REGION \
      --temp_location "gs://$BUCKET/tmp/" \
      --setup_file ./setup.py \
      --disk_size_gb 500 \
      --machine_type m1-ultramem-40 \
      --no_use_public_ips  \
      --network=$NETWORK \
      --subnetwork=regions/$REGION/subnetworks/$SUBNET \
      --job_name $USER-ar-zarr-full \
      --number_of_worker_harness_threads 20

    Generate zarr store from init_date and fill data from start_date. Default init_date will be 1900-01-01

    ```
    python src/netcdf_to_zarr.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/$USER-1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --start_date '1959-01-01' \
      --end_date '2021-12-31' \
      --init_date '1900-01-01' \
      --from_init_date
      --runner DataflowRunner \
      --project $PROJECT \
      --region $REGION \
      --temp_location "gs://$BUCKET/tmp/" \
      --setup_file ./setup.py \
      --disk_size_gb 500 \
      --machine_type m1-ultramem-40 \
      --no_use_public_ips  \
      --network=$NETWORK \
      --subnetwork=regions/$REGION/subnetworks/$SUBNET \
      --job_name $USER-ar-zarr-full \
      --number_of_worker_harness_threads 20
    ```

    Generate zarr store from init_date without data. Default init_date will be 1900-01-01. Static variables will be loaded.

    ```
    python src/netcdf_to_zarr.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/$USER-1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --start_date '1959-01-01' \
      --end_date '2021-12-31' \
      --init_date '1800-01-01' \
      --from_init_date \
      --only_initialize_store
    ```

    Seed data in the existing store.

    ```
    python src/update-data.py \
      --output_path="gs://gcp-public-data-arco-era5/ar/$USER-1959-2022-full_37-1h-0p25deg-chunk-1.zarr-v2" \
      --pressure_levels_group="full_37" \
      --time_chunk_size=1 \
      --start_date '1959-01-01' \
      --end_date '2021-12-31' \
      --init_date '1900-01-01' \
      --runner DataflowRunner \
      --project $PROJECT \
      --region $REGION \
      --temp_location "gs://$BUCKET/tmp/" \
      --setup_file ./setup.py \
      --disk_size_gb 500 \
      --machine_type m1-ultramem-40 \
      --no_use_public_ips  \
      --network=$NETWORK \
      --subnetwork=regions/$REGION/subnetworks/$SUBNET \
      --job_name $USER-ar-zarr-full \
      --number_of_worker_harness_threads 20
    ```

"""

# TODO(alvarosg): Make this pipeline resumable in case of error in the middle
# of execution.

__author__ = 'Matthew Willson, Alvaro Sanchez, Peter Battaglia, Stephan Hoyer, Stephan Rasp'

import dask
import fsspec
import logging

import apache_beam as beam
import datetime
import numpy as np
import pandas as pd
import typing as t
import xarray as xa
import xarray_beam as xb

from arco_era5 import (
    GCP_DIRECTORY,
    HOURS_PER_DAY,
    STATIC_VARIABLES,
    SINGLE_LEVEL_VARIABLES,
    MULTILEVEL_VARIABLES,
    TIME_RESOLUTION_HOURS,
    get_pressure_levels_arg,
    get_var_attrs_dict,
    read_multilevel_vars,
    read_single_level_vars,
    read_static_vars,
    daily_date_iterator,
    align_coordinates,
    parse_arguments,
    LoadTemporalDataForDateDoFn
)

INPUT_PATH = GCP_DIRECTORY
# TODO(alvarosg): Add pressure level chunk size.

def make_template(data_path: str, start_date: str, end_date: str, time_chunk_size: int,
                  pressure_levels_group: str) -> t.Tuple[xa.Dataset, t.Dict[str, int]]:
    """A lazy template with same dims/coords/vars as our expected results."""

    # Get the variable attributes.
    var_attrs_dict = get_var_attrs_dict(root_path=data_path)

    # Get some sample multi-level data to get coordinates, only for one var,
    # so it downloads quickly.
    logging.info("Downloading one variable of sample data for template.")
    date = datetime.datetime.strptime(end_date, '%Y-%m-%d') - datetime.timedelta(days=1)
    sample_multilevel_vars = align_coordinates(
        read_multilevel_vars(
            # Date is irrelevant.
            date.year,
            date.month,
            date.day,
            root_path=data_path,
            variables=MULTILEVEL_VARIABLES[:1],
            pressure_levels=get_pressure_levels_arg(pressure_levels_group)))
    logging.info("Finished downloading.")

    lat_size = sample_multilevel_vars.sizes["latitude"]
    lon_size = sample_multilevel_vars.sizes["longitude"]
    level_size = sample_multilevel_vars.sizes["level"]
    assert level_size == len(
        get_pressure_levels_arg(pressure_levels_group)
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

def offset_along_time_axis(start_date: str, year: int, month: int, day: int) -> int:
    """Offset in indices along the time axis, relative to start of the dataset."""
    # Note the length of years can vary due to leap years, so the chunk lengths
    # will not always be the same, and we need to do a proper date calculation
    # not just multiply by 365*24.
    time_delta = pd.Timestamp(
        year=year, month=month, day=day) - pd.Timestamp(start_date)
    return time_delta.days * HOURS_PER_DAY // TIME_RESOLUTION_HOURS


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
    pressure_levels_group: str,
    init_date: str,
    from_init_date: bool,
    only_initialize_store: bool
) -> t.Tuple[beam.Pipeline, beam.Pipeline]:
    """Defines a beam pipeline to convert the ERA5 NetCDF files to zarr."""

    template, chunk_sizes = make_template(
        input_path, init_date if from_init_date else start_date, end_date, time_chunk_size, pressure_levels_group)

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

    temporal_variables_chunks = None
    if not only_initialize_store:
        load_temporal_data_for_date_do_fn = LoadTemporalDataForDateDoFn(
            data_path=input_path,
            start_date=init_date if from_init_date else start_date,
            pressure_levels_group=pressure_levels_group
        )
        logging.info("Setting up temporal variables.")
        temporal_variables_chunks = (
                root
                | "DayIterator" >> beam.Create(daily_date_iterator(start_date, end_date))
                | "TemporalDataForDay" >> beam.ParDo(load_temporal_data_for_date_do_fn)
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
    if known_args.init_date != "1900-01-01" and (not known_args.from_init_date):
        raise RuntimeError("--init_date can only be used along with --from_init_date flag.")

    pipeline_args.extend(['--save_main_session', 'True'])

    if fs.exists(known_args.output_path):
        raise ValueError(f"{known_args.output_path} already exists")

    num_steps_per_day = HOURS_PER_DAY // TIME_RESOLUTION_HOURS
    if num_steps_per_day % known_args.time_chunk_size != 0:
        raise ValueError(
            f"time_chunk_size {known_args.time_chunk_size} must evenly divide {num_steps_per_day}"
        )

    with beam.Pipeline(argv=pipeline_args) as root:
        define_pipeline(
            root,
            input_path=INPUT_PATH,
            output_path=known_args.output_path,
            start_date=known_args.start_date,
            end_date=known_args.end_date,
            time_chunk_size=known_args.time_chunk_size,
            pressure_levels_group=known_args.pressure_levels_group,
            init_date=known_args.init_date,
            from_init_date=known_args.from_init_date,
            only_initialize_store=known_args.only_initialize_store
        )


if __name__ == "__main__":
    main()
