"""Information and reading utils for the ERA 5 dataset."""

__author__ = 'Matthew Willson, Alvaro Sanchez, Peter Battaglia, Stephan Hoyer, Stephan Rasp'

import apache_beam as beam
import argparse
import datetime
import fsspec
import immutabledict
import logging

import pathlib

import numpy as np
import pandas as pd
import typing as t
import xarray as xr
import xarray_beam as xb

TIME_RESOLUTION_HOURS = 1

GCP_DIRECTORY = "gs://gcp-public-data-arco-era5/raw"

STATIC_SUBDIR_TEMPLATE = "date-variable-static/2021/12/31/{variable}/static.nc"

SINGLE_LEVEL_SUBDIR_TEMPLATE = (
    "date-variable-single_level/{year}/{month:02d}/"
    "{day:02d}/{variable}/surface.nc")

MULTILEVEL_SUBDIR_TEMPLATE = (
    "date-variable-pressure_level/{year}/{month:02d}/"
    "{day:02d}/{variable}/{pressure_level}.nc")

STATIC_VARIABLES = (
    "type_of_low_vegetation",
    "type_of_high_vegetation",
    "standard_deviation_of_orography",
    "standard_deviation_of_filtered_subgrid_orography",
    "soil_type",
    "slope_of_sub_gridscale_orography",
    "low_vegetation_cover",
    "land_sea_mask",
    "lake_depth",
    "lake_cover",
    "high_vegetation_cover",
    "geopotential_at_surface",
    "anisotropy_of_sub_gridscale_orography",
    "angle_of_sub_gridscale_orography",
)

# TODO(alvarosg): Add more variables.
SINGLE_LEVEL_VARIABLES = (
    "total_precipitation",
    "total_column_water_vapour",
    "total_cloud_cover",
    "toa_incident_solar_radiation",
    "surface_pressure",
    "sea_surface_temperature",
    "sea_ice_cover",
    "mean_sea_level_pressure",
    "2m_temperature",
    "10m_v_component_of_wind",
    "10m_u_component_of_wind",
)

MULTILEVEL_VARIABLES = (
    "geopotential",
    "specific_humidity",
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "vertical_velocity",
)

# Variables that correspond to an integration over a `TIME_RESOLUTION_HOURS`
# interval, rather than an instantaneous sample in time.
# As per:
# https://confluence.ecmwf.int/pages/viewpage.action?pageId=197702790
# "ERA5 reanalysis (hourly data): accumulations are over the hour
# (the accumulation/processing "period) ending at the validity date/time"
# TODO(alvarosg): Verify somehow that it is true that these variables are indeed
# integrated for 1 hour, and not longer, because it is a bit suspicious that
# we don't have values for these variables until 1959-01-01 07:00:00, while
# for all other variables we have values since 1959-01-01 00:00:00, which may
# indicate at 6-8h accumulation.
CUMULATIVE_VARIABLES = ("total_precipitation",
                        "toa_incident_solar_radiation")

PRESSURE_LEVELS_GROUPS = immutabledict.immutabledict({
    "weatherbench_13":
        (50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000),
    "full_37": (1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 125, 150, 175, 200, 225,
                250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800,
                825, 850, 875, 900, 925, 950, 975, 1000)
})

# ERA5 uses "geopotential" for file names for both the static "geopotential"
# feature at surface, and the dynamic multilevel "geopotential". To avoid this
# clash, we will always rename the static one to "geopotential_at_surface".
_VARIABLE_TO_ERA5_FILE_NAME = {
    "geopotential_at_surface": "geopotential"
}

HOURS_PER_DAY = 24

def _read_nc_dataset(gpath_file):
    """Read the .nc dataset from disk."""
    path = str(gpath_file).replace('gs:/', 'gs://')
    with fsspec.open(path, mode="rb") as fid:
        dataset = xr.open_dataset(fid, engine="scipy", cache=False)
    # All dataset have a single data array in them, so we just return the array.
    assert len(dataset) == 1
    dataarray = next(iter(dataset.values()))
    if "expver" in dataarray.coords:
        # Recent ERA5 downloads (within 2-3 months old) can include data from ERA5T,
        # which is the temporary release of ERA5, which has not been validated.
        # For such recent data, all instantaneous variables are from ERA5T, while
        # accumulated variables can be a mix of ERA5 and ERA5T, because the earliest
        # times of the accumulation window spans back from the range covered by
        # ERA5T into the range covered by ERA5.
        # Such NetCDF files indicate which ERA5 vs ERA5T with the `expver` axis,
        # where `expver=1` means ERA5 and `expver=5` means ERA5T. For ERA5 times,
        # NaNs are present in the `expver=5` slice, and for ERA5T times, NaNs are
        # present in the `expver=1` slice.
        # We combine and remove the `expver` axes using the below command.
        # See: https://confluence.ecmwf.int/pages/viewpage.action?pageId=173385064
        # and: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation#ERA5:datadocumentation-Dataupdatefrequency  # pylint: disable=line-too-long
        # for further details.

        all_dims_except_time = tuple(set(dataarray.dims) - {"time"})
        # Should have only trailing nans.
        a = dataarray.sel(expver=1).isnull().any(dim=all_dims_except_time)
        # Should having only leading nans.
        b = dataarray.sel(expver=5).isnull().any(dim=all_dims_except_time)
        disjoint_nans = bool(next(iter((a ^ b).all().data_vars.values())))
        assert disjoint_nans, "The nans are not disjoint in expver=1 vs 5"
        dataarray = dataarray.sel(expver=1).combine_first(dataarray.sel(expver=5))
    return dataarray


def read_static_vars(variables=STATIC_VARIABLES, root_path=GCP_DIRECTORY):
    """xr.Dataset with static variables for single level data from nc files."""
    root_path = pathlib.Path(root_path)
    output = {}
    for variable in variables:
        if variable in _VARIABLE_TO_ERA5_FILE_NAME:
            era5_variable = _VARIABLE_TO_ERA5_FILE_NAME[variable]
        else:
            era5_variable = variable
        relative_path = STATIC_SUBDIR_TEMPLATE.format(variable=era5_variable)
        output[variable] = _read_nc_dataset(root_path / relative_path)
    return xr.Dataset(output)


def read_single_level_vars(year, month, day, variables=SINGLE_LEVEL_VARIABLES,
                           root_path=GCP_DIRECTORY):
    """xr.Dataset with variables for singel level data from nc files."""
    root_path = pathlib.Path(root_path)
    output = {}
    for variable in variables:
        relative_path = SINGLE_LEVEL_SUBDIR_TEMPLATE.format(
            year=year, month=month, day=day, variable=variable)
        output[variable] = _read_nc_dataset(root_path / relative_path)
    return xr.Dataset(output)


def read_multilevel_vars(year,
                         month,
                         day,
                         variables=MULTILEVEL_VARIABLES,
                         pressure_levels=PRESSURE_LEVELS_GROUPS["full_37"],
                         root_path=GCP_DIRECTORY):
    """xr.Dataset with variables for all levels from nc files."""
    root_path = pathlib.Path(root_path)
    output = {}
    for variable in variables:
        pressure_data = []
        for pressure_level in pressure_levels:
            relative_path = MULTILEVEL_SUBDIR_TEMPLATE.format(
                year=year, month=month, day=day, variable=variable,
                pressure_level=pressure_level)
            single_level_data_array = _read_nc_dataset(root_path / relative_path)
            single_level_data_array.coords["level"] = pressure_level
            pressure_data.append(
                single_level_data_array.expand_dims(dim="level", axis=1))
        output[variable] = xr.concat(pressure_data, dim="level")
    return xr.Dataset(output)


def get_var_attrs_dict(root_path=GCP_DIRECTORY):
    """Returns attributes for all variables."""
    root_path = pathlib.Path(root_path)

    # The variable attributes should be independent of the date chosen here
    # so we just choose any date.
    year = 2021
    month = 1
    day = 1
    pressure_level = PRESSURE_LEVELS_GROUPS["full_37"][-1]

    # TODO(alvarosg): Store a version of this somewhere so we don't actually
    # have to download the data files to get this into.
    var_attrs_dict = {}
    for variables, template, rename_dict in [
        (STATIC_VARIABLES, STATIC_SUBDIR_TEMPLATE, _VARIABLE_TO_ERA5_FILE_NAME),
        (SINGLE_LEVEL_VARIABLES, SINGLE_LEVEL_SUBDIR_TEMPLATE, {}),
        (MULTILEVEL_VARIABLES, MULTILEVEL_SUBDIR_TEMPLATE, {}),
    ]:
        for variable in variables:
            if variable in rename_dict:
                era5_variable = rename_dict[variable]
            else:
                era5_variable = variable
            relative_path = template.format(
                year=year, month=month, day=day, variable=era5_variable,
                pressure_level=pressure_level)
            data_array = _read_nc_dataset(root_path / relative_path)
            data_array.attrs["short_name"] = data_array.name
            var_attrs_dict[variable] = data_array.attrs
    return var_attrs_dict


def daily_date_iterator(start_date: str, end_date: str
                        ) -> t.Iterable[t.Tuple[int, int, int]]:
    """Iterates all (year, month, day) tuples between start_date and end_date."""
    date_range = pd.date_range(start=start_date, end=end_date, inclusive='left')
    for date in date_range:
        yield date.year, date.month, date.day


def align_coordinates(dataset: xr.Dataset) -> xr.Dataset:
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

def get_pressure_levels_arg(pressure_levels_group: str):
    return PRESSURE_LEVELS_GROUPS[pressure_levels_group]

class LoadTemporalDataForDateDoFn(beam.DoFn):
    def __init__(self, data_path, start_date, pressure_levels_group):
        self.data_path = data_path
        self.start_date = start_date
        self.pressure_levels_group = pressure_levels_group

    def process(self, args):

        """Loads temporal data for a day, with an xarray_beam key for it.."""
        year, month, day = args
        logging.info("Loading NetCDF files for %d-%d-%d", year, month, day)

        try:
            single_level_vars = read_single_level_vars(
                year,
                month,
                day,
                variables=SINGLE_LEVEL_VARIABLES,
                root_path=self.data_path)
            multilevel_vars = read_multilevel_vars(
                year,
                month,
                day,
                variables=MULTILEVEL_VARIABLES,
                pressure_levels=get_pressure_levels_arg(self.pressure_levels_group),
                root_path=self.data_path)
        except BaseException as e:
            # Make sure we print the date as part of the error for easier debugging
            # if something goes wrong. Note "from e" will also raise the details of the
            # original exception.
            raise Exception(f"Error loading {year}-{month}-{day}") from e

        # It is crucial to actually "load" as otherwise we get a pickle error.
        single_level_vars = single_level_vars.load()
        multilevel_vars = multilevel_vars.load()

        dataset = xr.merge([single_level_vars, multilevel_vars])
        dataset = align_coordinates(dataset)
        offsets = {"latitude": 0, "longitude": 0, "level": 0,
                   "time": offset_along_time_axis(self.start_date, year, month, day)}
        key = xb.Key(offsets, vars=set(dataset.data_vars.keys()))
        logging.info("Finished loading NetCDF files for %s-%s-%s", year, month, day)
        yield key, dataset
        dataset.close()

def offset_along_time_axis(start_date: str, year: int, month: int, day: int) -> int:
    """Offset in indices along the time axis, relative to start of the dataset."""
    # Note the length of years can vary due to leap years, so the chunk lengths
    # will not always be the same, and we need to do a proper date calculation
    # not just multiply by 365*24.
    time_delta = pd.Timestamp(
        year=year, month=month, day=day) - pd.Timestamp(start_date)
    return time_delta.days * HOURS_PER_DAY // TIME_RESOLUTION_HOURS

def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--output_path", type=str, required=True,
                        help="Path to the destination Zarr archive.")
    parser.add_argument('-s', "--start_date", default='2020-01-01',
                        help='Start date, iso format string.')
    parser.add_argument('-e', "--end_date", default='2020-01-02',
                        help='End date, iso format string.')
    parser.add_argument('--find-missing', action='store_true', default=False,
                        help='Print all paths to missing input data.')  # implementation pending
    parser.add_argument("--pressure_levels_group", type=str, default="weatherbench_13",
                        help="Group label for the set of pressure levels to use.")
    parser.add_argument("--time_chunk_size", type=int, required=True,
                        help="Number of 1-hourly timesteps to include in a \
                        single chunk. Must evenly divide 24.")
    parser.add_argument("--init_date", type=str, default='1900-01-01',
                        help="Date to initialize the zarr store.")
    parser.add_argument("--from_init_date", action='store_true', default=False,
                        help="To initialize the store from some previous date (--init_date). i.e. 1900-01-01")
    parser.add_argument("--only_initialize_store", action='store_true', default=False,
                        help="Initialize zarr store without data.")

    return parser.parse_known_args()
