# Copyright 2023 Google LLC
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

"""Information and reading utils for the ERA 5 dataset."""

__author__ = 'Matthew Willson, Alvaro Sanchez, Peter Battaglia, Stephan Hoyer, Stephan Rasp'

import apache_beam as beam
import argparse
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
HOURS_PER_DAY = 24
INIT_TIME = '1900-01-01'

GCP_DIRECTORY = "gs://gcp-public-data-arco-era5/raw"

SINGLE_LEVEL_SUBDIR_TEMPLATE = (
    "date-variable-single_level/{year}/{month:02d}/"
    "{day:02d}/{variable}/surface.nc")

MULTILEVEL_SUBDIR_TEMPLATE = (
    "date-variable-pressure_level/{year}/{month:02d}/"
    "{day:02d}/{variable}/{pressure_level}.nc")

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
    "volumetric_soil_water_layer_1",
    "volumetric_soil_water_layer_2",
    "volumetric_soil_water_layer_3",
    "volumetric_soil_water_layer_4",
    "mean_top_net_long_wave_radiation_flux",
    "mean_top_net_short_wave_radiation_flux",
    "high_vegetation_cover",
    "ice_temperature_layer_1",
    "ice_temperature_layer_2",
    "ice_temperature_layer_3",
    "ice_temperature_layer_4",
    "lake_cover",
    "lake_depth",
    "lake_ice_depth",
    "lake_ice_temperature",
    "lake_mix_layer_depth",
    "lake_mix_layer_temperature",
    "lake_total_layer_temperature",
    "land_sea_mask",
    "leaf_area_index_high_vegetation",
    "leaf_area_index_low_vegetation",
    "low_vegetation_cover",
    "skin_temperature",
    "snow_depth",
    "soil_temperature_level_1",
    "soil_temperature_level_2",
    "soil_temperature_level_3",
    "soil_temperature_level_4",
    "soil_type",
    "type_of_high_vegetation",
    "type_of_low_vegetation",
    "100m_u_component_of_wind",
    "100m_v_component_of_wind",
    "10m_u_component_of_neutral_wind",
    "10m_v_component_of_neutral_wind",
    "10m_wind_gust_since_previous_post_processing",
    "2m_dewpoint_temperature",
    "air_density_over_the_oceans",
    "angle_of_sub_gridscale_orography",
    "anisotropy_of_sub_gridscale_orography",
    "benjamin_feir_index",
    "boundary_layer_dissipation",
    "boundary_layer_height",
    "charnock",
    "clear_sky_direct_solar_radiation_at_surface",
    "cloud_base_height",
    "coefficient_of_drag_with_waves",
    "convective_available_potential_energy",
    "convective_inhibition",
    "convective_precipitation",
    "convective_rain_rate",
    "convective_snowfall",
    "convective_snowfall_rate_water_equivalent",
    "downward_uv_radiation_at_the_surface",
    "duct_base_height",
    "eastward_gravity_wave_surface_stress",
    "eastward_turbulent_surface_stress",
    "evaporation",
    "forecast_albedo",
    "forecast_logarithm_of_surface_roughness_for_heat",
    "forecast_surface_roughness",
    "free_convective_velocity_over_the_oceans",
    "friction_velocity",
    "geopotential_at_surface",
    "gravity_wave_dissipation",
    "high_cloud_cover",
    "instantaneous_10m_wind_gust",
    "instantaneous_eastward_turbulent_surface_stress",
    "instantaneous_large_scale_surface_precipitation_fraction",
    "instantaneous_moisture_flux",
    "instantaneous_northward_turbulent_surface_stress",
    "instantaneous_surface_sensible_heat_flux",
    "k_index",
    "lake_bottom_temperature",
    "lake_shape_factor",
    "large_scale_precipitation",
    "large_scale_precipitation_fraction",
    "large_scale_rain_rate",
    "large_scale_snowfall",
    "large_scale_snowfall_rate_water_equivalent",
    "low_cloud_cover",
    "maximum_2m_temperature_since_previous_post_processing",
    "maximum_individual_wave_height",
    "maximum_total_precipitation_rate_since_previous_post_processing",
    "mean_boundary_layer_dissipation",
    "mean_convective_precipitation_rate",
    "mean_convective_snowfall_rate",
    "mean_direction_of_total_swell",
    "mean_direction_of_wind_waves",
    "mean_eastward_gravity_wave_surface_stress",
    "mean_eastward_turbulent_surface_stress",
    "mean_evaporation_rate",
    "mean_gravity_wave_dissipation",
    "mean_large_scale_precipitation_fraction",
    "mean_large_scale_precipitation_rate",
    "mean_large_scale_snowfall_rate",
    "mean_northward_gravity_wave_surface_stress",
    "mean_northward_turbulent_surface_stress",
    "mean_period_of_total_swell",
    "mean_period_of_wind_waves",
    "mean_potential_evaporation_rate",
    "mean_runoff_rate",
    "mean_snow_evaporation_rate",
    "mean_snowfall_rate",
    "mean_snowmelt_rate",
    "mean_square_slope_of_waves",
    "mean_sub_surface_runoff_rate",
    "mean_surface_direct_short_wave_radiation_flux",
    "mean_surface_direct_short_wave_radiation_flux_clear_sky",
    "mean_surface_downward_long_wave_radiation_flux",
    "mean_surface_downward_long_wave_radiation_flux_clear_sky",
    "mean_surface_downward_short_wave_radiation_flux",
    "mean_surface_downward_short_wave_radiation_flux_clear_sky",
    "mean_surface_downward_uv_radiation_flux",
    "mean_surface_latent_heat_flux",
    "mean_surface_net_long_wave_radiation_flux",
    "mean_surface_net_long_wave_radiation_flux_clear_sky",
    "mean_surface_net_short_wave_radiation_flux",
    "mean_surface_net_short_wave_radiation_flux_clear_sky",
    "mean_surface_runoff_rate",
    "mean_surface_sensible_heat_flux",
    "mean_top_downward_short_wave_radiation_flux",
    "mean_top_net_long_wave_radiation_flux_clear_sky",
    "mean_top_net_short_wave_radiation_flux_clear_sky",
    "mean_total_precipitation_rate",
    "mean_vertical_gradient_of_refractivity_inside_trapping_layer",
    "mean_vertically_integrated_moisture_divergence",
    "mean_wave_direction",
    "mean_wave_direction_of_first_swell_partition",
    "mean_wave_direction_of_second_swell_partition",
    "mean_wave_direction_of_third_swell_partition",
    "mean_wave_period",
    "mean_wave_period_based_on_first_moment",
    "mean_wave_period_based_on_first_moment_for_swell",
    "mean_wave_period_based_on_first_moment_for_wind_waves",
    "mean_wave_period_based_on_second_moment_for_swell",
    "mean_wave_period_based_on_second_moment_for_wind_waves",
    "mean_wave_period_of_first_swell_partition",
    "mean_wave_period_of_second_swell_partition",
    "mean_wave_period_of_third_swell_partition",
    "mean_zero_crossing_wave_period",
    "medium_cloud_cover",
    "minimum_2m_temperature_since_previous_post_processing",
    "minimum_total_precipitation_rate_since_previous_post_processing",
    "minimum_vertical_gradient_of_refractivity_inside_trapping_layer",
    "model_bathymetry",
    "near_ir_albedo_for_diffuse_radiation",
    "near_ir_albedo_for_direct_radiation",
    "normalized_energy_flux_into_ocean",
    "normalized_energy_flux_into_waves",
    "normalized_stress_into_ocean",
    "northward_gravity_wave_surface_stress",
    "northward_turbulent_surface_stress",
    "ocean_surface_stress_equivalent_10m_neutral_wind_direction",
    "ocean_surface_stress_equivalent_10m_neutral_wind_speed",
    "peak_wave_period",
    "period_corresponding_to_maximum_individual_wave_height",
    "potential_evaporation",
    "precipitation_type",
    "runoff",
    "significant_height_of_combined_wind_waves_and_swell",
    "significant_height_of_total_swell",
    "significant_height_of_wind_waves",
    "significant_wave_height_of_first_swell_partition",
    "significant_wave_height_of_second_swell_partition",
    "significant_wave_height_of_third_swell_partition",
    "skin_reservoir_content",
    "slope_of_sub_gridscale_orography",
    "snow_albedo",
    "snow_density",
    "snow_evaporation",
    "snowfall",
    "snowmelt",
    "standard_deviation_of_filtered_subgrid_orography",
    "standard_deviation_of_orography",
    "sub_surface_runoff",
    "surface_latent_heat_flux",
    "surface_net_solar_radiation",
    "surface_net_solar_radiation_clear_sky",
    "surface_net_thermal_radiation",
    "surface_net_thermal_radiation_clear_sky",
    "surface_runoff",
    "surface_sensible_heat_flux",
    "surface_solar_radiation_downward_clear_sky",
    "surface_solar_radiation_downwards",
    "surface_thermal_radiation_downward_clear_sky",
    "surface_thermal_radiation_downwards",
    "temperature_of_snow_layer",
    "top_net_solar_radiation",
    "top_net_solar_radiation_clear_sky",
    "top_net_thermal_radiation",
    "top_net_thermal_radiation_clear_sky",
    "total_column_cloud_ice_water",
    "total_column_cloud_liquid_water",
    "total_column_ozone",
    "total_column_rain_water",
    "total_column_snow_water",
    "total_column_supercooled_liquid_water",
    "total_column_water",
    "total_sky_direct_solar_radiation_at_surface",
    "total_totals_index",
    "trapping_layer_base_height",
    "trapping_layer_top_height",
    "u_component_stokes_drift",
    "uv_visible_albedo_for_diffuse_radiation",
    "uv_visible_albedo_for_direct_radiation",
    "v_component_stokes_drift",
    "vertical_integral_of_divergence_of_cloud_frozen_water_flux",
    "vertical_integral_of_divergence_of_cloud_liquid_water_flux",
    "vertical_integral_of_divergence_of_geopotential_flux",
    "vertical_integral_of_divergence_of_kinetic_energy_flux",
    "vertical_integral_of_divergence_of_mass_flux",
    "vertical_integral_of_divergence_of_moisture_flux",
    "vertical_integral_of_divergence_of_ozone_flux",
    "vertical_integral_of_divergence_of_thermal_energy_flux",
    "vertical_integral_of_divergence_of_total_energy_flux",
    "vertical_integral_of_eastward_cloud_frozen_water_flux",
    "vertical_integral_of_eastward_cloud_liquid_water_flux",
    "vertical_integral_of_eastward_geopotential_flux",
    "vertical_integral_of_eastward_heat_flux",
    "vertical_integral_of_eastward_kinetic_energy_flux",
    "vertical_integral_of_eastward_mass_flux",
    "vertical_integral_of_eastward_ozone_flux",
    "vertical_integral_of_eastward_total_energy_flux",
    "vertical_integral_of_eastward_water_vapour_flux",
    "vertical_integral_of_energy_conversion",
    "vertical_integral_of_kinetic_energy",
    "vertical_integral_of_mass_of_atmosphere",
    "vertical_integral_of_mass_tendency",
    "vertical_integral_of_northward_cloud_frozen_water_flux",
    "vertical_integral_of_northward_cloud_liquid_water_flux",
    "vertical_integral_of_northward_geopotential_flux",
    "vertical_integral_of_northward_heat_flux",
    "vertical_integral_of_northward_kinetic_energy_flux",
    "vertical_integral_of_northward_mass_flux",
    "vertical_integral_of_northward_ozone_flux",
    "vertical_integral_of_northward_total_energy_flux",
    "vertical_integral_of_northward_water_vapour_flux",
    "vertical_integral_of_potential_and_internal_energy",
    "vertical_integral_of_potential_internal_and_latent_energy",
    "vertical_integral_of_temperature",
    "vertical_integral_of_thermal_energy",
    "vertical_integral_of_total_energy",
    "vertically_integrated_moisture_divergence",
    "wave_spectral_directional_width",
    "wave_spectral_directional_width_for_swell",
    "wave_spectral_directional_width_for_wind_waves",
    "wave_spectral_kurtosis",
    "wave_spectral_peakedness",
    "wave_spectral_skewness",
    "zero_degree_level"
)

MULTILEVEL_VARIABLES = (
    "geopotential",
    "specific_humidity",
    "temperature",
    "u_component_of_wind",
    "v_component_of_wind",
    "vertical_velocity",
    "potential_vorticity",
    "specific_cloud_ice_water_content",
    "specific_cloud_liquid_water_content",
    "fraction_of_cloud_cover",
    "ozone_mass_mixing_ratio"
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


def _read_nc_dataset(gpath_file):
    """Read a .nc NetCDF dataset from a cloud storage path and disk.

    Args:
        gpath_file (str): The cloud storage path to the NetCDF file.

    Returns:
        xarray.DataArray: The loaded NetCDF dataset.

    This function reads a NetCDF dataset from a cloud storage path and combines data
    from ERA5 and ERA5T versions if necessary.

    Example:
        >>> data = _read_nc_dataset("gs://bucket/data/era5_data.nc")
    """
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

        all_dims_except_time_and_expver = tuple(set(dataarray.dims) - {"time", "expver"})
        # Should have only trailing nans.
        a = dataarray.sel(expver=1).isnull().any(dim=all_dims_except_time_and_expver)
        # Should having only leading nans.
        b = dataarray.sel(expver=5).isnull().any(dim=all_dims_except_time_and_expver)
        disjoint_nans = bool((a ^ b).all().variable.values)
        assert disjoint_nans, "The nans are not disjoint in expver=1 vs 5"
        dataarray = dataarray.sel(expver=1).combine_first(dataarray.sel(expver=5))
    return dataarray


def read_single_level_vars(year, month, day, variables=SINGLE_LEVEL_VARIABLES,
                           root_path=GCP_DIRECTORY):
    """Read single-level variables for a specific date and return an xarray.Dataset.

    Args:
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        variables (list): List of variable names to read.
        root_path (str): Root directory where the NetCDF files are located.

    Returns:
        xarray.Dataset: A dataset containing the requested single-level
        variables for the given date.

    This function reads single-level variables from NetCDF files for a specific date
    and returns them as an xarray.Dataset.

    Example:
        >>> date_data = read_single_level_vars(2023, 9, 11, ["temperature", "humidity"])
    """
    root_path = pathlib.Path(root_path)
    output = {}
    for variable in variables:
        if variable in _VARIABLE_TO_ERA5_FILE_NAME:
            era5_variable = _VARIABLE_TO_ERA5_FILE_NAME[variable]
        else:
            era5_variable = variable
        relative_path = SINGLE_LEVEL_SUBDIR_TEMPLATE.format(
            year=year, month=month, day=day, variable=era5_variable)
        output[variable] = _read_nc_dataset(root_path / relative_path)
    return xr.Dataset(output)


def read_multilevel_vars(year,
                         month,
                         day,
                         variables=MULTILEVEL_VARIABLES,
                         pressure_levels=PRESSURE_LEVELS_GROUPS["full_37"],
                         root_path=GCP_DIRECTORY):
    """Read multilevel variables for a specific date and return an xarray.Dataset.

    Args:
        year (int): Year of the data.
        month (int): Month of the data.
        day (int): Day of the data.
        variables (list): List of variable names to read.
        pressure_levels (list): List of pressure levels to read.
        root_path (str): Root directory where the NetCDF files are located.

    Returns:
        xarray.Dataset: A dataset containing the requested multilevel variables for
        the given date.

    This function reads multilevel variables from NetCDF files for a specific date and
    returns them as an xarray.Dataset.

    Example:
        >>> date_data = read_multilevel_vars(2023, 9, 11, ["temperature", "humidity"],
        [1000, 850, 500])
    """
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
    """Return a dictionary of attributes for all variables.

    Args:
        root_path (str): Root directory where the NetCDF files are located.

    Returns:
        dict: A dictionary containing variable attributes.

    This function retrieves attributes for all variables from NetCDF files.

    Example:
        >>> variable_attrs = get_var_attrs_dict()
    """
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
        (SINGLE_LEVEL_VARIABLES, SINGLE_LEVEL_SUBDIR_TEMPLATE,
         _VARIABLE_TO_ERA5_FILE_NAME),
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
    """Iterate through all (year, month, day) tuples between start_date and
    end_date (inclusive).

    Args:
        start_date (str): The start date in ISO format (YYYY-MM-DD).
        end_date (str): The end date in ISO format (YYYY-MM-DD).

    Yields:
        tuple: A tuple containing the year, month, and day for each date in the range.

    Example:
        >>> for year, month, day in daily_date_iterator('2023-09-01', '2023-09-05'):
        ...     print(f"Year: {year}, Month: {month}, Day: {day}")
        Year: 2023, Month: 9, Day: 1
        Year: 2023, Month: 9, Day: 2
        Year: 2023, Month: 9, Day: 3
        Year: 2023, Month: 9, Day: 4
        Year: 2023, Month: 9, Day: 5
    """
    date_range = pd.date_range(start=start_date, end=end_date)
    for date in date_range:
        yield date.year, date.month, date.day


def align_coordinates(dataset: xr.Dataset) -> xr.Dataset:
    """Align coordinates of variables in the dataset before consolidation.

    Args:
        dataset (xr.Dataset): The dataset containing variables.

    Returns:
        xr.Dataset: The dataset with aligned coordinates.

    This function removes non-index coordinates and downcasts latitude and longitude
    coordinates to float32.
    """

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
    """A Beam DoFn for loading temporal data for a specific date.

    This class is responsible for loading temporal data for a given date, including both
    single-level and pressure-level variables.
    Args:
        data_path (str): The path to the data source.
        start_date (str): The start date in ISO format (YYYY-MM-DD).
        pressure_levels_group (str): The group label for the set of pressure levels.
    Methods:
        process(args): Loads temporal data for a specific date and yields it with an xarray_beam key.
    Example:
        >>> data_path = "gs://your-bucket/data/"
        >>> start_date = "2023-09-01"
        >>> pressure_levels_group = "weatherbench_13"
        >>> loader = LoadTemporalDataForDateDoFn(data_path, start_date, pressure_levels_group)
        >>> for result in loader.process((2023, 9, 11)):
        ...     key, dataset = result
        ...     print(f"Loaded data for key: {key}")
        ...     print(dataset)
    """
    def __init__(self, data_path, start_date, pressure_levels_group):
        """Initialize the LoadTemporalDataForDateDoFn.
        Args:
            data_path (str): The path to the data source.
            start_date (str): The start date in ISO format (YYYY-MM-DD).
            pressure_levels_group (str): The group label for the set of pressure levels.
        """
        self.data_path = data_path
        self.start_date = start_date
        self.pressure_levels_group = pressure_levels_group

    def process(self, args):
        """Load temporal data for a day, with an xarray_beam key for it.
        Args:
            args (tuple): A tuple containing the year, month, and day.
        Yields:
            tuple: A tuple containing an xarray_beam key and the loaded dataset.
        """
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
            raise RuntimeError(f"Error loading {year}-{month}-{day}") from e

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
    """Parse command-line arguments for the data processing pipeline.

    Args:
        desc (str): A description of the command-line interface.

    Returns:
        tuple: A tuple containing the parsed arguments as a namespace and a list of
        unknown arguments.

    Example:
        To parse command-line arguments, you can call this function like this:
        >>> parsed_args, unknown_args = parse_arguments("Data Processing Pipeline")
    """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--output_path", type=str, required=True,
                        help="Path to the destination Zarr archive.")
    parser.add_argument('-s', "--start_date", default='2020-01-01',
                        help='Start date, iso format string.')
    parser.add_argument('-e', "--end_date", default='2020-01-02',
                        help='End date, iso format string.')
    parser.add_argument("--pressure_levels_group", type=str, default="weatherbench_13",
                        help="Group label for the set of pressure levels to use.")
    parser.add_argument("--time_chunk_size", type=int, required=True,
                        help="Number of 1-hourly timesteps to include in a \
                        single chunk. Must evenly divide 24.")
    parser.add_argument("--init_date", type=str, default=INIT_TIME,
                        help="Date to initialize the zarr store.")
    parser.add_argument("--from_init_date", action='store_true', default=False,
                        help="To initialize the store from some previous date (--init_date). i.e. 1900-01-01")
    parser.add_argument("--only_initialize_store", action='store_true', default=False,
                        help="Initialize zarr store without data.")

    return parser.parse_known_args()
