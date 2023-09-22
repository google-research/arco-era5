import argparse
import datetime
import gcsfs
import itertools
import logging
import multiprocessing
import os
import re
import subprocess
import zarr

import pandas as pd
import typing as t
import xarray as xr

from arco_era5 import update_config_files, get_previous_month_dates, get_secret

# DIRECTORY = "/weather/config_files"
DIRECTORY = "/usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/raw"
FIELD_NAME = "date"
PROJECT = os.environ.get("PROJECT")
PROJECT1 = os.environ.get("PROJECT1")
REGION = os.environ.get("REGION")
BUCKET = os.environ.get("BUCKET")
SDK_CONTAINER_IMAGE = os.environ.get("SDK_CONTAINER_IMAGE")
# MANIFEST_LOCATION = os.environ.get("MANIFEST_LOCATION") # 
API_KEY_PATTERN = re.compile(r"^API_KEY_\d+$")
API_KEY_LIST = []

# File Templates
MODELLEVEL_DIR_VAR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/{year:04d}/{year:04d}{month:02d}{day:02d}_hres_{chunk}.grb2_{level}_{var}.grib"
MODELLEVEL_DIR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Daily/{year:04d}/{year:04d}{month:02d}{day:02d}_hres_{chunk}.grb2"
SINGLELEVEL_DIR_VAR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year:04d}/{year:04d}{month:02d}_hres_{chunk}.grb2_{level}_{var}.grib"
SINGLELEVEL_DIR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{year:04d}/{year:04d}{month:02d}_hres_{chunk}.grb2"
PRESSURELEVEL_DIR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/{pressure}.nc"
AR_SINGLELEVEL_DIR_TEMPLATE = "gs://gcp-public-data-arco-era5/raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/surface.nc"
AR_SINGLELEVEL_DIR_TEMPLATE_LOCAL = "gs://dabhis_temp/raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/{chunk}/surface.nc"


# Data Chunks
model_level_chunks = ["dve", "tw", "o3q", "qrqs"]
single_level_chunks = ["cape", "cisst", "sfc", "tcol", "soil_depthBelowLandLayer_istl1",
                       "soil_depthBelowLandLayer_istl2", "soil_depthBelowLandLayer_istl3",
                       "soil_depthBelowLandLayer_istl4", "soil_depthBelowLandLayer_stl1",
                       "soil_depthBelowLandLayer_stl2", "soil_depthBelowLandLayer_stl3",
                       "soil_depthBelowLandLayer_stl4", "soil_depthBelowLandLayer_swvl1",
                       "soil_depthBelowLandLayer_swvl2", "soil_depthBelowLandLayer_swvl3",
                       "soil_depthBelowLandLayer_swvl4", "soil_surface_tsn", "lnsp", "zs",
                       "rad", "pcp_surface_cp", "pcp_surface_crr", "pcp_surface_csf",
                       "pcp_surface_csfr", "pcp_surface_es", "pcp_surface_lsf",
                       "pcp_surface_lsp", "pcp_surface_lspf", "pcp_surface_lsrr",
                       "pcp_surface_lssfr", "pcp_surface_ptype", "pcp_surface_rsn",
                       "pcp_surface_sd", "pcp_surface_sf", "pcp_surface_smlt",
                       "pcp_surface_tp"]
pressure_level_chunks = ["geopotential", "specific_humidity", "temperature",
                         "u_component_of_wind", "v_component_of_wind", "vertical_velocity",
                         "potential_vorticity", "specific_cloud_ice_water_content",
                         "specific_cloud_liquid_water_content","fraction_of_cloud_cover",
                         "ozone_mass_mixing_ratio"]
ar_single_level_chunks = ["total_precipitation", "total_column_water_vapour", "total_cloud_cover", 
                          "toa_incident_solar_radiation", "surface_pressure", "sea_surface_temperature", 
                          "sea_ice_cover", "mean_sea_level_pressure", "2m_temperature", "10m_v_component_of_wind", 
                          "10m_u_component_of_wind", "volumetric_soil_water_layer_1", "volumetric_soil_water_layer_2", 
                          "volumetric_soil_water_layer_3", "volumetric_soil_water_layer_4", 
                          "mean_top_net_long_wave_radiation_flux", "mean_top_net_short_wave_radiation_flux", 
                          "high_vegetation_cover", "ice_temperature_layer_1", "ice_temperature_layer_2", 
                          "ice_temperature_layer_3", "ice_temperature_layer_4", "lake_cover", "lake_depth", 
                          "lake_ice_depth", "lake_ice_temperature", "lake_mix_layer_depth", "lake_mix_layer_temperature", 
                          "lake_total_layer_temperature", "land_sea_mask", "leaf_area_index_high_vegetation", 
                          "leaf_area_index_low_vegetation", "low_vegetation_cover", "skin_temperature", 
                          "snow_depth", "soil_temperature_level_1", "soil_temperature_level_2", "soil_temperature_level_3", 
                          "soil_temperature_level_4", "soil_type", "type_of_high_vegetation", "type_of_low_vegetation", 
                          "100m_u_component_of_wind", "100m_v_component_of_wind", "10m_u_component_of_neutral_wind", 
                          "10m_v_component_of_neutral_wind", "10m_wind_gust_since_previous_post_processing", 
                          "2m_dewpoint_temperature", "air_density_over_the_oceans", "angle_of_sub_gridscale_orography", 
                          "anisotropy_of_sub_gridscale_orography", "benjamin_feir_index", "boundary_layer_dissipation", 
                          "boundary_layer_height", "charnock", "clear_sky_direct_solar_radiation_at_surface", "cloud_base_height", 
                          "coefficient_of_drag_with_waves", "convective_available_potential_energy", "convective_inhibition", 
                          "convective_precipitation", "convective_rain_rate", "convective_snowfall", 
                          "convective_snowfall_rate_water_equivalent", "downward_uv_radiation_at_the_surface", "duct_base_height", 
                          "eastward_gravity_wave_surface_stress", "eastward_turbulent_surface_stress", "evaporation", "forecast_albedo", 
                          "forecast_logarithm_of_surface_roughness_for_heat", "forecast_surface_roughness", 
                          "free_convective_velocity_over_the_oceans", "friction_velocity", "geopotential_at_surface", 
                          "gravity_wave_dissipation", "high_cloud_cover", "instantaneous_10m_wind_gust", 
                          "instantaneous_eastward_turbulent_surface_stress", "instantaneous_large_scale_surface_precipitation_fraction", 
                          "instantaneous_moisture_flux", "instantaneous_northward_turbulent_surface_stress", 
                          "instantaneous_surface_sensible_heat_flux", "k_index", "lake_bottom_temperature", 
                          "lake_shape_factor", "large_scale_precipitation", "large_scale_precipitation_fraction", 
                          "large_scale_rain_rate", "large_scale_snowfall", "large_scale_snowfall_rate_water_equivalent", 
                          "low_cloud_cover", "maximum_2m_temperature_since_previous_post_processing", "maximum_individual_wave_height", 
                          "maximum_total_precipitation_rate_since_previous_post_processing", "mean_boundary_layer_dissipation", 
                          "mean_convective_precipitation_rate", "mean_convective_snowfall_rate", "mean_direction_of_total_swell", 
                          "mean_direction_of_wind_waves", "mean_eastward_gravity_wave_surface_stress", 
                          "mean_eastward_turbulent_surface_stress", "mean_evaporation_rate", "mean_gravity_wave_dissipation", 
                          "mean_large_scale_precipitation_fraction", "mean_large_scale_precipitation_rate", 
                          "mean_large_scale_snowfall_rate", "mean_northward_gravity_wave_surface_stress", 
                          "mean_northward_turbulent_surface_stress", "mean_period_of_total_swell", "mean_period_of_wind_waves", 
                          "mean_potential_evaporation_rate", "mean_runoff_rate", "mean_snow_evaporation_rate", 
                          "mean_snowfall_rate", "mean_snowmelt_rate", "mean_square_slope_of_waves", "mean_sub_surface_runoff_rate", 
                          "mean_surface_direct_short_wave_radiation_flux", "mean_surface_direct_short_wave_radiation_flux_clear_sky", 
                          "mean_surface_downward_long_wave_radiation_flux", "mean_surface_downward_long_wave_radiation_flux_clear_sky", 
                          "mean_surface_downward_short_wave_radiation_flux", "mean_surface_downward_short_wave_radiation_flux_clear_sky", 
                          "mean_surface_downward_uv_radiation_flux", "mean_surface_latent_heat_flux", 
                          "mean_surface_net_long_wave_radiation_flux", "mean_surface_net_long_wave_radiation_flux_clear_sky", 
                          "mean_surface_net_short_wave_radiation_flux", "mean_surface_net_short_wave_radiation_flux_clear_sky", 
                          "mean_surface_runoff_rate", "mean_surface_sensible_heat_flux", "mean_top_downward_short_wave_radiation_flux", 
                          "mean_top_net_long_wave_radiation_flux_clear_sky", "mean_top_net_short_wave_radiation_flux_clear_sky", 
                          "mean_total_precipitation_rate", "mean_vertical_gradient_of_refractivity_inside_trapping_layer", 
                          "mean_vertically_integrated_moisture_divergence", "mean_wave_direction", 
                          "mean_wave_direction_of_first_swell_partition", "mean_wave_direction_of_second_swell_partition", 
                          "mean_wave_direction_of_third_swell_partition", "mean_wave_period", "mean_wave_period_based_on_first_moment", 
                          "mean_wave_period_based_on_first_moment_for_swell", "mean_wave_period_based_on_first_moment_for_wind_waves", 
                          "mean_wave_period_based_on_second_moment_for_swell", "mean_wave_period_based_on_second_moment_for_wind_waves", 
                          "mean_wave_period_of_first_swell_partition", "mean_wave_period_of_second_swell_partition", 
                          "mean_wave_period_of_third_swell_partition", "mean_zero_crossing_wave_period", "medium_cloud_cover", 
                          "minimum_2m_temperature_since_previous_post_processing", 
                          "minimum_total_precipitation_rate_since_previous_post_processing", 
                          "minimum_vertical_gradient_of_refractivity_inside_trapping_layer", 
                          "model_bathymetry", "near_ir_albedo_for_diffuse_radiation", "near_ir_albedo_for_direct_radiation", 
                          "normalized_energy_flux_into_ocean", "normalized_energy_flux_into_waves", "normalized_stress_into_ocean", 
                          "northward_gravity_wave_surface_stress", "northward_turbulent_surface_stress", 
                          "ocean_surface_stress_equivalent_10m_neutral_wind_direction", 
                          "ocean_surface_stress_equivalent_10m_neutral_wind_speed", "peak_wave_period", 
                          "period_corresponding_to_maximum_individual_wave_height", "potential_evaporation", 
                          "precipitation_type", "runoff", "significant_height_of_combined_wind_waves_and_swell", 
                          "significant_height_of_total_swell", "significant_height_of_wind_waves", 
                          "significant_wave_height_of_first_swell_partition", "significant_wave_height_of_second_swell_partition", 
                          "significant_wave_height_of_third_swell_partition", "skin_reservoir_content", "slope_of_sub_gridscale_orography", 
                          "snow_albedo", "snow_density", "snow_evaporation", "snowfall", "snowmelt", 
                          "standard_deviation_of_filtered_subgrid_orography", "standard_deviation_of_orography", "sub_surface_runoff", 
                          "surface_latent_heat_flux", "surface_net_solar_radiation", "surface_net_solar_radiation_clear_sky", 
                          "surface_net_thermal_radiation", "surface_net_thermal_radiation_clear_sky", "surface_runoff", 
                          "surface_sensible_heat_flux", "surface_solar_radiation_downward_clear_sky", "surface_solar_radiation_downwards", 
                          "surface_thermal_radiation_downward_clear_sky", "surface_thermal_radiation_downwards", "temperature_of_snow_layer", 
                          "top_net_solar_radiation", "top_net_solar_radiation_clear_sky", "top_net_thermal_radiation", 
                          "top_net_thermal_radiation_clear_sky", "total_column_cloud_ice_water", "total_column_cloud_liquid_water", 
                          "total_column_ozone", "total_column_rain_water", "total_column_snow_water", 
                          "total_column_supercooled_liquid_water", "total_column_water", "total_sky_direct_solar_radiation_at_surface", 
                          "total_totals_index", "trapping_layer_base_height", "trapping_layer_top_height", "u_component_stokes_drift", 
                          "uv_visible_albedo_for_diffuse_radiation", "uv_visible_albedo_for_direct_radiation", "v_component_stokes_drift", 
                          "vertical_integral_of_divergence_of_cloud_frozen_water_flux", 
                          "vertical_integral_of_divergence_of_cloud_liquid_water_flux", 
                          "vertical_integral_of_divergence_of_geopotential_flux", "vertical_integral_of_divergence_of_kinetic_energy_flux", 
                          "vertical_integral_of_divergence_of_mass_flux", "vertical_integral_of_divergence_of_moisture_flux", 
                          "vertical_integral_of_divergence_of_ozone_flux", "vertical_integral_of_divergence_of_thermal_energy_flux", 
                          "vertical_integral_of_divergence_of_total_energy_flux", "vertical_integral_of_eastward_cloud_frozen_water_flux", 
                          "vertical_integral_of_eastward_cloud_liquid_water_flux", "vertical_integral_of_eastward_geopotential_flux", 
                          "vertical_integral_of_eastward_heat_flux", "vertical_integral_of_eastward_kinetic_energy_flux", 
                          "vertical_integral_of_eastward_mass_flux", "vertical_integral_of_eastward_ozone_flux", 
                          "vertical_integral_of_eastward_total_energy_flux", "vertical_integral_of_eastward_water_vapour_flux", 
                          "vertical_integral_of_energy_conversion", "vertical_integral_of_kinetic_energy", 
                          "vertical_integral_of_mass_of_atmosphere", "vertical_integral_of_mass_tendency", 
                          "vertical_integral_of_northward_cloud_frozen_water_flux", "vertical_integral_of_northward_cloud_liquid_water_flux", 
                          "vertical_integral_of_northward_geopotential_flux", "vertical_integral_of_northward_heat_flux", 
                          "vertical_integral_of_northward_kinetic_energy_flux", "vertical_integral_of_northward_mass_flux", 
                          "vertical_integral_of_northward_ozone_flux", "vertical_integral_of_northward_total_energy_flux", 
                          "vertical_integral_of_northward_water_vapour_flux", "vertical_integral_of_potential_and_internal_energy", 
                          "vertical_integral_of_potential_internal_and_latent_energy", "vertical_integral_of_temperature", 
                          "vertical_integral_of_thermal_energy", "vertical_integral_of_total_energy", 
                          "vertically_integrated_moisture_divergence", "wave_spectral_directional_width", 
                          "wave_spectral_directional_width_for_swell", "wave_spectral_directional_width_for_wind_waves", 
                          "wave_spectral_kurtosis", "wave_spectral_peakedness", "wave_spectral_skewness", "zero_degree_level"]

ar_single_level_chunks_local = ['2m_temperature']
pressure_level = [1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 125, 150, 175, 200, 225, 250,
                  300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850,
                  875, 900, 925, 950, 975, 1000]

ZARR_FILES_LIST = ['gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
                   'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2',
                   'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2',
                   'gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2',
                   'gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2',
                   'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2']

ZARR_LOCAL = ['gs://dabhis_temp/ar/11-full_37-1h-0p25deg-chunk-1-2023-3months.zarr-v3']
BQ_LOCAL = ["grid-intelligence-sandbox.dabhis_test.full-11_37-1h-0p25deg-chunk-1-v3-2023-3month"]

BQ_TABLES_LIST = ["grid-intelligence-sandbox.dabhis_test.full_37-1h-0p25deg-chunk-1-v3",
                  "grid-intelligence-sandbox.dabhis_test.model-level-moisture-v2",
                  "grid-intelligence-sandbox.dabhis_test.model-level-wind-v2",
                  "grid-intelligence-sandbox.dabhis_test.single-level-forecast-v2",
                  "grid-intelligence-sandbox.dabhis_test.single-level-reanalysis-v2",
                  "grid-intelligence-sandbox.dabhis_test.single-level-surface-v2"]

# Logger Configuration
logger = logging.getLogger(__name__)

dates_data = get_previous_month_dates()

# Functions
def date_range(start_date: str, end_date: str) -> t.List[datetime.datetime]:
    """
    Generates a list of datetime objects within a given date range.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
        List[datetime.datetime]: A list of datetime objects.
    """
    return [
        ts.to_pydatetime()
        for ts in pd.date_range(start=start_date, end=end_date, freq="D").to_list()
    ]


def replace_non_alphanumeric_with_hyphen(input_string):
    # Use a regular expression to replace non-alphanumeric characters with hyphens
    return re.sub(r'[^a-z0-9-]', '-', input_string)


def subprocess_run(command: str):
    """
    Runs a subprocess with the given command and prints the output.

    Args:
        command (str): The command to run.
    """

    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    with process.stdout:
        try:
            for line in iter(process.stdout.readline, b""):
                log_message = line.decode("utf-8").strip()
                logger.info(log_message)
                print(log_message)
        except subprocess.CalledProcessError as e:
            logger.error(
                f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}'
            )
            print(f'Failed to execute dataflow job due to {e.stderr.decode("utf-8")}')


def raw_data_download_dataflow_job():
    """
    Launches a Dataflow job to process weather data.
    """
    current_day = datetime.date.today()
    job_name = f"raw-data-download-arco-era5-{current_day.month}-{current_day.year}"

    command = (
        f"python weather_dl/weather-dl /weather/config_files/era5_ml_dve.cfg --runner "
        f"DataflowRunner --project {PROJECT} --region {REGION} --temp_location "
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--sdk_container_image {SDK_CONTAINER_IMAGE} --experiment use_runner_v2 --use-local-code"
        # f"--manifest-location {MANIFEST_LOCATION} "
    )
    local_command = (
        f"python weather_dl/weather-dl /usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/raw/era5_sl_hourly.cfg --runner "
        f"DataflowRunner --project {PROJECT} --region {REGION} --temp_location "
        f'"gs://{BUCKET}/tmp/" --disk_size_gb 260 --job_name {job_name} '
        f"--sdk_container_image {SDK_CONTAINER_IMAGE} --experiment use_runner_v2 --use-local-code"
        # f"--manifest-location {MANIFEST_LOCATION} "
    )
    subprocess_run(local_command)
    

def check_data_availability(co_date_range: t.List, ar_date_range: t.List):
    """
    Checks the availability of data for a given date range.

    Args:
        co_date_range (List[datetime.datetime]): Date range for climate data.
        ar_date_range (List[datetime.datetime]): Date range for atmospheric reanalysis data.

    Returns:
        int: 1 if data is missing, 0 if data is available.
    """

    PROGRESS = itertools.cycle("".join([c * 10 for c in "|/–-\\"]))
    fs = gcsfs.GCSFileSystem(project="grid-intelligence-sandbox")  # update with ai-for-weather
    all_uri = []

    for date in co_date_range:
        for chunk in model_level_chunks:
            if "_" in chunk:
                chunk_, level, var = chunk.split("_")
                all_uri.append(
                    MODELLEVEL_DIR_VAR_TEMPLATE.format( year=date.year, month=date.month, 
                                                       day=date.day, chunk=chunk_, 
                                                       level=level, var=var))
                continue
            all_uri.append(
                MODELLEVEL_DIR_TEMPLATE.format(
                    year=date.year, month=date.month, day=date.day, chunk=chunk))

    for chunk in single_level_chunks:
        if "_" in chunk:
            chunk_, level, var = chunk.split("_")
            all_uri.append(
                SINGLELEVEL_DIR_VAR_TEMPLATE.format(
                    year=date.year, month=date.month, chunk=chunk_, level=level, var=var))
            continue
        all_uri.append(
            SINGLELEVEL_DIR_TEMPLATE.format(
                year=date.year, month=date.month, chunk=chunk))
    local_all_uri = []
    for date in ar_date_range:
        for chunk in pressure_level_chunks + ar_single_level_chunks_local:
            if chunk in pressure_level_chunks:
                for pressure in pressure_level:
                    all_uri.append(
                        PRESSURELEVEL_DIR_TEMPLATE.format(year=date.year,month=date.month, 
                                                          day=date.day, chunk=chunk, 
                                                          pressure=pressure))
            else:
                local_all_uri.append(
                    AR_SINGLELEVEL_DIR_TEMPLATE_LOCAL.format(
                        year=date.year, month=date.month, day=date.day, chunk=chunk))

    data_is_missing = False
    for path in local_all_uri:
        print(next(PROGRESS), end="\r")
        if not fs.exists(path):
            data_is_missing = True
            print(path)

    return 1 if data_is_missing else 0


def convert_to_date(date_str: str) -> datetime.date:
    """
    Converts a date string in the format 'YYYY-MM-DD' to a datetime object.

    Args:
        date_str (str): The date string to convert.

    Returns:
        datetime.datetime: A datetime object representing the input date.
    """
    return datetime.datetime.strptime(date_str, '%Y-%m-%d').date()


def resize_zarr_target(target_store: str, end_date: datetime, init_date: str, interval: int = 24) -> None:
    """
    Resizes a Zarr target and consolidates metadata.

    Args:
        target_store (str): The Zarr target store path.
        end_date (str): The end date in the format 'YYYY-MM-DD'.
        init_date (str): The initial date of the zarr store in the format of str.
        interval (int, optional): The interval to use for resizing. Default is 24.

    Returns:
        None
    """
    print("inside the file.")
    ds = xr.open_zarr(target_store)
    print("dataset opened.")
    zf = zarr.open(target_store)
    print("zarr opened.")
    day_diff = end_date - convert_to_date(init_date)
    total = (day_diff.days + 1) * interval
    time = zf["time"]
    existing = time.size
    if existing != total:
        time.resize(total)
        time[slice(existing, total)] = list(range(existing, total))
        logger.info(f"Consolidated Time for {target_store}.")
        print(f"Consolidated Time for {target_store}.")
        for vname, var in ds.data_vars.items():
            if "time" in var.dims:
                shape = [ ds[i].size for i in var.dims ]
                shape[0] = total
                zf[vname].resize(shape)
        logger.info(f"Resized data vars of {target_store}.")
        print(f"Resized data vars of {target_store}.")
        zarr.consolidate_metadata(zf.store)
    else:
        logger.info(f"data is already resized for {target_store}.")
        print(f"data is already resized for {target_store}.")


def ingest_data_in_zarr_dataflow_job(target_path: str, start_date: str, end_date: str, init_date: str) -> None:
    """
    Ingests data into a Zarr store and runs a Dataflow job.

    Args:
        target_path (str): The target Zarr store path.
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.
        init_date (str): The initial date of the zarr store in the format of str.

    Returns:
        None
    """
    job_name = target_path.split('/')[-1]
    job_name = os.path.splitext(job_name)[0]
    job_name = f"zarr-data-ingestion-{replace_non_alphanumeric_with_hyphen(job_name)}-{start_date}-to-{end_date}"

    command = (
        f"python /weather/config_files/data-ingest.py --output_path {target_path} -s {start_date} -e {end_date} "
        f"--pressure_levels_group full_37 --temp_location gs://{BUCKET}/temp --runner DataflowRunner "
        f"--project {PROJECT} --region {REGION} --experiments use_runner_v2 --worker_machine_type n2-highmem-16 "
        f"--disk_size_gb 250 --setup_file /usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/setup.py --job_name {job_name} --number_of_worker_harness_threads 1"
        f" --init_date {init_date}"
    )
    command_local = (
        f"python /usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/src/data-automate/data-ingest.py --output_path {target_path} -s {start_date} -e {end_date} "
        f"--pressure_levels_group full_37 --temp_location gs://{BUCKET}/temp --runner DataflowRunner "
        f"--project {PROJECT} --region {REGION} --experiments use_runner_v2 --worker_machine_type n2-highmem-16 "
        f"--disk_size_gb 250 --setup_file /usr/local/google/home/dabhis/github_repo/arco-new/arco-era5/setup.py --job_name {job_name} --number_of_worker_harness_threads 1"
        f" --init_date {init_date}"
    )
    subprocess_run(command_local)


def ingest_data_in_bigquery_dataflow_job(zarr_file: str, table_name: str, zarr_kwargs: str) -> None:
    """
    Ingests data from a Zarr file into BigQuery and runs a Dataflow job.

    Args:
        zarr_file (str): The Zarr file path.
        table_name (str): The name of the BigQuery table.
        zarr_kwargs (Any): Additional arguments for the Zarr ingestion.

    Returns:
        None
    """
    job_name = zarr_file.split('/')[-1]
    job_name = os.path.splitext(job_name)[0]
    job_name = f"data-ingestion-into-bq-{replace_non_alphanumeric_with_hyphen(job_name)}"
    
    command = (
        f"python weather_mv/weather-mv bq --uris {zarr_file} --output_table {table_name} --runner DataflowRunner "
        f"--project {PROJECT} --region {REGION} --temp_location gs://{BUCKET}/tmp --job_name {job_name} "
        f"--use-local-code --zarr --disk_size_gb 500 --machine_type n2-highmem-4 --number_of_worker_harness_threads 1 "
        f"--zarr_kwargs {zarr_kwargs} "
    )

    subprocess_run(command)


def process_zarr_and_table(z_file: str, table: str, start_date: str, end_date: str, init_date: str):
    try:
        logger.info(f"Resizing zarr file: {z_file} started.")
        print(f"Resizing zarr file: {z_file} started.")
        resize_zarr_target(z_file, end_date, init_date)
        logger.info(f"Resizing zarr file: {z_file} completed.")
        print(f"Resizing zarr file: {z_file} completed.")
        logger.info(f"data ingesting for {z_file} is started.")
        print(f"data ingesting for {z_file} is started.")
        ingest_data_in_zarr_dataflow_job(z_file, start_date, end_date, init_date)
        logger.info(f"data ingesting for {z_file} is completed.")
        print(f"data ingesting for {z_file} is completed.")
        start = f' "start_date": "{start_date}" '
        end = f'"end_date": "{end_date}" '
        zarr_kwargs = "'{" + f'{start},{end}' + "}'"
        logger.info(f"data ingesting into BQ table: {table} started.")
        print(f"data ingesting into BQ table: {table} started.")
        ingest_data_in_bigquery_dataflow_job(z_file, table, zarr_kwargs)
        logger.info(f"data ingesting into BQ table: {table} completed.")
        print(f"data ingesting into BQ table: {table} completed.")
    except Exception as e:
        logger.error(f"An error occurred in process_zarr_and_table for {z_file}: {str(e)}")
        print(f"An error occurred in process_zarr_and_table for {z_file}: {str(e)}")


def process(z_file: str, table: str, init_date: str):
    # Function to process a single pair of z_file and table
    if '/ar/' in z_file:
        process_zarr_and_table(z_file, table, dates_data["first_day_first_prev"], dates_data["last_day_first_prev"], init_date)
    else:
        process_zarr_and_table(z_file, table, dates_data["first_day_third_prev"], dates_data["last_day_third_prev"], init_date)


def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:
    """Parse command-line arguments for the data processing pipeline.

    Args:
        desc (str): A description of the command-line interface.

    Returns:
        tuple: A tuple containing the parsed arguments as a namespace and a list of unknown arguments.

    Example:
        To parse command-line arguments, you can call this function like this:
        >>> parsed_args, unknown_args = parse_arguments("Data Processing Pipeline")
    """
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument("--init_date", type=str, default='1900-01-01',
                        help="Date to initialize the zarr store.")

    return parser.parse_known_args()


if __name__ == "__main__":
    try:
        parsed_args, unknown_args = parse_arguments("Parse arguments.")

        logger.info("program is started.")
        print("program is started.")
        co_date_range = date_range(
            dates_data["first_day_third_prev"], dates_data["last_day_third_prev"]
        )
        ar_date_range = date_range(
            dates_data["first_day_first_prev"], dates_data["last_day_first_prev"]
        )
        
        for env_var in os.environ:
            if API_KEY_PATTERN.match(env_var):
                api_key_value = os.environ.get(env_var)
                API_KEY_LIST.append(api_key_value)

        additional_content = ""
        for count, secret_key in enumerate(API_KEY_LIST):
            secret_key_value = get_secret(secret_key)
            additional_content += f'parameters.api{count}\n\
                api_url={secret_key_value["api_url"]}\napi_key={secret_key_value["api_key"]}\n\n'

        update_config_files(DIRECTORY, FIELD_NAME, additional_content)
        logger.info("Raw data downloading start.")
        print("Raw data downloading start.")
        raw_data_download_dataflow_job()
        logger.info("Raw data downloaded successfully.")
        print("Raw data downloaded successfully.")

        data_is_missing = 1  # Initialize with a non-zero value
        while data_is_missing:
            data_is_missing = check_data_availability(co_date_range, ar_date_range)
            if data_is_missing:
                logger.info("data is missing..")
                print("data is missing..")
                raw_data_download_dataflow_job()
        logger.info("Data availability check completed.")
        print("Data availability check completed.")


        # for z_file, table in zip(ZARR_LOCAL, BQ_LOCAL):
        #     process(z_file, table, parsed_args.init_date)
        arg_tuples = [(z_file, table, parsed_args.init_date) for z_file, table in zip(ZARR_LOCAL, BQ_LOCAL)]
        with multiprocessing.Pool() as p:
            p.starmap(process, arg_tuples)

        logger.info("All data ingested into BQ.")
        print("All data ingested into BQ.")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")