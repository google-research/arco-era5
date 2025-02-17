# Copyright 2024 Google LLC
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
variables_full_names = {
            'cc': 'fraction_of_cloud_cover',
            'ciwc': 'specific_cloud_ice_water_content',
            'clwc': 'specific_cloud_liquid_water_content',
            'crwc': 'specific_rain_water_content',
            'cswc': 'specific_snow_water_content',
            'd': 'divergence',
            'o3': 'ozone_mass_mixing_ratio',
            'q': 'specific_humidity',
            't': 'temperature',
            'vo': 'vorticity',
            'w': 'vertical_velocity',
            'u': 'u_component_of_wind',
            'v': 'v_component_of_wind',
            'z': 'geopotential'
        }
zarr_files = {'ml_wind': 'gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2/',
              'ml_moisture': 'gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2/',
              'sl_surface': 'gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2/'}

ARCO_ERA5_ZARR_FILES = {
    "ar": "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3",
    "ml_moisture": "gs://gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2",
    "ml_wind": "gs://gcp-public-data-arco-era5/co/model-level-wind.zarr-v2",
    "sl_forecast": "gs://gcp-public-data-arco-era5/co/single-level-forecast.zarr-v2",
    "sl_reanalysis": "gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr-v2",
    "sl_surface": "gs://gcp-public-data-arco-era5/co/single-level-surface.zarr-v2"
}
