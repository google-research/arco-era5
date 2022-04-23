# Copyright 2022 Google LLC
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
from setuptools import setup, find_packages

setup(
    name='era5-to-zarr',
    packaging=find_packages('src'),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    description="Scripts to convert Alphabet's copy of Era 5 to Zarr.",
    platforms=['darwin', 'linux'],
    python_requires='>=3.7, <3.9',
    install_requires=[
        'apache_beam[gcp]',
        'pangeo-forge-recipes==0.8.3',
        'pandas',
        'gcsfs',
        'cfgrib',
        'google-weather-tools>=0.3',
    ],
    tests_require=['pytest'],
)