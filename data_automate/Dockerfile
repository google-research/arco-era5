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
# ==============================================================================

FROM continuumio/miniconda3:latest

# Update miniconda
RUN conda update conda -y

# Add the mamba solver for faster builds
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba

# Clone the weather-tools and create conda env using environment.yml of weather-tools.
ARG weather_tools_git_rev=main
RUN git clone https://github.com/google/weather-tools.git /weather
WORKDIR /weather
RUN git checkout "${weather_tools_git_rev}"
RUN rm -r /weather/weather_*/test_data/
RUN conda env create -f environment.yml -n weather-tools-with-arco-era5 --debug

# Activate the conda env and update the PATH
ARG CONDA_ENV_NAME=weather-tools-with-arco-era5
RUN echo "source activate ${CONDA_ENV_NAME}" >> ~/.bashrc
ENV PATH /opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH
RUN pip install -e .

# Install gcloud alpha
RUN apt-get update -y
RUN gcloud components install alpha --quiet

# Clone the arco-era5 and add dependency into the environment.
ARG arco_era5_git_rev='avro-bq'
RUN git clone https://github.com/google-research/arco-era5.git /arco-era5
WORKDIR /arco-era5
RUN git checkout "${arco_era5_git_rev}"
RUN pip install -e .

ENV PROJECT='anthromet-ingestion'
ENV REGION='us-west3'
ENV BUCKET='dabhis_temp'
ENV SDK_CONTAINER_IMAGE="gcr.io/grid-intelligence-sandbox/arco-era5:model"
ENV MANIFEST_LOCATION='fs://manifest?projectId=anthromet-ingestion'
ENV API_KEY_1='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_1/versions/1'
ENV API_KEY_2='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_2/versions/1'
ENV PYTHON_PATH='/opt/conda/envs/weather-tools-with-arco-era5/bin/python'
ENV BQ_TABLES_LIST='["gcp-public-data-era5.era5.ar-era5-v0", "grid-intelligence-sandbox.dabhis_test.model-level-moisture-v2", "grid-intelligence-sandbox.dabhis_test.model-level-wind-v2", "grid-intelligence-sandbox.dabhis_test.single-level-forecast-v2", "grid-intelligence-sandbox.dabhis_test.single-level-reanalysis-v2", "grid-intelligence-sandbox.dabhis_test.single-level-surface-v2"]'
ENV REGION_LIST='["us-east1", "us-west4", "us-central1", "us-east4", "us-east1", "us-west4"]'
ENV AR_RAW_AVRO_FILE='gs://gcp-public-data-era5/ar-raw-avro'
ENV ZARR_AVRO_CONVERSION_NETWORK='arco-era5'
ENV ZARR_AVRO_CONVERSION_SUBNET='regions/us-central1/subnetworks/arco-era5-subnet'

RUN apt-get update && apt-get -y install cron vim
COPY cron-file /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN ls /usr/bin/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

RUN touch /var/log/cron.log
CMD printenv | grep -v "no_proxy" >> /etc/environment && cron && tail -f /var/log/cron.log
