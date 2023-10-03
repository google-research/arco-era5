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

ARG py_version=3.8
FROM apache/beam_python${py_version}_sdk:2.40.0 as beam_sdk
FROM continuumio/miniconda3:latest

# Update miniconda
RUN conda update conda -y

# Add the mamba solver for faster builds
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba

# remove below line at last
COPY . /arco-era5

# Create conda env using environment.yml
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

# Copy files from official SDK image, including script/dependencies.
COPY --from=beam_sdk /opt/apache/beam /opt/apache/beam

# add whole arco-era5 into a docker-image.
# ARG arco_era5_git_rev=bq-automate # change branch name
# RUN git clone https://github.com/google-research/arco-era5.git /arco-era5
WORKDIR /arco-era5
# RUN git checkout "${arco_era5_git_rev}"
RUN pip install google-cloud-secret-manager==2.0.0
RUN pip install -e .

# remove this variables at last
ENV PROJECT='anthromet-ingestion'
ENV REGION='us-west3'
ENV BUCKET='dabhis_temp'
ENV SDK_CONTAINER_IMAGE="gcr.io/grid-intelligence-sandbox/miniconda3-beam:weather-tools-with-aria2"
ENV API_KEY_1='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_1/versions/1'
ENV API_KEY_2='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_2/versions/1'

ENTRYPOINT ["python", "src/raw-to-zarr-to-bq.py"]
