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

# add whole arco-era5 into a docker-image.
# ARG arco_era5_git_rev=bq-automate # change branch name
# RUN git clone https://github.com/google-research/arco-era5.git /arco-era5
WORKDIR /arco-era5
# RUN git checkout "${arco_era5_git_rev}"
RUN pip install -e .

# remove this variables at last
ENV PROJECT='anthromet-ingestion'
ENV REGION='us-west3'
ENV BUCKET='dabhis_temp'
ENV SDK_CONTAINER_IMAGE="gcr.io/grid-intelligence-sandbox/miniconda3-beam:weather-tools-with-aria2"
ENV MANIFEST_LOCATION='fs://manifest?projectId=anthromet-ingestion'
ENV API_KEY_1='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_1/versions/1'
ENV API_KEY_2='projects/anthromet-ingestion/secrets/ARCO-ERA5_licence_key_2/versions/1'
ENV PYTHON_PATH='/opt/conda/envs/weather-tools-with-arco-era5/bin/python'

RUN apt-get update && apt-get -y install cron vim
COPY cron-file /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN ls /usr/bin/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

RUN touch /var/log/cron.log
CMD printenv | grep -v "no_proxy" >> /etc/environment && cron && tail -f /var/log/cron.log
