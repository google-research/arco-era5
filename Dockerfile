ARG py_version=3.8
FROM apache/beam_python${py_version}_sdk:2.40.0 as beam_sdk
FROM continuumio/miniconda3:latest

# # Update miniconda
# RUN conda update conda -y
RUN pwd

# Add the mamba solver for faster builds
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba

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

RUN conda install -c conda-forge google-cloud-sdk==410.0.0 -y
# Install gcloud alpha
RUN apt-get update -y
RUN gcloud components install alpha --quiet

COPY dist/arco_era5-0.1.0-py3-none-any.whl ./
RUN pip install arco_era5-0.1.0-py3-none-any.whl

# Copy files from official SDK image, including script/dependencies.
COPY --from=beam_sdk /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]
