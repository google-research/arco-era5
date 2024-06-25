ARG py_version=3.9
FROM apache/beam_python${py_version}_sdk:2.40.0 as beam_sdk
FROM continuumio/miniconda3:latest

# Update miniconda
RUN conda update conda -y

# Add the mamba solver for faster builds
RUN conda install -n base conda-libmamba-solver
RUN conda config --set solver libmamba

COPY environment.yml ./
RUN conda env create -f environment.yml --debug

# Activate the conda env and update the PATH
ARG CONDA_ENV_NAME=era5
RUN echo "source activate ${CONDA_ENV_NAME}" >> ~/.bashrc
ENV PATH /opt/conda/envs/${CONDA_ENV_NAME}/bin:$PATH

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
