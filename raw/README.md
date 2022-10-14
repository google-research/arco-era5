# Ingesting ERA5

Here, we document steps for acquiring and pre-processing raw ERA5 data for cloud-optimization. In this directory, we've
included configuration files to describe and expediently acquire the data.

## Downloading raw data from Copernicus

All data can be ingested from [Copernicus](https://cds.climate.copernicus.eu/#!/home) with `google-weather-tools`,
specifically `weather-dl` (see [weather-tools.readthedocs.io](https://weather-tools.readthedocs.io/)).

_Pre-requisites_:

1. Install the weather tools, with at least version 0.3.1:
   ```shell
   pip install google-weather-tools>=0.3.1
   ```
2. Acquire one or more licenses from [Copernicus](https://cds.climate.copernicus.eu/user/register?destination=/api-how-to).
   > Recommended: Download configs allow users to specify multiple API keys in a single data request via
   > ["parameter subsections"](https://weather-tools.readthedocs.io/en/latest/Configuration.html#subsections). We
   > highly recommend that institutions pool licenses together for faster downloads.

3. Set up a cloud project with sufficient permissions to use cloud storage (such as
   [GCS](https://cloud.google.com/storage)) and a Beam runner (such as [Dataflow](https://cloud.google.com/dataflow)).
   > Note: Other cloud systems should work too, such as S3 and Elastic Map Reduce. However, these are untested. If you
   > experience an error here, please let us know by [filing an issue](https://github.com/google/weather-tools/issues).

_Steps_:

1. Update the `parameters` section of the desired config file, e.g. `raw/era5_ml_dv.cfg`, with the appropriate
   information.
    1. First, update the `target_path` to point to the right cloud bucket.
    2. Add one or more CDS API keys, as
       is [described here](https://weather-tools.readthedocs.io/en/latest/Configuration.html#copernicus-cds).
2. (optional, recommended) Preview the download with a dry run:
   ```shell
   weather-dl raw/era5_ml_dv.cfg --dry-run 
   ```
3. Once the config looks sounds, execute the download on your
   preferred [Beam runner](https://beam.apache.org/documentation/runners/capability-matrix/), for example,
   the [Apache Spark runner](https://beam.apache.org/documentation/runners/spark/). We ingested data with
   GCP's [Dataflow runner](https://beam.apache.org/documentation/runners/dataflow/), like so:
   ```shell
   export PROJECT=<your-project-id>
   export BUCKET=<your-gcs-bucket>
   export REGION=us-central1
   weather-dl raw/era5_mv_dv.cfg \
    --runner DataflowRunner \
    --project $PROJECT \
    --region $REGION \
    --temp_location "gs://$BUCKET/tmp/" \
    --disk_size_gb 75 \
    --job_name era5-ml-dv
   ```

   If you'd like to download the data locally, you can run the following, though this isn't recommended (the data is
   large!):
   ```shell
   weather-dl raw/era5_mv_dv.cfg --local-run
   ```

   Check out the [`weather-dl` docs](https://weather-tools.readthedocs.io/en/latest/weather_dl/README.html) for more
   information.
4. Repeat for the rest of the config files.

## Preparing grib data for conversion

Grib is an idiosyncratic format. For example, a single grib file can contain multiple level types, standard table
versions, or grids. This often makes grib
files [difficult to open](https://github.com/ecmwf/cfgrib#filter-heterogeneous-grib-files). The system we've employed to
convert data to Zarr, Pangeo Forge Recipes, is
not ([yet](https://github.com/pangeo-forge/pangeo-forge-recipes/issues/244)) able to handle this complexity. Thus, to
prepare the raw data for conversion, we need to perform one additional processing step: splitting grib files by
variable. This can be done with `google-weather-tools`, specifically `weather-sp` (see
[weather-tools.readthedocs.io](https://weather-tools.readthedocs.io/)).

The only datasets we needed to split by variable are `soil` and `pcp`, since they mix levels and table versions. These
steps will prepare the data for conversion by scripts in the `src/` directory.

_Pre-requisites_:

1. Install the weather tools, with at least version 0.3.0:
   ```shell
   pip install google-weather-tools>=0.3.0
   ```
2. Acquire read access to the datasets (e.g. via `era5_sfc_soil.cfg`) from some cloud storage bucket.

_Steps_:

1. Preview the data split by running the following command. Make sure to change the file paths if the data locations
   differ.
   ```shell
   export DATASET=soil
   weather-sp --input-pattern "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/**/*_hres_$DATASET.grb2" \
     --output-template "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{1}/{0}.grb2_{level}_{shortName}.grib" \
     --dry-run
   ```
2. Execute the data split on your
   preferred [Beam runner](https://beam.apache.org/documentation/runners/capability-matrix/). For example, here are the
   arguments to run the splitter on [Dataflow](https://beam.apache.org/documentation/runners/dataflow/):
   ```shell
   export DATASET=soil
   
   export PROJECT=<your-project>
   export BUCKET=<your-bucket>
   export REGION=us-central1

   weather-sp --input-pattern "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/**/*_hres_$DATASET.grb2" \
     --output-template "gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES/Month/{1}/{0}.grb2_{level}_{shortName}.grib" \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location gs://$BUCEKT/tmp \
     --disk_size_gb 100 \
     --job_name split-soil-data
   ```
3. Repeat this process, except change the dataset to `pcp`:
   ```
   export DATASET=pcp
   ```