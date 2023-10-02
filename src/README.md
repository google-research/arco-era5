# ARCO ERA5 Data Processing Script

This script is designed for processing and ingesting ARCO ERA5 weather data into Zarr file and BigQuery. It includes multiple data processing steps and Dataflow jobs to ensure the data is ready for analysis. Below, you'll find important information on how to set up and use this script effectively.

### Purpose

The ARCO ERA5 Data Processing Script is designed to:

- Downloading raw data from Copernicus automatically on monthly basis using of [Cloud-Run](https://cloud.google.com/run).This can be done with `google-weather-tools`, specifically `weather-dl` (see
[weather-tools.readthedocs.io](https://weather-tools.readthedocs.io/)).
- splitting grib files(`soil` and `pcp` files) by
variable. This can be done with `google-weather-tools`, specifically `weather-sp` (see
[weather-tools.readthedocs.io](https://weather-tools.readthedocs.io/)).
- Validate data availability and ensure all required files are present.
- Ingest data into zarr files(`gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3` & files of this directory `gs://gcp-public-data-arco-era5/co`)
- Ingest processed data into BigQuery for analysis.


_Steps_:

1. Set up a Cloud project with sufficient permissions to use cloud storage (such as
   [GCS](https://cloud.google.com/storage)) and a Beam runner (such as [Dataflow](https://cloud.google.com/dataflow)).
   > Note: Other cloud systems should work too, such as S3 and Elastic Map Reduce. However, these are untested. If you
   > experience an error here, please let us know by [filing an issue](https://github.com/google/weather-tools/issues).

2. Acquire one or more licenses from [Copernicus](https://cds.climate.copernicus.eu/user/register?destination=/api-how-to).
   > Recommended: Download configs allow users to specify multiple API keys in a single data request via
   > ["parameter subsections"](https://weather-tools.readthedocs.io/en/latest/Configuration.html#subsections). We
   > highly recommend that institutions pool licenses together for faster downloads.

3. Add the all CDS licenses into the [secret-manager](https://cloud.google.com/secret-manager) with value likes this: {"api_url": "URL", "api_key": "KEY"}
   > NOTE: for every API_KEY there must be unique secret-key.

4. Set this **ENV** variables.
   * `PROJECT` 
   * `REGION`
   * `BUCKET`
   * `SDK_CONTAINER_IMAGE`
   * `MANIFEST_LOCATION`
   * `API_KEY_*`

   Here, API_KEY_* is access of [secret-manager key](https://cloud.google.com/secret-manager) and it's value is looks like this :: projects/PROJECT_NAME/secrets/SECRET_KEY_NAME/versions/1

   NOTE: API_KEY is must follow this format: `API_KEY_*`. here * is any value.

5. Run python file.
   > python arco-era5/src/main-file.py
