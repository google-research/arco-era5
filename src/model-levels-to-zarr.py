"""Convert Model level Era 5 Data to an unprocessed Zarr dataset.

Example:
    Check if there's any missing data:
    ```
    python src/model-levels-to-zarr.py gs://anthromet-external-era5/model-level-reanalysis.zarr gs://$BUCKET/ml-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --setup_file ./setup.py \
     --find-missing
    ```

    Perform the conversion...
    ```
    python src/model-levels-to-zarr.py gs://anthromet-external-era5/model-level-reanalysis.zarr gs://$BUCKET/ml-cache/ \
     --start 1979-01-01 \
     --end 2021-07-01 \
     --runner DataflowRunner \
     --project $PROJECT \
     --region $REGION \
     --temp_location "gs://$BUCKET/tmp/" \
     --setup_file ./setup.py \
     --experiment=use_runner_v2 \
     --disk_size_gb 50 \
     --machine_type n2-highmem-2 \
     --sdk_container_image=gcr.io/ai-for-weather/ecmwf-beam-worker:latest \
     --job_name model-levels-to-zarr
    ```
"""
import datetime
import logging

from src.common import run, parse_args

if __name__ == "__main__":
    logging.getLogger('pangeo_forge_recipes').setLevel(logging.DEBUG)
    logging.getLogger().setLevel(logging.INFO)

    def make_path(time: datetime.datetime, chunk: str) -> str:
        """Make path to Era5 data from timestamp and variable."""

        # Handle chunks that have been manually split into one-variable files.
        if '_' in chunk:
            chunk_, level, var = chunk.split('_')
            return (
                f"gs://external-data-ai-for-weather/ERA5GRIB/HRES/Daily/"
                f"{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk_}.grb2_{level}_{var}.grib"
            )

        return (
            f"gs://external-data-ai-for-weather/ERA5GRIB/HRES/Daily/"
            f"{time.year:04d}/{time.year:04d}{time.month:02d}{time.day:02d}_hres_{chunk}.grb2"
        )

    default_chunks = ['dve', 'o3q', 'qrqs', 'tw']

    run(make_path, *parse_args('Convert Era 5 Model Level data to Zarr', default_chunks))
