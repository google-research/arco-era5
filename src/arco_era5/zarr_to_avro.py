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
import apache_beam as beam
import argparse
import datetime
import geojson
import json
import logging
import numpy as np
import pandas as pd
import typing as t
import xarray as xr
import xarray_beam as xbeam

from apache_beam.utils import retry
from xarray.core.utils import ensure_us_time_resolution
from arco_era5 import replace_non_alphanumeric_with_hyphen

logger = logging.getLogger(__name__)

input_chunks = {"time": 1, "level": 1, "latitude": 200, "longitude": 400}

def fetch_geo_point(lat: float, long: float) -> str:
    """Calculates a geography point from an input latitude and longitude."""
    LATITUDE_RANGE = (-90, 90)
    LONGITUDE_RANGE = (-180, 180)
    if lat > LATITUDE_RANGE[1] or lat < LATITUDE_RANGE[0]:
        raise ValueError(f"Invalid latitude value '{lat}'")
    if long > LONGITUDE_RANGE[1] or long < LONGITUDE_RANGE[0]:
        raise ValueError(f"Invalid longitude value '{long}'")
    point = geojson.dumps(geojson.Point((long, lat)))
    return point

def get_lat_lon_range(value: float, lat_lon: str, is_point_out_of_bound: bool,
                      lat_grid_resolution: float, lon_grid_resolution: float) -> t.List:
    """Calculate the latitude, longitude point range point latitude, longitude and grid resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    This function gives the `.` point in the above example.
    """
    if lat_lon == 'latitude':
        if is_point_out_of_bound:
            return [-90 + lat_grid_resolution, 90 - lat_grid_resolution]
        else:
            return [value + lat_grid_resolution, value - lat_grid_resolution]
    else:
        if is_point_out_of_bound:
            return [-180 + lon_grid_resolution, 180 - lon_grid_resolution]
        else:
            return [value + lon_grid_resolution, value - lon_grid_resolution]

def bound_point(latitude: float, longitude: float, lat_grid_resolution: float, lon_grid_resolution: float) -> t.List:
    """Calculate the bound point based on latitude, longitude and grid resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    This function gives the `*` point in the above example.
    """
    lat_in_bound = latitude in [90.0, -90.0]
    lon_in_bound = longitude in [-180.0, 180.0]

    lat_range = get_lat_lon_range(latitude, "latitude", lat_in_bound,
                                  lat_grid_resolution, lon_grid_resolution)
    lon_range = get_lat_lon_range(longitude, "longitude", lon_in_bound,
                                  lat_grid_resolution, lon_grid_resolution)
    lower_left = [lon_range[1], lat_range[1]]
    upper_left = [lon_range[1], lat_range[0]]
    upper_right = [lon_range[0], lat_range[0]]
    lower_right = [lon_range[0], lat_range[1]]
    return [lower_left, upper_left, upper_right, lower_right]

def fetch_geo_polygon(latitude: float, longitude: float, lat_grid_resolution: float, lon_grid_resolution: float) -> str:
    """Create a Polygon based on latitude, longitude and resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    In order to create the polygon, we require the `*` point as indicated in the above example.
    To determine the position of the `*` point, we find the `.` point.
    The `get_lat_lon_range` function gives the `.` point and `bound_point` gives the `*` point.
    """
    lat_lon_bound = bound_point(latitude, longitude, lat_grid_resolution, lon_grid_resolution)
    polygon = geojson.dumps(geojson.Polygon([[
        (lat_lon_bound[0][0], lat_lon_bound[0][1]),  # lower_left
        (lat_lon_bound[1][0], lat_lon_bound[1][1]),  # upper_left
        (lat_lon_bound[2][0], lat_lon_bound[2][1]),  # upper_right
        (lat_lon_bound[3][0], lat_lon_bound[3][1]),  # lower_right
        (lat_lon_bound[0][0], lat_lon_bound[0][1]),  # lower_left
    ]]))
    return polygon

def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    # Note: The order of processing is significant.
    logger.debug('Serializing to JSON')

    # pd.isna() returns ndarray if input is not scalar therefore checking if value is scalar.
    if (np.isscalar(value) and pd.isna(value)) or value is None:
        return None
    elif np.issubdtype(type(value), np.floating):
        return float(value)
    elif isinstance(value, set):
        value = list(value)
        return np.where(pd.isna(value), None, value).tolist()
    elif isinstance(value, np.ndarray):
        # Will return a scaler if array is of size 1, else will return a list.
        # Replace all NaNs, NaTs with None.
        return np.where(pd.isna(value), None, value).tolist()
    elif isinstance(value, datetime.datetime) or isinstance(value, str) or isinstance(value, np.datetime64):
        # Assume strings are ISO format timestamps...
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            # ... if they are not, assume serialization is already correct.
            return value
        except TypeError:
            # ... maybe value is a numpy datetime ...
            try:
                value = ensure_us_time_resolution(value).astype(datetime.datetime)
            except AttributeError:
                # ... value is a datetime object, continue.
                pass

        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()

        # We assume here that naive timestamps are in UTC timezone.
        return int(value.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)
    elif isinstance(value, datetime.timedelta):
        return value.total_seconds()
    elif isinstance(value, np.timedelta64):
        # Return time delta in seconds.
        return float(value / np.timedelta64(1, 's'))
    # This check must happen after processing np.timedelta64 and np.datetime64.
    elif np.issubdtype(type(value), np.integer):
        return int(value)

    return value

def df_to_rows(rows: pd.DataFrame, ds: xr.Dataset, uri: str) -> t.Iterator[t.Dict]:
    first_ts_raw = (
        ds.time[0].values if isinstance(ds.time.values, np.ndarray)
        else ds.time.values
    )
    first_time_step = to_json_serializable_type(first_ts_raw)
    for _, row in rows.iterrows():
        row = row.astype(object).where(pd.notnull(row), None)
        row = {k: to_json_serializable_type(v) for k, v in row.items()}

        # Add import metadata.
        data_import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).timestamp()
        row['data_import_time'] = int(data_import_time * 1000)
        row['data_uri'] = uri
        row['data_first_step'] = first_time_step

        longitude = ((row['longitude'] + 180) % 360) - 180
        row['geo_point'] = fetch_geo_point(row['latitude'], longitude)

        latitudes = np.arange(90, -90.25, -.25).tolist()
        longitudes = np.arange(0, 360, .25).tolist()

        latitude_length = len(latitudes)
        longitude_length = len(longitudes)

        latitude_range = np.ptp(latitudes)
        longitude_range = np.ptp(longitudes)
        lat_grid_resolution = abs(latitude_range / latitude_length) / 2
        lon_grid_resolution = abs(longitude_range / longitude_length) / 2
        row['geo_polygon'] = (
            fetch_geo_polygon(row["latitude"], longitude, lat_grid_resolution, lon_grid_resolution)
        )

        beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()

        yield row


@retry.with_exponential_backoff(
    num_retries=5,
    logger=logger.warning,
    initial_delay_secs=1,
    max_delay_secs=60
)
def chunks_to_rows(key: xbeam.Key, ds: xr.Dataset, input_chunks: t.Dict[str, t.Any] = {}): # -> t.Iterator[t.Dict]:

    offset = { k: key.offsets[k] for k in input_chunks.keys() }

    logger = logging.getLogger(__name__)

    logger.info(f"Processing for time: {offset}")

    uri = ds.attrs.get('data_uri', '')

    df = ds.to_dataframe().reset_index()

    yield from df_to_rows(df[:], ds, uri)
  
def parse_arguments(desc: str) -> t.Tuple[argparse.Namespace, t.List[str]]:

    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-i', "--uri", required=True, help='input zarr file.')
    parser.add_argument('-m', "--month", required=True, help='month of the data which needs to change, iso format string.')
    parser.add_argument('-o', "--output", required=True, help='Output path to store raw files.')

    return parser.parse_known_args()

def run_pipeline(ds: xr.Dataset, month: str, input_chunks: t.Dict, output: str, pipeline_args: t.List[str]):

    ds = ds.sel(time=month)

    avro_scheam = json.load(open("/arco-era5/src/arco_era5/era5-avro-schema.json"))

    with beam.Pipeline(argv=pipeline_args) as p:
        _ = (
            p
            | "DatasetToChunks" >> xbeam.DatasetToChunks(ds, input_chunks)
            | "ExtractRows" >> beam.FlatMapTuple(chunks_to_rows, input_chunks=input_chunks)
            | "WriteToAvro" >> beam.io.WriteToAvro(f"{output}/{month}/ar", schema=avro_scheam)
        )

if __name__ == "__main__":

    logging.getLogger().setLevel(logging.INFO)
  
    known_args, pipeline_args = parse_arguments("Update Data Slice")

    ds, _ = xbeam.open_zarr(known_args.uri)
    month = known_args.month

    pipeline_args.extend(['--job_name', f"ar-avro-generation-{replace_non_alphanumeric_with_hyphen(month)}", '--save_main_session'])
    run_pipeline(ds, month, input_chunks, output=known_args.output, pipeline_args=pipeline_args)
