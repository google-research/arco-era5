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
import typing as t
from datetime import datetime, timedelta
from google.cloud import storage

BUCKET = "gcp-public-data-arco-era5"
START_YEAR = 1979
END_YEAR = 2022


def list_files_in_directory(bucket_name: str, directory_prefix: str) -> t.List[str]:
    """Lists files in a specific directory within a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    return [blob.name for blob in bucket.list_blobs(prefix=directory_prefix)]


def find_extra_files(directory_files: t.List[str], desired_files: t.List[str]
                     ) -> t.List[str]:
    """Returns a list of files that are in the directory but not in the desired list."""
    return [file for file in directory_files if file not in desired_files]


def generate_daily_file_names(year: int) -> t.List[str]:
    """Generates a list of daily file names for a given year."""
    generated_files = []
    current_date = datetime(year, 1, 1)
    end_date = datetime(year + 1, 1, 1)
    delta = timedelta(days=1)

    while current_date < end_date:
        formatted_date = current_date.strftime('%Y%m%d')
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_dve.grb2')
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_o3q.grb2')
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_qrqs.grb2')
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_tw.grb2')
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_tuvw.grb2')
        current_date += delta
    return generated_files


def generate_monthly_file_names(year: int) -> t.List[str]:
    """Generates a list of desired monthly file names for a given year."""
    monthly_dates = [f"{year}{month:02d}" for month in range(1, 13)]
    desired_files_monthly = []

    for month in monthly_dates:
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_cape.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_cisst.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_lnsp.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_cp.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_crr.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_csf.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_csfr.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_es.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_lsf.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_lsp.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_lspf.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_lsrr.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_lssfr.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_ptype.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_rsn.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_sd.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_sf.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_smlt.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_tp.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_rad.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_sfc.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_istl1.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_istl2.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_istl3.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_istl4.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_stl1.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_stl2.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_stl3.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_stl4.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_swvl1.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_swvl2.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_swvl3.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_swvl4.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_surface_tsn.grib')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_tcol.grb2')
        desired_files_monthly.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_zs.grb2')

    return desired_files_monthly


def validate_and_report_missing_files(bucket_name: str, directory_prefix: str,
                                      desired_files: t.List[str]):
    """Validates and reports missing files in the specified directory for a given
    year."""
    try:
        actual_files = list_files_in_directory(bucket_name, directory_prefix)
        extra_files = find_extra_files(actual_files, desired_files)

        # Here first element of actual_files is directory_prefix itself
        # so we aren't considered it.
        # Like if directory_prefix = 'raw/ERA5GRIB/HRES/Daily/2002' then
        # actual_files[0] = 'raw/ERA5GRIB/HRES/Daily/2002'.
        if len(extra_files) > 1:
            print(f"Extra files in {directory_prefix}:")
            for file in extra_files[1:]:
                print(file)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    for year in range(START_YEAR, END_YEAR + 1):
        # Process daily files
        directory_prefix_daily = f"raw/ERA5GRIB/HRES/Daily/{year}/"
        desired_files_daily = generate_daily_file_names(year)
        validate_and_report_missing_files(BUCKET, directory_prefix_daily,
                                          desired_files_daily)

        # Process monthly files
        directory_prefix_monthly = f"raw/ERA5GRIB/HRES/Month/{year}/"
        desired_files_monthly = generate_monthly_file_names(year)
        validate_and_report_missing_files(BUCKET, directory_prefix_monthly,
                                          desired_files_monthly)

    print("Finished")
