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
import fsspec
import pandas as pd
import typing as t

BUCKET = "gcp-public-data-arco-era5"
START_YEAR = 1979
END_YEAR = 2022


def list_files_in_directory(bucket_name: str, directory_prefix: str) -> t.List[str]:
    """List files in a directory of a cloud storage bucket using fsspec.

    Args:
        bucket_name (str): The name of the cloud storage bucket.
        directory_prefix (str): The prefix of the directory.

    Returns:
        List[str]: A list of file names in the directory.
    """
    fs = fsspec.filesystem('gs')
    files = fs.ls(f'{bucket_name}/{directory_prefix}')
    return [file[len(bucket_name) + 1:] for file in files]


def find_extra_files(directory_files: t.List[str], desired_files: t.List[str]
                     ) -> t.List[str]:
    """Returns a list of files that are in the directory but not in the desired list.

    Args:
        directory_files (List[str]): List of files in the directory.
        desired_files (List[str]): List of desired file names.

    Returns:
        List[str]: A list of file names that are extra in the directory.
    """
    return list(set(directory_files) - set(desired_files))


def generate_daily_file_names(year: int, chunks: t.List) -> t.List[str]:
    """Generate a list of daily file names for a given year.

    Args:
        year (int): The year for which to generate file names.
        chunks (List):  A list of the daily files chunks.
    Returns:
        List[str]: A list of generated daily file names.
    """
    generated_files = []
    start_date = pd.Timestamp(year, 1, 1)
    end_date = pd.Timestamp(year + 1, 1, 1)
    date_range = pd.date_range(start_date, end_date, freq='D')

    for date in date_range:
        formatted_date = date.strftime('%Y%m%d')
        for chunk in chunks:
            generated_files.append(
                f'raw/ERA5GRIB/HRES/Daily/{year}/{formatted_date}_hres_{chunk}.grb2')
    return generated_files


def generate_monthly_file_names(year: int,
                                monthly_chunks: t.List, monthly_chunks_pcp: t.List,
                                monthly_chunks_soil: t.List) -> t.List[str]:
    """Generates a list of desired monthly file names for a given year."""
    monthly_dates = [f"{year}{month:02d}" for month in range(1, 13)]
    generated_files = []

    for month in monthly_dates:
        for chunk in monthly_chunks:
            generated_files.append(
                f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_{chunk}.grb2')
        for chunk in monthly_chunks_pcp:
            generated_files.append(
                f"raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_pcp.grb2_surface_{chunk}.grib")
        for chunk in monthly_chunks_soil:
            generated_files.append(
                f"raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_depthBelowLandLayer_{chunk}.grib")
        generated_files.append(
            f'raw/ERA5GRIB/HRES/Month/{year}/{month}_hres_soil.grb2_surface_tsn.grib')
    return generated_files


def validate_and_report_missing_files(bucket_name: str, directory_prefix: str,
                                      desired_files: t.List[str]):
    """Validates and reports missing files in the specified directory for a given year.

    Args:
        bucket_name (str): The name of the cloud storage bucket.
        directory_prefix (str): The prefix of the directory.
        desired_files (List[str]): List of desired file names.
    """
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
        else:
            print(f"There is no extra files in {directory_prefix}.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == '__main__':
    for year in range(START_YEAR, END_YEAR + 1):
        # Process daily files
        daily_chunks = ['dve', 'o3q', 'qrqs', 'tw', 'tuvw']
        directory_prefix_daily = f"raw/ERA5GRIB/HRES/Daily/{year}/"
        desired_files_daily = generate_daily_file_names(year, daily_chunks)
        validate_and_report_missing_files(BUCKET, directory_prefix_daily,
                                          desired_files_daily)

        # Process monthly files
        monthly_chunks = ['cape', 'cisst', 'lnsp', 'pcp', 'rad', 'sfc', 'soil', 'tcol',
                          'zs']
        monthly_chunks_pcp = ['cp', 'crr', 'csf', 'csfr', 'es', 'lsf', 'lsp', 'lspf',
                              'lsrr', 'lssfr', 'ptype', 'rsn', 'sd', 'sf', 'smlt', 'tp']
        monthly_chunks_soil = ['istl1', 'istl2', 'istl3', 'istl4', 'stl1', 'stl2',
                               'stl3', 'stl4', 'swvl1', 'swvl2', 'swvl3', 'swvl4']
        directory_prefix_monthly = f"raw/ERA5GRIB/HRES/Month/{year}/"
        desired_files_monthly = generate_monthly_file_names(
            year, monthly_chunks, monthly_chunks_pcp, monthly_chunks_soil)
        validate_and_report_missing_files(BUCKET, directory_prefix_monthly,
                                          desired_files_monthly)

    print("Finished")
