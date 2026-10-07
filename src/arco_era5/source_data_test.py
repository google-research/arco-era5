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

"""Regression tests for reading ERA5 and ERA5T NetCDF files."""

import pathlib
import tempfile
import unittest

import numpy as np
import xarray as xr

from . import source_data


class ReadNetCDFTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.path = pathlib.Path(self.temp_dir.name) / "temperature.nc"
        self.expected = xr.DataArray(
            np.arange(8, dtype=np.float32).reshape(2, 2, 2),
            dims=("time", "latitude", "longitude"),
            coords={
                "time": np.array(["2026-07-01T00", "2026-07-01T01"],
                                 dtype="datetime64[ns]"),
                "latitude": [1.0, 0.0],
                "longitude": [0.0, 1.0],
            },
            name="t",
            attrs={"units": "K"},
        )

    def read(self, array):
        array.to_dataset().to_netcdf(self.path, engine="h5netcdf")
        return source_data._read_nc_dataset(self.path)

    def test_combines_complementary_experiment_versions(self):
        era5 = self.expected.copy(deep=True)
        era5.values[1] = np.nan
        era5t = self.expected.copy(deep=True)
        era5t.values[0] = np.nan
        for versions, fields in [([1, 5], [era5, era5t]),
                                 ([5, 1], [era5t, era5])]:
            with self.subTest(versions=versions):
                array = xr.concat(fields, xr.IndexVariable("expver", versions))
                xr.testing.assert_identical(self.read(array), self.expected)

    def test_prefers_era5_and_only_fills_missing_values_from_era5t(self):
        era5 = self.expected.copy(deep=True)
        era5.values[0, 0, 0] = np.nan
        era5.values[1, 1, 1] = np.nan
        era5t = xr.full_like(self.expected, 99.0)
        era5t.values[1, 1, 1] = np.nan
        expected = self.expected.copy(deep=True)
        expected.values[0, 0, 0] = 99.0
        expected.values[1, 1, 1] = np.nan
        array = xr.concat([era5, era5t], xr.IndexVariable("expver", [1, 5]))
        xr.testing.assert_identical(self.read(array), expected)

    def test_single_experiment_dimension(self):
        for version in (1, 5):
            with self.subTest(version=version):
                array = self.expected.expand_dims(expver=[version])
                xr.testing.assert_identical(self.read(array), self.expected)

    def test_modern_time_dependent_experiment_coordinate(self):
        array = self.expected.rename(time="valid_time").assign_coords(
            expver=("valid_time", ["0001", "0005"]), number=0,
        )
        xr.testing.assert_identical(self.read(array), self.expected)

    def test_scalar_experiment_coordinate(self):
        array = self.expected.assign_coords(expver=1)
        xr.testing.assert_identical(self.read(array), self.expected)

    def test_no_experiment_coordinate(self):
        xr.testing.assert_identical(self.read(self.expected), self.expected)


if __name__ == "__main__":
    unittest.main()
