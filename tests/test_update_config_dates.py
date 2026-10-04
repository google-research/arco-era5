# Copyright 2026 Google LLC
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

import datetime
import unittest
from unittest import mock

import importlib.util
from enum import Enum
from pathlib import Path
import sys
import types

PACKAGE_DIR = Path(__file__).resolve().parents[1] / "src" / "arco_era5"
package = types.ModuleType("arco_era5")
package.__path__ = [str(PACKAGE_DIR)]
sys.modules.setdefault("arco_era5", package)

utils = types.ModuleType("arco_era5.utils")


class ExecTypes(Enum):
    ERA5 = "era5"
    ERA5T_DAILY = "daily"
    ERA5T_MONTHLY = "monthly"


utils.ExecTypes = ExecTypes
sys.modules["arco_era5.utils"] = utils

config_spec = importlib.util.spec_from_file_location(
    "arco_era5.update_config_files", PACKAGE_DIR / "update_config_files.py")
update_config_files = importlib.util.module_from_spec(config_spec)
sys.modules[config_spec.name] = update_config_files
config_spec.loader.exec_module(update_config_files)


class PreviousMonthDatesTest(unittest.TestCase):

    def _dates(self, today, mode):
        class FixedDate(datetime.date):
            @classmethod
            def today(cls):
                return cls(today.year, today.month, today.day)

        with mock.patch.object(update_config_files.datetime, "date", FixedDate):
            return update_config_files.get_previous_month_dates(mode)

    def test_era5_uses_exact_third_previous_calendar_month(self):
        cases = [
            (datetime.date(2026, 1, 31), datetime.date(2025, 10, 1), datetime.date(2025, 10, 31)),
            (datetime.date(2026, 3, 1), datetime.date(2025, 12, 1), datetime.date(2025, 12, 31)),
            (datetime.date(2024, 5, 31), datetime.date(2024, 2, 1), datetime.date(2024, 2, 29)),
            (datetime.date(2025, 5, 31), datetime.date(2025, 2, 1), datetime.date(2025, 2, 28)),
            (datetime.date(2026, 12, 31), datetime.date(2026, 9, 1), datetime.date(2026, 9, 30)),
        ]
        for today, first, last in cases:
            with self.subTest(today=today):
                result = self._dates(today, ExecTypes.ERA5.value)
                self.assertEqual(result["first_day"], first)
                self.assertEqual(result["last_day"], last)
                self.assertEqual(result["sl_year"], f"{first.year:04d}")
                self.assertEqual(result["sl_month"], f"{first.month:02d}")

    def test_monthly_mode_still_returns_immediately_previous_full_month(self):
        cases = [
            (datetime.date(2026, 1, 1), datetime.date(2025, 12, 1), datetime.date(2025, 12, 31)),
            (datetime.date(2024, 3, 31), datetime.date(2024, 2, 1), datetime.date(2024, 2, 29)),
            (datetime.date(2025, 3, 1), datetime.date(2025, 2, 1), datetime.date(2025, 2, 28)),
        ]
        for today, first, last in cases:
            with self.subTest(today=today):
                result = self._dates(today, ExecTypes.ERA5T_MONTHLY.value)
                self.assertEqual((result["first_day"], result["last_day"]), (first, last))

    def test_daily_mode_remains_exactly_six_days_behind(self):
        for today in (datetime.date(2026, 1, 3), datetime.date(2024, 3, 2),
                      datetime.date(2026, 10, 4)):
            with self.subTest(today=today):
                result = self._dates(today, ExecTypes.ERA5T_DAILY.value)
                expected = today - datetime.timedelta(days=6)
                self.assertEqual(result["first_day"], expected)
                self.assertEqual(result["last_day"], expected)

    def test_get_month_range_handles_year_and_leap_boundaries(self):
        cases = [
            (datetime.date(2026, 1, 15), datetime.date(2025, 12, 1), datetime.date(2025, 12, 31)),
            (datetime.date(2024, 3, 1), datetime.date(2024, 2, 1), datetime.date(2024, 2, 29)),
            (datetime.date(2025, 3, 1), datetime.date(2025, 2, 1), datetime.date(2025, 2, 28)),
        ]
        for date, first, last in cases:
            with self.subTest(date=date):
                self.assertEqual(update_config_files.get_month_range(date), (first, last))


if __name__ == "__main__":
    unittest.main()
