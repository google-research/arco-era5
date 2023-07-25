import configparser
import datetime
import json
import os
import tempfile
import unittest

from unittest.mock import patch, MagicMock

# with . or not below??
from fetch import (
    new_config_file,
    get_month_range,
    get_single_level_dates,
    get_previous_month_dates,
    update_config_files,
    get_secret,
)

class TestFetchFunctions(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()
        self.config_file = os.path.join(self.temp_dir, "test_config.cfg")
        with open(self.config_file, "w") as file:
            file.write(
                "[parameters]\nclient=cds\ndataset=reanalysis-era5-complete\n\
                target_path=gs://gcp-public-data-arco-era5/raw/ERA5GRIB/HRES\
                /Daily/{date:%%Y/%%Y%%m%d}_hres_dve.grb2\npartition_keys=\n\t\
                dates\n\n[selection]\nclass=ea\nstream=oper\nexpver=1\ntype=an\n\
                levtype=ml\nlevelist=1/to/137\ndate=1979-01-01/to/2023-03-31\n\
                time=00/to/23\nparam=138/155\n"
            )
        self.first_day_first_prev = datetime.date(2023, 7, 1)
        self.last_day_first_prev = datetime.date(2023, 7, 31)
        self.first_day_third_prev = datetime.date(2023, 5, 1)
        self.last_day_third_prev = datetime.date(2023, 5, 31)
        self.sl_year, self.sl_month = "2023", "05"
        self.sl_first_date, self.sl_last_date = "01", "31"
        self.additional_content = "[parameters.test]\napi_url=test_url\napi_key=\
            test_key\n\n"

    def tearDown(self):
        os.remove(self.config_file)
        os.rmdir(self.temp_dir)

    def test_new_config_file(self):
        section_name = 'parameters.test'
        section_api_url = 'test_url'
        section_api_key = 'test_key'
        additional_content = f'{section_name}\napi_url={section_api_url}\n\
            api_key={section_api_key}\n\n'
        
        new_config_file(
            self.config_file, "date", additional_content,
            False, False, self.first_day_first_prev,
            self.last_day_first_prev, self.first_day_third_prev,
            self.last_day_third_prev, self.sl_year, self.sl_month,
            self.sl_first_date, self.sl_last_date)

        config = configparser.ConfigParser()
        config.read(self.config_file)
        self.assertIn(section_name, config.sections())
        self.assertEqual(config.get('selection', 'date'),
                            f'{self.first_day_third_prev}/to/{self.last_day_third_prev}')
        self.assertEqual(config.get(section_name, 'api_url'),
                            section_api_url)
        self.assertEqual(config.get(section_name, 'api_key'),
                            section_api_key)

    def test_new_config_file_with_co_file(self):
        co_file = True
        single_level_file = False

        new_config_file(
            self.config_file, "date", self.additional_content,
            co_file, single_level_file, self.first_day_first_prev,
            self.last_day_first_prev, self.first_day_third_prev,
            self.last_day_third_prev, self.sl_year, self.sl_month,
            self.sl_first_date, self.sl_last_date)
        
        config = configparser.ConfigParser()
        config.read(self.config_file)

        self.assertEqual(config.get('selection', 'date'),
                            f'{self.first_day_first_prev}/to/{self.last_day_first_prev}')

    def test_new_config_file_with_single_level_file(self):
        co_file = False
        single_level_file = True

        new_config_file(self.config_file, 'date', self.additional_content,
                        co_file, single_level_file, self.first_day_first_prev,
                        self.last_day_first_prev, self.first_day_third_prev,
                        self.last_day_third_prev, self.sl_year, self.sl_month,
                        self.sl_first_date, self.sl_last_date)

        config = configparser.ConfigParser()
        config.read(self.config_file)

        self.assertEqual(config.get('selection', 'year'), self.sl_year)
        self.assertEqual(config.get('selection', 'month'), self.sl_month)
        self.assertEqual(config.get('selection', 'day'), 
                         f'{self.sl_first_date}/to/{self.sl_last_date}')
    
    def test_get_month_range(self):
        # Test get_month_range function
        first_day, last_day = get_month_range(datetime.date(2023, 7, 18))
        self.assertEqual(first_day, datetime.date(2023, 6, 1))
        self.assertEqual(last_day, datetime.date(2023, 6, 30))

    def test_get_single_level_dates(self):
        # Test get_single_level_dates function
        first_day = datetime.date(2023, 7, 1)
        last_day = datetime.date(2023, 7, 31)
        year, month, first_date, last_date = get_single_level_dates(first_day, last_day)
        self.assertEqual(year, "2023")
        self.assertEqual(month, "07")
        self.assertEqual(first_date, "01")
        self.assertEqual(last_date, "31")

    def test_get_previous_month_dates(self):
        # Test get_previous_month_dates function
        prev_month_data = get_previous_month_dates()
        self.assertIn("first_day_first_prev", prev_month_data)
        self.assertIn("last_day_first_prev", prev_month_data)
        self.assertIn("first_day_third_prev", prev_month_data)
        self.assertIn("last_day_third_prev", prev_month_data)
        self.assertIn("sl_year", prev_month_data)
        self.assertIn("sl_month", prev_month_data)
        self.assertIn("sl_first_date", prev_month_data)
        self.assertIn("sl_last_date", prev_month_data)

    def test_update_config_files(self):
        # Test update_config_files function
        update_config_files(
            self.temp_dir, "date", self.additional_content)

    @patch("fetch.secretmanager.SecretManagerServiceClient")
    def test_get_secret_success(self, mock_secretmanager):
        secret_data = {
            "api_url": "https://example.com/api",
            "api_key": "my_secret_api_key"
        }
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = json.dumps(secret_data)
        mock_secretmanager.return_value.access_secret_version.return_value = (
            mock_response)

        api_key = "projects/my-project/secrets/my-secret/versions/latest"
        result = get_secret(api_key)
        self.assertEqual(result, secret_data)

    @patch("fetch.secretmanager.SecretManagerServiceClient")
    def test_get_secret_failure(self, mock_secretmanager):
        mock_secretmanager.return_value.access_secret_version.side_effect = ( 
            Exception("Error retrieving secret") )
        api_key = "projects/my-project/secrets/my-secret/versions/latest"
        with self.assertRaises(Exception):
            get_secret(api_key)


if __name__ == "__main__":
    unittest.main()
