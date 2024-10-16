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
import datetime
import gcsfs
import logging

import typing as t

from .utils import generate_urls

logger = logging.getLogger(__name__)

def check_data_availability(data_date_range: t.List[datetime.datetime], type: str = None) -> bool:
    """Checks the availability of data for a given date range.

    Args:
        data_date_range (List[datetime.datetime]): Date range for CO data.

    Returns:
        int: 1 if data is missing, 0 if data is available.
    """

    fs = gcsfs.GCSFileSystem()
    start_date = data_date_range[0].strftime("%Y/%m/%d")
    end_date = data_date_range[-1].strftime("%Y/%m/%d")

    all_uri = generate_urls(data_date_range, start_date, end_date, type)

    data_is_missing = False
    for path in all_uri:
        if not fs.exists(path):
            data_is_missing = True
            logger.info(path)

    return True if data_is_missing else False
