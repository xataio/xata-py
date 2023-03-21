#
# Licensed to Xatabase, Inc under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Xatabase, Inc licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You
# may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import unittest

from pytz import timezone, utc
from datetime import datetime

from xata.helpers import to_rfc339

class TestHelpersToRfc3339(unittest.TestCase):
    def test_to_rfc3339(self):
        dt1 = datetime.strptime("2023-03-20 13:42:00", "%Y-%m-%d %H:%M:%S")
        assert to_rfc339(dt1) == "2023-03-20T13:42:00+00:00"

        dt2 = datetime.strptime("2023-03-20 13:42", "%Y-%m-%d %H:%M")
        assert to_rfc339(dt2) == "2023-03-20T13:42:00+00:00"

        dt3 = datetime.strptime("2023-03-20", "%Y-%m-%d")
        assert to_rfc339(dt3) == "2023-03-20T00:00:00+00:00"

        dt4 = datetime.strptime("2023-03-20 13:42:16", "%Y-%m-%d %H:%M:%S")
        tz4 = timezone("Europe/Vienna")
        assert to_rfc339(dt4, tz4) == "2023-03-20T13:42:16+01:05"

        dt4 = datetime.strptime("2023-03-20 13:42:16", "%Y-%m-%d %H:%M:%S")
        tz4 = timezone("America/New_York")
        assert to_rfc339(dt4, tz4) == "2023-03-20T13:42:16-04:56"

        dt6 = datetime.strptime("2023-03-20 13:42:16", "%Y-%m-%d %H:%M:%S")
        assert to_rfc339(dt6, utc) == "2023-03-20T13:42:16+00:00"
