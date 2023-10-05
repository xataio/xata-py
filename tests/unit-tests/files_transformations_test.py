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

from xata.client import XataClient

class TestFileTransformations(unittest.TestCase):
    def test_transform_url(self):
        client = XataClient(api_key="api_key", workspace_id="ws_id")

        url = client.files().transform_url(
            "https://us-east-1.storage.xata.sh/4u1fh2o6p10blbutjnphcste94",
            {
                "height": 100, 
            }
        )

        expected = "https://us-east-1.storage.xata.sh/transform/height=100/4u1fh2o6p10blbutjnphcste94"
        self.assertEqual(url, expected)

        