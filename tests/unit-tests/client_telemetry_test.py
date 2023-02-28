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

import utils

from xata.client import XataClient, __version__


class TestXataClient(unittest.TestCase):
    def test_telemetry_headers(self):
        api_key = "this-key-42"
        client1 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers1 = client1.get_headers()

        assert "x-xata-client-id" in headers1
        assert utils.PATTERNS_UUID4.match(headers1["x-xata-client-id"])
        assert "x-xata-session-id" in headers1
        assert utils.PATTERNS_UUID4.match(headers1["x-xata-session-id"])
        assert headers1["x-xata-client-id"] != headers1["x-xata-session-id"]
        assert "x-xata-agent" in headers1
        assert headers1["x-xata-agent"] == f"client=PY_SDK;version={__version__};"

        api_key = "this-key-42"
        client2 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers2 = client2.get_headers()

        assert headers1["x-xata-client-id"] != headers2["x-xata-client-id"]
        assert headers1["x-xata-session-id"] != headers2["x-xata-session-id"]

        assert headers1["x-xata-agent"] == headers2["x-xata-agent"]
        assert headers1["user-agent"] == headers2["user-agent"]

    def test_telemetry_sessions(self):
        api_key = "this-key-42"
        client1 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers1 = client1.get_headers()

        api_key = "this-key-42"
        client2 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers2 = client2.get_headers()

        assert headers1 != headers2

        assert headers1["x-xata-client-id"] != headers2["x-xata-client-id"]
        assert headers1["x-xata-session-id"] != headers2["x-xata-session-id"]

        assert headers1["x-xata-agent"] == headers2["x-xata-agent"]
        assert headers1["user-agent"] == headers2["user-agent"]
