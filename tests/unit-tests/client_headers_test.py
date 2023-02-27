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

    def test_delete_header(self):
        client = XataClient(api_key="api_key", workspace_id="ws_id")
        proof = client.get_headers()
        assert "user-agent" in proof
        assert "x-xata-client-id" in proof

        assert client.delete_header("user-agent")
        assert "user-agent" not in client.get_headers()

        assert client.delete_header("X-XaTa-cLienT-Id")
        assert "x-xata-client-id" not in client.get_headers()

        assert client.delete_header("user-agent") == False
        assert client.delete_header("Not-Existing-Header") == False


    def test_set_and_delete_header(self):
        client = XataClient(api_key="api_key_123", workspace_id="ws_id")
        proof = client.get_headers()
        assert "new-header" not in proof
        assert "x-proxy-timeout" not in proof

        client.set_header("new-header", "testing")
        new_headers = client.get_headers()

        #assert proof != new_headers
        assert "new-header" in new_headers
        assert new_headers["new-header"] == "testing"

        assert client.delete_header("new-header")
        #assert proof == client.get_headers()
        #assert new_headers != client.get_headers()

        client.set_header("X-PROXY-TIMEOUT", "1200")
        assert "x-proxy-timeout" in client.get_headers()
        assert "X-PROXY-TIMEOUT" not in client.get_headers()
