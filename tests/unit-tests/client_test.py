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

import os
import re
import unittest

import pytest

from xata.client import XataClient, __version__

PATTERNS_UUID4 = re.compile(r"^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$", re.IGNORECASE)
PATTERNS_SDK_VERSION = re.compile(r"^[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}$")


class TestXataClient(unittest.TestCase):

    def test_init_api_key_with_params(self):
        api_key = "param_ABCDEF123456789"

        client = XataClient(api_key=api_key, workspace_id="ws_id")
        cfg = client.get_config()

        assert "apiKey" in cfg
        assert api_key == cfg["apiKey"]
        assert "apiKeyLocation" in cfg
        assert "parameter" == cfg["apiKeyLocation"]

    def test_init_api_key_via_envvar(self):
        api_key = "envvar_ABCDEF123456789"
        os.environ["XATA_API_KEY"] = api_key

        client = XataClient(workspace_id="ws_id")
        cfg = client.get_config()

        assert "apiKey" in cfg
        assert api_key == cfg["apiKey"]
        assert "apiKeyLocation" in cfg
        assert "env" == cfg["apiKeyLocation"]

    def test_init_api_key_via_xatarc(self):
        # api_key = "xatarc_ABCDEF123456789"
        # TODO
        pass

    def test_init_api_key_via_dotenv(self):
        # api_key = "dotenv_ABCDEF123456789"
        # TODO
        pass

    def test_init_db_url(self):
        db_url = "https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert "workspaceId" in cfg
        assert "py-sdk-unit-test-12345" == cfg["workspaceId"]
        assert "region" in cfg
        assert "eu-west-1" == cfg["region"]
        assert "dbName" in cfg
        assert "testopia-042" == cfg["dbName"]

    def test_init_db_url_invalid_combinations(self):
        with pytest.raises(Exception):
            XataClient(db_url="db_url", workspace_id="ws_id")

        with pytest.raises(Exception):
            XataClient(db_url="db_url", db_name="db_name")

        with pytest.raises(Exception):
            XataClient(db_url="db_url", workspace_id="ws_id", db_name="db_name")

    def test_sdk_version(self):
        db_url = "https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert "version" in cfg
        assert PATTERNS_SDK_VERSION.match(cfg["version"])
        assert __version__ == cfg["version"]

    def test_telemetry_headers(self):
        api_key = "this-key-42"
        client1 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers1 = client1.get_headers()

        assert len(headers1) == 5
        assert "authorization" in headers1
        assert headers1["authorization"] == f"Bearer {api_key}"
        assert "x-xata-client-id" in headers1
        assert PATTERNS_UUID4.match(headers1["x-xata-client-id"])
        assert "x-xata-session-id" in headers1
        assert PATTERNS_UUID4.match(headers1["x-xata-session-id"])
        assert headers1["x-xata-client-id"] != headers1["x-xata-session-id"]
        assert "x-xata-agent" in headers1
        assert headers1["x-xata-agent"] == f"client=PY_SDK;version={__version__};"
        assert "user-agent" in headers1
        assert headers1["user-agent"] == f"xataio/xata-py:{__version__}"

        api_key = "this-key-42"
        client2 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers2 = client2.get_headers()

        assert headers1["x-xata-client-id"] != headers2["x-xata-client-id"]
        assert headers1["x-xata-session-id"] != headers2["x-xata-session-id"]

        assert headers1["x-xata-agent"] == headers2["x-xata-agent"]
        assert headers1["user-agent"] == headers2["user-agent"]

    def test_get_and_set_db_branch_name(self):
        db_name = "db-123"
        db_url = ""
        branch_name = "fancy-new-feature-branch"

        client = XataClient(api_key="api_key", workspace_id="ws_id", db_name=db_name, branch_name=branch_name)
        assert client.get_db_branch_name() == f"{db_name}:{branch_name}"

        client.set_db_and_branch_names(None, "different-branch")
        assert client.get_db_branch_name() == f"{db_name}:different-branch"

        client.set_db_and_branch_names("different-db")
        assert client.get_db_branch_name() == f"different-db:different-branch"

        client.set_db_and_branch_names(db_name, branch_name)
        assert client.get_db_branch_name() == f"{db_name}:{branch_name}"
        assert client.get_db_branch_name("foo") == f"foo:{branch_name}"
        assert client.get_db_branch_name("foo", "bar") == f"foo:bar"

