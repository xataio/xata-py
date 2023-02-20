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
import unittest

import pytest

from xata.client import XataClient


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
