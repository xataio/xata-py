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

from xata.client import XataClient, DEFAULT_REGION


class TestClientInit(unittest.TestCase):
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

    def test_init_region(self):
        client1 = XataClient(api_key="api_key", workspace_id="ws_id")
        assert "region" in client1.get_config()
        assert DEFAULT_REGION == client1.get_config()["region"]

        region = "a-region-42"
        assert DEFAULT_REGION != region
        client2 = XataClient(api_key="api_key", workspace_id="ws_id", region=region)
        assert "region" in client2.get_config()
        assert region == client2.get_config()["region"]

    def test_init_region_from_db_url(self):
        db_url = "https://unit-tests-abc123.us-west-2.xata.sh/db/docs"
        client = XataClient(api_key='xau_redacted', db_url=db_url)
        assert "region" in client.get_config()
        assert "us-west-2" == client.get_config()["region"]

    def test_init_region_from_envvars(self):
        env_region = "this-region-123"
        assert DEFAULT_REGION != env_region
        os.environ["XATA_REGION"] = env_region

        client1 = XataClient(api_key="api_key", workspace_id="ws_id")
        assert "region" in client1.get_config()
        assert env_region != client1.get_config()["region"]
        assert DEFAULT_REGION == client1.get_config()["region"]
        # ^ if the workspace Id is passed via param, then the env var of
        # region is ignore. it must passed via param as well. see client init

        os.environ["XATA_WORKSPACE_ID"] = "lalilu-123456"
        client2 = XataClient(api_key="api_key")
        assert env_region == client2.get_config()["region"]
        assert DEFAULT_REGION != client2.get_config()["region"]
        assert "lalilu-123456" == client2.get_config()["workspaceId"]


