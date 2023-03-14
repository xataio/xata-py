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

import pytest
import utils

from xata.client import DEFAULT_BRANCH_NAME, XataClient, __version__


class TestClientInternals(unittest.TestCase):
    def test_sdk_version(self):
        db_url = "https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert "version" in cfg
        assert utils.PATTERNS_SDK_VERSION.match(cfg["version"])
        assert __version__ == cfg["version"]

    def test_get_and_set_db_branch_name(self):
        db_name = "db-123"
        branch_name = "fancy-new-feature-branch"

        client = XataClient(
            api_key="api_key",
            workspace_id="ws_id",
            db_name=db_name,
            branch_name=branch_name,
        )
        assert client.get_db_branch_name() == f"{db_name}:{branch_name}"

        client.set_db_and_branch_names(None, "different-branch")
        assert client.get_db_branch_name() == f"{db_name}:different-branch"

        client.set_db_and_branch_names("different-db")
        assert client.get_db_branch_name() == "different-db:different-branch"

        client.set_db_and_branch_names(db_name, branch_name)
        assert client.get_db_branch_name() == f"{db_name}:{branch_name}"
        assert client.get_db_branch_name("foo") == f"foo:{branch_name}"
        assert client.get_db_branch_name("foo", "bar") == "foo:bar"

    def test_get_config(self):
        api_key = "api_key-abc"
        ws_id = "ws-idddddd"
        db_name = "my_db-042"
        branch_name = "super-duper-feature"
        region = "r-e-g-i-o-n-1"

        client = XataClient(
            api_key=api_key,
            workspace_id=ws_id,
            db_name=db_name,
            branch_name=branch_name,
            region=region,
        )

        conf = client.get_config()
        assert len(conf) == 7

        assert "dbName" in conf
        assert "branchName" in conf
        assert "apiKey" in conf
        assert "apiKeyLocation" in conf
        assert "workspaceId" in conf
        assert "region" in conf
        assert "version" in conf

        assert conf["dbName"] == db_name
        assert conf["branchName"] == branch_name
        assert conf["apiKey"] == api_key
        assert conf["apiKeyLocation"] == "parameter"
        assert conf["workspaceId"] == ws_id
        assert conf["region"] == region
        assert conf["version"] == __version__

        client.set_db_and_branch_names("avengers", "where-is-the-hulk")
        assert client.get_config() != conf

        assert client.get_config()["dbName"] != conf["dbName"]
        assert client.get_config()["branchName"] != conf["branchName"]

        assert client.get_config()["dbName"] == "avengers"
        assert client.get_config()["branchName"] == "where-is-the-hulk"

        assert client.get_config()["apiKey"] == conf["apiKey"]

    def test_parse_database_url_simple(self):
        db_url = "https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert "py-sdk-unit-test-12345" == cfg["workspaceId"]
        assert "eu-west-1" == cfg["region"]
        assert "testopia-042" == cfg["dbName"]
        assert "main" == cfg["branchName"]

        ws, r, db, bn = client._parse_database_url(db_url)
        assert "py-sdk-unit-test-12345" == ws
        assert "eu-west-1" == r
        assert "testopia-042" == db
        assert "main" == bn

    def test_parse_database_url_with_branch_name(self):
        db_url = "https://test-abc345.us-west-7.xata.sh/db/db-name:pr1234-unit-tests"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert "test-abc345" == cfg["workspaceId"]
        assert "us-west-7" == cfg["region"]
        assert "db-name" == cfg["dbName"]
        assert "pr1234-unit-tests" == cfg["branchName"]

        ws, r, db, bn = client._parse_database_url(db_url)
        assert "test-abc345" == ws
        assert "us-west-7" == r
        assert "db-name" == db
        assert "pr1234-unit-tests" == bn

    def test_parse_database_url_with_empty_branch_name(self):
        db_url = "https://ws-id.region.xata.sh/db/db-name:"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert DEFAULT_BRANCH_NAME == cfg["branchName"]
        assert "db-name" == cfg["dbName"]

    def test_parse_database_url_with_custom_base_url(self):
        db_url = "https://test-abc345.us-west-7.xatabase-playground.co.at/db/db-name:pr1234-unit-tests"
        client = XataClient(db_url=db_url)
        cfg = client.get_config()

        assert True
        # TODO assert custom base url

    def test_parse_database_url_with_invalid_urls(self):
        # Invalid workspace.region
        with pytest.raises(Exception):
            XataClient(db_url="https://invalid.xata.sh/db/db-name:pr1234-unit-tests")

        # Missing db name
        with pytest.raises(Exception):
            XataClient(db_url="https://ws-id.region.xata.sh/db/")
