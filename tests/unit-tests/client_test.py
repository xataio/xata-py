# -*- coding: utf-8 -*-

import os
import re
import unittest

import pytest

from xata.client import XataClient

PATTERNS_UUID4 = re.compile(r"^[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}$", re.IGNORECASE)


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

    def test_init_telemetry_headers(self):
        api_key = "this-key-42"
        client1 = XataClient(api_key=api_key, workspace_id="ws_id")
        headers = client1.get_headers()

        assert len(headers) == 3
        assert "authorization" in headers
        assert headers["authorization"] == f"Bearer {api_key}"
        assert "x-xata-client-id" in headers
        assert PATTERNS_UUID4.match(headers["x-xata-client-id"])
        assert "x-xata-session-id" in headers
        assert PATTERNS_UUID4.match(headers["x-xata-session-id"])
