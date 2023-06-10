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


class TestClientConfigGetters(unittest.TestCase):
    def test_get_region(self):
        region = "eu-south-42"
        client = XataClient(api_key="api_key", workspace_id="ws_id", region=region)
        assert client.get_region() == client.get_config()["region"]
        assert region == client.get_region()

    def test_get_database_name(self):
        db_name = "the-marvelous-world-of-dbs-001"
        client = XataClient(api_key="api_key", workspace_id="ws_id", db_name=db_name)
        assert client.get_database_name() == client.get_config()["dbName"]
        assert db_name == client.get_database_name()

    def test_get_branch_name(self):
        branch_name = "my-amazing/new-feature"
        client = XataClient(api_key="api_key", workspace_id="ws_id", branch_name=branch_name)
        assert client.get_branch_name() == client.get_config()["branchName"]
        assert branch_name == client.get_branch_name()

    def test_get_workspace_id(self):
        ws_id = "a-workspace-01se45"
        client = XataClient(api_key="api_key", workspace_id=ws_id)
        assert client.get_workspace_id() == client.get_config()["workspaceId"]
        assert ws_id == client.get_workspace_id()
