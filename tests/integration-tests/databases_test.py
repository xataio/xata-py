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

import utils

from xata.client import XataClient


class TestDatabasesNamespace(object):
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_create_database(self):
        r = self.client.databases().createDatabase(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

    def test_list_databases(self):
        r = self.client.databases().getDatabaseList()
        assert r.status_code == 200
        assert "databases" in r.json()
        assert len(r.json()["databases"]) > 0
        assert "name" in r.json()["databases"][0]
        assert "region" in r.json()["databases"][0]
        assert "createdAt" in r.json()["databases"][0]

        # TODO find db record in list
        # assert r.json()["databases"][0]["name"] == self.db_name
        # assert r.json()["databases"][0]["region"] == self.get_config()["region"]

        r = self.client.databases().getDatabaseList("NonExistingWorkspaceId")
        assert r.status_code == 401

    def test_get_database_metadata(self):
        r = self.client.databases().getDatabaseMetadata(self.db_name)
        assert r.status_code == 200
        assert "name" in r.json()
        assert "region" in r.json()
        assert "createdAt" in r.json()
        assert r.json()["name"] == self.db_name
        assert r.json()["region"] == self.client.get_config()["region"]

        r = self.client.databases().getDatabaseMetadata(
            self.db_name, workspace_id="NonExistingWorkspaceId"
        )
        assert r.status_code == 401

        r = self.client.databases().getDatabaseMetadata("NonExistingDatabase")
        assert r.status_code == 404

    def test_update_database_metadata(self):
        metadata = {"ui": {"color": "green"}}
        r_old = self.client.databases().getDatabaseMetadata(self.db_name)
        assert r_old.status_code == 200
        r_new = self.client.databases().updateDatabaseMetadata(self.db_name, metadata)
        assert r_new.status_code == 200
        assert "name" in r_new.json()
        assert "region" in r_new.json()
        assert "createdAt" in r_new.json()
        assert "ui" in r_new.json()
        assert r_new.json()["name"] == self.db_name
        assert r_new.json()["region"] == self.client.get_config()["region"]
        assert r_old.json() != r_new.json()
        assert r_new.json()["ui"] == metadata["ui"]

        r = self.client.databases().updateDatabaseMetadata(self.db_name, {})
        assert r.status_code == 400
        r = self.client.databases().updateDatabaseMetadata(
            "NonExistingDatabase", metadata
        )
        assert r.status_code == 400

        r = self.client.databases().updateDatabaseMetadata(
            self.db_name, metadata, workspace_id="NonExistingWorkspaceId"
        )
        assert r.status_code == 401

    # run last for cleanup
    def test_delete_database(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200
        assert r.json()["status"] == "completed"

        r = self.client.databases().deleteDatabase("NonExistingDatabase")
        assert r.status_code == 404

        r = self.client.databases().deleteDatabase(
            self.db_name, workspace_id="NonExistingWorkspaceId"
        )
        assert r.status_code == 401

    def test_get_available_regions(self):
        r = self.client.databases().listRegions()
        assert r.status_code == 200
        assert "regions" in r.json()
        assert len(r.json()["regions"]) == 3

        r = self.client.databases().listRegions(workspace_id="NonExistingWorkspaceId")
        assert r.status_code == 401
