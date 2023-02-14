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


class TestClass(object):
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        # create database
        r = self.client.databases().createDatabase(
            self.client.get_config()["workspaceId"],
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable(self.client.get_db_branch_name(), "Posts")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            self.client.get_db_branch_name(),
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "labels", "type": "multiple"},
                    {"name": "slug", "type": "string"},
                    {"name": "text", "type": "text"},
                ]
            },
        )
        assert r.status_code == 200

    @classmethod
    def teardown_class(self):
        r = self.client.databases().deleteDatabase(
            self.client.get_config()["workspaceId"], self.db_name
        )
        assert r.status_code == 200

    def test_get_branch_list(self):
        r = self.client.branch().getBranchList(self.db_name)
        assert r.status_code == 200
        assert "databaseName" in r.json()
        assert "branches" in r.json()
        assert r.json()["databaseName"] == self.db_name
        assert len(r.json()["branches"]) == 1
        assert "name" in r.json()["branches"][0]
        assert "createdAt" in r.json()["branches"][0]
        assert r.json()["branches"][0]["name"] == "main"

        r = self.client.branch().getBranchList("NonExistingDatabase")
        assert r.status_code == 404

    def test_get_branch_details(self):
        r = self.client.branch().getBranchDetails(self.client.get_db_branch_name())
        assert r.status_code == 200
        assert "databaseName" in r.json()
        assert "branchName" in r.json()
        assert "metadata" in r.json()
        assert "schema" in r.json()
        assert r.json()["databaseName"] == self.client.get_config()["dbName"]
        # TODO be exhastive testing the ^ dict keys

        r = self.client.branch().getBranchDetails("NonExistingDatabase")
        assert r.status_code == 400

    def test_create_database_branch(self):
        payload = {
            "from": "main",
            "metadata": {
                "repository": "github.com/xataio/xata-py",
                "branch": "integration-testing-%s" % utils.get_random_string(6),
                "stage": "testing",
            },
        }
        """
        r = self.client.branch().createBranch(self.client.get_db_branch_name(), payload)
        assert r.json() == ""
        assert r.status_code == 201
        assert "databaseName" in r.json()
        assert "branchName" in r.json()
        assert "status" in r.json()
        assert r.json()["databaseName"] == self.client.get_config()["dbName"]
        assert r.json()["branchName"] == payload["metadata"]["branch"]
        assert r.json()["status"] == "completed"

        pytest.branch["branch"] = payload

        r = self.client.branch().createBranch(self.client.get_db_branch_name(), payload)
        assert r.status_code == 422
        """
        r = self.client.branch().createBranch("NonExistingDbBranchName", payload)
        assert r.status_code == 400

        r = self.client.branch().createBranch(self.client.get_db_branch_name(), {})
        assert r.status_code == 422

    def test_get_branch_metadata(self):
        r = self.client.branch().getBranchMetadata(self.client.get_db_branch_name())
        assert r.status_code == 200

        # TODO test from a previously created branch
        # assert "repository" in r.json()
        # assert "branch" in r.json()
        # assert "stage" in r.json()

        r = self.client.branch().getBranchMetadata("NonExistingDbBranchName")
        assert r.status_code == 400

    def test_get_branch_stats(self):
        r = self.client.branch().getBranchStats(self.client.get_db_branch_name())
        assert r.status_code == 200
        assert "timestamp" in r.json()
        assert "interval" in r.json()
        # TODO test more ^ dict keys

        r = self.client.branch().getBranchStats("NonExistingDbBranchName")
        assert r.status_code == 400
