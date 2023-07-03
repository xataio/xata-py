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

import pytest
import utils

from xata.client import XataClient


class TestBranchNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        # create database
        assert (
            self.client.databases()
            .create(
                self.db_name,
                {
                    "region": self.client.get_config()["region"],
                    "branchName": self.client.get_config()["branchName"],
                },
            )
            .is_success()
        )
        assert self.client.table().create("Posts").is_success
        assert (
            self.client.table()
            .set_schema(
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
            .is_success()
        )

    def teardown_class(self):
        r = self.client.databases().delete(self.db_name)
        assert r.is_success()

    def test_get_branch_list(self):
        r = self.client.branch().get_branches(self.db_name)
        assert r.is_success()
        assert "databaseName" in r
        assert "branches" in r
        assert r["databaseName"] == self.db_name
        assert len(r["branches"]) == 1
        assert "name" in r["branches"][0]
        assert "createdAt" in r["branches"][0]
        assert r["branches"][0]["name"] == "main"

        r = self.client.branch().get_branches("NonExistingDatabase")
        assert r.status_code() == 404
        assert not r.is_success()

    def test_get_branch_details(self):
        r = self.client.branch().get_details()
        assert r.is_success()
        assert "databaseName" in r
        assert "branchName" in r
        assert "metadata" in r
        assert "schema" in r
        assert r["databaseName"] == self.client.get_config()["dbName"]
        # TODO be exhastive testing the ^ dict keys

        r = self.client.branch().get_details("NonExistingDatabase")
        assert r.status_code() == 404
        assert not r.is_success()

    def test_create_database_branch(self):
        payload = {
            "from": "main",
            "metadata": {
                "repository": "github.com/xataio/xata-py",
                "branch": "integration-testing-%s" % utils.get_random_string(6),
                "stage": "testing",
            },
        }
        r = self.client.branch().create(payload, branch_name="new-super-duper-feature")
        assert r.is_success()
        assert "databaseName" in r
        assert "branchName" in r
        assert "status" in r
        assert r["databaseName"] == self.client.get_config()["dbName"]
        assert r["branchName"] == "new-super-duper-feature"
        assert r["status"] == "completed"

        pytest.branch["branch"] = payload

        r = self.client.branch().create(payload, branch_name="the-incredible-hulk", _from="avengers")
        assert r.status_code() == 400
        assert not r.is_success()

        r = self.client.branch().create(payload, db_name="NOPE", branch_name=self.branch_name)
        assert r.status_code() == 404
        assert not r.is_success()

        r = self.client.branch().create({})
        assert r.status_code() == 422
        assert not r.is_success()

    def test_get_branch_metadata(self):
        r = self.client.branch().get_metadata()
        assert r.is_success()

        # TODO test from a previously created branch
        # assert "repository" in r
        # assert "branch" in r
        # assert "stage" in r

        r = self.client.branch().get_metadata(branch_name=self.branch_name)
        assert r.is_success()
        r = self.client.branch().get_metadata(db_name=self.db_name)
        assert r.is_success()

        r = self.client.branch().get_metadata(db_name="NOPE")
        assert r.status_code() == 404
        assert not r.is_success()
        r = self.client.branch().get_metadata(branch_name="shrug")
        assert r.status_code() == 404
        assert not r.is_success()

    def test_get_branch_stats(self):
        r = self.client.branch().get_stats()
        assert r.is_success()
        assert "timestamp" in r
        assert "interval" in r
        # TODO test more ^ dict keys
