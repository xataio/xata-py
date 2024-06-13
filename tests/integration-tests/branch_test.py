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

import pytest
import utils

from xata.client import XataClient


class TestBranchNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().set_schema("Posts", utils.get_posts_schema()).is_success()

    def teardown_class(self):
        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().delete(self.db_name).is_success()
        else:
            assert self.client.table().delete("Posts").is_success()
            for b in self.client.branch().list(self.db_name).get("branches"):
                if b["name"] != "main":
                    self.client.branch().delete(branch_name=b["name"])

    def test_get_branch_list(self):
        r = self.client.branch().list(self.db_name)
        assert r.is_success()
        assert "databaseName" in r
        assert "branches" in r
        assert r["databaseName"] == self.db_name
        assert len(r["branches"]) == 1
        assert "name" in r["branches"][0]
        assert "createdAt" in r["branches"][0]
        assert r["branches"][0]["name"] == "main"

        r = self.client.branch().list("NonExistingDatabase")
        assert r.status_code == 404
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
        assert r.status_code == 404
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

        r = self.client.branch().create(payload, branch_name="the-incredible-hulk", from_="avengers")
        assert r.status_code == 400
        assert not r.is_success()

        r = self.client.branch().create(payload, db_name="NOPE", branch_name=self.branch_name)
        assert r.status_code == 404
        assert not r.is_success()

        r = self.client.branch().create({})
        assert r.status_code == 422
        assert not r.is_success()

    def test_create_database_branch_from_other_branch_with_param(self):
        payload = {
            "metadata": {
                "repository": "github.com/xataio/xata-py",
                "branch": "integration-testing-%s" % utils.get_random_string(6),
                "stage": "testing",
            },
        }
        r = self.client.branch().create(payload, branch_name="source-from")
        assert r.status_code == 201

        r = self.client.branch().create(payload, branch_name="new-branch", from_="source-from")
        assert r.status_code == 201
        assert r["status"] == "completed"

        assert (
            not self.client.branch().create(payload, branch_name="the-incredible-hulk", from_="avengers").is_success()
        )
        assert (
            not self.client.branch()
            .create(payload, db_name="marvel-042", branch_name="the-incredible-hulk", from_="avengers")
            .is_success()
        )

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
        assert r.status_code == 404
        assert not r.is_success()
        r = self.client.branch().get_metadata(branch_name="shrug")
        assert r.status_code == 404
        assert not r.is_success()

    def test_get_branch_stats(self):
        r = self.client.branch().get_stats()
        assert r.is_success()
        assert "timestamp" in r
        assert "interval" in r
        # TODO test more ^ dict keys
