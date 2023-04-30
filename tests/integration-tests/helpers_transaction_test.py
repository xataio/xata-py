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

import time

import pytest
import utils
from faker import Faker

from xata.client import XataClient
from xata.helpers import Transaction


class TestHelpersTransaction(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = Faker()

        # create database
        r = self.client.databases().createDatabase(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable("Posts")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "content", "type": "text"},
                ]
            },
        )
        assert r.status_code == 200

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200

    @pytest.fixture
    def record(self) -> dict:
        return self._get_record()

    def _get_record(self) -> dict:
        return {
            "title": self.fake.company(),
            "content": self.fake.text(),
        }

    def test_insert_records_and_response_shape(self, record: dict):
        trx = Transaction(self.client)
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        response = trx.run()

        assert "status_code" in response
        assert "success" in response
        assert "has_errors" in response
        assert "error_indexes" in response
        assert "results" in response
        assert "stats" in response
        assert "insert" in response["stats"]
        assert "update" in response["stats"]
        assert "delete" in response["stats"]
        assert "get" in response["stats"]

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["error_indexes"] == []
        assert response["stats"]["insert"] == 5
        assert response["stats"]["update"] == 0
        assert response["stats"]["delete"] == 0
        assert response["stats"]["get"] == 0
        assert len(response["results"]) == 5

        r = self.client.data().queryTable("Posts", {})
        assert len(r.json()["records"]) == 5

    def test_insert_records_with_create_only_option(self, record: dict):
        trx = Transaction(self.client)
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        response = trx.run()

        overwrite_me = [x["id"] for x in response["results"]]
        assert len(overwrite_me) > 0

        trx = Transaction(self.client)
        trx.insert("Posts", {"id": overwrite_me[0], "title": "a new title #1"}, True)
        trx.insert("Posts", {"id": overwrite_me[1], "title": "a new title #2"}, False)
        response = trx.run()

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["stats"]["insert"] == 2
        assert response["stats"]["update"] == 0
        assert response["stats"]["delete"] == 0
        assert response["stats"]["get"] == 0
        assert len(response["results"]) == 2

        r = self.client.data().queryTable("Posts", {})
        assert len(r.json()["records"]) == 5

    def test_delete_records(self, record: dict):
        setup = Transaction(self.client)
        for it in range(1, 10):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        delete_me = [x["id"] for x in response["results"]]
        assert len(delete_me) > 0

        trx = Transaction(self.client)
        for rid in delete_me:
            trx.delete("Posts", rid)
        response = trx.run()

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["error_indexes"] == []
        assert response["stats"]["insert"] == 0
        assert response["stats"]["update"] == 0
        assert response["stats"]["delete"] == len(delete_me)
        assert response["stats"]["get"] == 0
        assert len(response["results"]) == len(delete_me)

        r = self.client.data().queryTable("Posts", {})
        assert len(r.json()["records"]) == 5

    def test_delete_records_with_columns(self, record: dict):
        setup = Transaction(self.client)
        for it in range(0, 5):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        delete_me = [x["id"] for x in response["results"]]
        assert len(delete_me) > 0

        trx = Transaction(self.client)
        trx.delete("Posts", delete_me[0])
        trx.delete("Posts", delete_me[1], ["title"])
        trx.delete("Posts", delete_me[2], ["content"])
        trx.delete("Posts", delete_me[3], ["title", "content"])
        trx.delete("Posts", delete_me[4], ["id", "title", "content"])
        response = trx.run()

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["stats"]["delete"] == 5

        assert "columns" not in response["results"][0]
        assert list(response["results"][1]["columns"].keys()) == ["title"]
        assert list(response["results"][2]["columns"].keys()) == ["content"]
        assert list(response["results"][3]["columns"].keys()) == ["content", "title"]
        assert list(response["results"][4]["columns"].keys()) == ["content", "id", "title"]

        r = self.client.data().queryTable("Posts", {})
        assert len(r.json()["records"]) == 5

    def test_get_records(self, record: dict):
        setup = Transaction(self.client)
        for it in range(0, 10):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        get_me = [x["id"] for x in response["results"]]
        assert len(get_me) > 0

        trx = Transaction(self.client)
        for rid in get_me:
            trx.get("Posts", rid)
        response = trx.run()

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["error_indexes"] == []
        assert response["stats"]["insert"] == 0
        assert response["stats"]["update"] == 0
        assert response["stats"]["delete"] == 0
        assert response["stats"]["get"] == len(get_me)
        assert len(response["results"]) == len(get_me)

    def test_get_records_with_columns(self, record: dict):
        setup = Transaction(self.client)
        setup.insert("Posts", self._get_record())
        response = setup.run()
        recordId = response["results"][0]["id"]

        trx = Transaction(self.client)
        trx.get("Posts", recordId)
        trx.get("Posts", recordId, ["title"])
        trx.get("Posts", recordId, ["content"])
        trx.get("Posts", recordId, ["title", "content"])
        trx.get("Posts", recordId, ["id", "title", "content"])
        response = trx.run()

        assert response["status_code"] == 200
        assert response["success"]
        assert not response["has_errors"]
        assert response["stats"]["get"] == 5

        assert list(response["results"][0]["columns"].keys()) == ["id", "xata"]
        assert list(response["results"][1]["columns"].keys()) == ["title"]
        assert list(response["results"][2]["columns"].keys()) == ["content"]
        assert list(response["results"][3]["columns"].keys()) == ["content", "title"]
        assert list(response["results"][4]["columns"].keys()) == ["content", "id", "title"]
