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
        r = self.client.databases().create(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().create("Posts")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setSchema(
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
        r = self.client.databases().delete(self.db_name)
        assert r.status_code == 200

    @pytest.fixture
    def record(self) -> dict:
        return self._get_record()

    def _get_record(self) -> dict:
        return {
            "title": self.fake.company(),
            "content": self.fake.text(),
        }

    def test_insert_records_and_response_shape(self):
        trx = Transaction(self.client)
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        response = trx.run()

        assert "status_code" in response
        assert "has_errors" in response
        assert "results" in response
        assert "errors" in response

        assert response["status_code"] == 200
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == 5

        r = self.client.data().query("Posts", {})
        assert len(r.json()["records"]) == 5

    def test_insert_records_with_create_only_option_existing_record(self):
        trx = Transaction(self.client)
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        response = trx.run()

        overwrite_me = [x["id"] for x in response["results"]]
        assert len(overwrite_me) > 0

        trx = Transaction(self.client)
        trx.insert("Posts", {"id": overwrite_me[0], "title": "a new title #1", "content": "yes!"}, True)
        trx.insert("Posts", {"id": overwrite_me[1], "title": "a new title #2", "content": "yeah!"}, False)
        response = trx.run()

        assert response["has_errors"]
        assert response["status_code"] == 400
        assert len(response["errors"]) == 1

    def test_insert_records_with_create_only_option_new_record_id(self):
        before_insert = len(self.client.data().query("Posts", {}).json()["records"])

        trx = Transaction(self.client)
        trx.insert("Posts", {"id": "record-123", "title": "a new title #1", "content": "yes!"}, True)
        trx.insert("Posts", {"id": "record-042", "title": "a new title #2", "content": "yeah!"}, False)
        response = trx.run()

        assert not response["has_errors"]
        assert response["status_code"] == 200
        assert len(response["errors"]) == 0

        after_insert = len(self.client.data().query("Posts", {}).json()["records"])
        assert before_insert == (after_insert - 2)

    def test_delete_records(self):
        setup = Transaction(self.client)
        for it in range(1, 10):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        delete_me = [x["id"] for x in response["results"]]
        assert len(delete_me) > 0
        before_delete = len(self.client.data().query("Posts", {}).json()["records"])

        trx = Transaction(self.client)
        for rid in delete_me:
            trx.delete("Posts", rid)
        response = trx.run()

        assert response["status_code"] == 200
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == len(delete_me)

        r = self.client.data().query("Posts", {})
        assert len(r.json()["records"]) == (before_delete - len(delete_me))

    def test_delete_records_with_columns(self):
        setup = Transaction(self.client)
        for it in range(0, 5):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        delete_me = [x["id"] for x in response["results"]]
        assert len(delete_me) > 0
        before_delete = len(self.client.data().query("Posts", {}).json()["records"])

        trx = Transaction(self.client)
        trx.delete("Posts", delete_me[0])
        trx.delete("Posts", delete_me[1], ["title"])
        trx.delete("Posts", delete_me[2], ["content"])
        trx.delete("Posts", delete_me[3], ["title", "content"])
        trx.delete("Posts", delete_me[4], ["id", "title", "content"])
        response = trx.run()

        assert response["status_code"] == 200
        assert not response["has_errors"]

        assert "columns" not in response["results"][0]
        assert list(response["results"][1]["columns"].keys()) == ["title"]
        assert list(response["results"][2]["columns"].keys()) == ["content"]
        assert list(response["results"][3]["columns"].keys()) == ["content", "title"]
        assert list(response["results"][4]["columns"].keys()) == ["content", "id", "title"]

        r = self.client.data().query("Posts", {})
        assert len(r.json()["records"]) == (before_delete - len(delete_me))

    def test_get_records(self):
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
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == len(get_me)

    def test_get_records_with_columns(self):
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
        assert not response["has_errors"]

        assert list(response["results"][0]["columns"].keys()) == ["id", "xata"]
        assert list(response["results"][1]["columns"].keys()) == ["title"]
        assert list(response["results"][2]["columns"].keys()) == ["content"]
        assert list(response["results"][3]["columns"].keys()) == ["content", "title"]
        assert list(response["results"][4]["columns"].keys()) == ["content", "id", "title"]

    def test_update_records(self):
        setup = Transaction(self.client)
        for it in range(0, 5):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        update_me = [x["id"] for x in response["results"]]
        assert len(update_me) > 0

        trx = Transaction(self.client)
        for rid in update_me:
            trx.update("Posts", rid, {"title": rid})
        response = trx.run()

        assert response["status_code"] == 200
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == len(update_me)

    def test_update_records_via_upsert(self):
        before_upsert = len(self.client.data().query("Posts", {}).json()["records"])

        trx = Transaction(self.client)
        for rid in range(0, 5):
            trx.update("Posts", str(rid), {"title": f"title: {rid}!", "content": "upserted"}, True)
        response = trx.run()

        assert response["status_code"] == 200
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == 5

        after_upsert = len(self.client.data().query("Posts", {}).json()["records"])
        assert before_upsert == after_upsert

    def test_mixed_operations(self):
        setup = Transaction(self.client)
        for it in range(0, 6):
            setup.insert("Posts", self._get_record())
        response = setup.run()
        ids = [x["id"] for x in response["results"]]
        assert len(ids) > 0

        trx = Transaction(self.client)
        trx.get("Posts", ids[0])
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.delete("Posts", ids[1], ["content"])
        # trx.update("Posts", ids[2])
        trx.get("Posts", ids[3])
        trx.insert("Posts", self._get_record())
        trx.delete("Posts", ids[4], ["content"])
        trx.delete("Posts", ids[5], ["content"])
        trx.insert("Posts", self._get_record())
        response = trx.run()

        assert response["status_code"] == 200
        assert not response["has_errors"]
        assert response["errors"] == []
        assert len(response["results"]) == 9

    def test_max_operations_exceeded(self):
        trx = Transaction(self.client)
        for it in range(0, 1000):
            trx.get("Posts", it)
        with pytest.raises(Exception) as exc:
            trx.get("Posts", 1001)
        assert exc is not None
        # assert str(exc) == f"Maximum amount of {TRX_MAX_OPERATIONS} transaction operations exceeded."

    def test_run_trx_next_batch_of_operations(self):
        trx = Transaction(self.client)
        for it in range(0, 3):
            trx.insert("Posts", self._get_record())
        r = trx.run()
        assert not r["has_errors"]

        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        trx.insert("Posts", self._get_record())
        r = trx.run()
        assert not r["has_errors"]

    def test_has_errors_insert(self):
        before_insert = len(self.client.data().query("Posts", {}).json()["records"])

        trx = Transaction(self.client)
        trx.insert("Posts", self._get_record())  # good
        trx.insert("PostsThatDoNotExist", self._get_record())  # bad
        trx.insert("Posts", self._get_record())  # good
        trx.insert("Posts", {"foo": "bar"})  # bad
        response = trx.run()

        assert response["status_code"] == 400
        assert response["has_errors"]
        assert len(response["errors"]) == 2
        assert response["errors"][0]["index"] == 1
        assert response["errors"][1]["index"] == 3
        assert len(response["results"]) == 0

        after_insert = len(self.client.data().query("Posts", {}).json()["records"])
        assert before_insert == after_insert
