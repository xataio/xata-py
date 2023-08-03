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


class TestRecordsBranchTransactionsNamespace(object):
    """
    POST /db/{db_branch_name}/transaction
    :link https://xata.io/docs/api-reference/db/db_branch_name/transaction#branch-transaction
    """

    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(db_name=self.db_name)
        self.record_ids = []
        self.fake = utils.get_faker()

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().set_schema("Posts", utils.get_posts_schema()).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_insert_only(self):
        payload = {
            "operations": [
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])
        assert "id" in r["results"][0]
        assert "operation" in r["results"][0]
        assert "rows" in r["results"][0]
        assert "insert" == r["results"][0]["operation"]
        assert 1 == r["results"][0]["rows"]

        pytest.branch_transactions["record_ids"] = [x["id"] for x in r["results"]]

    def test_get_only(self):
        payload = {
            "operations": [
                {"get": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][0]}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][1]}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][2]}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])
        assert "columns" in r["results"][0]
        assert "id" in r["results"][0]["columns"]
        assert "xata" in r["results"][0]["columns"]
        assert "version" in r["results"][0]["columns"]["xata"]
        assert "operation" in r["results"][0]
        assert "get" == r["results"][0]["operation"]

        assert pytest.branch_transactions["record_ids"][0] == r["results"][0]["columns"]["id"]
        assert pytest.branch_transactions["record_ids"][1] == r["results"][1]["columns"]["id"]
        assert pytest.branch_transactions["record_ids"][2] == r["results"][2]["columns"]["id"]

    def test_get_with_columns(self):
        payload = {
            "operations": [
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][0],
                        "columns": ["id", "title", "labels"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][1],
                        "columns": ["slug"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][2],
                        "columns": ["content", "xata"],
                    }
                },
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])
        assert "columns" in r["results"][0]

        assert "id" in r["results"][0]["columns"]
        assert "id" not in r["results"][1]["columns"]
        assert "id" not in r["results"][2]["columns"]

        assert "title" in r["results"][0]["columns"]
        assert "labels" in r["results"][0]["columns"]
        assert "slug" in r["results"][1]["columns"]
        assert "content" in r["results"][2]["columns"]

        assert "xata" not in r["results"][0]["columns"]
        # assert "xata" in r["results"][2]["columns"]
        # FIXME ^

    def test_get_only_with_existing_and_nonexisting_records(self):
        payload = {
            "operations": [
                {"get": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][0]}},
                {"get": {"table": "Posts", "id": "unknown-1"}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][2]}},
                {"get": {"table": "Posts", "id": "unknown-2"}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])

        assert {} == r["results"][1]["columns"]
        assert {} == r["results"][3]["columns"]
        assert "get" == r["results"][1]["operation"]
        assert "get" == r["results"][3]["operation"]

        assert pytest.branch_transactions["record_ids"][0] == r["results"][0]["columns"]["id"]
        assert pytest.branch_transactions["record_ids"][2] == r["results"][2]["columns"]["id"]

    def test_delete_only(self):
        payload = {
            "operations": [
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][9]}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][8]}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][7]}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])
        assert "columns" not in r["results"][0]
        assert "operation" in r["results"][0]
        assert "rows" in r["results"][0]
        assert "delete" == r["results"][0]["operation"]
        assert 1 == r["results"][0]["rows"]

    def test_delete_with_columns(self):
        payload = {
            "operations": [
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][6], "columns": ["title"]}},
                {
                    "delete": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][5],
                        "columns": ["slug", "xata"],
                    }
                },
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][4]}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])

        assert "columns" in r["results"][0]
        assert "title" in r["results"][0]["columns"]
        assert "columns" in r["results"][1]
        assert "slug" in r["results"][1]["columns"]
        # assert "xata" in r["results"][1]["columns"]
        # FIXME ^

    def test_mixed_operations(self):
        payload = {
            "operations": [
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][0],
                        "columns": ["id", "title", "labels"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][1],
                        "columns": ["slug"],
                    }
                },
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][3], "columns": ["title"]}},
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][2],
                        "columns": ["content", "xata"],
                    }
                },
                {
                    "delete": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["record_ids"][2],
                        "columns": ["slug", "xata"],
                    }
                },
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][1]}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][0]}},
            ]
        }

        r = self.client.records().transaction(payload)
        assert r.is_success()
        assert "results" in r
        assert len(r["results"]) == len(payload["operations"])
