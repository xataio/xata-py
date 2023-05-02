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
from faker import Faker

from xata.client import XataClient


class TestRecordsBranchTransactionsNamespace(object):
    """
    POST /db/{db_branch_name}/transaction
    :link https://xata.io/docs/api-reference/db/db_branch_name/transaction#branch-transaction
    """

    def setup_class(self):
        self.client = XataClient(db_name="sandbox-py", branch_name="main")
        self.fake = Faker()
        self.record_ids = []

    @pytest.fixture
    def record(self) -> dict:
        return self._get_record()

    def _get_record(self) -> dict:
        return {
            "title": self.fake.company(),
            "labels": [self.fake.domain_word(), self.fake.domain_word()],
            "slug": self.fake.catch_phrase(),
            "content": self.fake.text(),
        }

    def teardown_class(self):
        payload = {"operations": []}
        for id in pytest.branch_transactions["record_ids"]:
            payload["operations"].append({"delete": {"table": "Posts", "id": id}})
        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200

    def test_insert_only(self, record: dict):
        payload = {
            "operations": [
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])
        assert "id" in r.json()["results"][0]
        assert "operation" in r.json()["results"][0]
        assert "rows" in r.json()["results"][0]
        assert "insert" == r.json()["results"][0]["operation"]
        assert 1 == r.json()["results"][0]["rows"]

        pytest.branch_transactions["record_ids"] = [x["id"] for x in r.json()["results"]]

    def test_get_only(self, record: dict):
        payload = {
            "operations": [
                {"get": {"table": "Posts", "id": pytest.branch_transactions["hardcoded_ids"][0]}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["hardcoded_ids"][1]}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["hardcoded_ids"][2]}},
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])
        assert "columns" in r.json()["results"][0]
        assert "id" in r.json()["results"][0]["columns"]
        assert "xata" in r.json()["results"][0]["columns"]
        assert "version" in r.json()["results"][0]["columns"]["xata"]
        assert "operation" in r.json()["results"][0]
        assert "get" == r.json()["results"][0]["operation"]

        assert pytest.branch_transactions["hardcoded_ids"][0] == r.json()["results"][0]["columns"]["id"]
        assert pytest.branch_transactions["hardcoded_ids"][1] == r.json()["results"][1]["columns"]["id"]
        assert pytest.branch_transactions["hardcoded_ids"][2] == r.json()["results"][2]["columns"]["id"]

    def test_get_with_columns(self, record: dict):
        payload = {
            "operations": [
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][0],
                        "columns": ["id", "title", "labels"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][1],
                        "columns": ["timestamp", "slug"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][2],
                        "columns": ["content", "xata"],
                    }
                },
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])
        assert "columns" in r.json()["results"][0]

        assert "id" in r.json()["results"][0]["columns"]
        assert "id" not in r.json()["results"][1]["columns"]
        assert "id" not in r.json()["results"][2]["columns"]

        assert "title" in r.json()["results"][0]["columns"]
        assert "labels" in r.json()["results"][0]["columns"]
        assert "timestamp" in r.json()["results"][1]["columns"]
        assert "slug" in r.json()["results"][1]["columns"]
        assert "content" in r.json()["results"][2]["columns"]

        assert "xata" not in r.json()["results"][0]["columns"]
        # assert "xata" in r.json()["results"][2]["columns"]
        # FIXME ^

    def test_get_only_with_existing_and_nonexisting_records(self, record: dict):
        payload = {
            "operations": [
                {"get": {"table": "Posts", "id": pytest.branch_transactions["hardcoded_ids"][0]}},
                {"get": {"table": "Posts", "id": "unknown-1"}},
                {"get": {"table": "Posts", "id": pytest.branch_transactions["hardcoded_ids"][2]}},
                {"get": {"table": "Posts", "id": "unknown-2"}},
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])

        assert {} == r.json()["results"][1]["columns"]
        assert {} == r.json()["results"][3]["columns"]
        assert "get" == r.json()["results"][1]["operation"]
        assert "get" == r.json()["results"][3]["operation"]

        assert pytest.branch_transactions["hardcoded_ids"][0] == r.json()["results"][0]["columns"]["id"]
        assert pytest.branch_transactions["hardcoded_ids"][2] == r.json()["results"][2]["columns"]["id"]

    def test_delete_only(self, record: dict):
        payload = {
            "operations": [
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][9]}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][8]}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][7]}},
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])
        assert "columns" not in r.json()["results"][0]
        assert "operation" in r.json()["results"][0]
        assert "rows" in r.json()["results"][0]
        assert "delete" == r.json()["results"][0]["operation"]
        assert 1 == r.json()["results"][0]["rows"]

    def test_delete_with_columns(self, record: dict):
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

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])

        assert "columns" in r.json()["results"][0]
        assert "title" in r.json()["results"][0]["columns"]
        assert "columns" in r.json()["results"][1]
        assert "slug" in r.json()["results"][1]["columns"]
        # assert "xata" in r.json()["results"][1]["columns"]
        # FIXME ^

    def test_mixed_operations(self, record: dict):
        payload = {
            "operations": [
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][0],
                        "columns": ["id", "title", "labels"],
                    }
                },
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][1],
                        "columns": ["timestamp", "slug"],
                    }
                },
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][3], "columns": ["title"]}},
                {
                    "get": {
                        "table": "Posts",
                        "id": pytest.branch_transactions["hardcoded_ids"][2],
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
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][1]}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"insert": {"table": "Posts", "record": self._get_record()}},
                {"delete": {"table": "Posts", "id": pytest.branch_transactions["record_ids"][0]}},
            ]
        }

        r = self.client.records().branchTransaction(payload)
        assert r.status_code == 200
        assert "results" in r.json()
        assert len(r.json()["results"]) == len(payload["operations"])

        pytest.branch_transactions["record_ids"] = [x["id"] for x in r.json()["results"] if "insert" == x["operation"]]
