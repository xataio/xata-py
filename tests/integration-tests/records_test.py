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


class TestRecordsNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = Faker()
        self.record_id = utils.get_random_string(24)

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
                    {"name": "labels", "type": "multiple"},
                    {"name": "slug", "type": "string"},
                    {"name": "text", "type": "text"},
                ]
            },
            db_name=self.db_name,
            branch_name=self.branch_name,
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
            "labels": [self.fake.domain_word(), self.fake.domain_word()],
            "slug": self.fake.catch_phrase(),
            "text": self.fake.text(),
        }

    def test_insert_record(self, record: dict):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data
        """
        r = self.client.records().insertRecord("Posts", record)
        assert r.status_code == 201
        assert "id" in r.json()
        assert "xata" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["xata"]["version"] == 0

        r = self.client.records().insertRecord("NonExistingTable", record)
        assert r.status_code == 404

    def test_insert_record_with_id(self, record: dict):
        """
        PUT /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().insertRecordWithID("Posts", self.record_id, record)
        assert r.status_code == 201
        assert "id" in r.json()
        assert "xata" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["id"] == self.record_id
        assert r.json()["xata"]["version"] == 0

        r = self.client.records().insertRecordWithID("Posts", self.record_id, record, createOnly=False)
        assert r.status_code == 200
        assert r.json()["xata"]["version"] == 1

        r = self.client.records().insertRecordWithID("Posts", self.record_id, record, createOnly=True)
        assert r.status_code == 422

        r = self.client.records().insertRecordWithID("Posts", "", record)
        assert r.status_code == 404

    def test_get_record(self, record: dict):
        """
        GET /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().getRecord("Posts", self.record_id)
        assert r.status_code == 200
        assert "id" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["id"] == self.record_id
        assert r.json()["xata"]["version"] == 1
        assert len(r.json().keys()) == len(record.keys()) + 2
        keep = r.json()

        r = self.client.records().getRecord("Posts", self.record_id, columns=["id", "slug"])
        assert r.status_code == 200
        assert r.json()["id"] == self.record_id
        assert r.json()["slug"] == keep["slug"]
        assert len(r.json().keys()) == 3
        assert r.json() != keep

        r = self.client.records().getRecord("Posts", "#######")
        assert r.status_code == 404

        r = self.client.records().getRecord(
            "NonExistingTable",
            self.record_id,
        )
        assert r.status_code == 404

    def test_update_record(self, record: dict):
        """
        PATCH /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        proof = self.client.records().getRecord("Posts", self.record_id)
        assert proof.status_code == 200

        r = self.client.records().updateRecordWithID("Posts", self.record_id, record)
        assert r.status_code == 200
        assert "id" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["id"] == self.record_id
        assert r.json()["xata"]["version"] == proof.json()["xata"]["version"] + 1

        r = self.client.records().getRecord("Posts", self.record_id)
        assert r.status_code == 200
        assert r.json()["slug"] == record["slug"]
        assert r.json()["slug"] != proof.json()["slug"]
        assert r.json()["title"] == record["title"]
        assert r.json()["title"] != proof.json()["title"]

        r = self.client.records().updateRecordWithID("NonExistingTable", self.record_id, record)
        assert r.status_code == 404

        r = self.client.records().updateRecordWithID("Posts", "NonExistingRecordId", record)
        assert r.status_code == 404

    def test_upsert_record(self, record: dict):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        rec_id = utils.get_random_string(24)
        r = self.client.records().upsertRecordWithID("Posts", rec_id, record)
        assert r.status_code == 201

        r = self.client.records().getRecord("Posts", rec_id)
        assert r.status_code == 200
        assert r.json()["id"] == rec_id
        proof = r.json()

        update = self._get_record()
        assert record != update
        r = self.client.records().upsertRecordWithID("Posts", rec_id, update)
        assert r.status_code == 200

        r = self.client.records().getRecord("Posts", rec_id)
        assert r.status_code == 200
        assert r.json()["id"] == rec_id
        assert r.json() != proof

    def test_delete_record(self):
        """
        DELETE /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().deleteRecord("Posts", self.record_id)
        assert r.status_code == 204

        r = self.client.records().deleteRecord("Posts", self.record_id)
        assert r.status_code == 204

    def test_bulk_insert_table_records(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/bulk
        """
        posts = [self._get_record() for i in range(10)]

        r = self.client.records().bulkInsertTableRecords("Posts", {"records": posts})
        assert r.status_code == 200
