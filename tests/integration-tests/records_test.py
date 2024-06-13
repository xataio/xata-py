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


class TestRecordsNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(db_name=self.db_name)
        self.fake = utils.get_faker()
        self.record_id = utils.get_random_string(24)

        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().set_schema("Posts", utils.get_posts_schema()).is_success()

    def teardown_class(self):
        assert self.client.table().delete("Posts").is_success()
        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().delete(self.db_name).is_success()

    @pytest.fixture
    def record(self) -> dict:
        return utils.get_post()

    def test_insert_record(self, record: dict):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data
        """
        r = self.client.records().insert("Posts", record)
        assert r.is_success()
        assert "id" in r
        assert "xata" in r
        assert "version" in r["xata"]
        assert "createdAt" in r["xata"]
        assert "updatedAt" in r["xata"]
        assert r["xata"]["version"] == 0

        r = self.client.records().insert("NonExistingTable", record)
        assert r.status_code == 404

    def test_insert_record_with_id(self, record: dict):
        """
        PUT /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().insert_with_id("Posts", self.record_id, record)
        assert r.is_success()
        assert "id" in r
        assert "xata" in r
        assert "version" in r["xata"]
        assert "createdAt" in r["xata"]
        assert "updatedAt" in r["xata"]
        assert r["id"] == self.record_id
        assert r["xata"]["version"] == 0

        r = self.client.records().insert_with_id("Posts", self.record_id, record, create_only=False)
        assert r.is_success()
        assert r["xata"]["version"] == 1

        r = self.client.records().insert_with_id("Posts", self.record_id, record, create_only=True)
        assert r.status_code == 422

        r = self.client.records().insert_with_id("Posts", "", record)
        assert r.status_code == 404

    def test_get_record(self, record: dict):
        """
        GET /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().get("Posts", self.record_id)
        assert r.is_success()
        assert "id" in r
        assert "version" in r["xata"]
        assert "createdAt" in r["xata"]
        assert "updatedAt" in r["xata"]
        assert r["id"] == self.record_id
        assert r["xata"]["version"] == 1
        assert len(r.keys()) == len(record.keys()) + 2
        keep = r

        r = self.client.records().get("Posts", self.record_id, columns=["id", "slug"])
        assert r.is_success()
        assert r["id"] == self.record_id
        assert r["slug"] == keep["slug"]
        assert len(r.keys()) == 3
        assert r != keep

        r = self.client.records().get("Posts", "#######")
        assert r.status_code == 404

        r = self.client.records().get("NonExistingTable", self.record_id)
        assert r.status_code == 404

    def test_update_record(self, record: dict):
        """
        PATCH /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        proof = self.client.records().get("Posts", self.record_id)
        assert proof.status_code == 200

        r = self.client.records().update("Posts", self.record_id, record)
        assert r.is_success()
        assert "id" in r
        assert "version" in r["xata"]
        assert "createdAt" in r["xata"]
        assert "updatedAt" in r["xata"]
        assert r["id"] == self.record_id
        assert r["xata"]["version"] == proof["xata"]["version"] + 1

        r = self.client.records().get("Posts", self.record_id)
        assert r.is_success()
        assert r["slug"] == record["slug"]
        assert r["slug"] != proof["slug"]
        assert r["title"] == record["title"]
        assert r["title"] != proof["title"]

        r = self.client.records().update("NonExistingTable", self.record_id, record)
        assert r.status_code == 404

        r = self.client.records().update("Posts", "NonExistingRecordId", record)
        assert r.status_code == 404

    def test_upsert_record(self, record: dict):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        rec_id = utils.get_random_string(24)
        r = self.client.records().upsert("Posts", rec_id, record)
        assert r.is_success()

        r = self.client.records().get("Posts", rec_id)
        assert r.is_success()
        assert r["id"] == rec_id
        proof = r

        update = utils.get_post()
        assert record != update
        r = self.client.records().upsert("Posts", rec_id, update)
        assert r.is_success()

        r = self.client.records().get("Posts", rec_id)
        assert r.is_success()
        assert r["id"] == rec_id
        assert r != proof

    def test_delete_record(self):
        """
        DELETE /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        r = self.client.records().delete("Posts", self.record_id)
        assert r.status_code == 204
        assert r.is_success()

    def test_delete_record_columns_returning(self, record: dict):
        inserted = self.client.records().insert("Posts", record)
        assert inserted.is_success()

        r = self.client.records().delete("Posts", inserted["id"], columns=["labels", "slug"])
        assert r.status_code == 200
        assert r.is_success()
        assert "id" in r
        assert "slug" in r
        assert "labels" in r
        assert "xata" in r

        assert "title" not in r
        assert "content" not in r

    def test_bulk_insert_table_records(self):
        r = self.client.records().bulk_insert("Posts", {"records": utils.get_posts(10)})
        assert r.is_success()
        assert "recordIDs" in r
        assert len(r["recordIDs"]) == 10

    def test_bulk_insert_table_records_with_column_projections(self):
        r = self.client.records().bulk_insert("Posts", {"records": utils.get_posts(10)}, columns=["title", "slug"])
        assert r.is_success()

        assert "records" in r
        assert len(r["records"]) == 10
        assert "id" in r["records"][0]
        assert "slug" in r["records"][0]
        assert "title" in r["records"][0]
        assert "xata" in r["records"][0]

        assert "recordIDs" not in r
        assert "labels" not in r["records"][0]
        assert "content" not in r["records"][0]
