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

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
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
        assert self.client.databases().delete(self.db_name).is_success()

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

        r = self.client.records().update_with_id("Posts", self.record_id, record)
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

        r = self.client.records().update_with_id("NonExistingTable", self.record_id, record)
        assert r.status_code == 404

        r = self.client.records().update_with_id("Posts", "NonExistingRecordId", record)
        assert r.status_code == 404

    def test_upsert_record(self, record: dict):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        """
        rec_id = utils.get_random_string(24)
        r = self.client.records().upsert_with_id("Posts", rec_id, record)
        assert r.is_success()

        r = self.client.records().get("Posts", rec_id)
        assert r.is_success()
        assert r["id"] == rec_id
        proof = r

        update = self._get_record()
        assert record != update
        r = self.client.records().upsert_with_id("Posts", rec_id, update)
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

        r = self.client.records().delete("Posts", self.record_id)
        assert r.status_code == 204
        assert r.is_success()

    def test_bulk_insert_table_records(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/bulk
        """
        posts = [self._get_record() for i in range(10)]

        r = self.client.records().bulk_insert("Posts", {"records": posts})
        assert r.is_success()
