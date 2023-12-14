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
from xata.helpers import BulkProcessor


class TestHelpersBulkProcessor(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(db_name=self.db_name)
        self.fake = Faker()

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().create("Users").is_success()

        # create schema
        assert (
            self.client.table()
            .set_schema(
                "Posts",
                {
                    "columns": [
                        {"name": "title", "type": "string"},
                        {"name": "text", "type": "text"},
                    ]
                },
            )
            .is_success()
        )
        assert (
            self.client.table()
            .set_schema(
                "Users",
                {
                    "columns": [
                        {"name": "username", "type": "string"},
                        {"name": "email", "type": "string"},
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
            "text": self.fake.text(),
        }

    def _get_user(self) -> dict:
        return {
            "username": self.fake.name(),
            "email": self.fake.email(),
        }

    def test_bulk_insert_records(self, record: dict):
        bp = BulkProcessor(
            self.client,
            thread_pool_size=1,
        )
        bp.put_records("Posts", [self._get_record() for x in range(42)])
        bp.flush_queue()

        r = self.client.data().summarize("Posts", {"summaries": {"proof": {"count": "*"}}})
        assert r.is_success()
        assert "summaries" in r
        assert r["summaries"][0]["proof"] == 42

        stats = bp.get_stats()
        assert stats["total"] == 42
        assert stats["queue"] == 0
        assert stats["failed_batches"] == 0
        assert stats["tables"]["Posts"] == 42
        assert stats["total_batches"] == 2

    def test_flush_queue(self):
        assert self.client.sql().query('DELETE FROM "Posts" WHERE 1 = 1').is_success()

        bp = BulkProcessor(
            self.client,
            thread_pool_size=4,
            batch_size=50,
            flush_interval=1,
        )
        bp.put_records("Posts", [self._get_record() for x in range(1000)])
        bp.flush_queue()

        r = self.client.data().summarize("Posts", {"summaries": {"proof": {"count": "*"}}})
        assert r.is_success()
        assert "summaries" in r
        assert r["summaries"][0]["proof"] == 1000

        stats = bp.get_stats()
        assert stats["total"] == 1000
        assert stats["queue"] == 0
        assert stats["failed_batches"] == 0
        assert stats["total_batches"] == 20
        assert stats["tables"]["Posts"] == 1000

    def test_flush_queue_many_threads(self):
        assert self.client.sql().query('DELETE FROM "Users" WHERE 1 = 1').is_success()

        bp = BulkProcessor(
            self.client,
            thread_pool_size=8,
            batch_size=10,
        )
        bp.put_records("Users", [self._get_user() for x in range(750)])
        bp.flush_queue()

        r = self.client.data().summarize("Users", {"summaries": {"proof": {"count": "*"}}})
        assert r.is_success()
        assert "summaries" in r
        assert r["summaries"][0]["proof"] == 750

        stats = bp.get_stats()
        assert stats["total"] == 750
        assert stats["queue"] == 0
        assert stats["failed_batches"] == 0
        assert stats["total_batches"] == 75
        assert stats["tables"]["Users"] == 750

    def test_multiple_tables(self):
        assert self.client.sql().query('DELETE FROM "Posts" WHERE 1 = 1').is_success()
        assert self.client.sql().query('DELETE FROM "Users" WHERE 1 = 1').is_success()

        bp = BulkProcessor(
            self.client,
            thread_pool_size=3,
            batch_size=42,
        )
        for it in range(33):
            bp.put_records("Posts", [self._get_record() for x in range(9)])
            bp.put_records("Users", [self._get_user() for x in range(7)])
        bp.flush_queue()

        r = self.client.data().summarize("Posts", {"summaries": {"proof": {"count": "*"}}})
        assert r.is_success()
        assert "summaries" in r
        assert r["summaries"][0]["proof"] == 33 * 9

        r = self.client.data().summarize("Users", {"summaries": {"proof": {"count": "*"}}})
        assert r.is_success()
        assert "summaries" in r
        assert r["summaries"][0]["proof"] == 33 * 7

        stats = bp.get_stats()
        assert stats["queue"] == 0
        assert stats["failed_batches"] == 0
        assert stats["total_batches"] == 14
        assert stats["tables"]["Posts"] == 33 * 9
        assert stats["tables"]["Users"] == 33 * 7
        assert stats["total"] == stats["tables"]["Posts"] + stats["tables"]["Users"]
