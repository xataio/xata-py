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
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = Faker()

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()

        # create schema
        r = self.client.table().set_schema(
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "text", "type": "text"},
                ]
            },
        )
        assert r.is_success()

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

    def test_bulk_insert_records(self, record: dict):
        pt = 2
        bp = BulkProcessor(
            self.client,
            thread_pool_size=1,
            batch_size=5,
            flush_interval=1,
            processing_timeout=pt,
        )
        bp.put_records("Posts", [self._get_record() for x in range(10)])

        # wait until indexed :shrug:
        time.sleep(pt)
        utils.wait_until_records_are_indexed("Posts")

        r = self.client.search_and_filter().search_table("Posts", {})
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) > 0
        assert len(r["records"]) <= 10
