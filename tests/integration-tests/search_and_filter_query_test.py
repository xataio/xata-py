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

import utils
from faker import Faker

from xata.client import XataClient


class TestSearchAndFilterQueryApi(object):
    """
    POST /db/{db_branch_name}/tables/{table_name}/query
    """

    def setup_class(self):
        self.fake = Faker()
        self.db_name = utils.get_db_name()
        self.record_id = utils.get_random_string(24)
        self.posts = utils.get_posts(50)
        self.client = XataClient(db_name=self.db_name)

        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().set_schema("Posts", utils.get_posts_schema()).is_success()
        assert self.client.records().bulk_insert("Posts", {"records": self.posts}).is_success()
        utils.wait_until_records_are_indexed("Posts", "title", self.client)

    def teardown_class(self):
        assert self.client.table().delete("Posts").is_success()
        if not os.environ.get("XATA_STATIC_DB_NAME"):
            assert self.client.databases().delete(self.db_name).is_success()

    def test_query_table(self):
        payload = {
            "columns": ["title", "slug"],
            "sort": {"slug": "desc"},
            "page": {"size": 5},
        }
        r = self.client.data().query("Posts", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) > 0
        assert "meta" in r
        assert "id" in r["records"][0]
        assert "xata" in r["records"][0]
        assert "title" in r["records"][0]
        assert "slug" in r["records"][0]
        assert "text" not in r["records"][0]

    def test_find_all(self):
        all_1 = self.client.data().query("Posts")
        assert all_1.is_success()

        all_2 = self.client.data().query("Posts", {})
        assert all_2.is_success()

        assert len(all_1) == len(all_2)

    def test_unknown_table(self):
        assert not self.client.data().query("NonExistingTable", {"columns": ["title", "slug"]}).is_success()

    def test_query_unknown_columns(self):
        assert not self.client.data().query("Posts", {"columns": ["does", "not", "exist"]}).is_success()

    def test_query_empty_columns(self):
        r = self.client.data().query("Posts", {"columns": ["*"]})
        assert r.is_success()
        assert len(r["records"]) > 0
