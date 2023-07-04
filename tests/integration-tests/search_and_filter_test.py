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

import utils
from faker import Faker

from xata.client import XataClient


class TestSearchAndFilterNamespace(object):
    def setup_class(self):
        self.fake = Faker()
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.record_id = utils.get_random_string(24)
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        r = self.client.table().set_schema(
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
        assert r.is_success()

        # ingests posts
        self.posts = [
            {
                "title": self.fake.company(),
                "labels": [self.fake.domain_word(), self.fake.domain_word()],
                "slug": self.fake.catch_phrase(),
                "text": self.fake.text(),
            }
            for i in range(10)
        ]
        assert self.client.records().bulk_insert("Posts", {"records": self.posts}).is_success()
        utils.wait_until_records_are_indexed("Posts")

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_query_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/query
        """
        payload = {
            "columns": ["title", "slug"],
            "sort": {"slug": "desc"},
            "page": {"size": 5},
        }
        r = self.client.search_and_filter().query("Posts", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 5
        assert "meta" in r
        assert "id" in r["records"][0]
        assert "xata" in r["records"][0]
        assert "title" in r["records"][0]
        assert "slug" in r["records"][0]
        assert "text" not in r["records"][0]

        r = self.client.search_and_filter().query("NonExistingTable", payload)
        assert r.status_code == 404

    def test_query_unknown_columns(self):
        assert not self.client.search_and_filter().query("Posts", {"columns": ["does", "not", "exist"]}).is_success()

    def test_query_empty_columns(self):
        r = self.client.search_and_filter().query("Posts", {"columns": [""]})
        assert r.is_success()
        assert len(r["records"]) > 0

    def test_search_branch(self):
        """
        POST /db/{db_branch_name}/search
        """
        payload = {"query": self.posts[0]["title"]}
        r = self.client.search_and_filter().search_branch(payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) >= 1
        assert "id" in r["records"][0]
        assert "xata" in r["records"][0]
        assert "title" in r["records"][0]
        assert r["records"][0]["title"] == self.posts[0]["title"]

        r = self.client.search_and_filter().search_branch({"tables": [""], "query": "woopsie!"})
        assert r.status_code == 400

        r = self.client.search_and_filter().search_branch({"invalid": "query"})
        assert r.status_code == 400

    def test_search_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/search
        """
        payload = {"query": self.posts[0]["title"]}
        r = self.client.search_and_filter().search_table("Posts", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) >= 1
        assert "id" in r["records"][0]
        assert "xata" in r["records"][0]
        assert "title" in r["records"][0]
        assert r["records"][0]["title"] == self.posts[0]["title"]

        r = self.client.search_and_filter().search_table("Posts", {})
        assert r.is_success()

        r = self.client.search_and_filter().search_table("NonExistingTable", payload)
        assert r.status_code == 404

        r = self.client.search_and_filter().search_table("Posts", {"invalid": "query"})
        assert r.status_code == 400

    def test_summarize_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/summarize
        """
        payload = {"columns": ["title", "slug"]}
        r = self.client.search_and_filter().summarize("Posts", payload)
        assert r.is_success()
        assert "summaries" in r
        assert len(r["summaries"]) > 1

        r = self.client.search_and_filter().summarize("NonExistingTable", payload)
        assert r.status_code == 404

    def test_aggregate_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/aggregate
        """
        payload = {"aggs": {"titles": {"count": "*"}}}
        r = self.client.search_and_filter().aggregate("Posts", payload)
        assert r.is_success()
        assert "aggs" in r
        assert "titles" in r["aggs"]
        assert r["aggs"]["titles"] == len(self.posts)

        r = self.client.search_and_filter().aggregate("NonExistingTable", payload)
        assert r.status_code == 404

        r = self.client.search_and_filter().aggregate("Posts", {"aggs": {"foo": "bar"}})
        assert r.status_code == 400
