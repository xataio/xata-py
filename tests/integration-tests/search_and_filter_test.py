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


class TestClass(object):
    def setup_class(self):
        self.fake = Faker()
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.record_id = utils.get_random_string(24)
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        # create database
        r = self.client.databases().createDatabase(
            self.client.get_config()["workspaceId"],
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable(
            "Posts", db_name=self.db_name, branch_name=self.branch_name
        )
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
        r = self.client.records().bulkInsertTableRecords(
            "Posts",
            {"records": self.posts},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200
        utils.wait_until_records_are_indexed("Posts")

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(
            self.client.get_config()["workspaceId"], self.db_name
        )
        assert r.status_code == 200

    def test_query_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/query
        """
        payload = {
            "columns": ["title", "slug"],
            "sort": {"slug": "desc"},
            "page": {"size": 5},
        }
        r = self.client.search_and_filter().queryTable(
            "Posts", payload, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 5
        assert "meta" in r.json()
        assert "id" in r.json()["records"][0]
        assert "xata" in r.json()["records"][0]
        assert "title" in r.json()["records"][0]
        assert "slug" in r.json()["records"][0]
        assert "text" not in r.json()["records"][0]

        r = self.client.search_and_filter().queryTable(
            "NonExistingTable",
            payload,
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 404

        r = self.client.search_and_filter().queryTable(
            "Posts",
            {"columns": [""]},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 400

    def test_search_branch(self):
        """
        POST /db/{db_branch_name}/search
        """
        payload = {"query": self.posts[0]["title"]}
        r = self.client.search_and_filter().searchBranch(
            payload, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) >= 1
        assert "id" in r.json()["records"][0]
        assert "xata" in r.json()["records"][0]
        assert "title" in r.json()["records"][0]
        assert r.json()["records"][0]["title"] == self.posts[0]["title"]

        r = self.client.search_and_filter().searchBranch(
            {"tables": [""], "query": "woopsie!"},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
#        assert r.status_code == 500

        r = self.client.search_and_filter().searchBranch(
            {"invalid": "query"}, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 400

    def test_search_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/search
        """
        payload = {"query": self.posts[0]["title"]}
        r = self.client.search_and_filter().searchTable(
            "Posts", payload, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) >= 1
        assert "id" in r.json()["records"][0]
        assert "xata" in r.json()["records"][0]
        assert "title" in r.json()["records"][0]
        assert r.json()["records"][0]["title"] == self.posts[0]["title"]

        r = self.client.search_and_filter().searchTable(
            "Posts", {}, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200

        r = self.client.search_and_filter().searchTable(
            "NonExistingTable",
            payload,
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 404

        r = self.client.search_and_filter().searchTable(
            "Posts",
            {"invalid": "query"},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 400

    def test_summarize_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/summarize
        """
        payload = {"columns": ["title", "slug"]}
        r = self.client.search_and_filter().summarizeTable(
            "Posts", payload, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200
        assert "summaries" in r.json()
        assert len(r.json()["summaries"]) > 1

        r = self.client.search_and_filter().summarizeTable(
            "NonExistingTable",
            payload,
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 404

    def test_aggregate_table(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/aggregate
        """
        payload = {"aggs": {"titles": {"count": "*"}}}
        r = self.client.search_and_filter().aggregateTable(
            "Posts", payload, db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 200
        assert "aggs" in r.json()
        assert "titles" in r.json()["aggs"]
        assert r.json()["aggs"]["titles"] == len(self.posts)

        r = self.client.search_and_filter().aggregateTable(
            "NonExistingTable",
            payload,
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 404

        r = self.client.search_and_filter().aggregateTable(
            "Posts",
            {"aggs": {"foo": "bar"}},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 400
