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

from xata.client import XataClient


class TestSearchAndFilterVectorSearchEndpoint(object):
    """
    POST /db/{db_branch_name}/tables/{table_name}/vectorSearch
    """

    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

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
        r = self.client.table().createTable("users")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            "users",
            {
                "columns": [
                    {"name": "full_name", "type": "string"},
                    {
                        "name": "full_name_vec",
                        "type": "vector",
                        "vector": {"dimension": 4},
                    },
                ]
            },
        )
        assert r.status_code == 200

        self.users = [
            {"full_name": "r1", "full_name_vec": [0.1, 0.2, 0.3, 0.5]},
            {"full_name": "r2", "full_name_vec": [4, 3, 2, 1]},
            {"full_name": "r3", "full_name_vec": [0.5, 0.2, 0.3, 0.1]},
            {"full_name": "r4", "full_name_vec": [1, 2, 3, 4]},
        ]

        r = self.client.records().bulkInsertTableRecords("users", {"records": self.users})
        assert r.status_code == 200
        utils.wait_until_records_are_indexed("users")

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200

    def test_vector_search_table_simple(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        rec1 = r.json()["records"]
        assert len(rec1) == 4
        res_order = [x["full_name"] for x in rec1]
        assert res_order == ["r4", "r1", "r2", "r3"]

        payload = {
            "column": "full_name_vec",
            "queryVector": [0.4, 0.3, 0.2, 0.1],
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        rec2 = r.json()["records"]
        assert len(rec2) == 4
        res_order = [x["full_name"] for x in rec2]
        assert res_order == ["r2", "r3", "r4", "r1"]
        assert rec1 != rec2

    def test_vector_search_table_size_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "size": 2,
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 2

        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "size": 1000,
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 4

    def test_vector_search_table_similarity_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "similarityFunction": "l1",  # euclidian
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        res_order = [x["full_name"] for x in r.json()["records"]]
        assert res_order[0:2] == ["r4", "r2"]  # only test the first two items
        # TODO ^ flaky test, better fine grained data set to query
        # assumption tie breaker between r3 and r1 flips

    def test_vector_search_table_filter_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 2
        res_order = [x["full_name"] for x in r.json()["records"]]
        assert res_order == ["r4", "r3"]

    def test_vector_search_table_filter_and_size_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "size": 1,
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 1
        assert r.json()["records"][0]["full_name"] == "r4"

    def test_vector_search_table_filter_and_size_and_similarity_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "similarityFunction": "l1",
            "size": 1,
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vectorSearchTable("users", payload)
        assert r.status_code == 200
        assert "records" in r.json()
        assert len(r.json()["records"]) == 1
        assert r.json()["records"][0]["full_name"] == "r4"
