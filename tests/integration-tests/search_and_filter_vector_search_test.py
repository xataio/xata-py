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

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("users").is_success()

        # create schema
        r = self.client.table().set_schema(
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
        assert r.is_success()

        self.users = [
            {"full_name": "r1", "full_name_vec": [0.1, 0.2, 0.3, 0.5]},
            {"full_name": "r2", "full_name_vec": [4, 3, 2, 1]},
            {"full_name": "r3", "full_name_vec": [0.5, 0.2, 0.3, 0.1]},
            {"full_name": "r4", "full_name_vec": [1, 2, 3, 4]},
        ]

        r = self.client.records().bulk_insert("users", {"records": self.users})
        assert r.is_success()
        utils.wait_until_records_are_indexed("users")

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_vector_search_table_simple(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        rec1 = r["records"]
        assert len(rec1) == 4
        res_order = [x["full_name"] for x in rec1]
        assert res_order == ["r4", "r1", "r2", "r3"]

        payload = {
            "column": "full_name_vec",
            "queryVector": [0.4, 0.3, 0.2, 0.1],
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        rec2 = r["records"]
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
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 2

        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "size": 1000,
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 4

    def test_vector_search_table_similarity_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "similarityFunction": "l1",  # euclidian
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        res_order = [x["full_name"] for x in r["records"]]
        assert res_order[0:2] == ["r4", "r2"]  # only test the first two items
        # TODO ^ flaky test, better fine grained data set to query
        # assumption tie breaker between r3 and r1 flips

    def test_vector_search_table_filter_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 2
        res_order = [x["full_name"] for x in r["records"]]
        assert res_order == ["r4", "r3"]

    def test_vector_search_table_filter_and_size_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "size": 1,
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 1
        assert r["records"][0]["full_name"] == "r4"

    def test_vector_search_table_filter_and_size_and_similarity_param(self):
        payload = {
            "column": "full_name_vec",
            "queryVector": [1, 2, 3, 4],
            "similarityFunction": "l1",
            "size": 1,
            "filter": {"full_name": {"$any": ["r3", "r4"]}},
        }
        r = self.client.search_and_filter().vector_search("users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 1
        assert r["records"][0]["full_name"] == "r4"
