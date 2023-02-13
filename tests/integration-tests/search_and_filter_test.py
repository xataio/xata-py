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

import string

import utils
import pytest

from xata.client import XataClient
from xata.errors import BadRequestException

class TestClass(object):

    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient()
        utils.create_demo_db(self.client, self.db_name)
        self.client.set_db_and_branch_names(self.db_name, self.branch_name)
        utils.wait_until_records_are_indexed("Posts")
        """
        r = self.client.records().bulkInsertTableRecords(
            self.branch_name,
            "Posts",
            [], # ["title", "labels", "slug", "text"],
            {"records": self.get_posts()}
        )
        assert r.status_code == 200
        """

        for post in self.get_posts():
            self.client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

    @classmethod
    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.client.get_config()['workspaceId'], self.db_name)
        assert r.status_code == 200

    def test_search_simple(self):
        result = self.client.search("hello")
        assert "records" in result
        assert len(result["records"]) == len(self.get_posts())

        result = self.client.search("apples")
        assert "records" in result
        assert len(result["records"]) == 1

    def test_search_with_params(self):
        result = self.client.search(
            "hello",
            {
                "fuzziness": 1,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == len(self.get_posts())

        result = self.client.search(
            "apples and bananas",
            {
                "fuzziness": 0,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_with_no_hits(self):
        result = self.client.search("12345")
        assert "records" in result
        assert len(result["records"]) == 0


    def test_search_errorcases(self):
        with pytest.raises(BadRequestException) as exc:
            self.client.search("invalid", {"i-am": "invalid"})
        assert exc is not None


    # ------------------------------------------------------- #
    #
    # Search Table
    #
    # ------------------------------------------------------- #
    def test_search_table_simple(self):
        result = self.client.search_table("Posts", "hello")
        assert "records" in result
        assert len(result["records"]) == len(self.get_posts())

        result = self.client.search_table("Posts", "apples")
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_table_with_params(self):
        result = self.client.search_table(
            "Posts",
            "hello",
            {
                "fuzziness": 1,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == len(self.get_posts())

        result = self.client.search_table(
            "Posts",
            "apples and bananas",
            {
                "fuzziness": 0,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_table_with_no_hits(self):
        result = self.client.search_table("Posts", "watermelon")
        assert "records" in result
        assert len(result["records"]) == 0

    def test_search_table_errorcases(self):
        result = self.client.search_table("MissingTable", "hello")
        assert "message" in result
        assert result["message"] == f"table [{self.db_name}:main/MissingTable] not found"

        with pytest.raises(BadRequestException) as exc:
            self.client.search_table("Posts", "invalid", {"i-am": "invalid"})
        assert exc is not None

    @classmethod
    def get_posts(self) -> list[str]:
        """
        List of three Posts
        """
        return [
            {
                "title": "Hello world",
                "labels": ["hello", "world"],
                "slug": "hello-world",
                "text": "This is a test post",
            },
            {
                "title": "HeLLo universe",
                "labels": ["hello", "universe"],
                "slug": "hello-universe",
                "text": "hello, is it me you're looking for?",
            },
            {
                "title": "HELlO internet",
                "labels": ["hello", "internet"],
                "slug": "hello-internet",
                "text": "I like to eat apples and bananas",
            },
        ]