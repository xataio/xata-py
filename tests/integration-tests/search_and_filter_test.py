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
        pass

    @classmethod
    def teardown_class(self):
        pass

    # ------------------------------------------------------- #
    #
    # Search
    #
    # ------------------------------------------------------- #
    def test_search_simple(self, client: XataClient, demo_db: string, posts: list[str]):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search("hello")
        assert "records" in result
        assert len(result["records"]) == len(posts)

        result = client.search("apples")
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_with_params(self, client: XataClient, demo_db: string, posts: list[str]):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search(
            "hello",
            {
                "fuzziness": 1,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == len(posts)

        result = client.search(
            "apples and bananas",
            {
                "fuzziness": 0,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_with_no_hits(self, client: XataClient, demo_db: string, posts: list[str]):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search("12345")
        assert "records" in result
        assert len(result["records"]) == 0


    def test_search_errorcases(client: XataClient, demo_db: string, posts: list[str]):
        with pytest.raises(BadRequestException) as exc:
            client.search("invalid", {"i-am": "invalid"})
        assert exc is not None


    # ------------------------------------------------------- #
    #
    # Search Table
    #
    # ------------------------------------------------------- #
    def test_search_table_simple(self, client: XataClient, demo_db: string, posts: list[str]):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search_table("Posts", "hello")
        assert "records" in result
        assert len(result["records"]) == len(posts)

        result = client.search_table("Posts", "apples")
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_table_with_params(
        client: XataClient, demo_db: string, posts: list[str]
    ):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search_table(
            "Posts",
            "hello",
            {
                "fuzziness": 1,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == len(posts)

        result = client.search_table(
            "Posts",
            "apples and bananas",
            {
                "fuzziness": 0,
                "prefix": "phrase",
            },
        )
        assert "records" in result
        assert len(result["records"]) == 1


    def test_search_table_with_no_hits(self,
        client: XataClient, demo_db: string, posts: list[str]
    ):
        for post in posts:
            client.create("Posts", record=post)
        utils.wait_until_records_are_indexed("Posts")

        result = client.search_table("Posts", "watermelon")
        assert "records" in result
        assert len(result["records"]) == 0


    def test_search_table_errorcases(self, client: XataClient, demo_db: string, posts: list[str]):
        result = client.search_table("MissingTable", "hello")
        assert "message" in result
        assert result["message"] == f"table [{demo_db}:main/MissingTable] not found"

        with pytest.raises(BadRequestException) as exc:
            client.search_table("Posts", "invalid", {"i-am": "invalid"})
        assert exc is not None