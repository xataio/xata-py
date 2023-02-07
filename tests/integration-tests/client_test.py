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

import random
import string
import time

import pytest

from xata.client import XataClient
from xata.errors import BadRequestException, RecordNotFoundException


def create_demo_db(client, db_name):
    client.put(f"/dbs/{db_name}", cp=True, json={"region": "us-east-1"})

    client.put(f"/db/{db_name}:main/tables/Posts")
    client.put(f"/db/{db_name}:main/tables/Users")
    client.put(
        f"/db/{db_name}:main/tables/Posts/schema",
        json={
            "columns": [
                {"name": "title", "type": "string"},
                {"name": "labels", "type": "multiple"},
                {"name": "slug", "type": "string"},
                {"name": "text", "type": "text"},
                {
                    "name": "author",
                    "type": "link",
                    "link": {
                        "table": "Users",
                    },
                },
                {"name": "createdAt", "type": "datetime"},
                {"name": "views", "type": "int"},
            ]
        },
    )

    client.put(
        f"/db/{db_name}:main/tables/Users/schema",
        json={
            "columns": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "email"},
                {"name": "bio", "type": "text"},
            ]
        },
    )


def delete_db(client, db_name):
    client.delete(f"/dbs/{db_name}", cp=True)


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


@pytest.fixture
def client() -> XataClient:
    return XataClient()


@pytest.fixture
def demo_db(client: XataClient) -> string:
    db_name = f"sdk-py-e2e-test-{get_random_string(6)}"
    create_demo_db(client, db_name)
    client.set_db_and_branch_names(db_name, "main")
    yield db_name
    delete_db(client, db_name)


@pytest.fixture
def posts() -> list[str]:
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


def _wait_until_records_are_indexed(table: str):
    """
    Wait for the records to be index in order to able to search them
    """
    # TODO remove in favour of wait loop with aggs
    # when aggs are available
    time.sleep(10)


# ------------------------------------------------------- #
#
# Create
#
# ------------------------------------------------------- #
def test_create_and_query(client: XataClient, demo_db: string):
    client.create(
        "Posts",
        record={
            "title": "Hello world",
            "labels": ["hello", "world"],
            "slug": "hello-world",
            "text": "This is a test post",
        },
    )

    rec = client.get_first("Posts")
    assert rec["title"] == "Hello world"
    assert rec["labels"] == ["hello", "world"]
    assert rec["slug"] == "hello-world"
    assert rec["text"] == "This is a test post"


def test_create_with_id(client: XataClient, demo_db: string):
    client.create(
        "Posts",
        id="helloWorld",
        record={"title": "Hello world"},
    )

    with pytest.raises(BadRequestException) as exc:
        client.create(
            "Posts",
            id="helloWorld",
            record={"title": "Hello new world"},
        )
    assert exc.value.status_code == 422
    assert (
        exc.value.message
        == "record with ID [helloWorld] already exists in table [Posts]"
    )

    rec = client.get_first("Posts")
    assert rec["title"] == "Hello world"


def test_create_or_update(client: XataClient, demo_db: string):
    recId = client.create_or_update(
        "Posts",
        "helloWorld",
        record={"title": "Hello world"},
    )
    assert recId == "helloWorld"

    recId = client.create_or_update(
        "Posts",
        "helloWorld",
        record={"slug": "hello_world"},
    )
    assert recId == "helloWorld"

    record = client.get_first("Posts", filter={"id": "helloWorld"})
    assert {"title": "Hello world", "slug": "hello_world"}.items() <= record.items()


def test_create_or_replace(client: XataClient, demo_db: string):
    recId = client.create_or_replace(
        "Posts",
        "helloWorld",
        record={"title": "Hello world"},
    )
    assert recId == "helloWorld"

    recId = client.create_or_replace(
        "Posts",
        "helloWorld",
        record={"slug": "hello_world"},
    )
    assert recId == "helloWorld"

    record = client.get_by_id("Posts", "helloWorld")
    assert {"slug": "hello_world"}.items() <= record.items()
    assert record.get("title") is None


def test_create_and_get(client: XataClient, demo_db: string):
    recId = client.create(
        "Posts",
        record={"title": "Hello world"},
    )
    assert recId is not None

    rec = client.get_by_id("Posts", recId)
    assert {"title": "Hello world"}.items() <= rec.items()

    rec = client.get_by_id("Posts", "something")
    assert rec is None


# ------------------------------------------------------- #
#
# Update
#
# ------------------------------------------------------- #
def test_update(client: XataClient, demo_db: string):
    recId = client.create(
        "Posts",
        record={"title": "Hello world"},
    )

    record = client.update(
        "Posts",
        recId,
        record={"slug": "hello_world"},
    )
    assert {"title": "Hello world", "slug": "hello_world"}.items() <= record.items()

    record = client.update(
        "Posts",
        recId,
        record={"title": "Hi World"},
    )
    assert {"title": "Hi World", "slug": "hello_world"}.items() <= record.items()

    record = client.update(
        "Posts",
        "TEST",
        record={"title": "Hi World"},
    )
    assert record is None


def test_update_ifVersion(client: XataClient, demo_db: string):
    recId = client.create(
        "Posts",
        record={"title": "Hello world"},
    )

    record = client.update(
        "Posts",
        recId,
        record={"slug": "hello_world"},
        ifVersion=0,
    )
    assert {"title": "Hello world", "slug": "hello_world"}.items() <= record.items()

    client.update(
        "Posts",
        recId,
        record={"slug": "hello_world_one"},
        ifVersion=1,
    )

    record = client.get_by_id("Posts", recId)
    assert {"title": "Hello world", "slug": "hello_world_one"}.items() <= record.items()

    client.update(
        "Posts",
        recId,
        record={"slug": "hello_world_two"},
        ifVersion=1,
    )

    record = client.get_by_id("Posts", recId)
    assert {"title": "Hello world", "slug": "hello_world_one"}.items() <= record.items()


# ------------------------------------------------------- #
#
# Delete
#
# ------------------------------------------------------- #
def test_delete_record(client: XataClient, demo_db: string):
    recId = client.create("Posts", record={"title": "Hello world"})

    record = client.delete_record("Posts", recId)
    assert {"title": "Hello world"}.items() <= record.items()

    with pytest.raises(RecordNotFoundException) as exc:
        client.delete_record("Posts", recId)
    assert exc is not None


# ------------------------------------------------------- #
#
# Search
#
# ------------------------------------------------------- #
def test_search_simple(client: XataClient, demo_db: string, posts: list[str]):
    for post in posts:
        client.create("Posts", record=post)
    _wait_until_records_are_indexed("Posts")

    result = client.search("hello")
    assert "records" in result
    assert len(result["records"]) == len(posts)

    result = client.search("apples")
    assert "records" in result
    assert len(result["records"]) == 1


def test_search_with_params(client: XataClient, demo_db: string, posts: list[str]):
    for post in posts:
        client.create("Posts", record=post)
    _wait_until_records_are_indexed("Posts")

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


def test_search_with_no_hits(client: XataClient, demo_db: string, posts: list[str]):
    for post in posts:
        client.create("Posts", record=post)
    _wait_until_records_are_indexed("Posts")

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
def test_search_table_simple(client: XataClient, demo_db: string, posts: list[str]):
    for post in posts:
        client.create("Posts", record=post)
    _wait_until_records_are_indexed("Posts")

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
    _wait_until_records_are_indexed("Posts")

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


def test_search_table_with_no_hits(
    client: XataClient, demo_db: string, posts: list[str]
):
    for post in posts:
        client.create("Posts", record=post)
    _wait_until_records_are_indexed("Posts")

    result = client.search_table("Posts", "watermelon")
    assert "records" in result
    assert len(result["records"]) == 0


def test_search_table_errorcases(client: XataClient, demo_db: string, posts: list[str]):
    result = client.search_table("MissingTable", "hello")
    assert "message" in result
    assert result["message"] == f"table [{demo_db}:main/MissingTable] not found"

    with pytest.raises(BadRequestException) as exc:
        client.search_table("Posts", "invalid", {"i-am": "invalid"})
    assert exc is not None
