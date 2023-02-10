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

import pytest

from xata.client import XataClient
from xata.errors import BadRequestException, RecordNotFoundException


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
