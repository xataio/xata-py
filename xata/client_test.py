import random
import string

import pytest

from . import XataClient
from .errors import BadRequestException, RecordNotFoundException


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


def test_delete_record(client: XataClient, demo_db: string):
    recId = client.create("Posts", record={"title": "Hello world"})

    record = client.delete_record("Posts", recId)
    assert {"title": "Hello world"}.items() <= record.items()

    with pytest.raises(RecordNotFoundException) as exc:
        client.delete_record("Posts", recId)
    assert exc is not None
