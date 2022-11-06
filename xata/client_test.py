import random
import string

import pytest

from xata.client import BadRequestException

from . import XataClient


def create_demo_db(client, dbName):
    client.put(f"/dbs/{dbName}", cp=True, json={"region": "us-east-1"})

    client.put(f"/db/{dbName}:main/tables/Posts")
    client.put(f"/db/{dbName}:main/tables/Users")
    client.put(
        f"/db/{dbName}:main/tables/Posts/schema",
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
        f"/db/{dbName}:main/tables/Users/schema",
        json={
            "columns": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "email"},
                {"name": "bio", "type": "text"},
            ]
        },
    )


def delete_db(client, dbName):
    client.delete(f"/dbs/{dbName}", cp=True)


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


@pytest.fixture
def client() -> XataClient:
    return XataClient()


@pytest.fixture
def demo_db(client: XataClient) -> string:
    dbName = f"sdk-py-e2e-test-{get_random_string(6)}"
    create_demo_db(client, dbName)
    yield dbName
    delete_db(client, dbName)


def test_create_and_query(client: XataClient, demo_db: string):
    client.create(
        "Posts",
        dbName=demo_db,
        branchName="main",
        record={
            "title": "Hello world",
            "labels": ["hello", "world"],
            "slug": "hello-world",
            "text": "This is a test post",
        },
    )

    rec = client.get_first("Posts", dbName=demo_db, branchName="main")
    assert rec["title"] == "Hello world"
    assert rec["labels"] == ["hello", "world"]
    assert rec["slug"] == "hello-world"
    assert rec["text"] == "This is a test post"


def test_create_with_id(client: XataClient, demo_db: string):
    client.create(
        "Posts",
        dbName=demo_db,
        branchName="main",
        id="helloWorld",
        record={"title": "Hello world"},
    )

    with pytest.raises(BadRequestException) as exc:
        client.create(
            "Posts",
            dbName=demo_db,
            branchName="main",
            id="helloWorld",
            record={"title": "Hello new world"},
        )
    assert exc.value.status_code == 422
    assert (
        exc.value.message
        == "record with ID [helloWorld] already exists in table [Posts]"
    )

    rec = client.get_first("Posts", dbName=demo_db, branchName="main")
    assert rec["title"] == "Hello world"
