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

from requests import Response

from xata.client import XataClient


def get_db_name() -> str:
    return f"sdk-py-e2e-test-{get_random_string(6)}"


def wait_until_records_are_indexed(table: str):
    """
    Wait for the records to be index in order to able to search them
    """
    # TODO remove in favour of wait loop with aggs
    # when aggs are available
    time.sleep(10)


def get_random_string(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def get_posts() -> list[str]:
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


def create_demo_db(client: XataClient, db_name: string):
    client.request("PUT", f"/dbs/{db_name}", cp=True, json={"region": "us-east-1"})

    client.request("PUT", f"/db/{db_name}:main/tables/Posts")
    client.request("PUT", f"/db/{db_name}:main/tables/Users")
    client.request(
        "PUT", 
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

    client.request(
        "PUT", 
        f"/db/{db_name}:main/tables/Users/schema",
        json={
            "columns": [
                {"name": "name", "type": "string"},
                {"name": "email", "type": "email"},
                {"name": "bio", "type": "text"},
            ]
        },
    )


def delete_db(client, db_name) -> Response:
    return client.databases().deleteDatabase(
        client.get_config()["workspaceId"], db_name
    )
