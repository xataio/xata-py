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
import utils

from xata.client import XataClient

def pytest_configure():
    pytest.workspaces = {
        "workspace": None,
        "member": None,
    }

def create_demo_db(client: XataClient, db_name: string):
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


@pytest.fixture
def client() -> XataClient:
    return XataClient()


@pytest.fixture
def demo_db(client: XataClient) -> string:
    db_name = f"sdk-py-e2e-test-{utils.get_random_string(6)}"
    create_demo_db(client, db_name)
    client.set_db_and_branch_names(db_name, "main")
    yield db_name
    delete_db(client, db_name)
