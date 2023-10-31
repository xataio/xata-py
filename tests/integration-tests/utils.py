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

import base64
import os
import random
import string
import time

import magic
from faker import Faker

faker = Faker()
Faker.seed(42)


def get_faker() -> Faker:
    return faker


def get_db_name() -> str:
    return f"sdk-integration-py-{get_random_string(6)}"


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


def get_attachments_schema() -> dict:
    return {
        "columns": [
            {"name": "title", "type": "string"},
            {"name": "one_file", "type": "file"},
            {"name": "many_files", "type": "file[]"},
        ]
    }


def get_file_name(file_name: str) -> str:
    return "%s/tests/data/attachments/%s" % (os.getcwd(), file_name)


def get_file_content(file_name: str) -> bytes:
    with open(file_name, "rb") as f:
        return f.read()


def get_file(file_name: str, public_url: bool = False, signed_url_timeout: int = 30):
    file_name = get_file_name(file_name)
    file_content = get_file_content(file_name)

    return {
        "name": file_name.replace("/", "_"),
        "mediaType": magic.from_file(file_name, mime=True),
        "base64Content": base64.b64encode(file_content).decode("ascii"),
        "enablePublicUrl": public_url,
        "signedUrlTimeout": signed_url_timeout,
    }


def get_post() -> dict:
    return {
        "title": get_faker().company(),
        "labels": [get_faker().domain_word(), get_faker().domain_word()],
        "slug": get_faker().catch_phrase(),
        "content": get_faker().text(),
    }


def get_posts(n: int = 3) -> list[str]:
    """
    List of three Posts
    """
    return [get_post() for i in range(n)]


def get_posts_schema() -> dict:
    return {
        "columns": [
            {"name": "title", "type": "string"},
            {"name": "labels", "type": "multiple"},
            {"name": "slug", "type": "string"},
            {"name": "content", "type": "text"},
        ]
    }
