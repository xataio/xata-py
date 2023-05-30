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
import base64

from faker import Faker

faker = Faker()
Faker.seed(42)

def get_faker() -> Faker:
    return faker

def get_db_name() -> str:
    return f"sdk-integration-test-py-{get_random_string(6)}"


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

def get_file(publicUrl: bool = True, signedUrlTimeout: int = 120, cat: str = None):
    if not cat:
        cat = random.choice(["image", "audio", "video", "text"])
    file_name = faker.file_path(depth=random.randint(0, 7), category=cat)
    # different file types
    if cat == "image":
        file_content = faker.image(size=(random.randint(10, 512), random.randint(10, 512)))
    else:
        file_content = faker.binary(random.randint(256, 1024))
    encoded_string = base64.b64encode(file_content).decode("ascii")
    return {
        "name": file_name.replace("/", "_"),
        "mediaType": faker.mime_type(category=cat),
        "base64Content": encoded_string,
        "enablePublicUrl": publicUrl,
        "signedUrlTimeout": signedUrlTimeout,
    }, file_content

def get_image(publicUrl: bool = True, signedUrlTimeout: int = 120):
    return get_file(publicUrl, signedUrlTimeout, "image")
