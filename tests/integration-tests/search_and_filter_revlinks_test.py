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

import utils
from faker import Faker

from xata.client import XataClient


class TestSearchAndFilterRevLinks(object):
    def setup_class(self):
        self.fake = Faker()
        self.db_name = utils.get_db_name()
        self.record_id = utils.get_random_string(24)
        self.client = XataClient(db_name=self.db_name)

        assert self.client.databases().create(self.db_name).is_success()

        assert self.client.table().create("Users").is_success()
        assert self.client.table().create("Posts").is_success()

        r = self.client.table().set_schema(
            "Users",
            {
                "columns": [
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                ]
            },
        )
        assert r.is_success()
        r = self.client.table().set_schema(
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "author", "type": "link", "link": {"table": "Users"}},
                    {"name": "slug", "type": "string"},
                ]
            },
        )
        assert r.is_success()

        ids = [self.fake.isbn13() for i in range(10)]
        self.users = [
            {
                "id": ids[i],
                "name": self.fake.name(),
                "email": self.fake.email(),
            }
            for i in range(len(ids))
        ]
        assert self.client.records().bulk_insert("Users", {"records": self.users}).is_success()
        self.posts = [
            {
                "title": self.fake.company(),
                "author": random.choice(ids),
                "slug": self.fake.catch_phrase(),
            }
            for i in range(50)
        ]
        assert self.client.records().bulk_insert("Posts", {"records": self.posts}).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_revlinks_with_alias(self):
        payload = {
            "columns": [
                "name",
                {
                    "name": "<-Posts.author",
                    "columns": ["title"],
                    "as": "posts",
                    "limit": 5,
                    "offset": 0,
                    "order": [{"column": "createdAt", "order": "desc"}],
                },
            ],
            "page": {"size": 5},
        }
        r = self.client.data().query("Users", payload)
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 5
        assert "name" in r["records"][0]
        assert "posts" in r["records"][0]
        assert "records" in r["records"][0]["posts"]

        posts = r["records"][0]["posts"]["records"]
        assert len(posts) >= 1
        assert len(posts) <= 5

        post = posts[0]
        assert "id" in post
        assert "title" in post

    def test_revlinks_without_alias(self):
        payload = {
            "columns": ["name", {"name": "<-Posts.author", "columns": ["title"]}],
            "page": {"size": 1},
        }
        r = self.client.data().query("Users", payload)
        assert r.is_success()
        assert "records" in r
        assert "name" in r["records"][0]
        assert "Postsauthor" in r["records"][0]
        assert "records" in r["records"][0]["Postsauthor"]

    def test_revlinks_with_limit_control(self):
        payload = {
            "columns": ["name", {"name": "<-Posts.author", "columns": ["title"], "as": "posts", "limit": 1}],
            "page": {"size": 5},
        }
        r = self.client.data().query("Users", payload)
        assert r.is_success()
        posts = r["records"][0]["posts"]["records"]
        assert len(posts) == 1
