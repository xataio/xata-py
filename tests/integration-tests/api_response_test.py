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

import utils

from xata.client import XataClient


class TestApiResponse(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(db_name=self.db_name)
        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Posts").is_success()
        assert self.client.table().set_schema("Posts", utils.get_posts_schema()).is_success()

        payload = {
            "operations": [
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
                {"insert": {"table": "Posts", "record": utils.get_post()}},
            ]
        }
        assert self.client.records().transaction(payload).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_is_success_true(self):
        user = XataClient().users().get()
        assert user.is_success()
        assert user.status_code >= 200
        assert user.status_code < 300

    def test_is_success_false_with_unknown_table(self):
        user = XataClient().records().get("Nope", "nope^2")
        assert not user.is_success()
        assert user.status_code == 404

    def test_direct_repr_and_json_are_the_same(self):
        user = XataClient().users().get()
        assert user.is_success()
        assert user == user.json()

    def test_get_cursor_and_has_more_results(self):
        query = {
            "columns": ["*"],
            "page": {"size": 9, "after": None},
        }
        posts = self.client.data().query("Posts", query)
        assert posts.is_success()
        assert len(posts["records"]) == 9
        assert posts.has_more_results()
        assert posts.get_cursor() is not None

        query = {
            "columns": ["*"],
            "page": {"size": 6, "after": posts.get_cursor()},
        }
        posts = self.client.data().query("Posts", query)
        assert posts.is_success()
        assert len(posts["records"]) == 1
        assert not posts.has_more_results()

    def test_error_message(self):
        user = self.client.records().get("Nope", "nope^2")
        assert not user.is_success()
        assert user.status_code > 299
        assert user.error_message is not None
        assert user.error_message.endswith("not found")

    def test_error_message_should_not_be_set(self):
        user = self.client.users().get()
        assert user.is_success()
        assert user.status_code < 300
        assert not user.error_message
