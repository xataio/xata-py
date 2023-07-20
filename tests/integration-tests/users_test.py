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


class TestUsersNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_get_user_api_keys(self):
        r = self.client.users().get()
        assert r.is_success()
        assert "id" in r
        assert "email" in r
        assert "fullname" in r
        assert "image" in r

    def test_create_user_api_keys(self):
        prev = self.client.users().get()
        assert prev.is_success()

        """
        user = {"fullname": "test-suite-%s" % utils.get_random_string(4)}
        assert user["fullname"] != prev["fullname"]

        now = self.client.users().update(user)
        assert now.is_success()
        assert "id" in now
        assert "email" in now
        assert "fullname" in now
        assert "image" in now
        assert now["fullname"] == user["fullname"]

        user["fullname"] = prev["fullname"]
        r = self.client.users().update(user)
        assert r.is_success()
        assert r["fullname"] == prev["fullname"]

        r = self.client.users().update({})
        assert not r.is_success()
        """
