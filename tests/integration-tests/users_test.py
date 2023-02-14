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


class TestClass(object):
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_get_user_api_keys(self):
        r = self.client.users().getUser()
        assert r.status_code == 200
        assert "id" in r.json()
        assert "email" in r.json()
        assert "fullname" in r.json()
        assert "image" in r.json()

    def test_create_user_api_keys(self):
        prev = self.client.users().getUser()
        assert prev.status_code == 200

        user = prev.json()
        del user["id"]
        user["fullname"] = "test-suite-%s" % utils.get_random_string(4)
        assert user["fullname"] != prev.json()["fullname"]

        now = self.client.users().updateUser(user)
        assert now.status_code == 200
        assert "id" in now.json()
        assert "email" in now.json()
        assert "fullname" in now.json()
        assert "image" in now.json()
        assert now.json()["fullname"] == user["fullname"]

        user["fullname"] = prev.json()["fullname"]
        r = self.client.users().updateUser(user)
        assert r.status_code == 200
        assert r.json()["fullname"] == prev.json()["fullname"]

        r = self.client.users().updateUser({})
        assert r.status_code == 400
