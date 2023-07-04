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


class TestAuthenticateNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.new_api_key = "one-key-to-rule-them-all-%s" % utils.get_random_string(6)
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_get_user_api_keys(self):
        r = self.client.authentication().get_user_api_keys()
        assert r.is_success()
        assert "keys" in r
        assert len(r["keys"]) > 0
        assert "name" in r["keys"][0]
        assert "createdAt" in r["keys"][0]

    def test_create_user_api_keys(self):
        r = self.client.authentication().get_user_api_keys()
        assert r.is_success()
        count = len(r["keys"])

        r = self.client.authentication().create_user_api_keys(self.new_api_key)
        assert r.is_success()
        assert "name" in r
        assert "key" in r
        assert "createdAt" in r
        assert self.new_api_key == r["name"]

        r = self.client.authentication().get_user_api_keys()
        assert len(r["keys"]) == (count + 1)

        r = self.client.authentication().create_user_api_keys(self.new_api_key)
        assert not r.is_success()

        r = self.client.authentication().create_user_api_keys("")
        assert not r.is_success()

    def test_delete_user_api_key(self):
        r = self.client.authentication().delete_user_api_keys(self.new_api_key)
        assert r.is_success()

        r = self.client.authentication().delete_user_api_keys("NonExistingApiKey")
        assert not r.is_success()
        assert r.status_code == 404
