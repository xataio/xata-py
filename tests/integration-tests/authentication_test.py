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
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.new_api_key = "one-key-to-rule-them-all-%s" % utils.get_random_string(6)
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_get_user_api_keys(self):
        r = self.client.authentication().getUserAPIKeys()
        assert r.status_code == 200
        assert "keys" in r.json()
        assert len(r.json()["keys"]) > 0
        assert "name" in r.json()["keys"][0]
        assert "createdAt" in r.json()["keys"][0]

    def test_create_user_api_keys(self):
        r = self.client.authentication().getUserAPIKeys()
        assert r.status_code == 200
        count = len(r.json()["keys"])

        r = self.client.authentication().createUserAPIKey(self.new_api_key)
        assert r.status_code == 201
        assert "name" in r.json()
        assert "key" in r.json()
        assert "createdAt" in r.json()
        assert self.new_api_key == r.json()["name"]

        r = self.client.authentication().getUserAPIKeys()
        assert len(r.json()["keys"]) == (count + 1)

        r = self.client.authentication().createUserAPIKey(self.new_api_key)
        assert r.status_code == 409

        r = self.client.authentication().createUserAPIKey("")
        assert r.status_code == 404

    def test_delete_user_api_key(self):
        r = self.client.authentication().deleteUserAPIKey(self.new_api_key)
        assert r.status_code == 204

        r = self.client.authentication().deleteUserAPIKey("NonExistingApiKey")
        assert r.status_code == 404
