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

import pytest
import utils

from xata.client import XataClient


class TestWorkspacesNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.workspace_name = "py-sdk-tests-%s" % utils.get_random_string(6)

    def test_create_new_workspace(self):
        r = self.client.workspaces().create_async(self.workspace_name, "sluginator")
        assert r.is_success()
        assert "id" in r
        assert "name" in r
        assert "slug" in r
        assert "plan" in r
        assert "memberCount" in r
        assert r["name"] == self.workspace_name
        assert r["slug"] == "sluginator"

        pytest.workspaces["workspace"] = r

        assert self.client.workspaces().delete(self.workspace_name).is_success()
