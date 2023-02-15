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


class TestClass(object):

    workspace_id = None

    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.workspace_name = "py-sdk-tests-%s" % utils.get_random_string(6)

    #
    # Workspace Ops
    #
    def test_list_workspaces(self):
        r = self.client.workspaces().getWorkspacesList()
        assert r.status_code == 200
        assert "workspaces" in r.json()
        assert len(r.json()["workspaces"]) > 0
        assert "id" in r.json()["workspaces"][0]
        assert "name" in r.json()["workspaces"][0]
        assert "slug" in r.json()["workspaces"][0]
        assert "role" in r.json()["workspaces"][0]

    def test_create_new_workspace(self):
        r = self.client.workspaces().createWorkspace(
            {
                "name": self.workspace_name,
                "slug": "sluginator",
            }
        )
        assert r.status_code == 201
        assert "id" in r.json()
        assert "name" in r.json()
        assert "slug" in r.json()
        assert "plan" in r.json()
        assert "memberCount" in r.json()
        assert r.json()["name"] == self.workspace_name
        assert r.json()["slug"] == "sluginator"

        pytest.workspaces["workspace"] = r.json()

    def test_get_workspace(self):
        r = self.client.workspaces().getWorkspace(pytest.workspaces["workspace"]["id"])
        assert r.status_code == 200
        assert "name" in r.json()
        assert "slug" in r.json()
        assert "id" in r.json()
        assert "memberCount" in r.json()
        assert "plan" in r.json()
        assert r.json()["name"] == pytest.workspaces["workspace"]["name"]
        assert r.json()["slug"] == pytest.workspaces["workspace"]["slug"]
        assert r.json()["plan"] == pytest.workspaces["workspace"]["plan"]
        assert r.json()["memberCount"] == pytest.workspaces["workspace"]["memberCount"]
        assert r.json()["id"] == pytest.workspaces["workspace"]["id"]

        r = self.client.workspaces().getWorkspace("NonExistingWorkspaceId")
        assert r.status_code == 403

    def test_update_workspace(self):
        payload = {
            "name": "new-workspace-name",
            "slug": "super-duper-new-slug",
        }
        r = self.client.workspaces().updateWorkspace(
            pytest.workspaces["workspace"]["id"], payload
        )
        assert r.status_code == 200
        assert "name" in r.json()
        assert "slug" in r.json()
        assert "id" in r.json()
        assert "memberCount" in r.json()
        assert "plan" in r.json()
        assert r.json()["name"] == payload["name"]
        assert r.json()["slug"] == payload["slug"]

        r = self.client.workspaces().updateWorkspace(
            pytest.workspaces["workspace"]["id"], {"name": "only-a-name"}
        )
        assert r.status_code == 200

        r = self.client.workspaces().updateWorkspace(
            pytest.workspaces["workspace"]["id"], {"slug": "only-a-slug"}
        )
        assert r.status_code == 400

    def test_delete_workspace(self):
        r = self.client.workspaces().deleteWorkspace(
            pytest.workspaces["workspace"]["id"]
        )
        assert r.status_code == 204
        pytest.workspaces["workspace"] = None

        r = self.client.workspaces().deleteWorkspace("NonExistingWorkspace")
        assert r.status_code == 403

    #
    # Workspace Member Ops
    #
    def test_get_workspace_members(self):
        r = self.client.workspaces().getWorkspaceMembersList(
            self.client.get_config()["workspaceId"]
        )
        assert r.status_code == 200
        assert "members" in r.json()
        assert "invites" in r.json()
        assert len(r.json()["members"]) > 0
        assert "userId" in r.json()["members"][0]
        assert "fullname" in r.json()["members"][0]
        assert "email" in r.json()["members"][0]
        assert "role" in r.json()["members"][0]

        pytest.workspaces["member"] = r.json()["members"][0]

        r = self.client.workspaces().getWorkspaceMembersList("NonExistingWorkspaceId")
        assert r.status_code == 403

    def test_update_workspace_member(self):
        payload = {
            "role": "owner"
            if pytest.workspaces["member"]["role"] == "maintainer"
            else "owner"
        }

        r = self.client.workspaces().updateWorkspaceMemberRole(
            self.client.get_config()["workspaceId"],
            pytest.workspaces["member"]["userId"],
            {"role": "spiderman"},
        )
        assert r.status_code == 400
        r = self.client.workspaces().updateWorkspaceMemberRole(
            "NonExistingWorkspaceId", pytest.workspaces["member"]["userId"], payload
        )
        assert r.status_code == 403
        r = self.client.workspaces().updateWorkspaceMemberRole(
            self.client.get_config()["workspaceId"], "NonExistingUserId", payload
        )
        assert r.status_code == 403
        r = self.client.workspaces().updateWorkspaceMemberRole(
            self.client.get_config()["workspaceId"],
            pytest.workspaces["member"]["userId"],
            {},
        )
        assert r.status_code == 400

        pytest.workspaces["member"] = None
