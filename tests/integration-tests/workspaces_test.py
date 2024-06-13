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
        self.db_name = utils.get_db_name(True)
        self.client = XataClient(db_name=self.db_name)
        self.workspace_name = "py-sdk-tests-%s" % utils.get_random_string(6)

    def test_list_workspaces(self):
        r = self.client.workspaces().list()
        assert r.is_success()
        assert "workspaces" in r
        assert len(r["workspaces"]) > 0
        assert "id" in r["workspaces"][0]
        assert "name" in r["workspaces"][0]
        assert "slug" in r["workspaces"][0]
        assert "role" in r["workspaces"][0]

    def test_create_new_workspace(self):
        r = self.client.workspaces().create(self.workspace_name, "sluginator")
        assert r.is_success()
        assert "id" in r
        assert "name" in r
        assert "slug" in r
        assert "plan" in r
        assert "memberCount" in r
        assert r["name"] == self.workspace_name
        assert r["slug"] == "sluginator"

        pytest.workspaces["workspace"] = r

    def test_create_new_workspace_without_slug(self):
        ws_id = "py-sdk-test-ws-without-slug-%s" % utils.get_random_string(4)
        r = self.client.workspaces().create(ws_id)
        assert r.is_success()
        assert r["name"] == ws_id
        assert r["slug"] == ws_id
        assert self.client.workspaces().delete(r["id"]).is_success()

    def test_get_workspace(self):
        r = self.client.workspaces().get(workspace_id=pytest.workspaces["workspace"]["id"])
        assert r.is_success()
        assert "name" in r
        assert "slug" in r
        assert "id" in r
        assert "memberCount" in r
        assert "plan" in r
        assert r["name"] == pytest.workspaces["workspace"]["name"]
        assert r["slug"] == pytest.workspaces["workspace"]["slug"]
        assert r["plan"] == pytest.workspaces["workspace"]["plan"]
        assert r["memberCount"] == pytest.workspaces["workspace"]["memberCount"]
        assert r["id"] == pytest.workspaces["workspace"]["id"]

        assert not self.client.workspaces().get("NonExistingWorkspaceId").is_success()

    def test_update_workspace(self):
        payload = {
            "name": "new-workspace-name",
            "slug": "super-duper-new-slug",
        }
        r = self.client.workspaces().update(
            payload,
            workspace_id=pytest.workspaces["workspace"]["id"],
        )
        assert r.is_success()
        assert "name" in r
        assert "slug" in r
        assert "id" in r
        assert "memberCount" in r
        assert "plan" in r
        assert r["name"] == payload["name"]
        assert r["slug"] == payload["slug"]

        r = self.client.workspaces().update(
            {"name": "only-a-name"},
            workspace_id=pytest.workspaces["workspace"]["id"],
        )
        assert r.is_success()

        assert (
            not self.client.workspaces()
            .update(
                {"slug": "only-a-slug"},
                workspace_id=pytest.workspaces["workspace"]["id"],
            )
            .is_success()
        )

    def test_delete_workspace(self):
        assert self.client.workspaces().delete(workspace_id=pytest.workspaces["workspace"]["id"]).is_success()
        pytest.workspaces["workspace"] = None

        assert not self.client.workspaces().delete(workspace_id="NonExistingWorkspace").is_success()

    #
    # Workspace Member Ops
    #
    def test_get_workspace_members(self):
        r = self.client.workspaces().get_members()
        assert r.is_success()
        assert "members" in r
        assert "invites" in r
        assert len(r["members"]) > 0
        assert "userId" in r["members"][0]
        assert "fullname" in r["members"][0]
        assert "email" in r["members"][0]
        assert "role" in r["members"][0]

        pytest.workspaces["member"] = r["members"][0]

        assert not self.client.workspaces().get_members(workspace_id="NonExistingWorkspaceId").is_success()

    def test_update_workspace_member(self):
        payload = {"role": "owner" if pytest.workspaces["member"]["role"] == "maintainer" else "owner"}

        assert (
            not self.client.workspaces()
            .update_member(pytest.workspaces["member"]["userId"], {"role": "spiderman"})
            .is_success()
        )
        assert (
            not self.client.workspaces()
            .update_member(
                pytest.workspaces["member"]["userId"],
                payload,
                workspace_id="NonExistingWorkspaceId",
            )
            .is_success()
        )
        assert not self.client.workspaces().update_member("NonExistingUserId", payload).is_success()
        assert not self.client.workspaces().update_member(pytest.workspaces["member"]["userId"], {}).is_success()

        pytest.workspaces["member"] = None
