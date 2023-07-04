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
from xata.errors import UnauthorizedException


class TestDatabasesNamespace(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def test_create_database(self):
        assert (
            self.client.databases()
            .create(
                self.db_name,
                {
                    "region": self.client.get_config()["region"],
                    "branchName": self.client.get_config()["branchName"],
                },
            )
            .is_success()
        )

    def test_list_databases(self):
        r = self.client.databases().list()
        assert r.is_success()
        assert "databases" in r
        assert len(r["databases"]) > 0
        assert "name" in r["databases"][0]
        assert "region" in r["databases"][0]
        assert "createdAt" in r["databases"][0]

        # TODO find db record in list
        # assert r["databases"][0]["name"] == self.db_name
        # assert r["databases"][0]["region"] == self.get_config()["region"]

        with pytest.raises(UnauthorizedException) as e:
            self.client.databases().list("NonExistingWorkspaceId")
        assert str(e.value)[0:23] == "code: 401, unauthorized"

    def test_get_database_metadata(self):
        r = self.client.databases().get_metadata(self.db_name)
        assert r.is_success()
        assert "name" in r
        assert "region" in r
        assert "createdAt" in r
        assert r["name"] == self.db_name
        assert r["region"] == self.client.get_config()["region"]

        with pytest.raises(UnauthorizedException) as e:
            self.client.databases().get_metadata(self.db_name, workspace_id="NonExistingWorkspaceId")
        assert str(e.value)[0:23] == "code: 401, unauthorized"

        r = self.client.databases().get_metadata("NonExistingDatabase")
        assert r.status_code() == 404

    def test_update_database_metadata(self):
        metadata = {"ui": {"color": "green"}}
        r_old = self.client.databases().get_metadata(self.db_name)
        assert r_old.is_success()
        r_new = self.client.databases().update_metadata(self.db_name, metadata)
        assert r_new.is_success()
        assert "name" in r_new
        assert "region" in r_new
        assert "createdAt" in r_new
        assert "ui" in r_new
        assert r_new["name"] == self.db_name
        assert r_new["region"] == self.client.get_config()["region"]
        assert r_old != r_new
        assert r_new["ui"] == metadata["ui"]

        r = self.client.databases().update_metadata(self.db_name, {})
        assert r.status_code() == 400
        r = self.client.databases().update_metadata("NonExistingDatabase", metadata)
        assert r.status_code() == 400

        with pytest.raises(UnauthorizedException) as e:
            self.client.databases().update_metadata(self.db_name, metadata, workspace_id="NonExistingWorkspaceId")
        assert str(e.value)[0:23] == "code: 401, unauthorized"

    # run last for cleanup
    def test_delete_database(self):
        r = self.client.databases().delete(self.db_name)
        assert r.is_success()
        assert r["status"] == "completed"

        r = self.client.databases().delete("NonExistingDatabase")
        assert r.status_code() == 404

        with pytest.raises(UnauthorizedException) as e:
            self.client.databases().delete(self.db_name, workspace_id="NonExistingWorkspaceId")
        assert str(e.value)[0:23] == "code: 401, unauthorized"

    def test_get_available_regions(self):
        r = self.client.databases().get_regions()
        assert r.is_success()
        assert "regions" in r
        assert len(r["regions"]) == 5
        assert "id" in r["regions"][0]
        assert "name" in r["regions"][0]

        with pytest.raises(UnauthorizedException) as e:
            self.client.databases().get_regions(workspace_id="NonExistingWorkspaceId")
        assert str(e.value)[0:23] == "code: 401, unauthorized"
