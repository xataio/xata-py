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


class TestTableNamespace(object):
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

        # create database
        r = self.client.databases().create(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.branch_name,
            },
        )
        assert r.status_code == 201

    @classmethod
    def teardown_class(self):
        r = self.client.databases().delete(self.db_name)
        assert r.status_code == 200

    @pytest.fixture
    def columns(self) -> dict:
        return {
            "columns": [
                {"name": "title", "type": "string"},
                {"name": "labels", "type": "multiple"},
                {"name": "slug", "type": "string"},
                {"name": "text", "type": "text"},
            ]
        }

    @pytest.fixture
    def new_column(self) -> dict:
        return {"name": "new_column", "type": "bool"}

    def test_create_table(self):
        r = self.client.table().create("Posts")
        assert r.status_code == 201
        assert r.json()["status"] == "completed"
        assert r.json()["tableName"] == "Posts"
        assert r.json()["branchName"] == self.client.get_db_branch_name()

    def test_update_table(self):
        r = self.client.table().create("RenameMe")
        assert r.status_code == 201

        r = self.client.table().update(
            "RenameMe",
            {"name": "NewName"},
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        assert "migrationID" in r.json()
        assert "parentMigrationID" in r.json()

    def test_delete_table(self):
        r = self.client.table().create("DeleteMe")
        assert r.status_code == 201

        r = self.client.table().delete("DeleteMe")
        assert r.status_code == 200
        assert r.json()["status"] == "completed"

        r = self.client.table().delete("NonExistingTable")
        assert r.status_code == 404

    def test_set_table_schema(self, columns: dict):
        r = self.client.table().set_schema("Posts", columns)
        assert r.status_code == 200

        r = self.client.table().set_schema("NonExistingTable", columns)
        assert r.status_code == 404

    def test_get_table_schema(self, columns):
        r = self.client.table().get_schema("Posts")
        assert r.status_code == 200
        assert columns == r.json()

        r = self.client.table().get_schema("NonExistingTable")
        assert r.status_code == 404

    def test_get_table_columns(self, columns: dict):
        r = self.client.table().get_columns("Posts")
        assert r.status_code == 200
        assert r.json() == columns

        r = self.client.table().get_columns("NonExistingTable")
        assert r.status_code == 404

    def test_add_column(self, columns: dict, new_column: dict):
        r = self.client.table().add_column("Posts", new_column)
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        assert "migrationID" in r.json()
        assert "parentMigrationID" in r.json()

        r = self.client.table().get_columns("Posts")
        assert r.status_code == 200
        columns["columns"].append(new_column)
        assert r.json() == columns

        r = self.client.table().add_column("NonExistingTable", {"name": "foo"})
        assert r.status_code == 404

        r = self.client.table().add_column("Posts", {"name": "foo"})
        assert r.status_code == 400
        r = self.client.table().add_column("Posts", {"type": "bar"})
        assert r.status_code == 400

    def test_get_column(self, new_column: dict):
        r = self.client.table().get_column("Posts", new_column["name"])
        assert r.status_code == 200
        assert r.json() == new_column

        r = self.client.table().get_column("Posts", "NonExistingColumn")
        assert r.status_code == 404

        r = self.client.table().get_column("NonExistingTable", new_column["name"])
        assert r.status_code == 404

    def test_update_column(self, new_column: dict):
        newer_column = {"name": "a-newer-column"}
        r = self.client.table().update_column("Posts", new_column["name"], newer_column)
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        assert "migrationID" in r.json()
        assert "parentMigrationID" in r.json()

        r = self.client.table().get_column("Posts", newer_column["name"])
        assert r.status_code == 200

        r = self.client.table().update_column("Posts", newer_column["name"], {})
        assert r.status_code == 400

        r = self.client.table().update_column("Posts", new_column["name"], newer_column)
        assert r.status_code == 404

        r = self.client.table().update_column("NonExistingTable", new_column["name"], newer_column)
        assert r.status_code == 404

        r = self.client.table().update_column("Posts", "NonExistingColumn", newer_column)
        assert r.status_code == 404

    def test_delete_column(self, columns: dict):
        r = self.client.table().get_columns("Posts")
        assert r.status_code == 200
        assert (len(r.json()["columns"]) - 1) == len(columns["columns"])

        r = self.client.table().delete_column("Posts", "a-newer-column")
        assert r.status_code == 200
        assert r.json()["status"] == "completed"
        assert "migrationID" in r.json()
        assert "parentMigrationID" in r.json()

        r = self.client.table().delete_column("Posts", "a-newer-column")
        assert r.status_code == 404

        r = self.client.table().get_columns("Posts")
        assert r.status_code == 200
        assert r.json() == columns

    def test_deprecated_object_header(self):
        r = self.client.table().create("Posts01")
        assert r.status_code == 201
        assert "x-xata-message" in r.headers
        assert r.headers["x-xata-message"] == ""
