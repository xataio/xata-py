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

from faker import Faker
from xata.client import XataClient


class TestClass(object):
    @classmethod
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = Faker()

        # create database
        r = self.client.databases().createDatabase(
            self.client.get_config()["workspaceId"],
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable(self.client.get_db_branch_name(), "Posts")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            self.client.get_db_branch_name(),
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "labels", "type": "multiple"},
                    {"name": "slug", "type": "string"},
                    {"name": "text", "type": "text"},
                ]
            },
        )
        assert r.status_code == 200

    @classmethod
    def teardown_class(self):
        r = self.client.databases().deleteDatabase(
            self.client.get_config()["workspaceId"], self.db_name
        )
        assert r.status_code == 200

    def test_insert_record(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data
        """
        record = {
            "title": self.fake.company(),
            "labels": [self.fake.domain_word(), self.fake.domain_word()],
            "slug": self.fake.catch_phrase(),
            "text": self.fake.text(),
        }
        r = self.client.records().insertRecord(self.client.get_db_branch_name(), "Posts", [], record)
        assert r.status_code == 201
        assert "id" in r.json()
        assert "xata" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["xata"]["version"] == 0

        r = self.client.records().insertRecord(self.client.get_db_branch_name(), "NonExistingTable", [], record)
        assert r.status_code == 404

        r = self.client.records().insertRecord("NonExistingDbBranchName", "Posts", [], record)
        assert r.status_code == 400

    # TODO: GET /db/{db_branch_name}/tables/{table_name}/data/{record_id}
    # TODO: PUT /db/{db_branch_name}/tables/{table_name}/data/{record_id}
    # TODO: PATCH /db/{db_branch_name}/tables/{table_name}/data/{record_id}
    # TODO: DELET /db/{db_branch_name}/tables/{table_name}/data/{record_id}
    # TODO: POST /db/{db_branch_name}/tables/{table_name}/data/{record_id}

    def test_bulk_insert_table_records(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/bulk
        """
        r = self.client.records().bulkInsertTableRecords(
            self.client.get_db_branch_name(),
            "Posts",
            [],  # ["title", "labels", "slug", "text"],
            {"records": utils.get_posts()},
        )
        assert r.status_code == 200

    # TODO: /db/{db_branch_name}/transaction
