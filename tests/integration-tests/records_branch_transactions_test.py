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
from faker import Faker

from xata.client import XataClient


class TestRecordsBranchTransactionsNamespace(object):
    """
    POST /db/{db_branch_name}/transaction
    """
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = Faker()
        self.record_id = utils.get_random_string(24)

        # create database
        r = self.client.databases().createDatabase(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable("Posts")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            "Posts",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "labels", "type": "multiple"},
                    {"name": "slug", "type": "string"},
                    {"name": "text", "type": "text"},
                ]
            },
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200

    @pytest.fixture
    def record(self) -> dict:
        return self._get_record()

    def _get_record(self) -> dict:
        return {
            "title": self.fake.company(),
            "labels": [self.fake.domain_word(), self.fake.domain_word()],
            "slug": self.fake.catch_phrase(),
            "text": self.fake.text(),
        }

    def test_insert_only(self, record: dict):
        payload = {"operations": [
            {"insert": {"table": "Posts", "record": self._get_record()}},
            {"insert": {"table": "Posts", "record": self._get_record()}},
            {"insert": {"table": "Posts", "record": self._get_record()}},
            {"insert": {"table": "Posts", "record": self._get_record()}},
            {"insert": {"table": "Posts", "record": self._get_record()}}
        ]}

        r = self.client.records().branchTransaction("Posts", payload)
        assert "" == r.json()
        assert r.status_code == 201
        assert "id" in r.json()
        assert "xata" in r.json()
        assert "version" in r.json()["xata"]
        assert r.json()["xata"]["version"] == 0

    
    def test_error_cases(self, record: dict):
        r = self.client.records().branchTransaction({})
        assert r.status_code == 404
