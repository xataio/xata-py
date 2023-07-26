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


class TestSqlQuery(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(
            domain_core="api.staging-xata.dev",
            domain_workspace="staging-xata.dev",
            db_name=self.db_name,
            branch_name="main",
        )
        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Users").is_success()
        assert (
            self.client.table()
            .set_schema(
                "Users",
                {
                    "columns": [
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"}
                    ]
                },
            )
            .is_success()
        )
        # ingests posts
        self.posts = [
            {
                "name": utils.get_faker().name(),
                "email": utils.get_faker().email(),
            }
            for i in range(25)
        ]
        assert self.client.records().bulk_insert("Users", {"records": self.posts}).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_query(self):
        r = self.client.sql().query("SELECT * from \"Users\" LIMIT 5")
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 5

    def test_insert_with_params(self):
        r = self.client.sql().query("INSERT INTO \"Users\" (name, email) VALUES ($1, $2)", ["Keanu Reeves", "keanu@example.com"])
        assert r.is_success()
        assert "records" in r

    def test_query_with_params(self):
        r = self.client.sql().query("SELECT * from \"Users\" WHERE email = \"$1\"", ["keanu@example.com"])
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 1