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
            domain_core="api.staging-xata.dev", domain_workspace="staging-xata.dev", db_name=self.db_name
        )
        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Users").is_success()
        assert (
            self.client.table()
            .set_schema(
                "Users",
                {"columns": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]},
            )
            .is_success()
        )
        users = [
            {
                "name": utils.get_faker().name(),
                "email": utils.get_faker().email(),
            }
            for i in range(50)
        ]
        assert self.client.records().bulk_insert("Users", {"records": users}).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_query(self):
        r = self.client.sql().query('SELECT * FROM "Users" LIMIT 5')
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 5

    def test_query_on_non_existing_table(self):
        r = self.client.sql().query('SELECT * FROM "DudeWhereIsMyTable"')
        assert not r.is_success()

    def test_query_with_invalid_statement(self):
        r = self.client.sql().query("SELECT ' fr_o-")
        assert not r.is_success()

    def test_query_statement_with_missing_params(self):
        r = self.client.sql().query("SELECT * FROM \"Users\" WHERE email = '$1'")
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 0

    def test_query_statement_with_params_and_no_param_references(self):
        r = self.client.sql().query('SELECT * FROM "Users"', ["This is important"])
        assert not r.is_success()

    def test_query_statement_with_incorrect_amount_of_params(self):
        r = self.client.sql().query(
            'INSERT INTO "Users" (name, email) VALUES ($1, $2)', ["Shrek", "shrek@example.com", "Hi, I'm too much!"]
        )
        assert not r.is_success()

        r = self.client.sql().query('INSERT INTO "Users" (name, email) VALUES ($1, $2)', ["Shrek"])
        assert not r.is_success()

    def test_insert(self):
        r = self.client.sql().query(
            "INSERT INTO \"Users\" (name, email) VALUES ('Leslie Nielsen', 'leslie@example.com')"
        )
        assert r.is_success()
        assert "records" in r

    def test_insert_with_params(self):
        r = self.client.sql().query(
            'INSERT INTO "Users" (name, email) VALUES ($1, $2)', ["Keanu Reeves", "keanu@example.com"]
        )
        assert r.is_success()
        assert "records" in r

    def test_query_with_params(self):
        r = self.client.sql().query('SELECT * FROM "Users" WHERE email = $1', ["keanu@example.com"])
        assert r.is_success()
        assert "records" in r
        assert len(r["records"]) == 1
