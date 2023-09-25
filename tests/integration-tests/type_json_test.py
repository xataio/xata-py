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

import json

import utils
from faker import Faker

from xata.client import XataClient


class TestJsonColumnType(object):
    def setup_class(self):
        self.fake = Faker()
        self.db_name = utils.get_db_name()
        self.record_id = utils.get_random_string(24)
        self.client = XataClient(db_name=self.db_name)

        assert self.client.databases().create(self.db_name).is_success()

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_create_table_with_type(self):
        assert self.client.table().create("Posts").is_success()
        assert (
            self.client.table()
            .set_schema(
                "Posts",
                {
                    "columns": [
                        {"name": "title", "type": "string"},
                        {"name": "meta", "type": "json"},
                    ]
                },
            )
            .is_success()
        )

    def test_insert_records(self):
        result = self.client.records().bulk_insert(
            "Posts",
            {
                "records": [
                    {
                        "title": self.fake.catch_phrase(),
                        "meta": json.dumps({"number": it, "nested": {"random": it * it, "a": "b"}}),
                    }
                    for it in range(25)
                ]
            },
        )
        assert result.is_success()

    def test_query_records(self):
        result = self.client.data().query("Posts")
        assert result.is_success()
        assert len(result["records"]) > 5

        assert "meta" in result["records"][5]
        j = json.loads(result["records"][5]["meta"])
        assert "number" in j
        assert "nested" in j
        assert "random" in j["nested"]
        assert "a" in j["nested"]

        assert j["number"] == 5
        assert j["nested"]["random"] == 5 * 5
        assert j["nested"]["a"] == "b"
