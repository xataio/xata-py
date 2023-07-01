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


class TestRecordsFileOperations(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Files", "true")
        self.fake = utils.get_faker()

        r = self.client.databases().createDatabase(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201
        r = self.client.table().createTable("Attachments")
        assert r.status_code == 201

        # create schema
        r = self.client.table().setTableSchema(
            "Attachments",
            {
                "columns": [
                    {"name": "title", "type": "string"},
                    {"name": "one_file", "type": "file"},
                    {"name": "many_files", "type": "file[]"},
                ]
            },
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200

    def test_insert_record_with_files_and_read_it(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif"),
            "many_files": [utils.get_file("images/02.gif") for it in range(3)],
        }

        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()
        assert "id" in r.json()

        r = self.client.records().getRecord("Attachments", r.json()["id"])
        assert r.status_code == 200

        record = r.json()
        assert "id" not in record["one_file"]
        assert len(record["many_files"]) == len(payload["many_files"])
        # the id is used to address the file within the map
        assert "id" in record["many_files"][0]
        assert "id" in record["many_files"][1]
        assert "id" in record["many_files"][2]

        assert "name" in record["one_file"]
        assert "mediaType" in record["one_file"]
        # assert "size" in record["one_file"] # TODO should be here
        assert "name" in record["many_files"][0]
        assert "mediaType" in record["many_files"][0]

        assert record["title"] == payload["title"]
        assert len(list(record["one_file"].keys())) == 2
        assert len(list(record["many_files"][0].keys())) == 3

        r = self.client.records().getRecord(
            "Attachments", r.json()["id"], columns=["one_file.base64Content", "many_files.base64Content"]
        )
        assert r.status_code == 200
        record = r.json()

        assert payload["one_file"]["base64Content"] == record["one_file"]["base64Content"]
        assert payload["many_files"][0]["base64Content"] == record["many_files"][0]["base64Content"]
        assert payload["many_files"][1]["base64Content"] == record["many_files"][1]["base64Content"]
        assert payload["many_files"][2]["base64Content"] == record["many_files"][2]["base64Content"]
