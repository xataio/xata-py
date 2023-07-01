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
from requests import request

from xata.client import XataClient


class TestFilesMultipleFiles(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Files", "true")
        self.fake = utils.get_faker()

        r = self.client.databases().create(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        r = self.client.table().create("Attachments")
        assert r.status_code == 201
        r = self.client.table().set_schema(
            "Attachments",
            utils.get_attachments_schema(),
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200

    def teardown_class(self):
        r = self.client.databases().delete(self.db_name)
        assert r.status_code == 200

    def test_put_file_item(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "many_files": [
                utils.get_file("images/01.gif", public_url=True),
                utils.get_file("images/02.gif", public_url=True)
            ]
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        record = self.client.records().get("Attachments", rid, columns=["many_files.id", "many_files.url"])
        assert record.status_code == 200
        assert len(record.json()["many_files"]) == 2

        proof_1 = request("GET", record.json()["many_files"][0]["url"])
        assert proof_1.status_code == 200

        proof_2 = request("GET", record.json()["many_files"][1]["url"])
        assert proof_2.status_code == 200

        img_1 = utils.get_file_content(utils.get_file_name("images/01.gif"))
        img_2 = utils.get_file_content(utils.get_file_name("images/02.gif"))
        assert img_1 == proof_1.content
        assert img_2 == proof_2.content

        # overwrite item 1 with image 2
        file_1 = self.client.files().put_item("Attachments", rid, "many_files", record.json()["many_files"][0]["id"], img_2)
        assert file_1.status_code == 200
        assert "attributes" in file_1.json()
        assert "mediaType" in file_1.json()
        assert "name" in file_1.json()
        assert "size" in file_1.json()

        prev_url = record.json()["many_files"][0]["url"]
        record = self.client.records().get("Attachments", rid, columns=["many_files.id", "many_files.url"])
        assert prev_url != record.json()["many_files"][0]["url"]

        proof = request("GET", record.json()["many_files"][0]["url"])
        assert proof.status_code == 200
        assert proof.content == img_2

    def test_delete_file(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "many_files": [
                utils.get_file("images/01.gif", public_url=True),
                utils.get_file("images/02.gif", public_url=True)
            ]
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        record = self.client.records().get("Attachments", rid, columns=["many_files.id"])
        assert record.status_code == 200
        assert len(record.json()["many_files"]) == 2

        r = self.client.files().delete_item("Attachments", rid, "many_files", record.json()["many_files"][0]["id"])
        assert r.status_code == 200
        prev_id = record.json()["many_files"][0]["id"]

        record = self.client.records().get("Attachments", rid, columns=["many_files.id"])
        assert record.status_code == 200
        assert len(record.json()["many_files"]) == 1

        proof = self.client.files().get_item("Attachements", rid, "many_files", prev_id)
        assert proof.status_code == 404

    def test_get_item(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "many_files": [
                utils.get_file("images/01.gif", public_url=True),
                utils.get_file("images/02.gif", public_url=True)
            ]
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        record = self.client.records().get("Attachments", rid, columns=["many_files.id"])
        assert record.status_code == 200

        item = self.client.files().get_item("Attachments", rid, "many_files", record.json()["many_files"][0]["id"])
        assert item.status_code == 200
        assert item.content == utils.get_file_content(utils.get_file_name("images/01.gif"))
