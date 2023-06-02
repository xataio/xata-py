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


class TestFilesMultipleFiles(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Files", "true")
        self.fake = Faker()

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
        r = self.client.table().setSchema(
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
        r = self.client.databases().delete(self.db_name)
        assert r.status_code == 200

    def test_put_file_item(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        obj_1, raw_1 = utils.get_file()
        obj_2, raw_2 = utils.get_file()
        file = self.client.files().putItem(
            "Attachments", rid, "one_file", obj_1["base64Content"], "1234"
        )  # obj_1["mediaType"])

        assert file.status_code == 201, file.json()
        assert "attributes" in file.json()
        assert "mediaType" in file.json()
        assert "name" in file.json()
        assert "size" in file.json()

        """
        assert not file.json()["attributes"]
        assert obj["mediaType"] == file.json()["mediaType"]
        assert file.json()["name"] == ""
        assert file.json()["size"] > 0  # TODO test against actual values

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        # assert raw == file.raw
        # TODO ^

        proof = self.client.records().get("Attachments", rid)
        assert proof.status_code == 200, proof.json()
        assert proof.json()["one_file"]["mediaType"] == obj["mediaType"]
        assert proof.json()["one_file"]["name"] == ""
        assert len(list(proof.json()["one_file"].keys())) == 2

    def test_put_image(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        obj, raw = utils.get_image()
        file = self.client.files().put("Attachments", rid, "one_file", obj["base64Content"], obj["mediaType"])
        assert file.status_code == 201, file.json()
        assert "attributes" in file.json()
        # assert "width" in file.json()["attributes"]
        # assert "height" in file.json()["attributes"]

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        # assert raw == file.raw
        # TODO ^

        proof = self.client.records().get("Attachments", rid)
        assert proof.status_code == 200, proof.json()
        assert proof.json()["one_file"]["mediaType"] == obj["mediaType"]
        assert proof.json()["one_file"]["name"] == ""
        # assert proof.json()["one_file"]["attributes"]["height"] == file.json()["attributes"]["height"]
        # assert proof.json()["one_file"]["attributes"]["width"] == file.json()["attributes"]["width"]
        # assert len(list(proof.json()["one_file"].keys())) == 3
        # TODO ^

    def test_put_file_to_overwrite(self):
        obj_1, _ = utils.get_file()
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj_1,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()
        rid = r.json()["id"]

        first = self.client.files().get("Attachments", rid, "one_file")
        assert first.status_code == 200, first.json()

        obj_2, _ = utils.get_file()
        file = self.client.files().put("Attachments", rid, "one_file", obj_1["base64Content"], obj_2["mediaType"])
        assert file.status_code == 200, file.json()

        second = self.client.files().get("Attachments", rid, "one_file")
        assert second.status_code == 200, second.json()
        assert second.raw != first.raw

    def test_get_file(self):
        obj, raw = utils.get_file()
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj,
            "many_files": [],
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        # assert raw == file.raw
        # TODO ^

    def test_delete_file(self):
        obj, _ = utils.get_file()
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert "attributes" in file.json()
        assert "mediaType" in file.json()
        assert "name" in file.json()
        assert payload["one_file"]["mediaType"] == file.json()["mediaType"]
        assert payload["one_file"]["name"] == file.json()["name"]

        proof = self.client.records().get("Attachments", rid)
        assert proof.status_code == 200
        assert "one_file" not in proof.json()
        # null columns are not part of the response set

    def test_delete_image_response_with_attributes(self):
        obj, _ = utils.get_image()
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert "attributes" in file.json()
        assert "height" in file.json()["attributes"]
        assert "width" in file.json()["attributes"]
    """
