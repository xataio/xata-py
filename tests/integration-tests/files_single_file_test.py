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


class TestFilesSingleFile(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Files", "true")
        self.fake = Faker()

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
        r = self.client.table().setTableSchema(
            "Attachments",
            utils.get_attachments_schema(),
            db_name=self.db_name,
            branch_name=self.branch_name,
        )
        assert r.status_code == 200

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(self.db_name)
        assert r.status_code == 200

    def test_put_csv_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        csv = utils.get_file_content(utils.get_file_name("text/stocks.csv"))

        file = self.client.files().put("Attachments", rid, "one_file", csv)
        assert file.status_code == 201, file.json()
        assert "attributes" in file.json()
        assert "mediaType" in file.json()
        assert "name" in file.json()
        assert "size" in file.json()

        assert not file.json()["attributes"]
        #assert attachment["mediaType"] == file.json()["mediaType"]
        assert file.json()["name"] == ""
        assert file.json()["size"] > 0  # TODO test against actual values

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert csv == file.content

        proof = self.client.records().getRecord("Attachments", rid)
        assert proof.status_code == 200, proof.json()
        assert proof.json()["one_file"]["name"] == ""
        assert len(list(proof.json()["one_file"].keys())) == 2

    def test_put_image_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]

        meta = utils.get_file("images/01.gif")
        img = utils.get_file_content(utils.get_file_name("images/01.gif"))

        file = self.client.files().put("Attachments", rid, "one_file", img, meta["mediaType"])
        assert file.status_code == 201, file.json()
        assert "attributes" in file.json()
        assert "width" in file.json()["attributes"]
        assert "height" in file.json()["attributes"]

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert file.headers.get('content-type') == "image/gif"
        assert img == file.content

        proof = self.client.records().getRecord("Attachments", rid)
        assert proof.status_code == 200, proof.json()
        assert proof.json()["one_file"]["mediaType"] == meta["mediaType"]
        assert proof.json()["one_file"]["name"] == ""

    def test_get_file(self):
        obj = utils.get_file("archives/assets.zip")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj,
        }
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201

        rid = r.json()["id"]
        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()

        raw = utils.get_file_content(utils.get_file_name("archives/assets.zip"))
        assert raw == file.content

    def test_put_file_to_overwrite(self):
        img_1 = utils.get_file("images/01.gif")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": img_1,
        }
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()
        rid = r.json()["id"]

        first = self.client.files().get("Attachments", rid, "one_file")
        assert first.status_code == 200
        raw = utils.get_file_content(utils.get_file_name("images/01.gif"))
        assert raw == first.content

        img_2 = utils.get_file_content(utils.get_file_name("images/02.gif"))
        file = self.client.files().put("Attachments", rid, "one_file", img_2)
        assert file.status_code == 200

        second = self.client.files().get("Attachments", rid, "one_file")
        assert second.status_code == 200
        assert second.content != first.content

    def test_delete_file(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif"),
        }
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert "attributes" in file.json()
        assert "mediaType" in file.json()
        assert "name" in file.json()
        assert payload["one_file"]["mediaType"] == file.json()["mediaType"]
        assert payload["one_file"]["name"] == file.json()["name"]

        proof = self.client.records().getRecord("Attachments", rid)
        assert proof.status_code == 200
        assert "one_file" not in proof.json()
        # null columns are not part of the response set

    def test_delete_image_response_with_attributes(self):
        img = utils.get_file("images/02.gif")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": img,
        }
        r = self.client.records().insertRecord("Attachments", payload)
        assert r.status_code == 201, r.json()

        rid = r.json()["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.status_code == 200, file.json()
        assert "attributes" in file.json()
        assert "height" in file.json()["attributes"]
        assert "width" in file.json()["attributes"]