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
        self.fake = Faker()

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Attachments").is_success()
        assert (
            self.client.table()
            .set_schema(
                "Attachments",
                utils.get_attachments_schema(),
                db_name=self.db_name,
                branch_name=self.branch_name,
            )
            .is_success()
        )

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_put_csv_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        csv = utils.get_file_content(utils.get_file_name("text/stocks.csv"))

        file = self.client.files().put("Attachments", rid, "one_file", csv)
        assert file.is_success()
        assert "attributes" in file
        assert "mediaType" in file
        assert "name" in file
        assert "size" in file

        assert not file["attributes"]
        # assert attachment["mediaType"] == file["mediaType"]
        assert file["name"] == ""
        assert file["size"] > 0  # TODO test against actual values

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.is_success()
        assert csv == file.content

        proof = self.client.records().get("Attachments", rid)
        assert proof.is_success()
        assert proof["one_file"]["name"] == ""

    def test_put_image_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        meta = utils.get_file("images/01.gif")
        img = utils.get_file_content(utils.get_file_name("images/01.gif"))

        file = self.client.files().put("Attachments", rid, "one_file", img, meta["mediaType"])
        assert file.is_success()
        assert "attributes" in file
        assert "width" in file["attributes"]
        assert "height" in file["attributes"]

        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.is_success()
        assert file.headers.get("content-type") == "image/gif"
        assert img == file.content

        proof = self.client.records().get("Attachments", rid)
        assert proof.is_success()
        assert proof["one_file"]["mediaType"] == meta["mediaType"]
        assert proof["one_file"]["name"] == ""

    def test_get_file(self):
        obj = utils.get_file("archives/assets.zip")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": obj,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.is_success()

        raw = utils.get_file_content(utils.get_file_name("archives/assets.zip"))
        assert raw == file.content

    def test_put_file_to_overwrite(self):
        img_1 = utils.get_file("images/01.gif")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": img_1,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()
        rid = r["id"]

        first = self.client.files().get("Attachments", rid, "one_file")
        assert first.is_success()
        raw = utils.get_file_content(utils.get_file_name("images/01.gif"))
        assert raw == first.content

        img_2 = utils.get_file_content(utils.get_file_name("images/02.gif"))
        file = self.client.files().put("Attachments", rid, "one_file", img_2)
        assert file.is_success()

        second = self.client.files().get("Attachments", rid, "one_file")
        assert second.is_success()
        assert second.content != first.content

    def test_delete_file(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif"),
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.is_success()
        assert "attributes" in file
        assert "mediaType" in file
        assert "name" in file
        assert payload["one_file"]["mediaType"] == file["mediaType"]
        assert payload["one_file"]["name"] == file["name"]

        proof = self.client.records().get("Attachments", rid)
        assert proof.is_success()
        assert "one_file" not in proof
        # null columns are not part of the response set

    def test_delete_image_response_with_attributes(self):
        img = utils.get_file("images/02.gif")
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": img,
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        file = self.client.files().delete("Attachments", rid, "one_file")
        assert file.is_success()
        assert "attributes" in file
        assert "height" in file["attributes"]
        assert "width" in file["attributes"]
