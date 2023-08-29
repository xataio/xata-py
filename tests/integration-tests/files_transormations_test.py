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

import io

import pytest
import utils
from faker import Faker
from PIL import Image, ImageChops

from xata.client import XataClient


class TestFilesTransformations(object):
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

    def test_rotate_public_file(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True),
        }
        upload = self.client.records().insertRecord("Attachments", payload, columns=["one_file.url"])
        assert upload.status_code == 201

        img = utils.get_file_content(utils.get_file_name("images/03.png"))
        rot_180 = self.client.files().transform(upload.json()["one_file"]["url"], {"rotate": 180})
        assert img != rot_180

        # proof_rot_180 = Image.open(utils.get_file_name("images/03.png")).rotate(180)
        # rot_180_pil = Image.open(io.BytesIO(rot_180))
        # diff = ImageChops.difference(proof_rot_180, rot_180_pil)
        # assert diff.getbbox()

    def test_rotate_file_with_signed_url(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=False),
        }
        upload = self.client.records().insertRecord("Attachments", payload, columns=["one_file.signedUrl"])
        assert upload.status_code == 201

        img = utils.get_file_content(utils.get_file_name("images/03.png"))
        rot_180 = self.client.files().transform(upload.json()["one_file"]["signedUrl"], {"rotate": 180})
        assert img != rot_180

        # rot_180_pil = Image.open(io.BytesIO(rot_180))
        # proof_rot_180 = Image.open(utils.get_file_name("images/03.png")).rotate(180)
        # assert rot_180_pil == proof_rot_180

    def test_with_nested_operations(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True, signed_url_timeout=120),
        }
        upload = self.client.records().insertRecord("Attachments", payload, columns=["one_file.url"])
        assert upload.status_code == 201

        img = utils.get_file_content(utils.get_file_name("images/03.png"))
        self.client.files().transform(
            upload.json()["one_file"]["url"],
            {"rotate": 180, "blur": 50, "trim": {"top": 20, "right": 30, "bottom": 20, "left": 0}},
        )

        # rot_180_pil = Image.open(io.BytesIO(rot_180))
        # proof_rot_180 = Image.open(utils.get_file_name("images/03.png")).rotate(180)
        # assert rot_180_pil == proof_rot_180

    def test_unknown_operations(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True),
        }
        upload = self.client.records().insertRecord("Attachments", payload, columns=["one_file.url"])
        assert upload.status_code == 201

        with pytest.raises(Exception) as e:
            self.client.files().transform(upload.json()["one_file"]["url"], {})

        with pytest.raises(Exception) as e:
            self.client.files().transform(upload.json()["one_file"]["url"], {"donkey": "kong"})

    def test_unknown_image_id(self):
        # must fail with a 403
        with pytest.raises(Exception) as e:
            self.client.files().transform("https://us-east-1.storage.xata.sh/lalala", {"rotate": 90})

    def test_invalid_url(self):
        # must fail with a 403
        with pytest.raises(Exception) as e:
            self.client.files().transform("https:/xata.sh/oh-hello", {"rotate": 90})
