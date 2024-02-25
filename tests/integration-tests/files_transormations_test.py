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
import json

import pytest
import utils
from faker import Faker

from xata.client import XataClient

# from PIL import Image, ImageChops


class TestFilesTransformations(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.client = XataClient(db_name=self.db_name)
        self.fake = Faker()

        assert self.client.databases().create(self.db_name).is_success()
        assert self.client.table().create("Attachments").is_success()
        assert (
            self.client.table()
            .set_schema(
                "Attachments",
                utils.get_attachments_schema(),
                db_name=self.db_name,
            )
            .is_success()
        )

    def teardown_class(self):
        assert self.client.databases().delete(self.db_name).is_success()

    def test_rotate_public_file(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True),
        }
        upload = self.client.records().insert("Attachments", payload, columns=["one_file.url"])
        assert upload.is_success()

        img = utils.get_file_content(utils.get_file_name("images/03.png"))
        rot_180 = self.client.files().transform(upload["one_file"]["url"], {"rotate": 180})
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
        upload = self.client.records().insert("Attachments", payload, columns=["one_file.signedUrl"])
        assert upload.is_success()

        img = utils.get_file_content(utils.get_file_name("images/03.png"))
        rot_180 = self.client.files().transform(upload["one_file"]["signedUrl"], {"rotate": 180})
        assert img != rot_180

        # rot_180_pil = Image.open(io.BytesIO(rot_180))
        # proof_rot_180 = Image.open(utils.get_file_name("images/03.png")).rotate(180)
        # assert rot_180_pil == proof_rot_180

    def test_with_nested_operations(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True, signed_url_timeout=120),
        }
        upload = self.client.records().insert("Attachments", payload, columns=["one_file.url"])
        assert upload.is_success()

        # img = utils.get_file_content(utils.get_file_name("images/03.png"))
        self.client.files().transform(
            upload["one_file"]["url"],
            {"rotate": 180, "blur": 50, "trim": {"top": 20, "right": 30, "bottom": 20, "left": 0}},
        )

        # rot_180_pil = Image.open(io.BytesIO(rot_180))
        # proof_rot_180 = Image.open(utils.get_file_name("images/03.png")).rotate(180)
        # assert rot_180_pil == proof_rot_180

    def test_format_json_operations(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True, signed_url_timeout=120),
        }
        upload = self.client.records().insert("Attachments", payload, columns=["one_file.url"])
        assert upload.is_success()

        resp = self.client.files().transform(
            upload["one_file"]["url"],
            {"dpr": 2, "height": 200, "fit": "contain", "background": "pink", "format": "json"},
        )
        resp = json.load(io.BytesIO(resp))
        assert resp == {
            "height": 400,
            "original": {"file_size": 72786, "format": "image/png", "height": 1646, "width": 1504},
            "width": 365,
        }

    def test_unknown_operations(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/03.png", public_url=True),
        }
        upload = self.client.records().insert("Attachments", payload, columns=["one_file.url"])
        assert upload.is_success()

        with pytest.raises(Exception):
            self.client.files().transform(upload["one_file"]["url"], {})

        with pytest.raises(Exception):
            self.client.files().transform(upload["one_file"]["url"], {"donkey": "kong"})

    def test_unknown_image_id(self):
        # must fail with a 403
        with pytest.raises(Exception):
            self.client.files().transform("https://us-east-1.storage.xata.sh/lalala", {"rotate": 90})

    def test_invalid_url(self):
        # must fail with a 403
        with pytest.raises(Exception):
            self.client.files().transform("https:/xata.sh/oh-hello", {"rotate": 90})
