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

import pytest
import utils
from faker import Faker

from xata.client import XataClient
from PIL import Image



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

    def test_rotate_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        meta = utils.get_file("images/01.gif", public_url=True)
        img = utils.get_file_content(utils.get_file_name("images/01.gif"))
        file = self.client.files().put("Attachments", r["id"], "one_file", img, meta["mediaType"])
        assert file.is_success()

        # TODO get image it
        assert file == ""

        rot_180 = self.client.files().transform(file["one_file"]["id"], {"rotate": 180})
        assert img != rot_180

        proof_rot_180 = Image.open(utils.get_file_name("images/01.gif")).rotate(180)
        assert rot_180 == proof_rot_180

    def test_unknown_operations(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()
        rid = r["id"]
        meta = utils.get_file("images/01.gif", public_url=True)
        img = utils.get_file_content(utils.get_file_name("images/01.gif"))
        assert self.client.files().put("Attachments", rid, "one_file", img, meta["mediaType"]).is_success()

        with pytest.raises(Exception) as e:
            self.client.files().transform(r["id"], {})
        
        with pytest.raises(Exception) as e:
            self.client.files().transform(r["id"], {"donkey": "kong"})

    def test_unknown_image_id(self):
        # must fail with a 403
        with pytest.raises(Exception) as e:
            self.client.files().transform("lalala", {"rotate": 90})
