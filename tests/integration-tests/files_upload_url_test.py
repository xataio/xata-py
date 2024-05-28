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

import logging

from requests import request
from xata.client import XataClient

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

class TestFilesSingleFile(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.db_name = "Testing-DC-from-Philip"

        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.fake = utils.get_faker()

        """
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
        """

    def teardown_class(self):
        #assert self.client.databases().delete(self.db_name).is_success()
        pass

    def test_upload_file(self):
        payload = {"title": self.fake.catch_phrase()}
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()

        rid = r["id"]
        gif = utils.get_file_content(utils.get_file_name("images/01.gif"))

        resp = self.client.files().put("Attachments", rid, "one_file", gif)
        assert resp.status_code == ""
        assert resp.is_success()

        record = self.client.records().get("Attachments", rid, columns=["one_file.*", "one_file.uploadUrl"])
        assert record.is_success()
        assert "uploadUrl" in record["one_file"]

        # upload
        url = record["one_file"]["uploadUrl"]
        bin = utils.get_file_content(utils.get_file_name("images/02.gif"))
        resp = request("PUT", url, headers={"content-type": "image/gif"}, data=bin)
        assert resp.status_code == 201, resp.json()

        # validate
        file = self.client.files().get("Attachments", rid, "one_file")
        assert file.is_success()
        assert file.headers.get("content-type") == "image/gif"
        assert bin == file.content
        assert bin != gif
