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

import time

import utils
from requests import request

from xata.client import XataClient


class TestFilesAccess(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Files", "true")
        self.fake = utils.get_faker()

        assert (
            self.client.databases()
            .create(
                self.db_name,
                {
                    "region": self.client.get_config()["region"],
                    "branchName": self.client.get_config()["branchName"],
                },
            )
            .is_success()
        )

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

    def test_public_flag_true(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif", public_url=True),
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()
        rid = r["id"]

        file = self.client.records().get("Attachments", rid, columns=["one_file.signedUrl", "one_file.url"])
        assert file.is_success()
        assert "signedUrl" in file["one_file"]
        assert "url" in file["one_file"]

        proof_public = request("GET", file["one_file"]["url"])
        assert proof_public.status_code == 200

        proof_signed = request("GET", file["one_file"]["signedUrl"])
        assert proof_signed.status_code == 200

        img = utils.get_file_content(utils.get_file_name("images/01.gif"))
        assert img == proof_public.content
        assert img == proof_signed.content

    def test_public_flag_false(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif", public_url=False),
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()
        rid = r["id"]

        file = self.client.records().get("Attachments", rid, columns=["one_file.signedUrl", "one_file.url"])
        assert file.is_success()
        assert "signedUrl" in file["one_file"]
        assert "url" in file["one_file"]

        proof_public = request("GET", file["one_file"]["url"])
        assert proof_public.status_code == 403

        proof_signed = request("GET", file["one_file"]["signedUrl"])
        assert proof_signed.status_code == 200

        img = utils.get_file_content(utils.get_file_name("images/01.gif"))
        assert img == proof_signed.content

    def test_signed_url_expired(self):
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": utils.get_file("images/01.gif", public_url=False, signed_url_timeout=1),
        }
        r = self.client.records().insert("Attachments", payload)
        assert r.is_success()
        rid = r["id"]

        file = self.client.records().get("Attachments", rid, columns=["one_file.signedUrl"])
        assert file.is_success()
        assert "signedUrl" in file["one_file"]

        time.sleep(1)

        proof_signed = request("GET", file["one_file"]["signedUrl"])
        assert proof_signed.status_code == 403
