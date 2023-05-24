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
import random
import base64
import utils
from faker import Faker

from xata.client import XataClient


class TestRecordsFileOperations(object):
    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        # TODO remove staging
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name, domain_core="api.staging-xata.dev", domain_workspace="staging-xata.dev")
        self.fake = Faker()
        self.record_id = utils.get_random_string(24)

        # create database
        r = self.client.databases().create(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().create("Attachments")
        assert r.status_code == 201

        # create schema
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
        #pass

    def _get_file(self, publicUrl: bool = True) -> dict:
        cat = 'image'
        file_name = self.fake.file_path(depth=random.randint(0, 7), category=cat)
        file_content = self.fake.image(size=(random.randint(10, 512), random.randint(10, 512)))
        encoded_string = base64.b64encode(file_content).decode('ascii')
        return {
            "name": file_name.replace("/", "_"),
            "mediaType": self.fake.mime_type(category=cat),
            "base64Content": encoded_string,
            "enablePublicUrl": publicUrl,
            #"signedURLTimeout": 120, # 2 min
        }

    def test_insert_record_with_files_and_read_it(self):
        """
        POST /db/{db_branch_name}/tables/{table_name}/data
        """
        payload = {
            "title": self.fake.catch_phrase(),
            "one_file": self._get_file(),
            "many_files": [
                self._get_file(),
                self._get_file(),
                self._get_file(),
            ],
        }

        r = self.client.records().insert("Attachments", payload)
        assert r.status_code == 201
        assert "id" in r.json()

        r = self.client.records().get("Attachments", r.json()["id"])
        assert r.status_code == 200
        record = r.json()
        assert "id" not in record["one_file"]
        assert len(record["many_files"]) == len(payload["many_files"])
        assert "id" in record["many_files"][0]
        assert "id" in record["many_files"][1]
        assert "id" in record["many_files"][2]

        assert "name" in record["one_file"]
        assert "mediaType" in record["one_file"]
        assert "name" in record["many_files"][0]
        assert "mediaType" in record["many_files"][0]

        assert record["title"] == payload["title"]
        assert len(list(record["one_file"].keys())) == 2
        assert len(list(record["many_files"][0].keys())) == 3

