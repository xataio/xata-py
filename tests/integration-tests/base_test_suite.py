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

import base64
import random

import utils
from faker import Faker

from xata.client import XataClient


class BaseTestSuite(object):
    def setup_class(self, is_staging: bool = False):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.fake = Faker()

        if is_staging:
            self.client = XataClient(
                db_name=self.db_name,
                branch_name=self.branch_name,
                domain_core="api.staging-xata.dev",
                domain_workspace="staging-xata.dev",
            )
        else:
            self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)

    def create_database(self):
        r = self.client.databases().create(
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

    def create_table(self, table_name: str, schema: dict):
        r = self.client.table().create(table_name)
        assert r.status_code == 201, r.json()
        r = self.client.table().setSchema(table_name, schema, db_name=self.db_name, branch_name=self.branch_name)
        assert r.status_code == 200, r.json()

    def get_record_id(self) -> str:
        return utils.get_random_string(24)

    def get_file_record(self, publicUrl: bool = True) -> dict:
        cat = "image"
        file_name = self.fake.file_path(depth=random.randint(0, 7), category=cat)
        file_content = self.fake.image(size=(random.randint(10, 512), random.randint(10, 512)))
        encoded_string = base64.b64encode(file_content).decode("ascii")
        return {
            "name": file_name.replace("/", "_"),
            "mediaType": self.fake.mime_type(category=cat),
            "base64Content": encoded_string,
            "enablePublicUrl": publicUrl,
        }
