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

from xata.client import XataClient


class TestClass(object):
    """
    POST /db/{db_branch_name}/tables/{table_name}/ask
    """

    def setup_class(self):
        self.db_name = utils.get_db_name()
        self.branch_name = "main"
        self.client = XataClient(db_name=self.db_name, branch_name=self.branch_name)
        self.client.set_header("X-Xata-Ask-Enabled", "true")

        # create database
        r = self.client.databases().createDatabase(
            self.client.get_config()["workspaceId"],
            self.db_name,
            {
                "region": self.client.get_config()["region"],
                "branchName": self.client.get_config()["branchName"],
            },
        )
        assert r.status_code == 201

        # create table posts
        r = self.client.table().createTable(
            "Posts", db_name=self.db_name, branch_name=self.branch_name
        )
        assert r.status_code == 201

    def teardown_class(self):
        r = self.client.databases().deleteDatabase(
            self.client.get_config()["workspaceId"], self.db_name
        )
        assert r.status_code == 200

    def test_ask_table_for_response_shape_and_empty_response(self):
        # Use one call to 
        # - assert response shape
        # - empty results
        # - default response content-type
        payload = {
            "question": "how much is the fish?",
        }
        r = self.client.search_and_filter().askTable(
            "Posts", 
            payload, 
            db_name=self.db_name, 
            branch_name=self.branch_name,
        )
        assert r.status_code == 200
        assert "answer" in r.json()
        assert r.json()["answer"] == "No records found! I'm not able to help you sorry."
        assert "content-type" in r.headers
        assert r.headers["content-type"] == "application/json; charset=UTF-8"

    def test_ask_table_with_streaming_response_content_type(self):
        payload = {
            "question": "how much is the fish?",
        }
        r = self.client.search_and_filter().askTable(
            "Posts", 
            payload, 
            response_content_type="text/event-stream",
            db_name=self.db_name, 
            branch_name=self.branch_name,
        )
        assert r.status_code == 200
        assert "content-type" in r.headers
        #assert r.headers["content-type"] == "text/event-stream; charset=UTF-8"
        # TODO



        
