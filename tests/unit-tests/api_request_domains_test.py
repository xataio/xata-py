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

import unittest

from xata.client import DEFAULT_REGION, XataClient


class TestApiRequestCustomDomains(unittest.TestCase):
    def test_core_domain(self):
        domain = "api.hallo.hey"
        client = XataClient(
            api_key="123", db_url="https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042", domain_core=domain
        )
        assert "https://" + domain == client.databases().get_base_url()

    def test_workspace_domain(self):
        domain = "hello.is.it.me-you-are-looking.for"
        ws_id = "testopia-042"
        client = XataClient(workspace_id=ws_id, domain_workspace=domain, api_key="123")

        expected = "https://%s.%s.%s" % (ws_id, DEFAULT_REGION, domain)
        assert expected == client.table().get_base_url()

    def test_upload_base_url(self):
        client = XataClient(api_key="123", db_url="https://12345.region-42.staging-xata.dev/db/testopia-042")
        assert "https://12345.region-42.upload.staging-xata.dev" == client.databases().get_upload_base_url()
