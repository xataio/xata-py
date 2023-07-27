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


class TestApiRequestInternals(unittest.TestCase):
    def test_get_scope(self):
        client = XataClient(db_url="https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042")

        assert "core" == client.databases().get_scope()
        assert "workspace" == client.table().get_scope()

    def test_is_control_plane(self):
        client = XataClient(db_url="https://py-sdk-unit-test-12345.eu-west-1.xata.sh/db/testopia-042")

        assert client.databases().is_control_plane()
        assert not client.table().is_control_plane()

    def test_get_base_url(self):
        c = XataClient(workspace_id="testopia-ab2", domain_core="custom.name", domain_workspace="sub.subsub.name.lol")

        assert "https://custom.name" == c.authentication().get_base_url()

        expected = "https://testopia-ab2.%s.sub.subsub.name.lol" % DEFAULT_REGION
        assert expected == c.branch().get_base_url()
