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

import os
import unittest

from xata.client import DEFAULT_CONTROL_PLANE_DOMAIN, DEFAULT_DATA_PLANE_DOMAIN, XataClient


class TestClientWithCustomDomains(unittest.TestCase):

    def test_control_plane_param_init(self):
        domain = "a-control-plane.lol"
        client = XataClient(domain_core=domain)
        assert domain == client.get_config()["domain_core"]
        assert DEFAULT_DATA_PLANE_DOMAIN == client.get_config()["domain_workspace"]

    def test_data_plane_param_init(self):
        domain = "super-custom-domain.com"
        client = XataClient(domain_workspace=domain)
        assert domain == client.get_config()["domain_workspace"]
        assert DEFAULT_CONTROL_PLANE_DOMAIN == client.get_config()["domain_core"]

    def test_data_plane_db_url_param_init(self):
        db_url = "https://py-sdk-abc1234.my-region.super-custom-domain.com/db/custom-01"
        client = XataClient(db_url=db_url)
        assert "super-custom-domain.com" == client.get_config()["domain_workspace"]

    def test_data_plane_init_with_db_url_envar(self):
        os.environ["XATA_DATABASE_URL"] = "https://py-sdk-abc1234.my-region.envvar-domain.io/db/custom-01"
        client = XataClient()
        assert "envvar-domain.io" == client.get_config()["domain_workspace"]
        os.environ.pop("XATA_DATABASE_URL")

    def test_data_plane_db_url_complex_ones(self):
        db_url = "https://py-sdk-abc1234.my-region.super.custom-domain.co.at/db/custom-01"
        client = XataClient(db_url=db_url)
        assert "super.custom-domain.co.at" == client.get_config()["domain_workspace"]
