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


class TestNamespace(object):
    def test_direct_instance_in_namespace_without_client_dependencies(self):
        """
        Direct namespace invocation without implicit config
        :link https://github.com/xataio/xata-py/issues/57
        """
        users = XataClient().users()
        r1 = users.get()
        assert r1.is_success()

        client = XataClient()
        r2 = client.users().get()
        assert r2.is_success()

        assert r1 == r2

    def test_direct_instance_in_namespace_wit_client_dependencies(self):
        """
        Direct namespace invocation with dependency on internal client config
        :link https://github.com/xataio/xata-py/issues/57
        """
        db_name = utils.get_db_name()
        databases = XataClient().databases()

        # workspace_id should be provided by the the client implicitly
        assert databases.create(db_name, {"region": "eu-west-1"}).is_success()
        assert databases.get_metadata(db_name).is_success()
        assert databases.delete(db_name).is_success()
