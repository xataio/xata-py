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

from xata.client import XataClient


class TestApiResponse(object):
    def test_is_success_true(self):
        user = XataClient().users().get()
        assert user.is_success()
        assert user.status_code >= 200
        assert user.status_code < 300

    def test_is_success_false_with_unknown_table(self):
        user = XataClient().records().get("Nope", "nope^2")
        assert not user.is_success()
        assert user.status_code == 404

    def test_direct_repr_and_json_are_the_same(self):
        user = XataClient().users().get()
        assert user.is_success()
        assert user == user.json()

        