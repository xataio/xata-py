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

from requests import Response, request

from .errors import RateLimitException


class Namespace:
    """
    Parent class for Namespaces
    """

    def __init__(self, client):
        self.client = client

    def get_scope(self) -> str:
        return self.scope

    def is_control_plane(self) -> bool:
        return self.get_scope() == "core"

    def get_base_url(self) -> str:
        if self.is_control_plane():
            return "https://" + self.client.get_config()["domain_core"]
        # Base URL must be build on the fly as the region & workspace Id can change
        cfg = self.client.get_config()
        return "https://%s.%s.%s" % (cfg["workspaceId"], cfg["region"], cfg["domain_workspace"])

    def request(self, http_method: str, url_path: str, headers: dict = {}, payload: dict = None) -> Response:
        headers = {
            **headers,
            **self.client.get_headers(),
        }  # TODO use "|" when client py min version >= 3.9
        url = "%s/%s" % (self.get_base_url(), url_path.lstrip("/"))
        if payload is None:
            resp = request(http_method, url, headers=headers)
        else:
            resp = request(http_method, url, headers=headers, json=payload)

        if resp.status_code == 429:
            raise RateLimitException(f"Rate limited: {resp.json()}")

        return resp
