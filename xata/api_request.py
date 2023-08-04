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

import logging

from requests import Session, request

from xata.api_response import ApiResponse

from .errors import RateLimitError, UnauthorizedError, XataServerError


class ApiRequest:
    def __init__(self, client):
        self.session = Session()
        self.client = client
        self.logger = logging.getLogger(self.__class__.__name__)

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

    def request(
        self,
        http_method: str,
        url_path: str,
        headers: dict = {},
        payload: dict = None,
        data: bytes = None,
        is_streaming: bool = False,
    ) -> ApiResponse:
        """
        :param http_method: str
        :param url_path: str
        :headers: dict = {}
        :param payload: dict = None
        :param data: bytes = None
        :param is_streaming: bool = False

        :returns ApiResponse

        :raises RateLimitError
        :raises UnauthorizedError
        :raises ServerError
        """
        headers = {**headers, **self.client.get_headers()}
        url = "%s/%s" % (self.get_base_url(), url_path.lstrip("/"))

        # In order not exhaust the connection pool with open connections from unread streams
        # we opt for Session usage on all non-stream requests
        # https://requests.readthedocs.io/en/latest/user/advanced/#body-content-workflow
        if is_streaming:
            if payload is None and data is None:
                resp = request(http_method, url, headers=headers, stream=True)
            elif data is not None:
                resp = request(http_method, url, headers=headers, data=data, stream=True)
            else:
                resp = request(http_method, url, headers=headers, json=payload, stream=True)
        else:
            if payload is None and data is None:
                resp = self.session.request(http_method, url, headers=headers)
            elif data is not None:
                resp = self.session.request(http_method, url, headers=headers, data=data)
            else:
                resp = self.session.request(http_method, url, headers=headers, json=payload)

        # Any special status code we can raise an exception for ?
        if resp.status_code == 429:
            raise RateLimitError(f"code: {resp.status_code}, rate limited: {resp.json()}")
        if resp.status_code == 401:
            raise UnauthorizedError(f"code: {resp.status_code}, unauthorized: {resp.json()}")
        elif resp.status_code >= 500:
            raise XataServerError(f"code: {resp.status_code}, server error: {resp.text}")

        return ApiResponse(resp)
