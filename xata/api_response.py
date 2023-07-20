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
from typing import Union

import deprecation
from requests import Response
from requests.exceptions import JSONDecodeError


class ApiResponse(dict):
    def __init__(self, response: Response):
        self.response = response
        self.logger = logging.getLogger(self.__class__.__name__)

        # Don't serialize an empty response
        try:
            self.update(self.response.json())
        except JSONDecodeError:
            pass

        # log server message
        if "x-xata-message" in self.headers:
            self.logger.warn(self.headers["x-xata-message"])

    def server_message(self) -> Union[str, None]:
        """
        Get the server message from the response
        :returns str | None
        """
        return self.headers["x-xata-message"] if "x-xata-message" in self.headers else None

    def is_success(self) -> bool:
        """
        Was the request successful?
        :returns bool
        """
        return 200 <= self.status_code < 300

    def get_cursor(self) -> Union[str, None]:
        """
        If the response has a cursor, return it
        :returns str or None
        """
        try:
            return self.response.json()["meta"]["page"]["cursor"]
        except Exception:
            return None

    def has_more_results(self) -> bool:
        """
        Are there more result pages available?
        :return bool
        """
        try:
            return self.response.json()["meta"]["page"].get("more", False)
        except Exception:
            return False

    @deprecation.deprecated(
        deprecated_in="1.0.0a2",
        removed_in="2.0.0",
        details="This method is obsolete as this class directly returns a dict",
    )
    def json(self) -> dict:
        """
        Legacy support for requests.Response from 0.x
        :returns dict
        """
        return self.response.json()

    @property
    def status_code(self) -> int:
        """
        Get the status code of the response
        :returns int
        """
        return self.response.status_code

    @property
    def headers(self) -> dict:
        """
        Get the response headers
        :returns dict
        """
        return self.response.headers

    @property
    def content(self) -> bytes:
        """
        For files support, to get the file content
        :returns bytes
        """
        return self.response.content
