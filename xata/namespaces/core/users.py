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

# ------------------------------------------------------- #
# Users
# Users management
# Specification: core:v1.0
# Base URL: https://api.xata.io
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Users(Namespace):

    base_url = "https://api.xata.io"
    scope = "core"

    def getUser(
        self,
    ) -> Response:
        """
        Return details of the user making the request
        path: /user
        method: GET

        :return Response
        """
        url_path = "/user"
        return self.request("GET", url_path)

    def updateUser(self, payload: dict) -> Response:
        """
        Update user info
        path: /user
        method: PUT

        :param payload: dict Request Body
        :return Response
        """
        url_path = "/user"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def deleteUser(
        self,
    ) -> Response:
        """
        Delete the user making the request
        path: /user
        method: DELETE

        :return Response
        """
        url_path = "/user"
        return self.request("DELETE", url_path)
