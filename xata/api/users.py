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
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Users(ApiRequest):

    scope = "core"

    def get(self) -> ApiResponse:
        """
        Return details of the user making the request

        Reference: https://xata.io/docs/api-reference/user#get-user-details
        Path: /user
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json


        :returns ApiResponse
        """
        url_path = "/user"
        return self.request("GET", url_path)

    def update(self, payload: dict) -> ApiResponse:
        """
        Update user info

        Reference: https://xata.io/docs/api-reference/user#update-user-info
        Path: /user
        Method: PUT
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param payload: dict content

        :returns ApiResponse
        """
        url_path = "/user"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def delete(self) -> ApiResponse:
        """
        Delete the user making the request

        Reference: https://xata.io/docs/api-reference/user#delete-user
        Path: /user
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error


        :returns ApiResponse
        """
        url_path = "/user"
        return self.request("DELETE", url_path)
