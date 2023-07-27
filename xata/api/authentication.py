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
# Authentication
# Authentication and API Key management.
# Specification: core:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Authentication(ApiRequest):

    scope = "core"

    def get_user_api_keys(self) -> ApiResponse:
        """
        Retrieve a list of existing user API keys

        Reference: https://xata.io/docs/api-reference/user/keys#get-the-list-of-user-api-keys
        Path: /user/keys
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
        url_path = "/user/keys"
        return self.request("GET", url_path)

    def create_user_api_keys(self, key_name: str) -> ApiResponse:
        """
        Create and return new API key

        Reference: https://xata.io/docs/api-reference/user/keys/key_name#create-and-return-new-api-key
        Path: /user/keys/{key_name}
        Method: POST
        Response status codes:
        - 201: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param key_name: str API Key name

        :returns ApiResponse
        """
        url_path = f"/user/keys/{key_name}"
        return self.request("POST", url_path)

    def delete_user_api_keys(self, key_name: str) -> ApiResponse:
        """
        Delete an existing API key

        Reference: https://xata.io/docs/api-reference/user/keys/key_name#delete-an-existing-api-key
        Path: /user/keys/{key_name}
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param key_name: str API Key name

        :returns ApiResponse
        """
        url_path = f"/user/keys/{key_name}"
        return self.request("DELETE", url_path)
