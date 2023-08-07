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
# Other
# other
# Specification: core:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse

class Other(ApiRequest):

    scope = "core"

    def deleteOAuthAccessToken(self, token: str) -> ApiResponse:
       """
       Expires the access token for a third party app

       Reference: https://xata.io/docs/api-reference/user/oauth/tokens/token#delete-an-access-token-for-a-third-party-app
       Path: /user/oauth/tokens/{token}
       Method: DELETE
       Response status codes:
       - 204: No Content
       - 400: Bad Request
       - 401: Authentication Error
       - 404: Example response
       - 409: Example response
       - 5XX: Unexpected Error

       :param token: str 

       :returns ApiResponse
       """
       url_path = f"/user/oauth/tokens/{token}"
       return self.request("DELETE", url_path)

    def updateOAuthAccessToken(self, token: str, payload: dict) -> ApiResponse:
       """
       Updates partially the access token for a third party app

       Reference: https://xata.io/docs/api-reference/user/oauth/tokens/token#updates-an-access-token-for-a-third-party-app
       Path: /user/oauth/tokens/{token}
       Method: PATCH
       Response status codes:
       - 200: OK
       - 400: Bad Request
       - 401: Authentication Error
       - 404: Example response
       - 409: Example response
       - 5XX: Unexpected Error
       Response: application/json

       :param token: str 
       :param payload: dict content

       :returns ApiResponse
       """
       url_path = f"/user/oauth/tokens/{token}"
       headers = {"content-type": "application/json"}
       return self.request("PATCH", url_path, headers, payload)
