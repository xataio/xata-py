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
# Databases
# Workspace databases management.
# Specification: core:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Databases(ApiRequest):

    scope = "core"

    def list(self, workspace_id: str = None) -> ApiResponse:
        """
        List all databases available in your Workspace.

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs#list-databases
        Path: /workspaces/{workspace_id}/dbs
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/dbs"
        return self.request("GET", url_path)

    def get_metadata(self, db_name: str, workspace_id: str = None) -> ApiResponse:
        """
        Retrieve metadata of the given database

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs/db_name#get-database-metadata
        Path: /workspaces/{workspace_id}/dbs/{db_name}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        return self.request("GET", url_path)

    def create(
        self, db_name: str, workspace_id: str = None, region: str = None, branch_name: str = None
    ) -> ApiResponse:
        """
        Create Database with identifier name

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs/db_name#create-database
        Path: /workspaces/{workspace_id}/dbs/{db_name}
        Method: PUT
        Response status codes:
        - 201: Created
        - 400: Bad Request
        - 401: Authentication Error
        - 422: Example response
        - 423: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.
        :param region: str = None Which region to deploy. Default: region defined in the client, if not specified: us-east-1
        :param branch_name: str = None Which branch to create. Default: branch name used from the client, if not speicifed: main

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        payload = {
            "region": region if region else self.client.get_region(),
            "branchName": branch_name if branch_name else self.client.get_branch_name(),
        }
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def delete(self, db_name: str, workspace_id: str = None) -> ApiResponse:
        """
        Delete a database and all of its branches and tables permanently.

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs/db_name#delete-database
        Path: /workspaces/{workspace_id}/dbs/{db_name}
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        return self.request("DELETE", url_path)

    def update_metadata(self, db_name: str, payload: dict, workspace_id: str = None) -> ApiResponse:
        """
        Update the color of the selected database

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs/db_name#update-database-metadata
        Path: /workspaces/{workspace_id}/dbs/{db_name}
        Method: PATCH
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def rename(self, db_name: str, new_name: str, workspace_id: str = None) -> ApiResponse:
        """
        Change the name of an existing database

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/dbs/db_name/rename#rename-database
        Path: /workspaces/{workspace_id}/dbs/{db_name}/rename
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 422: Example response
        - 423: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str Current database name
        :param new_name: str New database name
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        payload = {"newName": new_name}
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}/rename"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def get_regions(self, workspace_id: str = None) -> ApiResponse:
        """
        List regions available to create a database on

        Reference: https://xata.io/docs/api-reference/workspaces/workspace_id/regions#list-available-regions
        Path: /workspaces/{workspace_id}/regions
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :returns ApiResponse
        """
        if workspace_id is None:
            workspace_id = self.client.get_workspace_id()
        url_path = f"/workspaces/{workspace_id}/regions"
        return self.request("GET", url_path)
