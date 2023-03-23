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
# Workspaces
# Workspaces management
# Specification: core:v1.0
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Workspaces(Namespace):

    scope = "core"

    def getWorkspacesList(
        self,
    ) -> Response:
        """
        Retrieve the list of workspaces the user belongs to

        Path: /workspaces
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json


        :return Response
        """
        url_path = "/workspaces"
        return self.request("GET", url_path)

    def createWorkspace(self, payload: dict) -> Response:
        """
        Creates a new workspace with the user requesting it as its single owner.

        Path: /workspaces
        Method: POST
        Response status codes:
        - 201: Created
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param payload: dict content

        :return Response
        """
        url_path = "/workspaces"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def getWorkspace(self, workspace_id: str = None) -> Response:
        """
        Retrieve workspace info from a workspace ID

        Path: /workspaces/{workspace_id}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}"
        return self.request("GET", url_path)

    def updateWorkspace(self, payload: dict, workspace_id: str = None) -> Response:
        """
        Update workspace info

        Path: /workspaces/{workspace_id}
        Method: PUT
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def deleteWorkspace(self, workspace_id: str = None) -> Response:
        """
        Delete the workspace with the provided ID

        Path: /workspaces/{workspace_id}
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}"
        return self.request("DELETE", url_path)

    def getWorkspaceMembersList(self, workspace_id: str = None) -> Response:
        """
        Retrieve the list of members of the given workspace

        Path: /workspaces/{workspace_id}/members
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/members"
        return self.request("GET", url_path)

    def updateWorkspaceMemberRole(self, user_id: str, payload: dict, workspace_id: str = None) -> Response:
        """
        Update a workspace member role.  Workspaces must always have at least one owner, so this
        operation will fail if trying to remove owner role from the last owner in the workspace.

        Path: /workspaces/{workspace_id}/members/{user_id}
        Method: PUT
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param user_id: str UserID
        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/members/{user_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def removeWorkspaceMember(self, user_id: str, workspace_id: str = None) -> Response:
        """
        Remove the member from the workspace

        Path: /workspaces/{workspace_id}/members/{user_id}
        Method: DELETE
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 403: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param user_id: str UserID
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/members/{user_id}"
        return self.request("DELETE", url_path)
