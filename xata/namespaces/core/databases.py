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

from requests import Response

from xata.namespace import Namespace


class Databases(Namespace):

    scope = "core"

    def getDatabaseList(self, workspace_id: str = None) -> Response:
        """
        List all databases available in your Workspace.

        Path: /workspaces/{workspace_id}/dbs
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/dbs"
        return self.request("GET", url_path)

    def getDatabaseMetadata(self, db_name: str, workspace_id: str = None) -> Response:
        """
        Retrieve metadata of the given database

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

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        return self.request("GET", url_path)

    def createDatabase(self, db_name: str, payload: dict, workspace_id: str = None) -> Response:
        """
        Create Database with identifier name

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
        :param payload: dict content
        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def deleteDatabase(self, db_name: str, workspace_id: str = None) -> Response:
        """
        Delete a database and all of its branches and tables permanently.

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

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        return self.request("DELETE", url_path)

    def updateDatabaseMetadata(self, db_name: str, payload: dict, workspace_id: str = None) -> Response:
        """
        Update the color of the selected database

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

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/dbs/{db_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def listRegions(self, workspace_id: str = None) -> Response:
        """
        List regions available to create a database on

        Path: /workspaces/{workspace_id}/regions
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param workspace_id: str = None The workspace identifier. Default: workspace Id from the client.

        :return Response
        """
        if workspace_id is None:
            workspace_id = self.client.get_config()["workspaceId"]
        url_path = f"/workspaces/{workspace_id}/regions"
        return self.request("GET", url_path)
