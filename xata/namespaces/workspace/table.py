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
# Table
# Table management.
# Specification: workspace:v1.0
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Table(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope = "workspace"

    def createTable(self, db_branch_name: str, table_name: str) -> Response:
        """
        Creates a new table with the given name. Returns 422 if a table with the same name already exists.
        path: /db/{db_branch_name}/tables/{table_name}
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("PUT", url_path)

    def deleteTable(self, db_branch_name: str, table_name: str) -> Response:
        """
        Deletes the table with the given name.
        path: /db/{db_branch_name}/tables/{table_name}
        method: DELETE

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("DELETE", url_path)

    def updateTable(
        self, db_branch_name: str, table_name: str, payload: dict
    ) -> Response:
        """
               Update table. Currently there is only one update operation supported: renaming the table by providing a new name.

        In the example below, we rename a table from “users” to “people”:

        ```json
        // PATCH /db/test:main/tables/users

        {
          "name": "people"
        }
        ```
               path: /db/{db_branch_name}/tables/{table_name}
               method: PATCH

               :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
               :param table_name: str The Table name
               :param payload: dict Request Body
               :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def getTableSchema(self, db_branch_name: str, table_name: str) -> Response:
        """
        Get table schema
        path: /db/{db_branch_name}/tables/{table_name}/schema
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        return self.request("GET", url_path)

    def setTableSchema(
        self, db_branch_name: str, table_name: str, payload: dict
    ) -> Response:
        """
        Update table schema
        path: /db/{db_branch_name}/tables/{table_name}/schema
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def getTableColumns(self, db_branch_name: str, table_name: str) -> Response:
        """
               Retrieves the list of table columns and their definition. This endpoint returns the column list with object columns being reported with their
        full dot-separated path (flattened).
               path: /db/{db_branch_name}/tables/{table_name}/columns
               method: GET

               :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
               :param table_name: str The Table name
               :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        return self.request("GET", url_path)

    def addTableColumn(
        self, db_branch_name: str, table_name: str, payload: dict
    ) -> Response:
        """
               Adds a new column to the table. The body of the request should contain the column definition. In the column definition, the 'name' field should
        contain the full path separated by dots. If the parent objects do not exists, they will be automatically created. For example,
        passing `"name": "address.city"` will auto-create the `address` object if it doesn't exist.
               path: /db/{db_branch_name}/tables/{table_name}/columns
               method: POST

               :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
               :param table_name: str The Table name
               :param payload: dict Request Body
               :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def getColumn(
        self, db_branch_name: str, table_name: str, column_name: str
    ) -> Response:
        """
        Get the definition of a single column. To refer to sub-objects, the column name can contain dots. For example `address.country`.
        path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :param column_name: str The Column name
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("GET", url_path)

    def deleteColumn(
        self, db_branch_name: str, table_name: str, column_name: str
    ) -> Response:
        """
        Deletes the specified column. To refer to sub-objects, the column name can contain dots. For example `address.country`.
        path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        method: DELETE

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :param column_name: str The Column name
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("DELETE", url_path)

    def updateColumn(
        self, db_branch_name: str, table_name: str, column_name: str, payload: dict
    ) -> Response:
        """
        Update column with partial data. Can be used for renaming the column by providing a new "name" field. To refer to sub-objects, the column name can contain dots. For example `address.country`.
        path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        method: PATCH

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`.
        :param table_name: str The Table name
        :param column_name: str The Column name
        :param payload: dict Request Body
        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)
