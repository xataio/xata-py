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
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Table(Namespace):

    scope = "workspace"

    def createTable(self, table_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Creates a new table with the given name.  Returns 422 if a table with the same name
        already exists.

        Path: /db/{db_branch_name}/tables/{table_name}
        Method: PUT
        Response status codes:
        - 201: Created
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("PUT", url_path)

    def deleteTable(self, table_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Deletes the table with the given name.

        Path: /db/{db_branch_name}/tables/{table_name}
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Not Found
        - 5XX: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("DELETE", url_path)

    def updateTable(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> Response:
        """
        Update table.  Currently there is only one update operation supported: renaming the table
        by providing a new name.  In the example below, we rename a table from “users” to
        “people”:  ```json // PATCH /db/test:main/tables/users  {   "name": "people" } ```

        Path: /db/{db_branch_name}/tables/{table_name}
        Method: PATCH
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def getTableSchema(self, table_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Get table schema

        Path: /db/{db_branch_name}/tables/{table_name}/schema
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        return self.request("GET", url_path)

    def setTableSchema(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> Response:
        """
        Update table schema

        Path: /db/{db_branch_name}/tables/{table_name}/schema
        Method: PUT
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 409: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def getTableColumns(self, table_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Retrieves the list of table columns and their definition.  This endpoint returns the
        column list with object columns being reported with their full dot-separated path
        (flattened).

        Path: /db/{db_branch_name}/tables/{table_name}/columns
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        return self.request("GET", url_path)

    def addTableColumn(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> Response:
        """
        Adds a new column to the table.  The body of the request should contain the column
        definition.  In the column definition, the 'name' field should contain the full path
        separated by dots.  If the parent objects do not exists, they will be automatically
        created.  For example, passing `"name": "address.city"` will auto-create the `address`
        object if it doesn't exist.

        Path: /db/{db_branch_name}/tables/{table_name}/columns
        Method: POST
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def getColumn(self, table_name: str, column_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Get the definition of a single column.  To refer to sub-objects, the column name can
        contain dots.  For example `address.country`.

        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("GET", url_path)

    def deleteColumn(self, table_name: str, column_name: str, db_name: str = None, branch_name: str = None) -> Response:
        """
        Deletes the specified column.  To refer to sub-objects, the column name can contain dots.
        For example `address.country`.

        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: DELETE
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("DELETE", url_path)

    def updateColumn(
        self, table_name: str, column_name: str, payload: dict, db_name: str = None, branch_name: str = None
    ) -> Response:
        """
        Update column with partial data.  Can be used for renaming the column by providing a new
        "name" field.  To refer to sub-objects, the column name can contain dots.  For example
        `address.country`.

        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: PATCH
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)
