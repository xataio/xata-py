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

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Table(ApiRequest):

    scope = "workspace"

    def create(self, table_name: str, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Creates a new table with the given name. Returns 422 if a table with the same name already exists.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name#create-table
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
        - default: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("PUT", url_path)

    def delete(self, table_name: str, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Deletes the table with the given name.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name#delete-table
        Path: /db/{db_branch_name}/tables/{table_name}
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Not Found
        - 5XX: Unexpected Error
        - default: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        return self.request("DELETE", url_path)

    def update(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Update table. Currently there is only one update operation supported: renaming the table by providing a new name.

        In the example below, we rename a table from “users” to “people”:

        ```json
        // PATCH /db/test:main/tables/users

        {
          "name": "people"
        }
        ```

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name#update-table
        Path: /db/{db_branch_name}/tables/{table_name}
        Method: PATCH
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def get_schema(self, table_name: str, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Get table schema

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/schema#get-table-schema
        Path: /db/{db_branch_name}/tables/{table_name}/schema
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        return self.request("GET", url_path)

    def set_schema(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Update table schema

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/schema#update-table-schema
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
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/schema"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def get_columns(self, table_name: str, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Retrieves the list of table columns and their definition. This endpoint returns the column list with object columns being reported with their
        full dot-separated path (flattened).

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/columns#list-table-columns
        Path: /db/{db_branch_name}/tables/{table_name}/columns
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        return self.request("GET", url_path)

    def add_column(self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Adds a new column to the table. The body of the request should contain the column definition.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/columns#create-new-column
        Path: /db/{db_branch_name}/tables/{table_name}/columns
        Method: POST
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def get_column(
        self, table_name: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
        """
        Get the definition of a single column.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/columns/column_name#get-column-information
        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error
        Response: application/json

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("GET", url_path)

    def delete_column(
        self, table_name: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
        """
        Deletes the specified column.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/columns/column_name#delete-column
        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: DELETE
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        return self.request("DELETE", url_path)

    def update_column(
        self, table_name: str, column_name: str, payload: dict, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
        """
        Update column with partial data. Can be used for renaming the column by providing a new "name" field.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/columns/column_name#update-column
        Path: /db/{db_branch_name}/tables/{table_name}/columns/{column_name}
        Method: PATCH
        Response status codes:
        - 200: Schema migration response with ID and migration status.
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param column_name: str The Column name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/columns/{column_name}"
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)
