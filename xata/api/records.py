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
# Records
# Record access API.
# Specification: workspace:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Records(ApiRequest):

    scope = "workspace"

    def transaction(self, payload: dict, db_name: str = None, branch_name: str = None) -> ApiResponse:
        """
        Execute a transaction on a branch

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/transaction#execute-a-transaction-on-a-branch
        Path: /db/{db_branch_name}/transaction
        Method: POST
        Response status codes:
        - 200: Returns the results of a successful transaction.
        - 400: Returns errors from a failed transaction.
        - 401: Authentication Error
        - 404: Example response
        - 429: Rate limit exceeded
        - 5XX: Unexpected Error
        - default: Unexpected Error
        Response: application/json

        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/transaction"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def insert(
        self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> ApiResponse:
        """
        Insert a new Record into the Table

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data#insert-record
        Path: /db/{db_branch_name}/tables/{table_name}/data
        Method: POST
        Response status codes:
        - 201: Record ID and metadata
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def get(
        self, table_name: str, record_id: str, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> ApiResponse:
        """
        Retrieve record by ID

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data/record_id#get-record-by-id
        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: GET
        Response status codes:
        - 200: Table Record Reponse
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("GET", url_path)

    def insert_with_id(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        create_only: bool = None,
        if_version: int = None,
    ) -> ApiResponse:
        """
        By default, IDs are auto-generated when data is insterted into Xata. Sending a request to this endpoint allows us to insert a record with a pre-existing ID, bypassing the default automatic ID generation.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data/record_id#insert-record-with-id
        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: PUT
        Response status codes:
        - 200: Record ID and metadata
        - 201: Record ID and metadata
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param create_only: bool = None
        :param if_version: int = None

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if create_only is not None:
            query_params.append(f"createOnly={create_only}")
        if if_version is not None:
            query_params.append(f"ifVersion={if_version}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def upsert(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        if_version: int = None,
    ) -> ApiResponse:
        """
        Upsert record with ID

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data/record_id#upsert-record-with-id
        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: POST
        Response status codes:
        - 200: Record ID and metadata
        - 201: Record ID and metadata
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param if_version: int = None

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if if_version is not None:
            query_params.append(f"ifVersion={if_version}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def delete(
        self, table_name: str, record_id: str, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> ApiResponse:
        """
        Delete record from table

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data/record_id#delete-record-from-table
        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: DELETE
        Response status codes:
        - 200: Table Record Reponse
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("DELETE", url_path)

    def update(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        if_version: int = None,
    ) -> ApiResponse:
        """
        Update record with ID

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/data/record_id#update-record-with-id
        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: PATCH
        Response status codes:
        - 200: Record ID and metadata
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param if_version: int = None

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if if_version is not None:
            query_params.append(f"ifVersion={if_version}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def bulk_insert(
        self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> ApiResponse:
        """
        Bulk insert records

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/tables/table_name/bulk#bulk-insert-records
        Path: /db/{db_branch_name}/tables/{table_name}/bulk
        Method: POST
        Status: Experimental
        Response status codes:
        - 200: OK
        - 400: Response with multiple errors of the bulk execution
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/bulk"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)
