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

from requests import Response

from xata.namespace import Namespace


class Records(Namespace):

    scope = "workspace"

    def branchTransaction(self, payload: dict, db_name: str = None, branch_name: str = None) -> Response:
        """
        Execute a transaction on a branch

        Path: /db/{db_branch_name}/transaction
        Method: POST
        Response status codes:
        - 200: Returns the results of a successful transaction.
        - 400: Returns errors from a failed transaction.
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/transaction"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def insertRecord(
        self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> Response:
        """
        Insert a new Record into the Table

        Path: /db/{db_branch_name}/tables/{table_name}/data
        Method: POST
        Response status codes:
        - 201: Record ID and version
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def getRecord(
        self, table_name: str, record_id: str, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> Response:
        """
        Retrieve record by ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: GET
        Response status codes:
        - 200: Table Record Reponse
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("GET", url_path)

    def insertRecordWithID(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        createOnly: bool = None,
        ifVersion: int = None,
    ) -> Response:
        """
        By default, IDs are auto-generated when data is insterted into Xata.  Sending a request to
        this endpoint allows us to insert a record with a pre-existing ID, bypassing the default
        automatic ID generation.

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: PUT
        Response status codes:
        - 200: Record ID and version
        - 201: Record ID and version
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param createOnly: bool = None
        :param ifVersion: int = None

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if createOnly is not None:
            query_params.append(f"createOnly={createOnly}")
        if ifVersion is not None:
            query_params.append(f"ifVersion={ifVersion}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def upsertRecordWithID(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        ifVersion: int = None,
    ) -> Response:
        """
        Upsert record with ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: POST
        Response status codes:
        - 200: Record ID and version
        - 201: Record ID and version
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param ifVersion: int = None

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if ifVersion is not None:
            query_params.append(f"ifVersion={ifVersion}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def deleteRecord(
        self, table_name: str, record_id: str, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> Response:
        """
        Delete record from table

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: DELETE
        Response status codes:
        - 200: Table Record Reponse
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("DELETE", url_path)

    def updateRecordWithID(
        self,
        table_name: str,
        record_id: str,
        payload: dict,
        db_name: str = None,
        branch_name: str = None,
        columns: list = None,
        ifVersion: int = None,
    ) -> Response:
        """
        Update record with ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        Method: PATCH
        Response status codes:
        - 200: Record ID and version
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters
        :param ifVersion: int = None

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        query_params = []
        if columns is not None:
            query_params.append("columns=%s" % ",".join(columns))
        if ifVersion is not None:
            query_params.append(f"ifVersion={ifVersion}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        headers = {"content-type": "application/json"}
        return self.request("PATCH", url_path, headers, payload)

    def bulkInsertTableRecords(
        self, table_name: str, payload: dict, db_name: str = None, branch_name: str = None, columns: list = None
    ) -> Response:
        """
        Bulk insert records

        Path: /db/{db_branch_name}/tables/{table_name}/bulk
        Method: POST
        Response status codes:
        - 200: OK
        - 400: Response with multiple errors of the bulk execution
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param columns: list = None Column filters

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/bulk"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)
