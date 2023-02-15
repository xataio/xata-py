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
# Base URL: https://{workspaceId}.{regionId}.xata.sh
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Records(Namespace):

    base_url = "https://{workspaceId}.{regionId}.xata.sh"
    scope = "workspace"

    def branchTransaction(self, db_branch_name: str, payload: dict) -> Response:
        """
        Execute a transaction on a branch
        path: /db/{db_branch_name}/transaction
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]

        :return Response
        """
        url_path = f"/db/{db_branch_name}/transaction"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def insertRecord(
        self, db_branch_name: str, table_name: str, payload: dict, columns: list = None
    ) -> Response:
        """
        Insert a new Record into the Table
        path: /db/{db_branch_name}/tables/{table_name}/data
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]
        :param columns: list = None Column filters [in: query, req: False]

        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def getRecord(
        self, db_branch_name: str, table_name: str, record_id: str, columns: list = None
    ) -> Response:
        """
        Retrieve record by ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: GET

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param record_id: str The Record name [in: path, req: True]
        :param columns: list = None Column filters [in: query, req: False]

        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("GET", url_path)

    def insertRecordWithID(
        self,
        db_branch_name: str,
        table_name: str,
        record_id: str,
        payload: dict,
        columns: list = None,
        createOnly: bool = None,
        ifVersion: int = None,
    ) -> Response:
        """
        By default, IDs are auto-generated when data is insterted into Xata. Sending a request to this endpoint allows us to insert a record with a pre-existing ID, bypassing the default automatic ID generation.
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: PUT

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param record_id: str The Record name [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]
        :param columns: list = None Column filters [in: query, req: False]
        :param createOnly: bool = None  [in: query, req: False]
        :param ifVersion: int = None  [in: query, req: False]

        :return Response
        """
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
        db_branch_name: str,
        table_name: str,
        record_id: str,
        payload: dict,
        columns: list = None,
        ifVersion: int = None,
    ) -> Response:
        """
        Upsert record with ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param record_id: str The Record name [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]
        :param columns: list = None Column filters [in: query, req: False]
        :param ifVersion: int = None  [in: query, req: False]

        :return Response
        """
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
        self, db_branch_name: str, table_name: str, record_id: str, columns: list = None
    ) -> Response:
        """
        Delete record from table
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: DELETE

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param record_id: str The Record name [in: path, req: True]
        :param columns: list = None Column filters [in: query, req: False]

        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        return self.request("DELETE", url_path)

    def updateRecordWithID(
        self,
        db_branch_name: str,
        table_name: str,
        record_id: str,
        payload: dict,
        columns: list = None,
        ifVersion: int = None,
    ) -> Response:
        """
        Update record with ID
        path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}
        method: PATCH

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param record_id: str The Record name [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]
        :param columns: list = None Column filters [in: query, req: False]
        :param ifVersion: int = None  [in: query, req: False]

        :return Response
        """
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
        self, db_branch_name: str, table_name: str, payload: dict, columns: list = None
    ) -> Response:
        """
        Bulk insert records
        path: /db/{db_branch_name}/tables/{table_name}/bulk
        method: POST

        :param db_branch_name: str The DBBranchName matches the pattern `{db_name}:{branch_name}`. [in: path, req: True]
        :param table_name: str The Table name [in: path, req: True]
        :param payload: dict content [in: requestBody, req: True]
        :param columns: list = None Column filters [in: query, req: False]

        :return Response
        """
        url_path = f"/db/{db_branch_name}/tables/{table_name}/bulk"
        if columns is not None:
            url_path += "?columns=%s" % ",".join(columns)
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)
