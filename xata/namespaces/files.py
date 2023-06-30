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
# Files
# Files
# Specification: core:v1.0
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Files(Namespace):

    scope = "workspace"

    def get_item(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        file_id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> Response:
        """
        Retrieves file content from an array by file ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: */*

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param file_id: str The File Identifier
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}"
        return self.request("GET", url_path)

    def put_item(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        file_id: str,
        data: bytes,
        db_name: str = None,
        branch_name: str = None,
    ) -> Response:
        """
        Uploads the file content to an array given the file ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}
        Method: PUT
        Response status codes:
        - 200: OK
        - 201: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param file_id: str The File Identifier
        :param data: bytes content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, data=data)

    def delete_item(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        file_id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> Response:
        """
        Deletes an item from an file array column given the file ID

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param file_id: str The File Identifier
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}"
        return self.request("DELETE", url_path)

    def get(
        self, table_name: str, record_id: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> Response:
        """
        Retrieves the file content from a file column

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file
        Method: GET
        Response status codes:
        - 200: OK
        - 204: no content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: */*

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file"
        return self.request("GET", url_path)

    def put(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        db_name: str = None,
        branch_name: str = None,
    ) -> Response:
        """
        Uploads the file content to the given file column

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file
        Method: PUT
        Response status codes:
        - 200: OK
        - 201: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 422: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param data: bytes
        :param content_type: str, Default: "application/octet-stream",
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file"
        headers = {"content-type": content_type}
        return self.request("PUT", url_path, headers, data=data)

    def delete(
        self, table_name: str, record_id: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> Response:
        """
        Deletes a file referred in a file column

        Path: /db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param table_name: str The Table name
        :param record_id: str The Record name
        :param column_name: str The Column name
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file"
        return self.request("DELETE", url_path)
