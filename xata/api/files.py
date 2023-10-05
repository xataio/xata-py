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

from requests import request

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse
from xata.errors import XataServerError


class Files(ApiRequest):

    scope = "workspace"

    def get_item(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        file_id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
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

        :return ApiResponse
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
        content_type: str = "application/octet-stream",
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
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
        :param content_type: str Default: "application/octet-stream"
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}"
        headers = {"content-type": content_type}
        return self.request("PUT", url_path, headers, data=data)

    def delete_item(
        self,
        table_name: str,
        record_id: str,
        column_name: str,
        file_id: str,
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
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

        :return ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file/{file_id}"
        return self.request("DELETE", url_path)

    def get(
        self, table_name: str, record_id: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
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

        :return ApiResponse
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
    ) -> ApiResponse:
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
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file"
        headers = {"content-type": content_type}
        return self.request("PUT", url_path, headers, data=data)

    def delete(
        self, table_name: str, record_id: str, column_name: str, db_name: str = None, branch_name: str = None
    ) -> ApiResponse:
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

        :return ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/tables/{table_name}/data/{record_id}/column/{column_name}/file"
        return self.request("DELETE", url_path)

    def transform_url(self, url: str, operations: dict[str, any]) -> str:
        """
        Image transformations url
        Returns the file url only for the given operations.
        All possible combinations: https://xata.io/docs/concepts/file-storage#image-transformations

        :param url: str Public or signed URL of the image
        :param operations: dict Image operations

        :return str
        """
        # valid url ?
        url_parts = url.split("/")
        if 4 < len(url_parts) < 5:
            raise Exception("invalid image url")

        # build operations - turn objects into lists
        ops = []
        for k, v in operations.items():
            # Coordinates on the gravity operation use an "x" as separator
            if type(v) is dict and k == "gravity":
                v = "x".join([str(x) for x in v.values()])
            # All the other ones use a semicolon.
            elif type(v) is dict:
                v = ";".join([str(x) for x in v.values()])
            ops.append(f"{k}={v}")

        if len(ops) == 0:
            raise Exception("missing image operations")

        region = url_parts[2].split(".")[0]
        file_id = url_parts[-1]

        # signed url
        if len(url_parts) == 5:
            return "https://%s.xata.sh/transform/%s/file/%s" % (region, ",".join(ops), file_id)
        # public url
        else:
            return "https://%s.storage.xata.sh/transform/%s/%s" % (region, ",".join(ops), file_id)

    def transform(self, url: str, operations: dict[str, any]) -> bytes:
        """
        Image transformations
        All possible combinations: https://xata.io/docs/concepts/file-storage#image-transformations

        :param url: str Public or signed URL of the image
        :param operations: dict Image operations

        :return Response
        """
        endpoint = self.transform_url(url, operations)

        resp = request("GET", endpoint)
        if resp.status_code != 200:
            raise XataServerError(f"code: {resp.status_code}, server error: {resp.text}")
        return resp.content
