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
# Sql
# SQL service access
# Specification: workspace:v1.0
# ------------------------------------------------------- #

from xata.api_request import ApiRequest
from xata.api_response import ApiResponse


class Sql(ApiRequest):

    scope = "workspace"

    def query(
        self,
        statement: str,
        params: list = None,
        consistency: str = "strong",
        db_name: str = None,
        branch_name: str = None,
    ) -> ApiResponse:
        """
        Run an SQL query across the database branch.

        Reference: https://xata.io/docs/api-reference/db/db_branch_name/sql#sql-query
        Path: /db/{db_branch_name}/sql
        Method: POST
        Response status codes:
        - 200: OK
        - 201: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 503: ServiceUnavailable
        - 5XX: Unexpected Error
        - default: Unexpected Error

        :param statement: str The statement to run
        :param params: dict The query parameters list. default: None
        :param consistency: str The consistency level for this request. default: strong
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :returns ApiResponse
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/sql"
        headers = {"content-type": "application/json"}
        payload = {
            "statement": statement,
            "params": params,
            "consistency": consistency,
        }
        return self.request("POST", url_path, headers, payload)
