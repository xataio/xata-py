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
# Branch
# Branch management.
# Specification: workspace:v1.0
# ------------------------------------------------------- #

from requests import Response

from xata.namespace import Namespace


class Branch(Namespace):

    scope = "workspace"

    def getBranchList(self, db_name: str) -> Response:
        """
        List all available Branches

        Path: /dbs/{db_name}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name

        :return Response
        """
        url_path = f"/dbs/{db_name}"
        return self.request("GET", url_path)

    def getBranchDetails(self, db_name: str = None, branch_name: str = None) -> Response:
        """
        Get branch schema and metadata

        Path: /db/{db_branch_name}
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}"
        return self.request("GET", url_path)

    def createBranch(self, payload: dict, db_name: str = None, branch_name: str = None, _from: str = None) -> Response:
        """
        Create Database branch

        Path: /db/{db_branch_name}
        Method: PUT
        Response status codes:
        - 201: Created
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 423: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.
        :param _from: str = None Name of source branch to branch the new schema from

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}"
        if _from is not None:
            url_path += "?from={_from}"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def deleteBranch(self, db_name: str = None, branch_name: str = None) -> Response:
        """
        Delete the branch in the database and all its resources

        Path: /db/{db_branch_name}
        Method: DELETE
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 409: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}"
        return self.request("DELETE", url_path)

    def getBranchMetadata(self, db_name: str = None, branch_name: str = None) -> Response:
        """
        Get Branch Metadata

        Path: /db/{db_branch_name}/metadata
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/metadata"
        return self.request("GET", url_path)

    def updateBranchMetadata(self, payload: dict, db_name: str = None, branch_name: str = None) -> Response:
        """
        Update the branch metadata

        Path: /db/{db_branch_name}/metadata
        Method: PUT
        Response status codes:
        - 204: No Content
        - 400: Bad Request
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error

        :param payload: dict content
        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/metadata"
        headers = {"content-type": "application/json"}
        return self.request("PUT", url_path, headers, payload)

    def getBranchStats(self, db_name: str = None, branch_name: str = None) -> Response:
        """
        Get branch usage metrics.

        Path: /db/{db_branch_name}/stats
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Example response
        - 401: Authentication Error
        - 404: Example response
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str = None The name of the database to query. Default: database name from the client.
        :param branch_name: str = None The name of the branch to query. Default: branch name from the client.

        :return Response
        """
        db_branch_name = self.client.get_db_branch_name(db_name, branch_name)
        url_path = f"/db/{db_branch_name}/stats"
        return self.request("GET", url_path)

    def getGitBranchesMapping(self, db_name: str) -> Response:
        """
        Lists all the git branches in the mapping, and their associated Xata branches.  Example
        response:  ```json {   "mappings": [       {         "gitBranch": "main",
        "xataBranch": "main"       },       {         "gitBranch": "gitBranch1",
        "xataBranch": "xataBranch1"       }       {         "gitBranch": "xataBranch2",
        "xataBranch": "xataBranch2"       }   ] } ```

        Path: /dbs/{db_name}/gitBranches
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name

        :return Response
        """
        url_path = f"/dbs/{db_name}/gitBranches"
        return self.request("GET", url_path)

    def addGitBranchesEntry(self, db_name: str, payload: dict) -> Response:
        """
        Adds an entry to the mapping of git branches to Xata branches.  The git branch and the
        Xata branch must be present in the body of the request.  If the Xata branch doesn't exist,
        a 400 error is returned.  If the git branch is already present in the mapping, the old
        entry is overwritten, and a warning message is included in the response.  If the git
        branch is added and didn't exist before, the response code is 204. If the git branch
        existed and it was overwritten, the response code is 201.  Example request:  ```json //
        POST https://tutorial-ng7s8c.xata.sh/dbs/demo/gitBranches {   "gitBranch": "fix/bug123",
        "xataBranch": "fix_bug" } ```

        Path: /dbs/{db_name}/gitBranches
        Method: POST
        Response status codes:
        - 201: Operation was successful with warnings
        - 204: Operation was successful without warnings
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param payload: dict content

        :return Response
        """
        url_path = f"/dbs/{db_name}/gitBranches"
        headers = {"content-type": "application/json"}
        return self.request("POST", url_path, headers, payload)

    def removeGitBranchesEntry(self, db_name: str, gitBranch: str) -> Response:
        """
        Removes an entry from the mapping of git branches to Xata branches.  The name of the git
        branch must be passed as a query parameter.  If the git branch is not found, the endpoint
        returns a 404 status code.  Example request:  ```json // DELETE https://tutorial-
        ng7s8c.xata.sh/dbs/demo/gitBranches?gitBranch=fix%2Fbug123 ```

        Path: /dbs/{db_name}/gitBranches
        Method: DELETE
        Response status codes:
        - 204: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 404: The git branch was not found in the mapping
        - 5XX: Unexpected Error

        :param db_name: str The Database Name
        :param gitBranch: str The Git Branch to remove from the mapping

        :return Response
        """
        url_path = f"/dbs/{db_name}/gitBranches"
        if gitBranch is not None:
            url_path += "?gitBranch={gitBranch}"
        return self.request("DELETE", url_path)

    def resolveBranch(self, db_name: str, gitBranch: str = None, fallbackBranch: str = None) -> Response:
        """
        In order to resolve the database branch, the following algorithm is used: * if the
        `gitBranch` was provided and is found in the [git branches mapping](/api-
        reference/dbs/db_name/gitBranches), the associated Xata branch is returned * else, if a
        Xata branch with the exact same name as `gitBranch` exists, return it * else, if
        `fallbackBranch` is provided and a branch with that name exists, return it * else, return
        the default branch of the DB (`main` or the first branch)  Example call:  ```json // GET
        https://tutorial-
        ng7s8c.xata.sh/dbs/demo/dbs/demo/resolveBranch?gitBranch=test&fallbackBranch=tsg ```
        Example response:  ```json {   "branch": "main",   "reason": {     "code":
        "DEFAULT_BRANCH",     "message": "Default branch for this database (main)"   } } ```

        Path: /dbs/{db_name}/resolveBranch
        Method: GET
        Response status codes:
        - 200: OK
        - 400: Bad Request
        - 401: Authentication Error
        - 5XX: Unexpected Error
        Response: application/json

        :param db_name: str The Database Name
        :param gitBranch: str = None The Git Branch
        :param fallbackBranch: str = None Default branch to fallback to

        :return Response
        """
        url_path = f"/dbs/{db_name}/resolveBranch"
        query_params = []
        if gitBranch is not None:
            query_params.append(f"gitBranch={gitBranch}")
        if fallbackBranch is not None:
            query_params.append(f"fallbackBranch={fallbackBranch}")
        if query_params:
            url_path += "?" + "&".join(query_params)
        return self.request("GET", url_path)
